use std::{
    collections::VecDeque,
    fs::create_dir_all,
    path::PathBuf,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, RwLock,
    },
};

use anyhow::{anyhow, Ok, Result};
use bytes::Bytes;

use crate::{
    iterator::{merge_iterator::MergeIterator, StorageIterator},
    kv::kv_pair::KeyValuePair,
    memory::memtable::{iterator::MemTableIterator, MemTable},
    table::{block_cache::BlockCache, builder::SSTBuilder},
};

use super::storage_state_options::StorageStateOptions;

const TOMBSTONE: &[u8] = &[];

pub struct StorageState {
    current_memtable: Arc<MemTable>,
    frozen_memtables: VecDeque<Arc<MemTable>>,
    l0_ssts: VecDeque<usize>,
    sst_builder: SSTBuilder,
    block_cache: Arc<BlockCache>,
    state_lock: RwLock<()>,
    counter: AtomicUsize,
    options: StorageStateOptions,
}

impl StorageState {
    pub fn open(options: StorageStateOptions) -> Result<Self> {
        // initialize directory if it doesn't exist
        create_dir_all(&options.path)?;

        let counter: AtomicUsize = AtomicUsize::new(0);
        let current_memtable = Arc::new(MemTable::new(counter.fetch_add(1, Ordering::SeqCst)));
        // newest to oldest frozen memtables
        let frozen_memtables: VecDeque<Arc<MemTable>> = VecDeque::new();
        // newest to oldest l0 SSTs
        let l0_ssts: VecDeque<usize> = VecDeque::new();

        let sst_builder = SSTBuilder::new(options.block_max_size_bytes);
        let block_cache = Arc::new(BlockCache::new(options.block_cache_size_bytes));

        Ok(Self {
            current_memtable,
            frozen_memtables,
            l0_ssts,
            sst_builder,
            block_cache,
            state_lock: RwLock::new(()),
            counter,
            options,
        })
    }
    pub fn get(&mut self, key: &[u8]) -> Option<Bytes> {
        let _rlock = self.state_lock.read().unwrap();

        let mut res = self.current_memtable.get(key);
        if res.is_none() {
            for memtable in &self.frozen_memtables {
                res = memtable.get(key);
                if res.is_some() {
                    break;
                }
            }
        }
        if let Some(val) = &res {
            if val == TOMBSTONE {
                return None;
            }
        }
        res
    }

    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        let current_memtable_size = self.current_memtable.get_size_bytes();
        if current_memtable_size > 0
            && current_memtable_size + key.len() + value.len() > self.options.sst_max_size_bytes
        {
            self.freeze_memtable()?;
        }
        let _rlock = self.state_lock.read().unwrap();
        self.current_memtable.put(key, value)
    }

    pub fn delete(&mut self, key: &[u8]) -> Result<()> {
        if self.get(key).is_none() {
            return Err(anyhow!("key cannot be deleted because it does not exist"));
        }
        let _rlock = self.state_lock.read().unwrap();
        self.current_memtable.put(key, TOMBSTONE)
    }

    fn freeze_memtable(&mut self) -> Result<()> {
        let new_memtable = MemTable::new(self.get_next_sst_id());

        let _wlock = self.state_lock.write().unwrap();
        // clone is safe here because no other threads can update current_memtable while lock is held
        self.current_memtable.freeze()?;
        self.frozen_memtables
            .push_front(self.current_memtable.clone());
        self.current_memtable = Arc::new(new_memtable);

        Ok(())
    }

    fn get_next_sst_id(&mut self) -> usize {
        self.counter.fetch_add(1, Ordering::SeqCst)
    }

    pub fn scan(&mut self) -> impl StorageIterator<Item = KeyValuePair> {
        // build memtable iterator
        let mut memtable_iterators = vec![MemTableIterator::new(&self.current_memtable)];
        for memtable in &self.frozen_memtables {
            memtable_iterators.push(MemTableIterator::new(memtable));
        }
        MergeIterator::new(memtable_iterators)
    }

    pub fn flush_next_memtable_to_l0(&mut self) -> Result<()> {
        let memtable_to_flush: Arc<MemTable>;
        {
            // acquire read lock to get last memtable
            let _rlock = self.state_lock.read().unwrap();
            let earliest_frozen_memtable = self.frozen_memtables.back();
            match earliest_frozen_memtable {
                Some(memtable) => memtable_to_flush = memtable.clone(),
                _ => return Ok(()),
            }
        }
        // add to SST builder outside of lock
        memtable_to_flush.flush(&mut self.sst_builder)?;
        {
            // acquire write
            let _wlock = self.state_lock.write().unwrap();
            // build the SST
            let sst_id = memtable_to_flush.get_id();
            let sst_builder = std::mem::replace(
                &mut self.sst_builder,
                SSTBuilder::new(self.options.block_max_size_bytes),
            );
            let sst = sst_builder.build(
                sst_id,
                self.get_sst_path(sst_id),
                Some(self.block_cache.clone()),
            )?;
            // add to L0 and remove from memtables
            self.l0_ssts.push_front(sst.get_id());
            self.frozen_memtables.pop_back();
        }
        Ok(())
    }

    fn get_sst_path(&self, sst_id: usize) -> PathBuf {
        self.options.path.join(format!("{:05}.sst", sst_id))
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use tempfile::tempdir;

    use crate::state::{storage_state::StorageState, storage_state_options::StorageStateOptions};

    #[test]
    fn test_storage_state_get_put() {
        let dir = tempdir().unwrap();
        let options = StorageStateOptions {
            sst_max_size_bytes: 128,
            block_max_size_bytes: 0,
            block_cache_size_bytes: 0,
            path: dir.path().to_owned(),
        };
        let mut storage_state = StorageState::open(options).unwrap();
        storage_state
            .put("hello".as_bytes(), "world".as_bytes())
            .unwrap();

        assert_eq!(
            storage_state.get("hello".as_bytes()).unwrap(),
            Bytes::from("world".as_bytes())
        );

        storage_state.delete("hello".as_bytes()).unwrap();
        assert_eq!(storage_state.get("hello".as_bytes()), None);
    }

    #[test]
    fn test_storage_state_freeze() {
        let dir = tempdir().unwrap();
        let options = StorageStateOptions {
            sst_max_size_bytes: 9,
            block_max_size_bytes: 0,
            block_cache_size_bytes: 0,
            path: dir.path().to_owned(),
        };
        let mut storage_state = StorageState::open(options).unwrap();
        storage_state
            .put("hello".as_bytes(), "world".as_bytes())
            .unwrap();
        // allow inserting at least one kv pair even if their size exceeds limit
        assert_eq!(storage_state.current_memtable.get_size_bytes(), 10);
        // new kv entry can't fit in current memtable, so the memtable should be frozen
        storage_state
            .put("another".as_bytes(), "entry".as_bytes())
            .unwrap();
        assert_eq!(storage_state.frozen_memtables.len(), 1);
        assert_eq!(storage_state.frozen_memtables[0].get_id(), 0);
        // only contains new kv entry
        assert_eq!(storage_state.current_memtable.get_id(), 1);
        assert_eq!(storage_state.current_memtable.get_size_bytes(), 12);

        // test get entries
        assert_eq!(
            storage_state.get("hello".as_bytes()).unwrap(),
            Bytes::from("world".as_bytes())
        );
        assert_eq!(
            storage_state.get("another".as_bytes()).unwrap(),
            Bytes::from("entry".as_bytes())
        );
        assert_eq!(storage_state.get("does_not_exist".as_bytes()), None);
    }

    #[test]
    fn test_scan() {
        let dir = tempdir().unwrap();
        let options = StorageStateOptions {
            sst_max_size_bytes: 4,
            block_max_size_bytes: 0,
            block_cache_size_bytes: 0,
            path: dir.path().to_owned(),
        };
        let mut storage_state = StorageState::open(options).unwrap();
        storage_state.put("k1".as_bytes(), "v1".as_bytes()).unwrap();
        // new kv entry can't fit in current memtable, so the memtable should be frozen
        storage_state.put("k2".as_bytes(), "v2".as_bytes()).unwrap();
        assert_eq!(storage_state.frozen_memtables.len(), 1);
        for (i, item) in storage_state.scan().enumerate() {
            assert!(item.key.get_key() == format!("k{}", i + 1));
        }
    }

    #[test]
    fn test_memtable_flush() {
        // set up storage state
        let dir = tempdir().unwrap();
        let options = StorageStateOptions {
            sst_max_size_bytes: 10,
            block_max_size_bytes: 0,
            block_cache_size_bytes: 0,
            path: dir.path().to_owned(),
        };
        let mut storage_state = StorageState::open(options).unwrap();
        storage_state
            .put("hello".as_bytes(), "world".as_bytes())
            .unwrap();
        storage_state.freeze_memtable().unwrap();
        assert_eq!(storage_state.frozen_memtables.len(), 1);
        assert!(storage_state.l0_ssts.is_empty());

        // flush the memtable
        let res = storage_state.flush_next_memtable_to_l0();
        assert!(res.is_ok());

        // assert sst created
        assert_eq!(storage_state.l0_ssts.len(), 1);
        assert!(storage_state.frozen_memtables.is_empty());
    }
}
