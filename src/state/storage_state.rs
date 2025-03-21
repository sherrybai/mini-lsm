use std::{collections::VecDeque, sync::{atomic::{AtomicUsize, Ordering}, Arc, RwLock}};

use anyhow::{anyhow, Result};
use bytes::Bytes;

use crate::{iterator::{merge_iterator::MergeIterator, StorageIterator}, kv::kv_pair::KeyValuePair, memory::memtable::{MemTable, iterator::MemTableIterator}};

use super::storage_state_options::StorageStateOptions;

const TOMBSTONE: &[u8] = &[];

pub struct StorageState {
    current_memtable: Arc<MemTable>,
    frozen_memtables: VecDeque<Arc<MemTable>>,
    state_lock: RwLock<()>,
    counter: AtomicUsize,
    options: StorageStateOptions
}

impl StorageState {
    pub fn new(options: StorageStateOptions) -> Self {
        let counter: AtomicUsize = AtomicUsize::new(0);
        let current_memtable = Arc::new(MemTable::new(counter.fetch_add(1, Ordering::SeqCst)));
        // newest to oldest frozen memtables
        let frozen_memtables: VecDeque<Arc<MemTable>> = VecDeque::new();

        Self {
            current_memtable,
            frozen_memtables,
            state_lock: RwLock::new(()),
            counter,
            options,
        }
    }
    pub fn get(&mut self, key: &[u8]) -> Option<Bytes> {
        let _rlock = self.state_lock.read().unwrap();
        
        let mut res = self.current_memtable.get(key);
        if res.is_none() {
            for memtable in &self.frozen_memtables {
                res = memtable.get(key);
                if res.is_some() {
                    break
                }
            }
        }
        if let Some(val) = &res {
            if val == TOMBSTONE {
                return None
            }
        } 
        res
    }

    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        let current_memtable_size = self.current_memtable.get_size_bytes();
        if current_memtable_size > 0 && current_memtable_size + key.len() + value.len() > self.options.sst_max_size_bytes {
            self.freeze_memtable()?;
        }
        let _rlock = self.state_lock.read().unwrap();
        self.current_memtable.put(key, value)
    }

    pub fn delete(&mut self, key: &[u8]) -> Result<()> {
        if self.get(key).is_none() {
            return Err(anyhow!("key cannot be deleted because it does not exist"))
        }
        let _rlock = self.state_lock.read().unwrap();
        self.current_memtable.put(key, TOMBSTONE)    
    }

    fn freeze_memtable(&mut self) -> Result<()> {
        let new_memtable = MemTable::new(self.get_next_sst_id());

        let _wlock = self.state_lock.write().unwrap();
        // clone is safe here because no other threads can update current_memtable while lock is held
        self.current_memtable.freeze()?;
        self.frozen_memtables.push_front(self.current_memtable.clone());
        self.current_memtable = Arc::new(new_memtable);

        Ok(())
    }

    fn get_next_sst_id(&mut self) -> usize {
        self.counter.fetch_add(1, Ordering::SeqCst)
    }

    pub fn scan(&mut self) -> impl StorageIterator<Item = KeyValuePair> {
        let mut iterators_to_merge = vec![MemTableIterator::new(&self.current_memtable)];
        for memtable in &self.frozen_memtables {
            iterators_to_merge.push(MemTableIterator::new(memtable));
        }
        MergeIterator::new(iterators_to_merge)
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use crate::state::{storage_state::StorageState, storage_state_options::StorageStateOptions};

    #[test]
    fn test_storage_state_get_put() {
        let options = StorageStateOptions {
            sst_max_size_bytes: 128
        };
        let mut storage_state = StorageState::new(options);
        storage_state
            .put("hello".as_bytes(), "world".as_bytes())
            .unwrap();

        assert_eq!(
            storage_state.get("hello".as_bytes()).unwrap(),
            Bytes::from("world".as_bytes())
        );

        storage_state.delete("hello".as_bytes()).unwrap();
        assert_eq!(
            storage_state.get("hello".as_bytes()), 
            None
        );
    }

    #[test]
    fn test_storage_state_freeze() {
        let options = StorageStateOptions {
            sst_max_size_bytes: 9
        };
        let mut storage_state = StorageState::new(options);
        storage_state
            .put("hello".as_bytes(), "world".as_bytes())
            .unwrap();
        // allow inserting at least one kv pair even if their size exceeds limit
        assert_eq!(
            storage_state.current_memtable.get_size_bytes(),
            10
        );
        // new kv entry can't fit in current memtable, so the memtable should be frozen
        storage_state
            .put("another".as_bytes(), "entry".as_bytes())
            .unwrap();
        assert_eq!(
            storage_state.frozen_memtables.len(),
            1
        );
        assert_eq!(
            storage_state.frozen_memtables[0].get_id(),
            0
        );
        // only contains new kv entry
        assert_eq!(
            storage_state.current_memtable.get_id(),
            1
        );
        assert_eq!(
            storage_state.current_memtable.get_size_bytes(),
            12
        );

        // test get entries
        assert_eq!(
            storage_state.get("hello".as_bytes()).unwrap(),
            Bytes::from("world".as_bytes())
        );
        assert_eq!(
            storage_state.get("another".as_bytes()).unwrap(),
            Bytes::from("entry".as_bytes())
        );
        assert_eq!(
            storage_state.get("does_not_exist".as_bytes()),
            None
        );
    }

    #[test]
    fn test_scan() {
        let options = StorageStateOptions {
            sst_max_size_bytes: 4
        };
        let mut storage_state = StorageState::new(options);
        storage_state
            .put("k1".as_bytes(), "v1".as_bytes())
            .unwrap();
        // new kv entry can't fit in current memtable, so the memtable should be frozen
        storage_state
            .put("k2".as_bytes(), "v2".as_bytes())
            .unwrap();
        assert_eq!(
            storage_state.frozen_memtables.len(),
            1
        );
        for (i, item) in storage_state.scan().enumerate() {
            assert!(item.key.get_key() == format!("k{}", i+1));
        }
    }
}