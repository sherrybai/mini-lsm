use std::{
    collections::VecDeque,
    fs::create_dir_all,
    iter,
    ops::Bound,
    path::PathBuf,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, RwLock,
    },
    thread,
    time::Duration,
};

use anyhow::{anyhow, Ok, Result};
use bytes::Bytes;
use storage_state_options::StorageStateOptions;

use crate::{
    iterator::{
        bounded_iterator::BoundedIterator, merge_iterator::MergeIterator,
        two_merge_iterator::TwoMergeIterator, StorageIterator,
    },
    kv::{kv_pair::KeyValuePair, timestamped_key::TimestampedKey},
    memory::memtable::MemTable,
    table::{block_cache::BlockCache, builder::SSTBuilder, iterator::SSTIterator, Sst},
    utils::range_overlap,
};

const TOMBSTONE: &[u8] = &[];

pub mod storage_state_options;

#[derive(Clone)]
struct StorageStateProtected {
    current_memtable: Arc<MemTable>,
    frozen_memtables: VecDeque<Arc<MemTable>>,
    l0_sst_ids: VecDeque<usize>,
    ssts: VecDeque<Arc<Sst>>,
}

pub struct StorageState {
    block_cache: Arc<BlockCache>,
    state_lock: Arc<RwLock<Arc<StorageStateProtected>>>,
    sst_counter: AtomicUsize,
    options: StorageStateOptions,
}

impl StorageState {
    pub fn open(options: StorageStateOptions) -> Result<Self> {
        // initialize directory if it doesn't exist
        create_dir_all(&options.path)?;

        let sst_counter: AtomicUsize = AtomicUsize::new(0);
        let current_memtable = Arc::new(MemTable::new(sst_counter.fetch_add(1, Ordering::SeqCst)));
        // newest to oldest frozen memtables
        let frozen_memtables: VecDeque<Arc<MemTable>> = VecDeque::new();
        // newest to oldest l0 SSTs
        let l0_sst_ids: VecDeque<usize> = VecDeque::new();
        let ssts: VecDeque<Arc<Sst>> = VecDeque::new();

        let block_cache = Arc::new(BlockCache::new(options.block_cache_size_bytes));

        let protected_state = StorageStateProtected {
            current_memtable,
            frozen_memtables,
            l0_sst_ids,
            ssts,
        };

        Ok(Self {
            block_cache,
            state_lock: Arc::new(RwLock::new(Arc::new(protected_state))),
            sst_counter,
            options,
        })
    }
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        let ro_snapshot = self.state_lock.read().unwrap();

        // look up value in memtables
        let mut res = ro_snapshot.current_memtable.get(key);
        if res.is_none() {
            for memtable in &ro_snapshot.frozen_memtables {
                res = memtable.get(key);
                if res.is_some() {
                    break;
                }
            }
        }
        if let Some(val) = &res {
            if val == TOMBSTONE {
                return Ok(None);
            }
            return Ok(res);
        }

        // if not found in memtable, look up in SSTs
        for sst in &ro_snapshot.ssts {
            if sst.maybe_contains_key(key) {
                let found_kv = SSTIterator::create_and_seek_to_key(
                    sst.clone(),
                    TimestampedKey::new(Bytes::copy_from_slice(key)),
                )?
                .peek();
                if found_kv.as_ref().is_some_and(|kv| kv.key.get_key() == key) {
                    let val = found_kv.unwrap().value;
                    if val == TOMBSTONE {
                        return Ok(None);
                    }
                    return Ok(Some(val));
                }
            }
        }
        Ok(None)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let current_memtable_size = {
            let ro_snapshot = self.state_lock.read().unwrap();
            ro_snapshot.current_memtable.get_size_bytes()
        };
        if current_memtable_size > 0
            && current_memtable_size + key.len() + value.len() > self.options.sst_max_size_bytes
        {
            self.freeze_memtable()?;
        }
        {
            let ro_snapshot = self.state_lock.read().unwrap();
            ro_snapshot.current_memtable.put(key, value)
        }
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        if self.get(key)?.is_none() {
            return Err(anyhow!("key cannot be deleted because it does not exist"));
        }
        self.put(key, TOMBSTONE)
    }

    fn freeze_memtable(&self) -> Result<()> {
        let new_memtable = MemTable::new(self.get_next_sst_id());

        let mut rw_guard = self.state_lock.write().unwrap();
        let mut rw_snapshot = rw_guard.as_ref().clone();
        rw_snapshot.current_memtable.freeze()?;
        rw_snapshot
            .frozen_memtables
            .push_front(rw_snapshot.current_memtable.clone());
        rw_snapshot.current_memtable = Arc::new(new_memtable);
        *rw_guard = Arc::new(rw_snapshot);

        Ok(())
    }

    fn get_next_sst_id(&self) -> usize {
        self.sst_counter.fetch_add(1, Ordering::SeqCst)
    }

    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<impl StorageIterator<Item = KeyValuePair>> {
        let ro_snapshot = {
            let guard = self.state_lock.read().unwrap();
            Arc::clone(&guard)
        };
        // build memtable iterator
        let memtables_snapshot = iter::once(ro_snapshot.current_memtable.clone())
            .chain(ro_snapshot.frozen_memtables.clone());
        let memtable_iterators = memtables_snapshot
            .map(|memtable| memtable.scan(lower, upper))
            .collect();
        let memtable_merge_iterator = MergeIterator::new(memtable_iterators);
        // build l0 sst iterator
        // ok to do this outside of read lock as sst files will never be modified
        let mut l0_sst_iterators = vec![];
        for sst in ro_snapshot.ssts.clone() {
            if !range_overlap(lower, upper, sst.get_first_key(), sst.get_last_key()) {
                continue;
            }
            let mut sst_iterator: SSTIterator;
            match lower {
                Bound::Included(lower_key) => {
                    sst_iterator = SSTIterator::create_and_seek_to_key(
                        sst,
                        TimestampedKey::new(Bytes::copy_from_slice(lower_key)),
                    )?;
                }
                Bound::Excluded(lower_key) => {
                    sst_iterator = SSTIterator::create_and_seek_to_key(
                        sst,
                        TimestampedKey::new(Bytes::copy_from_slice(lower_key)),
                    )?;
                    if sst_iterator.is_valid()
                        && sst_iterator
                            .peek()
                            .is_some_and(|kv| kv.key.get_key() == lower_key)
                    {
                        sst_iterator.next();
                    }
                }
                Bound::Unbounded => {
                    sst_iterator = SSTIterator::create_and_seek_to_first(sst)?;
                }
            }

            l0_sst_iterators.push(BoundedIterator::new(sst_iterator, upper));
        }
        let l0_sst_merge_iterator = MergeIterator::new(l0_sst_iterators);
        let two_merge_iterator =
            TwoMergeIterator::new(memtable_merge_iterator, l0_sst_merge_iterator);
        Ok(two_merge_iterator)
    }

    pub fn flush_next_memtable_to_l0(&self) -> Result<()> {
        let memtable_to_flush: Arc<MemTable>;
        {
            // acquire read lock to get last memtable
            let ro_snapshot = self.state_lock.read().unwrap();
            let earliest_frozen_memtable = ro_snapshot.frozen_memtables.back();
            match earliest_frozen_memtable {
                Some(memtable) => memtable_to_flush = memtable.clone(),
                _ => return Ok(()),
            }
        }
        // add to SST builder outside of lock
        let mut sst_builder: SSTBuilder = SSTBuilder::new(self.options.block_max_size_bytes);
        memtable_to_flush.flush(&mut sst_builder)?;
        {
            // acquire write
            let mut rw_guard = self.state_lock.write().unwrap();
            let mut rw_snapshot = rw_guard.as_ref().clone();
            // build the SST
            let sst_id = memtable_to_flush.get_id();
            let sst = sst_builder.build(
                sst_id,
                self.get_sst_path(sst_id),
                Some(self.block_cache.clone()),
            )?;
            // add to L0 and remove from memtables
            rw_snapshot.l0_sst_ids.push_front(sst.get_id());
            rw_snapshot.ssts.push_front(Arc::new(sst));
            rw_snapshot.frozen_memtables.pop_back();
            *rw_guard = Arc::new(rw_snapshot);
        }
        Ok(())
    }

    pub fn flush_all_memtables(&self) -> Result<()> {
        self.freeze_memtable()?;
        loop {
            let num_memtables = {
                let ro_snapshot = self.state_lock.read().unwrap();
                ro_snapshot.frozen_memtables.len()
            };
            if num_memtables == 0 { break; }
            self.flush_next_memtable_to_l0()?;
        }
        Ok(())
    }

    pub fn trigger_flush(&self) -> Result<()> {
        let should_trigger_flush = {
            let ro_snapshot = self.state_lock.read().unwrap();
            ro_snapshot.frozen_memtables.len() >= self.options.num_memtables_limit
        };
        if should_trigger_flush {
            self.flush_next_memtable_to_l0()
        } else {
            Ok(())
        }
    }

    pub fn spawn_flush_thread(
        self: &Arc<Self>,
        end_flush: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<thread::JoinHandle<()>>> {
        let this = self.clone();
        let handle = thread::spawn(move || {
            let ticker = crossbeam_channel::tick(Duration::from_millis(50));
            loop {
                crossbeam_channel::select! {
                    recv(ticker) -> _ => if let Err(e) = this.trigger_flush() {
                        eprintln!("error during background flush: {}", e);
                    },
                    recv(end_flush) -> _ => return
                }
            }
        });
        Ok(Some(handle))
    }

    fn get_sst_path(&self, sst_id: usize) -> PathBuf {
        self.options.path.join(format!("{:05}.sst", sst_id))
    }

    #[cfg(test)]
    fn get_snapshot(&self) -> Arc<StorageStateProtected> {
        let ro_snapshot = self.state_lock.read().unwrap();

        let res = ro_snapshot.as_ref().clone();
        Arc::new(res)
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Bound;

    use bytes::Bytes;
    use tempfile::tempdir;

    use crate::state::{storage_state_options::StorageStateOptions, StorageState};

    #[test]
    fn test_storage_state_get_put() {
        let dir = tempdir().unwrap();
        let options = StorageStateOptions {
            sst_max_size_bytes: 128,
            block_max_size_bytes: 0,
            block_cache_size_bytes: 0,
            path: dir.path().to_owned(),
            num_memtables_limit: 5,
        };
        let storage_state = StorageState::open(options).unwrap();
        storage_state
            .put("hello".as_bytes(), "world".as_bytes())
            .unwrap();

        assert_eq!(
            storage_state.get("hello".as_bytes()).unwrap().unwrap(),
            Bytes::from("world".as_bytes())
        );

        storage_state.delete("hello".as_bytes()).unwrap();
        assert_eq!(storage_state.get("hello".as_bytes()).unwrap(), None);
    }

    #[test]
    fn test_storage_state_freeze() {
        let dir = tempdir().unwrap();
        let options = StorageStateOptions {
            sst_max_size_bytes: 9,
            block_max_size_bytes: 0,
            block_cache_size_bytes: 0,
            path: dir.path().to_owned(),
            num_memtables_limit: 5,
        };
        let storage_state = StorageState::open(options).unwrap();
        storage_state
            .put("hello".as_bytes(), "world".as_bytes())
            .unwrap();
        // allow inserting at least one kv pair even if their size exceeds limit
        assert_eq!(
            storage_state
                .get_snapshot()
                .current_memtable
                .get_size_bytes(),
            10
        );
        // new kv entry can't fit in current memtable, so the memtable should be frozen
        storage_state
            .put("another".as_bytes(), "entry".as_bytes())
            .unwrap();
        let snapshot = storage_state.get_snapshot();
        assert_eq!(snapshot.frozen_memtables.len(), 1);
        assert_eq!(snapshot.frozen_memtables[0].get_id(), 0);
        // only contains new kv entry
        assert_eq!(snapshot.current_memtable.get_id(), 1);
        assert_eq!(snapshot.current_memtable.get_size_bytes(), 12);

        // test get entries
        assert_eq!(
            storage_state.get("hello".as_bytes()).unwrap().unwrap(),
            Bytes::from("world".as_bytes())
        );
        assert_eq!(
            storage_state.get("another".as_bytes()).unwrap().unwrap(),
            Bytes::from("entry".as_bytes())
        );
        assert_eq!(
            storage_state.get("does_not_exist".as_bytes()).unwrap(),
            None
        );
    }

    #[test]
    fn test_scan_memtables_only() {
        let dir = tempdir().unwrap();
        let options = StorageStateOptions {
            sst_max_size_bytes: 4,
            block_max_size_bytes: 0,
            block_cache_size_bytes: 0,
            path: dir.path().to_owned(),
            num_memtables_limit: 5,
        };
        let storage_state = StorageState::open(options).unwrap();
        storage_state.put("k1".as_bytes(), "v1".as_bytes()).unwrap();
        // new kv entry can't fit in current memtable, so the memtable should be frozen
        storage_state.put("k2".as_bytes(), "v2".as_bytes()).unwrap();
        assert_eq!(storage_state.get_snapshot().frozen_memtables.len(), 1);
        for (i, item) in storage_state
            .scan(Bound::Unbounded, Bound::Unbounded)
            .unwrap()
            .enumerate()
        {
            assert!(item.key.get_key() == format!("k{}", i + 1));
        }
    }

    #[test]
    fn test_get_scan_with_l0_ssts() {
        let dir = tempdir().unwrap();
        let options = StorageStateOptions {
            sst_max_size_bytes: 4,
            block_max_size_bytes: 4,
            block_cache_size_bytes: 0,
            path: dir.path().to_owned(),
            num_memtables_limit: 5,
        };
        let storage_state = StorageState::open(options).unwrap();
        storage_state.put("k1".as_bytes(), "v1".as_bytes()).unwrap();
        // new kv entry can't fit in current memtable, so the memtable should be frozen
        storage_state.put("k2".as_bytes(), "v2".as_bytes()).unwrap();
        assert_eq!(storage_state.get_snapshot().frozen_memtables.len(), 1);
        // flush to sst
        storage_state.flush_next_memtable_to_l0().unwrap();
        assert_eq!(storage_state.get_snapshot().frozen_memtables.len(), 0);
        assert_eq!(storage_state.get_snapshot().l0_sst_ids.len(), 1);
        // new kv entry can't fit in current memtable, so the memtable should be frozen
        storage_state.put("k3".as_bytes(), "v3".as_bytes()).unwrap();
        assert_eq!(storage_state.get_snapshot().frozen_memtables.len(), 1);

        assert_eq!(
            storage_state.get("k1".as_bytes()).unwrap().unwrap(),
            "v1".as_bytes()
        );
        assert_eq!(
            storage_state.get("k2".as_bytes()).unwrap().unwrap(),
            "v2".as_bytes()
        );
        assert_eq!(
            storage_state.get("k3".as_bytes()).unwrap().unwrap(),
            "v3".as_bytes()
        );
        assert!(storage_state.get("k2.5".as_bytes()).unwrap().is_none());

        for (i, item) in storage_state
            .scan(Bound::Unbounded, Bound::Unbounded)
            .unwrap()
            .enumerate()
        {
            assert!(item.key.get_key() == format!("k{}", i + 1));
        }

        // test bounded scan
        let mut bounded_iter = storage_state
            .scan(
                Bound::Included("k2".as_bytes()),
                Bound::Excluded("k3".as_bytes()),
            )
            .unwrap();
        assert_eq!(bounded_iter.next().unwrap().key.get_key(), "k2".as_bytes());
        assert!(bounded_iter.next().is_none());
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
            num_memtables_limit: 5,
        };
        let storage_state = StorageState::open(options).unwrap();
        storage_state
            .put("hello".as_bytes(), "world".as_bytes())
            .unwrap();
        storage_state.freeze_memtable().unwrap();
        assert_eq!(storage_state.get_snapshot().frozen_memtables.len(), 1);
        assert!(storage_state.get_snapshot().l0_sst_ids.is_empty());

        // flush the memtable
        let res = storage_state.flush_next_memtable_to_l0();
        assert!(res.is_ok());

        // assert sst created
        assert_eq!(storage_state.get_snapshot().l0_sst_ids.len(), 1);
        assert!(storage_state.get_snapshot().frozen_memtables.is_empty());
    }

    #[test]
    fn test_flush_all_memtables() {
        // set up storage state
        let dir = tempdir().unwrap();
        let options = StorageStateOptions {
            sst_max_size_bytes: 10,
            block_max_size_bytes: 0,
            block_cache_size_bytes: 0,
            path: dir.path().to_owned(),
            num_memtables_limit: 5,
        };
        let storage_state = StorageState::open(options).unwrap();
        storage_state
            .put("k1".as_bytes(), "v1".as_bytes())
            .unwrap();
        storage_state.freeze_memtable().unwrap();
        assert_eq!(storage_state.get_snapshot().frozen_memtables.len(), 1);
        storage_state
            .put("k2".as_bytes(), "v2".as_bytes())
            .unwrap();

        // flush the memtable
        let res = storage_state.flush_all_memtables();
        assert!(res.is_ok());

        // assert sst created
        assert_eq!(storage_state.get_snapshot().l0_sst_ids.len(), 2);
        assert!(storage_state.get_snapshot().frozen_memtables.is_empty());
    }
}
