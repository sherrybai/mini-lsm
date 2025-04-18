use std::{
    ops::Bound,
    sync::{Arc, Mutex},
    thread,
};

use anyhow::{anyhow, Result};
use bytes::Bytes;

use crate::{
    iterator::StorageIterator,
    state::{storage_state_options::StorageStateOptions, StorageState},
};

pub struct LsmStore {
    // send notification to end flush
    flush_notifier: crossbeam_channel::Sender<()>,
    // handle for flush thread
    flush_thread: Mutex<Option<thread::JoinHandle<()>>>,
    storage_state: Arc<StorageState>,
}

impl Drop for LsmStore {
    fn drop(&mut self) {
        self.flush_notifier.send(()).ok();
        // join all threads to avoid unexpected behavior
        // https://matklad.github.io/2019/08/23/join-your-threads.html
        let mut flush_thread = self.flush_thread.lock().unwrap();
        if let Some(thread) = flush_thread.take() {
            thread.join().unwrap();
        }
    }
}

impl LsmStore {
    pub fn open(options: StorageStateOptions) -> Result<LsmStore> {
        let storage_state = Arc::new(StorageState::open(options)?);

        // set up flush background thread
        let (flush_notifier, receiver) = crossbeam_channel::unbounded();
        let flush_thread = Mutex::new(storage_state.spawn_flush_thread(receiver)?);
        Ok(Self {
            flush_notifier,
            flush_thread,
            storage_state,
        })
    }

    pub fn close(&self) -> Result<()> {
        // end flush thread
        self.flush_notifier.send(()).ok();
        let mut flush_thread = self.flush_thread.lock().map_err(|e| anyhow!("{:?}", e))?;
        if let Some(thread) = flush_thread.take() {
            thread.join().map_err(|e| anyhow!("{:?}", e))
        } else {
            Ok(())
        }
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.storage_state.get(key)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.storage_state.put(key, value)
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.storage_state.delete(key)
    }

    pub fn scan(&self, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Result<impl StorageIterator> {
        self.storage_state.scan(lower, upper)
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use crate::state::storage_state_options::StorageStateOptions;

    use super::LsmStore;

    #[test]
    fn test_open_close() {
        let dir = tempdir().unwrap();
        let options = StorageStateOptions {
            sst_max_size_bytes: 128,
            block_max_size_bytes: 0,
            block_cache_size_bytes: 0,
            path: dir.path().to_owned(),
            num_memtables_limit: 5,
        };

        let store = LsmStore::open(options).unwrap();
        {
            let thread = store.flush_thread.lock().unwrap();
            assert!(!thread.as_ref().unwrap().is_finished());
        }
        store.close().unwrap();
        {
            let thread = store.flush_thread.lock().unwrap();
            // Option::take() replaces value in the mutex with None
            // JoinHandle is moved out of the option right before joining
            assert!(thread.as_ref().is_none());
        }
    }
}
