use std::{ops::Bound, sync::Arc, thread};

use anyhow::Result;
use bytes::Bytes;

use crate::{
    iterator::StorageIterator,
    kv::kv_pair::KeyValuePair,
    state::{storage_state::StorageState, storage_state_options::StorageStateOptions},
};

pub struct LsmStore {
    // send notification to end flush
    flush_notifier: crossbeam_channel::Sender<()>,
    // handle for flush thread
    // flush_thread: thread::JoinHandle<()>,
    storage_state: StorageState,
}

impl Drop for LsmStore {
    fn drop(&mut self) {
        self.flush_notifier.send(()).ok();
    }
}

impl LsmStore {
    pub fn open(options: StorageStateOptions) -> Result<LsmStore> {
        let storage_state = StorageState::open(options)?;

        // set up flush background thread
        let (flush_notifier, receiver) = crossbeam_channel::unbounded();

        Ok(Self { 
            flush_notifier,
            storage_state 
        })
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

    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<impl StorageIterator> {
        self.storage_state.scan(lower, upper)
    }
}
