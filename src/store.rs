use std::ops::Bound;

use anyhow::Result;
use bytes::Bytes;

use crate::{
    iterator::StorageIterator,
    kv::kv_pair::KeyValuePair,
    state::{storage_state::StorageState, storage_state_options::StorageStateOptions},
};

pub struct LsmStore {
    storage_state: StorageState,
}

impl LsmStore {
    pub fn open(options: StorageStateOptions) -> Result<LsmStore> {
        let storage_state = StorageState::open(options)?;
        Ok(Self { storage_state })
    }

    pub fn get(&mut self, key: &[u8]) -> Result<Option<Bytes>> {
        self.storage_state.get(key)
    }

    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.storage_state.put(key, value)
    }

    pub fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.storage_state.delete(key)
    }

    pub fn scan(
        &mut self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<impl StorageIterator + Iterator<Item = KeyValuePair>> {
        self.storage_state.scan(lower, upper)
    }
}
