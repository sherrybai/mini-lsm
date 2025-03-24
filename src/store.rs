use anyhow::Result;

use crate::state::{storage_state::StorageState, storage_state_options::StorageStateOptions};

pub struct LsmStore {
    #[allow(unused)]
    storage_state: StorageState,
}

impl LsmStore {
    pub fn open(options: StorageStateOptions) -> Result<LsmStore> {
        let storage_state = StorageState::open(options)?;
        Ok(Self { storage_state })
    }
}