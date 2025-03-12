use std::sync::{Arc, RwLock};

use anyhow::{Ok, Result};
use bytes::Bytes;

use crate::memory::memtable::MemTable;


pub struct StorageState {
    current_memtable: Arc<MemTable>,
    frozen_memtables: Vec<MemTable>,
    state_lock: RwLock<()>,
}

impl StorageState {
    fn new() -> Self {
        let current_memtable = Arc::new(MemTable::new(0));
        // oldest to newest frozen tables
        let frozen_memtables: Vec<MemTable> = Vec::new();

        Self {
            current_memtable,
            frozen_memtables,
            state_lock: RwLock::new(()),
        }
    }

    fn get(&mut self, key: &[u8]) -> Option<Bytes> {
        let _rlock = self.state_lock.read().unwrap();
        self.current_memtable.get(key)
    }

    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        let _rlock = self.state_lock.read().unwrap();
        self.current_memtable.put(key, value)
    }

    fn delete(&mut self, key: &[u8]) -> Result<()> {
        let _rlock = self.state_lock.read().unwrap();
        self.current_memtable.delete(key)    }
}

#[cfg(test)]
mod tests {
    use crate::state::storage_state::StorageState;

    #[test]
    fn test_storage_state() {

    }
}