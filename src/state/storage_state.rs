use anyhow::{Ok, Result};
use bytes::Bytes;
use crate::memory::memtable::MemTable;

const TOMBSTONE: &[u8] = &[];

pub struct StorageState {
    current_memtable: MemTable,
    frozen_memtables: Vec<MemTable>
}

impl StorageState {
    fn create() -> Self {
        let current_memtable = MemTable::new(0);
        // oldest to newest frozen tables
        let frozen_memtables: Vec<MemTable> = Vec::new();

        Self {
            current_memtable,
            frozen_memtables,
        }
    }

    fn get(&mut self, key: &[u8]) -> Option<Bytes> {
        self.current_memtable.get(key)
    }

    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.current_memtable.put(key, value)
    }

    fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.current_memtable.put(key, TOMBSTONE)
    }
}