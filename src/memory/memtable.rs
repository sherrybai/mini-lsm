use std::sync::Arc;

use anyhow::{Ok, Result};
use bytes::Bytes;

use crossbeam_skiplist::SkipMap;

pub struct MemTable {
    id: usize,
    entries: Arc<SkipMap<Bytes, Bytes>>,
}

impl MemTable {
    pub fn new(id: usize) -> Self {
        let entries: SkipMap<Bytes, Bytes> = SkipMap::new();
        Self {
            id: id,
            entries: Arc::new(entries),
        }
    }

    pub fn get(&self, key: &[u8]) -> Option<Bytes> {
        self.entries.get(key).map(|entry| entry.value().clone())
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.entries
            .insert(Bytes::copy_from_slice(key), Bytes::copy_from_slice(value));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use crate::memory::memtable::MemTable;

    #[test]
    fn test_memtable() {
        let memtable = MemTable::new(0);
        memtable
            .put("hello".as_bytes(), "world".as_bytes())
            .unwrap();

        assert_eq!(
            memtable.get("hello".as_bytes()).unwrap(),
            Bytes::from("world".as_bytes())
        );
    }
}
