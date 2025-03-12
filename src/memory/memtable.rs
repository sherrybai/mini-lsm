use std::sync::Arc;

use anyhow::{Ok, Result};
use bytes::Bytes;

use crossbeam_skiplist::SkipMap;

const TOMBSTONE: &[u8] = &[];

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
        let res = self.entries.get(key).map(|entry| entry.value().clone());
        if let Some(val) = &res {
            if val == TOMBSTONE {
                return None
            }
        }
        res
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.entries
            .insert(Bytes::copy_from_slice(key), Bytes::copy_from_slice(value));
        Ok(())
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.put(key,TOMBSTONE)
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

        memtable.delete("hello".as_bytes()).unwrap();
        assert_eq!(
            memtable.get("hello".as_bytes()), 
            None
        );
    }
}
