pub mod iterator;

use std::sync::{atomic::{AtomicBool, AtomicUsize, Ordering}, Arc};

use anyhow::{anyhow, Ok, Result};
use bytes::Bytes;

use crossbeam_skiplist::SkipMap;

pub struct MemTable {
    id: usize,
    pub(super) entries: Arc<SkipMap<Bytes, Bytes>>,
    size_bytes: AtomicUsize,
    mutable: AtomicBool,
}

impl Clone for MemTable {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            entries: self.entries.clone(),
            size_bytes: AtomicUsize::new(self.size_bytes.load(Ordering::SeqCst)),
            mutable: AtomicBool::new(self.mutable.load(Ordering::SeqCst)),
        }
    }
}

impl MemTable {
    pub fn new(id: usize) -> Self {
        let entries: SkipMap<Bytes, Bytes> = SkipMap::new();
        Self {
            id,
            entries: Arc::new(entries),
            size_bytes: AtomicUsize::new(0),
            mutable: AtomicBool::new(true),
        }
    }

    pub fn get(&self, key: &[u8]) -> Option<Bytes> {
        self.entries.get(key).map(|entry| entry.value().clone())
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        if !self.mutable.load(Ordering::SeqCst) {
            return Err(anyhow!("cannot modify immutable table"))
        }
        self.entries
            .insert(Bytes::copy_from_slice(key), Bytes::copy_from_slice(value));
        self.size_bytes.fetch_add(key.len() + value.len(), Ordering::SeqCst);
        Ok(())
    }

    pub fn get_id(&self) -> usize { self.id }

    pub fn get_size_bytes(&self) -> usize {
        self.size_bytes.load(Ordering::SeqCst)
    }

    pub fn freeze(&self) -> Result<()> {
        let res = self.mutable.compare_exchange(true, false, Ordering::SeqCst, Ordering::SeqCst);
        if res.is_err() {
            return Err(anyhow!("memtable already frozen"));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;

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

        assert!(memtable.freeze().is_ok());
        assert_eq!(memtable.mutable.load(Ordering::SeqCst), false);
        assert!(memtable.freeze().is_err())
    }
}
