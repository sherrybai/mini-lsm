pub mod iterator;

use std::{ops::Bound, sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
}};

use anyhow::{anyhow, Ok, Result};
use bytes::Bytes;

use crossbeam_skiplist::SkipMap;
use iterator::MemTableIterator;

use crate::table::builder::SSTBuilder;

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
            return Err(anyhow!("cannot modify immutable table"));
        }
        self.entries
            .insert(Bytes::copy_from_slice(key), Bytes::copy_from_slice(value));
        self.size_bytes
            .fetch_add(key.len() + value.len(), Ordering::SeqCst);
        Ok(())
    }

    pub fn scan(&self, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> MemTableIterator {
        MemTableIterator::new(self, lower, upper)
    }

    pub fn get_id(&self) -> usize {
        self.id
    }

    pub fn get_size_bytes(&self) -> usize {
        self.size_bytes.load(Ordering::SeqCst)
    }

    pub fn freeze(&self) -> Result<()> {
        let res = self
            .mutable
            .compare_exchange(true, false, Ordering::SeqCst, Ordering::SeqCst);
        if res.is_err() {
            return Err(anyhow!("memtable already frozen"));
        }
        Ok(())
    }

    pub fn flush(&self, sst_builder: &mut SSTBuilder) -> Result<()> {
        let iterator = MemTableIterator::new(self, Bound::Unbounded, Bound::Unbounded);
        for kv in iterator {
            sst_builder.add(kv)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{ops::Bound, sync::{atomic::Ordering, Arc}};

    use bytes::Bytes;
    use tempfile::tempdir;

    use crate::{
        kv::{kv_pair::KeyValuePair, timestamped_key::TimestampedKey},
        memory::memtable::MemTable,
        table::{builder::SSTBuilder, iterator::SSTIterator},
    };

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

    #[test]
    fn test_scan() {
        let memtable = MemTable::new(0);
        memtable
            .put("k1".as_bytes(), "v1".as_bytes())
            .unwrap();
        memtable
            .put("k2".as_bytes(), "v2".as_bytes())
            .unwrap();

        let mut iter = memtable.scan(Bound::Excluded("k1".as_bytes()), Bound::Included("k2".as_bytes()));
        assert_eq!(iter.next().unwrap().key.get_key(), "k2".as_bytes());
    }

    #[test]
    fn test_flush() {
        let memtable = MemTable::new(0);
        memtable
            .put("hello".as_bytes(), "world".as_bytes())
            .unwrap();

        let mut sst_builder = SSTBuilder::new(20);
        memtable.flush(&mut sst_builder).unwrap();

        let dir = tempdir().unwrap();
        let path = dir.path().join("test_memtable_flush.sst");
        let sst = sst_builder.build(0, path, None).unwrap();
        let mut sst_iterator = SSTIterator::create_and_seek_to_first(Arc::new(sst)).unwrap();
        assert_eq!(
            sst_iterator.next().unwrap(),
            KeyValuePair {
                key: TimestampedKey::new("hello".as_bytes().into()),
                value: "world".as_bytes().into()
            }
        );
    }
}
