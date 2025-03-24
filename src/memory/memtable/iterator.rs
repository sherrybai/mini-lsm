use std::{iter::Peekable, ops::Bound};
use std::sync::Arc;

use bytes::Bytes;
use crossbeam_skiplist::{map::Range, SkipMap};
use ouroboros::self_referencing;

use crate::{iterator::StorageIterator, kv::{kv_pair::KeyValuePair, timestamped_key::TimestampedKey}};

use super::MemTable;

type BytesBound = (Bound<Bytes>, Bound<Bytes>);

pub struct MemTableIterator {
    internal: MemTableIteratorInternal,
    current_kv: Option<KeyValuePair>
}

impl MemTableIterator {
    pub fn new(memtable: &MemTable, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Self {
        let bound = (
            lower.map(Bytes::copy_from_slice),
            upper.map(Bytes::copy_from_slice),
        );
        let mut new = Self {
            internal: MemTableIteratorInternal::new(memtable.entries.clone(), |map| map.range(bound).peekable()),
            current_kv: None
        };
        new.set_current_kv();
        new
    }

    fn set_current_kv(&mut self) {
        let new_entry = self.internal.with_sub_iterator_mut(
            |iterator| iterator.peek().map(|entry| KeyValuePair {
                key: TimestampedKey::new(entry.key().clone()), value: entry.value().clone()})
        );
        self.current_kv = new_entry;
    }
}

impl StorageIterator for MemTableIterator {
    fn peek(&mut self) -> Option<KeyValuePair> {
        self.current_kv.clone()
    }

    fn is_valid(&self) -> bool {
        true
    }
}

impl Iterator for MemTableIterator {
    type Item = KeyValuePair;
    fn next(&mut self) -> Option<KeyValuePair> {
        let next = self.internal.with_sub_iterator_mut(|iter| iter.next());
        let res = next.map(
            |entry| KeyValuePair {
                key: TimestampedKey::new(entry.key().clone()),
                value: entry.value().clone(),
            }
        );
        self.set_current_kv();
        res
    }
}

#[self_referencing]
pub struct MemTableIteratorInternal {
    map: Arc<SkipMap<Bytes, Bytes>>,
    #[borrows(map)]
    #[not_covariant]
    sub_iterator: Peekable<Range<'this, Bytes, BytesBound, Bytes, Bytes>>,
}

#[cfg(test)]
mod tests {
    use std::ops::Bound;

    use crate::{iterator::StorageIterator, kv::{kv_pair::KeyValuePair, timestamped_key::TimestampedKey}, memory::memtable::MemTable};

    use super::MemTableIterator;

    #[test]
    fn test_iterate() {
        let memtable = MemTable::new(0);
        let _ = memtable.put("hello".as_bytes(), "world".as_bytes());

        let mut iterator: MemTableIterator = MemTableIterator::new(&memtable, Bound::Unbounded, Bound::Unbounded);
        
        let expected_item = KeyValuePair { key: TimestampedKey::new("hello".as_bytes().into()), value: "world".as_bytes().into() };
        assert!(iterator.peek().is_some_and(|kv| kv == expected_item));

        assert!(iterator.next().is_some_and(|kv| kv == expected_item));
        assert!(iterator.next().is_none());
    }
}