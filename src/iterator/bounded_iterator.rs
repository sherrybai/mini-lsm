use std::cmp::Ordering;
use std::ops::Bound;

use bytes::Bytes;

use crate::iterator::StorageIterator;
use crate::kv::kv_pair::KeyValuePair;
use crate::kv::timestamped_key::TimestampedKey;

pub struct BoundedIterator<T> {
    sub_iterator: T,
    upper_bound: Bound<TimestampedKey>,
}

impl<T> BoundedIterator<T> where T: StorageIterator + Iterator<Item = KeyValuePair> {
    pub fn new(sub_iterator: T, bound: Bound<&[u8]>) -> Self {
        Self {
            sub_iterator,
            upper_bound: bound.map(|key| TimestampedKey::new(Bytes::copy_from_slice(key))),
        }
    }
}

impl<T> StorageIterator for BoundedIterator<T>
where
    T: StorageIterator + Iterator<Item = KeyValuePair>,
{
    fn peek(&mut self) -> Option<KeyValuePair> {
        match self.sub_iterator.peek() {
            Some(current_kv) => {
                match &self.upper_bound {
                    Bound::Included(upper_key) => {
                        match current_kv.key.cmp(upper_key) {
                            Ordering::Less | Ordering::Equal => {
                                Some(current_kv)
                            },
                            Ordering::Greater => {
                                None
                            },
                        }
                    },
                    Bound::Excluded(upper_key) => {
                        match current_kv.key.cmp(upper_key) {
                            Ordering::Less => {
                                Some(current_kv)
                            },
                            Ordering::Equal | Ordering::Greater => {
                                None
                            },
                        }
                    },
                    Bound::Unbounded => { Some(current_kv) },
                }
            },
            None => { None },
        }
    }

    fn is_valid(&self) -> bool {
        self.sub_iterator.is_valid()
    }
}

impl<T> Iterator for BoundedIterator<T>
where
    T: StorageIterator + Iterator<Item = KeyValuePair>,
{
    type Item = KeyValuePair;

    fn next(&mut self) -> Option<KeyValuePair> {
        match self.sub_iterator.peek() {
            Some(current_kv) => {
                match &self.upper_bound {
                    Bound::Included(upper_key) => {
                        match current_kv.key.cmp(upper_key) {
                            Ordering::Less | Ordering::Equal => {
                                self.sub_iterator.next()
                            },
                            Ordering::Greater => {
                                None
                            },
                        }
                    },
                    Bound::Excluded(upper_key) => {
                        match current_kv.key.cmp(upper_key) {
                            Ordering::Less => {
                                self.sub_iterator.next()
                            },
                            Ordering::Equal | Ordering::Greater => {
                                None
                            },
                        }
                    },
                    Bound::Unbounded => { 
                        self.sub_iterator.next()
                    },
                }
            },
            None => { None },
        }
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Bound;

    use crate::{kv::kv_pair::KeyValuePair, memory::memtable::{iterator::MemTableIterator, MemTable}};

    use super::BoundedIterator;

    #[test]
    fn test_bounded_iterator() {
        let memtable = MemTable::new(0);
        let _ = memtable.put("k1".as_bytes(), "v1".as_bytes());
        let _ = memtable.put("k2".as_bytes(), "v2".as_bytes());

        let mut iterator  = MemTableIterator::new(&memtable, Bound::Unbounded, Bound::Unbounded);
        let mut bounded_iterator = BoundedIterator::new(
            iterator,
            Bound::Included("k1".as_bytes())
        );
        let items: Vec<KeyValuePair> = bounded_iterator.into_iter().collect();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].key.get_key(), "k1".as_bytes());

        iterator = MemTableIterator::new(&memtable, Bound::Unbounded, Bound::Unbounded);
        bounded_iterator = BoundedIterator::new(
            iterator,
            Bound::Excluded("k1".as_bytes())
        );
        let items: Vec<KeyValuePair> = bounded_iterator.into_iter().collect();
        assert_eq!(items.len(), 0);

        iterator = MemTableIterator::new(&memtable, Bound::Unbounded, Bound::Unbounded);
        bounded_iterator = BoundedIterator::new(
            iterator,
            Bound::Unbounded
        );
        let items: Vec<KeyValuePair> = bounded_iterator.into_iter().collect();
        assert_eq!(items.len(), 2);
    }
}