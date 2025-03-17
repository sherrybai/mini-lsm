use std::{cmp::Reverse, collections::BinaryHeap};

use bytes::Bytes;

use crate::kv::{kv_pair::KeyValuePair, timestamped_key::TimestampedKey};

use super::StorageIterator;

pub struct MergeIterator<T: StorageIterator> {
    // first value: key value pair; second value: index of source iterator
    heap: BinaryHeap<Reverse<(KeyValuePair, usize)>>,
    iterators_to_merge: Vec<T>,
}

impl<T: StorageIterator> MergeIterator<T> {
    fn new(mut iterators_to_merge: Vec<T>) -> Self {
        let mut heap = BinaryHeap::new();
        for index in 0..iterators_to_merge.len() {
            let new_heap_kv = iterators_to_merge[index].next();
            if let Some(new_kv) = new_heap_kv {
                heap.push(Reverse((new_kv, index)));
            }
        }
        Self { heap, iterators_to_merge }
    }
}

impl<T: StorageIterator> StorageIterator for MergeIterator<T> {
    fn peek(&mut self) -> Option<KeyValuePair> {
        let opt = self.heap.peek();
        match opt {
            None => { return None },
            Some(Reverse((res_kv, _))) => {
                Some(res_kv.clone())
            }
        }
    }
    fn next(&mut self) -> Option<KeyValuePair> {
        let res = self.heap.pop();
        match res {
            None => { return None },
            Some(Reverse((res_kv, index))) => {
                let new_heap_kv = self.iterators_to_merge[index].next();
                if let Some(new_kv) = new_heap_kv {
                    self.heap.push(Reverse((new_kv, index)));
                }
                Some(res_kv)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{iterator::StorageIterator, kv::timestamped_key::TimestampedKey, memory::{memtable::MemTable, memtable_iterator::MemTableIterator}};

    use super::MergeIterator;

    #[test]
    fn test_iterate() {
        let memtable_1 = MemTable::new(0);
        let _ = memtable_1.put("k2".as_bytes(), "v2".as_bytes());
        let memtable_2 = MemTable::new(0);
        let _ = memtable_2.put("k3".as_bytes(), "v3".as_bytes());
        let memtable_3 = MemTable::new(0);
        let _ = memtable_3.put("k1".as_bytes(), "v1".as_bytes());
        let _ = memtable_3.put("k4".as_bytes(), "v4".as_bytes());

        let memtable_iter_1 = MemTableIterator::new(&memtable_1);
        let memtable_iter_2 = MemTableIterator::new(&memtable_2);
        let memtable_iter_3 = MemTableIterator::new(&memtable_3);

        
        let mut merge_iterator = MergeIterator::new(vec![memtable_iter_1, memtable_iter_2, memtable_iter_3]);

        for i in 0..4 {
            let key = TimestampedKey::new(format!("k{}", i+1).into());
            assert!(merge_iterator.peek().is_some_and(|kv| kv.key == key));
            assert!(merge_iterator.next().is_some_and(|kv| kv.key == key));
        }
    }
}