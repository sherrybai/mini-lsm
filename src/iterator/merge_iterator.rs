use std::{cmp::Reverse, collections::BinaryHeap};

use crate::kv::kv_pair::KeyValuePair;

use super::StorageIterator;

pub struct MergeIterator<T: StorageIterator> {
    // first value: key value pair; second value: index of source iterator
    heap: BinaryHeap<Reverse<(KeyValuePair, usize)>>,
    iterators_to_merge: Vec<T>,
    is_valid: bool,
}

impl<T> MergeIterator<T>
where
    T: StorageIterator + Iterator<Item = KeyValuePair>,
{
    pub fn new(mut iterators_to_merge: Vec<T>) -> Self {
        let mut is_valid = true;
        let mut heap: BinaryHeap<Reverse<(KeyValuePair, usize)>> = BinaryHeap::new();
        for (index, iterator) in iterators_to_merge.iter_mut().enumerate() {
            if !iterator.is_valid() {
                is_valid = false;
                break;
            }
            let new_heap_kv = iterator.next();
            if let Some(new_kv) = new_heap_kv {
                heap.push(Reverse((new_kv, index)));
            }
        }
        Self {
            heap,
            iterators_to_merge,
            is_valid,
        }
    }
}

impl<T> StorageIterator for MergeIterator<T>
where
    T: StorageIterator + Iterator<Item = KeyValuePair>,
{
    fn peek(&mut self) -> Option<KeyValuePair> {
        self.heap.peek().map(|Reverse((res_kv, _))| res_kv.clone())
    }

    fn is_valid(&self) -> bool {
        self.is_valid
    }
}

impl<T> Iterator for MergeIterator<T>
where
    T: StorageIterator + Iterator<Item = KeyValuePair>,
{
    type Item = KeyValuePair;
    fn next(&mut self) -> Option<KeyValuePair> {
        if !self.is_valid {
            return None;
        }
        let res = self.heap.pop();
        match res {
            None => None,
            Some(Reverse((res_kv, index))) => {
                if !self.iterators_to_merge[index].is_valid() {
                    self.is_valid = false;
                }
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
    use std::ops::Bound;

    use crate::{
        iterator::{
            test_iterator::TestIterator,
            StorageIterator,
        },
        kv::timestamped_key::TimestampedKey,
        memory::memtable::{iterator::MemTableIterator, MemTable},
    };

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

        let memtable_iter_1 = MemTableIterator::new(&memtable_1, Bound::Unbounded, Bound::Unbounded);
        let memtable_iter_2 = MemTableIterator::new(&memtable_2, Bound::Unbounded, Bound::Unbounded);
        let memtable_iter_3 = MemTableIterator::new(&memtable_3, Bound::Unbounded, Bound::Unbounded);

        let mut merge_iterator =
            MergeIterator::new(vec![memtable_iter_1, memtable_iter_2, memtable_iter_3]);

        for i in 0..4 {
            let key = TimestampedKey::new(format!("k{}", i + 1).into());
            assert!(merge_iterator.peek().is_some_and(|kv| kv.key == key));
            assert!(merge_iterator.next().is_some_and(|kv| kv.key == key));
        }
    }

    #[test]
    fn test_not_valid() {
        let test_iter_1 = TestIterator::new(1, 2);
        let test_iter_2 = TestIterator::new(2, 1);

        let mut merge_iterator = MergeIterator::new(vec![test_iter_1, test_iter_2]);
        assert_eq!(merge_iterator.next().unwrap().key.get_key(), "k1".as_bytes());
        assert!(merge_iterator.is_valid());
        assert_eq!(merge_iterator.next().unwrap().key.get_key(), "k1".as_bytes());
        assert!(!merge_iterator.is_valid());
    }
}
