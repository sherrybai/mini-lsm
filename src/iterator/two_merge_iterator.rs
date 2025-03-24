use crate::kv::kv_pair::KeyValuePair;

use super::StorageIterator;

pub struct TwoMergeIterator<X: StorageIterator, Y: StorageIterator> {
    sub_iters: (X, Y),
    current_kv: Option<KeyValuePair>,
    current_iter_index: bool,
    is_valid: bool,
}

impl<X, Y> TwoMergeIterator<X, Y>
where
    X: StorageIterator + Iterator<Item = KeyValuePair>,
    Y: StorageIterator + Iterator<Item = KeyValuePair>,
{
    pub fn new(sub_iter_1: X, sub_iter_2: Y) -> Self {
        let mut sub_iters = (sub_iter_1, sub_iter_2);
        let is_valid = sub_iters.0.is_valid() && sub_iters.1.is_valid();
        let (current_kv, current_iter_index) =
            Self::get_current_kv_and_iter_index(&mut sub_iters, is_valid);
        Self {
            sub_iters,
            current_kv,
            current_iter_index,
            is_valid: true,
        }
    }

    fn get_current_kv_and_iter_index(
        sub_iters: &mut (X, Y),
        is_valid: bool,
    ) -> (Option<KeyValuePair>, bool) {
        if !is_valid {
            (None, false)
        } else {
            let peek = (sub_iters.0.peek(), sub_iters.1.peek());
            match peek {
                (Some(kv0), Some(kv1)) => {
                    if kv0 < kv1 { (Some(kv0), false) } else { (Some(kv1), true) }
                }
                (Some(kv0), None) => { (Some(kv0), false) }
                (None, Some(kv1)) => { (Some(kv1), true) }
                (None, None) => { (None, false) }
            }
        }
    }
}

impl<X, Y> StorageIterator for TwoMergeIterator<X, Y>
where
    X: StorageIterator + Iterator<Item = KeyValuePair>,
    Y: StorageIterator + Iterator<Item = KeyValuePair>,
{
    fn peek(&mut self) -> Option<KeyValuePair> {
        self.current_kv.clone()
    }

    fn is_valid(&self) -> bool {
        self.is_valid
    }
}

impl<X, Y> Iterator for TwoMergeIterator<X, Y>
where
    X: StorageIterator + Iterator<Item = KeyValuePair>,
    Y: StorageIterator + Iterator<Item = KeyValuePair>,
{
    type Item = KeyValuePair;

    fn next(&mut self) -> Option<KeyValuePair> {
        let res = self.current_kv.clone();
        // increment the correct iterator
        if !self.current_iter_index {  // int(self.current_iter_index) == 0
            self.sub_iters.0.next();
            if !self.sub_iters.0.is_valid() {
                self.is_valid = false;
            }
        } else {  // int(self.current_iter_index) == 1
            self.sub_iters.1.next();
            if !self.sub_iters.1.is_valid() {
                self.is_valid = false;
            }
        }
        (self.current_kv, self.current_iter_index) =
            Self::get_current_kv_and_iter_index(&mut self.sub_iters, self.is_valid);
        res
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        iterator::{test_iterator::TestIterator, StorageIterator},
        kv::timestamped_key::TimestampedKey,
        memory::memtable::{iterator::MemTableIterator, MemTable},
    };

    use super::TwoMergeIterator;

    #[test]
    fn test_iterate() {
        let memtable_1 = MemTable::new(0);
        let _ = memtable_1.put("k2".as_bytes(), "v2".as_bytes());
        let _ = memtable_1.put("k1".as_bytes(), "v1".as_bytes());
        let _ = memtable_1.put("k4".as_bytes(), "v4".as_bytes());
        let memtable_2 = MemTable::new(0);
        let _ = memtable_2.put("k3".as_bytes(), "v3".as_bytes());

        let memtable_iter_1 = MemTableIterator::new(&memtable_1);
        let memtable_iter_2 = MemTableIterator::new(&memtable_2);

        let mut two_merge_iterator = TwoMergeIterator::new(memtable_iter_1, memtable_iter_2);

        for i in 0..4 {
            let key = TimestampedKey::new(format!("k{}", i + 1).into());
            assert!(two_merge_iterator.peek().is_some_and(|kv| kv.key == key));
            assert!(two_merge_iterator.next().is_some_and(|kv| kv.key == key));
        }
    }

    #[test]
    fn test_not_valid() {
        let test_iter_1 = TestIterator::new(1, 2);
        let test_iter_2 = TestIterator::new(2, 1);

        let mut merge_iterator = TwoMergeIterator::new(test_iter_1, test_iter_2);
        assert_eq!(merge_iterator.next().unwrap().key.get_key(), "k1".as_bytes());
        assert!(merge_iterator.is_valid());
        assert_eq!(merge_iterator.next().unwrap().key.get_key(), "k1".as_bytes());
        assert!(!merge_iterator.is_valid());
    }
}
