use std::collections::BinaryHeap;

use bytes::Bytes;

use crate::kv::{kv_pair::KeyValuePair, timestamped_key::TimestampedKey};

use super::StorageIterator;

pub struct MergeIterator<T: StorageIterator> {
    // first value: key value pair; second value: index of source iterator
    heap: BinaryHeap<(KeyValuePair, usize)>,
    iterators_to_merge: Vec<T>,
}

impl<T: StorageIterator> MergeIterator<T> {
    fn new(mut iterators_to_merge: Vec<T>) -> Self {
        let mut heap = BinaryHeap::new();
        for index in 0..iterators_to_merge.len() {
            let new_heap_kv = iterators_to_merge[index].next();
            if let Some(new_kv) = new_heap_kv {
                heap.push((new_kv, index));
            }
        }
        Self { heap, iterators_to_merge }
    }
}

impl<T: StorageIterator> StorageIterator for MergeIterator<T> {
    fn key(&self) -> Option<TimestampedKey> {
        self.heap.peek().map(|kv| kv.0.key.clone())
    }
    fn value(&self) -> Option<Bytes> {
        self.heap.peek().map(|kv| kv.0.value.clone())
    }
    fn next(&mut self) -> Option<KeyValuePair> {
        let res = self.heap.pop();
        match res {
            None => { return None },
            Some((res_kv, index)) => {
                let new_heap_kv = self.iterators_to_merge[index].next();
                if let Some(new_kv) = new_heap_kv {
                    self.heap.push((new_kv, index));
                }
                Some(res_kv)
            }
        }
    }
}
