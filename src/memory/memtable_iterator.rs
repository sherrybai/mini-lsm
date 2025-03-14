use std::sync::Arc;

use bytes::Bytes;
use crossbeam_skiplist::{map::Iter, SkipMap};
use ouroboros::self_referencing;

use crate::{iterator::StorageIterator, kv::{kv_pair::KeyValuePair, timestamped_key::TimestampedKey}};

#[self_referencing]
pub struct MemtableIterator {
    map: Arc<SkipMap<Bytes, Bytes>>,
    #[borrows(map)]
    #[not_covariant]
    sub_iterator: Iter<'this, Bytes, Bytes>,
    current_kv: Option<KeyValuePair>,
}

impl StorageIterator for MemtableIterator {
    fn key(&self) -> Option<crate::kv::timestamped_key::TimestampedKey> {
        self.with_current_kv(|kv: &Option<KeyValuePair>| kv.clone().map(|kv_actual| kv_actual.key))
    }

    fn value(&self) -> Option<Bytes> {
        self.with_current_kv(|kv: &Option<KeyValuePair>| kv.clone().map(|kv_actual| kv_actual.value))
    }

    fn next(&mut self) -> Option<KeyValuePair> {
        let next = self.with_sub_iterator_mut(|iter| iter.next());
        let res = next.map(
            |entry| KeyValuePair {
                key: TimestampedKey::new(entry.key().clone()),
                value: entry.value().clone(),
            }
        );

        let mut new_entry= self.with_sub_iterator_mut(
            |iterator| iterator.peekable().peek().map(|entry| KeyValuePair {
                key: TimestampedKey::new(entry.key().clone()), value: entry.value().clone()})
        );
        self.with_current_kv_mut(|mut kv| kv = &mut new_entry);
        res
    }
}