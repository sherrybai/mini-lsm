use bytes::Bytes;

use crate::kv::{kv_pair::KeyValuePair, timestamped_key::TimestampedKey};

pub mod merge_iterator;
pub trait StorageIterator {
    fn key(&self) -> Option<TimestampedKey>;
    fn value(&self) -> Option<Bytes>;
    fn next(&mut self) -> Option<KeyValuePair>;
}