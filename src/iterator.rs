use crate::kv::kv_pair::KeyValuePair;

pub mod merge_iterator;
pub trait StorageIterator: Iterator {
    fn peek(&mut self) -> Option<KeyValuePair>;
}