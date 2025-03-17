use crate::kv::kv_pair::KeyValuePair;

pub mod merge_iterator;
pub trait StorageIterator {
    fn peek(&mut self) -> Option<KeyValuePair>;
    fn next(&mut self) -> Option<KeyValuePair>;
}