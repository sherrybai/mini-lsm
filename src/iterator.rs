use crate::kv::kv_pair::KeyValuePair;

pub mod merge_iterator;
pub mod two_merge_iterator;
pub mod bounded_iterator;
#[cfg(test)]
pub mod test_iterator;

pub trait StorageIterator: Iterator {
    fn peek(&mut self) -> Option<KeyValuePair>;
    fn is_valid(&self) -> bool;
}