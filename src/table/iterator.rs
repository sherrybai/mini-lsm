use crate::{iterator::StorageIterator, kv::kv_pair::KeyValuePair};

pub struct SSTIterator {

}

impl SSTIterator {

}

impl StorageIterator for SSTIterator {
    fn peek(&mut self) -> Option<crate::kv::kv_pair::KeyValuePair> {
        todo!()
    }
}

impl Iterator for SSTIterator {
    type Item = KeyValuePair;
    
    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}