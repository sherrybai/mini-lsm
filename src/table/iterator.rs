use crate::{block::metadata::BlockMetadata, iterator::StorageIterator, kv::kv_pair::KeyValuePair};

use super::SST;

pub struct SSTIterator {
    block_meta_list: Vec<BlockMetadata>,
}

impl SSTIterator {
    fn new(sst: SST) -> Self {
        todo!()
    }
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