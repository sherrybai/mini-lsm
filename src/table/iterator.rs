use std::sync::Arc;

use crate::{block::{iterator::BlockIterator, metadata::BlockMetadata, Block}, iterator::StorageIterator, kv::{kv_pair::KeyValuePair, timestamped_key::TimestampedKey}};

use super::SST;

pub struct SSTIterator {
    sst: Arc<SST>,
    block_index: usize,
    block_iterator: BlockIterator,
}

impl SSTIterator {
    pub fn create_and_seek_to_first(sst: Arc<SST>) -> Self {
        // load the first block
        let block = sst.file.load_block_to_mem(0);
        let block_iterator = BlockIterator::create_and_seek_to_first(Arc::new(block));
        Self {
            sst,
            block_index: 0,
            block_iterator
        }
    }

    pub fn create_and_seek_to_key(sst: Arc<SST>, key: TimestampedKey) -> Self {
        let block_index = Self::get_block_index_for_key(sst.clone(), &key);
        let block = sst.file.load_block_to_mem(block_index);
        let block_iterator = BlockIterator::create_and_seek_to_key(Arc::new(block), key);
        Self {
            sst,
            block_index,
            block_iterator
        }
    }

    pub fn seek_to_key(&mut self, key: TimestampedKey) {
        self.block_index = Self::get_block_index_for_key(self.sst.clone(), &key);
        let block = self.sst.file.load_block_to_mem(self.block_index);
        self.block_iterator = BlockIterator::create_and_seek_to_key(Arc::new(block), key);
    }

    fn get_block_index_for_key(sst: Arc<SST>, key: &TimestampedKey) -> usize {
        let (mut lo, mut hi) = (0, sst.meta_blocks.len() - 1);
        // seek to last block with first_key less than or equal to key
        while lo < hi {
            let mid = (lo + hi) / 2;
            let first_key = sst.meta_blocks[mid].get_first_key();
            if *first_key < key.get_key() {
                lo = mid;
            } else if *first_key > key.get_key() {
                hi = mid - 1;
            } else {
                return mid;
            }
        }
        return (lo + hi) / 2;
    }

    fn get_current_meta_block(&self) -> &BlockMetadata {
        &self.sst.meta_blocks[self.block_index]
    }
}

impl StorageIterator for SSTIterator {
    fn peek(&mut self) -> Option<KeyValuePair> {
        self.block_iterator.peek()
    }
}

impl Iterator for SSTIterator {
    type Item = KeyValuePair;
    
    fn next(&mut self) -> Option<KeyValuePair> {
        if self.block_index >= self.sst.meta_blocks.len() {
            return None;
        }
        let current_kv = self.peek();
        if current_kv.is_none() { return None };
        let current_key = current_kv.expect("returned early if none").key;
        if current_key.get_key() < self.get_current_meta_block().get_last_key() {
            self.block_iterator.next()
        } else {
            self.block_index += 1;
            if self.block_index >= self.sst.meta_blocks.len() {
                return None;
            }
            // load noew block
            let block = self.sst.file.load_block_to_mem(self.block_index);
            self.block_iterator = BlockIterator::create_and_seek_to_first(Arc::new(block));
            self.block_iterator.peek()  // first element in new block
        }
    }
}