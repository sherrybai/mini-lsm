use std::sync::Arc;

use crate::{
    iterator::StorageIterator,
    kv::{kv_pair::KeyValuePair, timestamped_key::TimestampedKey},
};

use super::Block;

pub struct BlockIterator {
    block: Arc<Block>,
    current_offset: u16,
}

impl BlockIterator {
    fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        Self {
            block,
            current_offset: 0,
        }
    }

    fn create_and_seek_to_key(block: Arc<Block>, key: TimestampedKey) -> Self {
        let mut res = Self {
            block,
            current_offset: 0,
        };
        res.seek_to_key(key);
        res
    }

    fn seek_to_first(&mut self) {
        self.current_offset = 0;
    }

    fn seek_to_key(&mut self, key: TimestampedKey) {
        // seek to first key greater than or equal to key
        let num_elements = self.block.offsets.len();

        // binary search for the key in range 0..num_elements
        let (mut lo, mut hi) = (0, num_elements - 1);
        while lo <= hi {
            let mid = (lo + hi) / 2;
            self.current_offset = self.block.offsets[mid];
            let kv_size = if mid == num_elements - 1 {
                self.block.end_of_data_offset - self.current_offset
            } else {
                self.block.offsets[mid + 1] - self.current_offset
            };
        }
    }
}

impl StorageIterator for BlockIterator {
    fn peek(&mut self) -> Option<KeyValuePair> {
        todo!()
    }
}

impl Iterator for BlockIterator {
    type Item = KeyValuePair;
    fn next(&mut self) -> Option<KeyValuePair> {
        todo!()
    }
}
