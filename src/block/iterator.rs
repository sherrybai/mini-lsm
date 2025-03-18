use std::sync::Arc;

use bytes::Bytes;

use crate::{
    iterator::StorageIterator,
    kv::{kv_pair::KeyValuePair, timestamped_key::TimestampedKey},
};

use super::Block;

pub struct BlockIterator {
    block: Arc<Block>,
    current_index: usize,
    current_kv: Option<KeyValuePair>,
}

impl BlockIterator {
    fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        let mut res = Self {
            block,
            current_index: 0,
            current_kv: None
        };
        res.current_kv = Some(res.parse_current_kv());
        res
    }

    fn create_and_seek_to_key(block: Arc<Block>, key: TimestampedKey) -> Self {
        let mut res = Self {
            block,
            current_index: 0,
            current_kv: None
        };
        res.seek_to_key(key);
        res
    }

    fn seek_to_first(&mut self) {
        self.current_index = 0;
    }

    fn seek_to_key(&mut self, key: TimestampedKey) {
        // seek to first key greater than or equal to key
        // binary search for the key in range 0..num_elements
        let (mut lo, mut hi) = (0, self.block.offsets.len() - 1);
        while lo <= hi {
            let mid = (lo + hi) / 2;
            self.current_index = mid;
            self.current_kv = Some(self.parse_current_kv());
            let raw_key = self.current_kv.clone().expect("kv was just set to Some").key.get_key();
            if raw_key < key.get_key() {
                lo = mid + 1;
            } else if raw_key > key.get_key() {
                hi = mid - 1;
            } else {
                return;
            }
        }
    }

    fn parse_current_kv(&self) -> KeyValuePair {
        let current_offset = self.block.offsets[self.current_index];

        // parse key
        let key_contents_offset = current_offset + 2;
        let key_size = u16::from_be_bytes(
            self.block.data[current_offset.into()..key_contents_offset.into()]
                .try_into()
                .expect("chunk of size 2"),
        );
        let key_slice =
            &self.block.data[key_contents_offset.into()..(key_contents_offset + key_size).into()];
        // parse value
        let value_contents_offset = key_contents_offset + key_size + 2;
        let value_size = u16::from_be_bytes(
            self.block.data[(value_contents_offset - 2).into()..value_contents_offset.into()]
                .try_into()
                .expect("chunk of size 2"),
        );
        let value_slice = &self.block.data
            [value_contents_offset.into()..(value_contents_offset + value_size).into()];
        KeyValuePair {
            key: TimestampedKey::new(Bytes::copy_from_slice(key_slice)),
            value: Bytes::copy_from_slice(value_slice)
        }
    }
}

impl StorageIterator for BlockIterator {
    fn peek(&mut self) -> Option<KeyValuePair> {
        self.current_kv.clone()
    }
}

impl Iterator for BlockIterator {
    type Item = KeyValuePair;
    fn next(&mut self) -> Option<KeyValuePair> {
        self.current_index += 1;
        self.parse_current_kv();
        self.current_kv.clone()
    }
}
