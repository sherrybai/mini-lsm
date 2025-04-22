use std::{cmp::Ordering, sync::Arc};

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
    first_key: Bytes,
}

impl BlockIterator {
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        let first_key = block.get_first_key();
        let mut res = Self {
            block,
            current_index: 0,
            current_kv: None,
            first_key,
        };
        res.current_kv = res.parse_current_kv();
        res
    }

    pub fn create_and_seek_to_key(block: Arc<Block>, key: TimestampedKey) -> Self {
        let first_key = block.get_first_key();
        let mut res = Self {
            block,
            current_index: 0,
            current_kv: None,
            first_key,
        };
        res.seek_to_key(key);
        res
    }

    pub fn seek_to_first(&mut self) {
        self.current_index = 0;
    }

    pub fn seek_to_key(&mut self, key: TimestampedKey) {
        // seek to first key greater than or equal to key
        // binary search for the key in range 0..num_elements
        let (mut lo, mut hi) = (0, self.block.offsets.len() - 1);
        while lo < hi {
            let mid = (lo + hi) / 2;
            self.current_index = mid;
            self.current_kv = self.parse_current_kv();
            let raw_key = self
                .current_kv
                .clone()
                .expect("mid is less than length of block offsets")
                .key
                .get_key();
            match raw_key.cmp(&key.get_key()) {
                Ordering::Less => lo = mid + 1,
                Ordering::Greater => hi = mid,
                Ordering::Equal => return,
            }
        }
        let mid = (lo + hi) / 2;
        self.current_index = mid;
        self.current_kv = self.parse_current_kv();
    }

    fn parse_current_kv(&self) -> Option<KeyValuePair> {
        if self.current_index == self.block.offsets.len() {
            return None;
        }

        let current_offset = self.block.offsets[self.current_index];
        // parse key
        let key_contents_offset: usize;
        let key_vec: Vec<u8>;
        let value_contents_offset: usize;
        if self.current_index == 0 {
            key_contents_offset = current_offset as usize + 2;
            let key_size = u16::from_be_bytes(
                self.block.data[current_offset.into()..key_contents_offset]
                    .try_into()
                    .expect("chunk of size 2"),
            ) as usize;
            key_vec =
                self.block.data[key_contents_offset.into()..(key_contents_offset + key_size).into()].to_vec();
            value_contents_offset = key_contents_offset + key_size + 2;
        } else {
            key_contents_offset = current_offset as usize + 4;
            let key_overlap_len = u16::from_be_bytes(
                self.block.data[current_offset as usize..current_offset as usize + 2]
                    .try_into()
                    .expect("chunk of size 2"),
            ) as usize;
            let key_overlap = &self.first_key[..key_overlap_len];
            let rest_key_len = u16::from_be_bytes(
                self.block.data[current_offset as usize + 2..key_contents_offset as usize]
                    .try_into()
                    .expect("chunk of size 2"),
            ) as usize;
            let rest_key = &self.block.data[key_contents_offset..key_contents_offset + rest_key_len];
            key_vec = [key_overlap, rest_key].concat().to_vec();
            value_contents_offset = key_contents_offset + rest_key_len + 2;
        }
        // parse value
        let value_size = u16::from_be_bytes(
            self.block.data[(value_contents_offset - 2).into()..value_contents_offset.into()]
                .try_into()
                .expect("chunk of size 2"),
        ) as usize;
        let value_slice = &self.block.data
            [value_contents_offset..value_contents_offset + value_size];
        Some(KeyValuePair {
            key: TimestampedKey::new(Bytes::from(key_vec)),
            value: Bytes::copy_from_slice(value_slice),
        })
    }
}

impl StorageIterator for BlockIterator {
    fn peek(&mut self) -> Option<KeyValuePair> {
        self.current_kv.clone()
    }

    fn is_valid(&self) -> bool {
        true
    }
}

impl Iterator for BlockIterator {
    type Item = KeyValuePair;
    fn next(&mut self) -> Option<KeyValuePair> {
        let res = self.current_kv.clone()?;
        // update next item
        self.current_index += 1;
        self.current_kv = self.parse_current_kv();

        Some(res)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;

    use crate::{
        block::{builder::BlockBuilder, iterator::BlockIterator},
        iterator::StorageIterator,
        kv::{kv_pair::KeyValuePair, timestamped_key::TimestampedKey},
    };

    #[test]
    fn test_create_and_seek_to_first() {
        let mut block_builder = BlockBuilder::new(32);
        assert!(block_builder
            .add(KeyValuePair {
                key: TimestampedKey::new("k1".as_bytes().into()),
                value: "v1".as_bytes().into()
            })
            .is_ok());
        assert!(block_builder
            .add(KeyValuePair {
                key: TimestampedKey::new("k2".as_bytes().into()),
                value: "v2".as_bytes().into()
            })
            .is_ok());

        let block = Arc::new(block_builder.build());

        let mut block_iterator = BlockIterator::create_and_seek_to_first(block);
        assert_eq!(block_iterator.first_key, "k1".as_bytes());
        assert!(block_iterator.peek().is_some());
        assert_eq!(
            block_iterator
                .peek()
                .expect("checked for none")
                .key
                .get_key(),
            "k1".as_bytes()
        );

        for (i, kv) in block_iterator.enumerate() {
            assert_eq!(kv.key.get_key(), format!("k{}", i + 1).as_bytes());
        }
    }

    #[test]
    fn test_seek_to_key() {
        let mut block_builder = BlockBuilder::new(50);
        assert!(block_builder
            .add(KeyValuePair {
                key: TimestampedKey::new("k1".as_bytes().into()),
                value: "v1".as_bytes().into()
            })
            .is_ok());
        assert!(block_builder
            .add(KeyValuePair {
                key: TimestampedKey::new("k3".as_bytes().into()),
                value: "v3".as_bytes().into()
            })
            .is_ok());
        assert!(block_builder
            .add(KeyValuePair {
                key: TimestampedKey::new("k4".as_bytes().into()),
                value: "v4".as_bytes().into()
            })
            .is_ok());

        let block = Arc::new(block_builder.build());

        let mut block_iterator = BlockIterator::create_and_seek_to_first(block.clone());
        // not present in block
        let key = TimestampedKey::new(Bytes::copy_from_slice("k2".as_bytes()));
        block_iterator.seek_to_key(key.clone());
        assert!(block_iterator.peek().is_some());
        // seek should return first element that is greater than or equal to k2
        assert_eq!(
            block_iterator
                .peek()
                .expect("checked for none")
                .key
                .get_key(),
            "k3".as_bytes()
        );

        block_iterator = BlockIterator::create_and_seek_to_key(block.clone(), key.clone());
        assert!(block_iterator.peek().is_some());
        assert_eq!(
            block_iterator
                .peek()
                .expect("checked for none")
                .key
                .get_key(),
            "k3".as_bytes()
        );
    }
}
