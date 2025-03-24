use std::sync::Arc;

use anyhow::Result;

use crate::{
    block::iterator::BlockIterator,
    iterator::StorageIterator,
    kv::{kv_pair::KeyValuePair, timestamped_key::TimestampedKey},
};

use super::Sst;

pub struct SSTIterator {
    sst: Arc<Sst>,
    block_index: usize,
    block_iterator: BlockIterator,
    current_kv: Option<KeyValuePair>,
    is_valid: bool,
}

impl SSTIterator {
    pub fn create_and_seek_to_first(sst: Arc<Sst>) -> Result<Self> {
        // load the first block
        let block = sst.read_block_cached( 0)?;
        let mut block_iterator = BlockIterator::create_and_seek_to_first(block);
        let current_kv = block_iterator.peek();
        Ok(Self {
            sst,
            block_index: 0,
            block_iterator,
            current_kv,
            is_valid: true,
        })
    }

    pub fn create_and_seek_to_key(sst: Arc<Sst>, key: TimestampedKey) -> Result<Self> {
        let block_index = sst.get_block_index_for_key(&key);
        let block = sst.read_block_cached(block_index)?;
        let mut block_iterator = BlockIterator::create_and_seek_to_key(block, key);
        let current_kv = block_iterator.peek();
        Ok(Self {
            sst,
            block_index,
            block_iterator,
            current_kv,
            is_valid: true,
        })
    }

    pub fn seek_to_key(&mut self, key: TimestampedKey) -> Result<()> {
        self.block_index = self.sst.get_block_index_for_key(&key);
        let block = self.sst.read_block_cached(self.block_index)?;
        self.block_iterator = BlockIterator::create_and_seek_to_key(block, key);
        self.current_kv = self.block_iterator.peek();
        Ok(())
    }
}

impl StorageIterator for SSTIterator {
    fn peek(&mut self) -> Option<KeyValuePair> {
        self.current_kv.clone()
    }

    fn is_valid(&self) -> bool {
        self.is_valid
    }
}

impl Iterator for SSTIterator {
    type Item = KeyValuePair;

    fn next(&mut self) -> Option<KeyValuePair> {
        if !self.is_valid
            || self.block_index >= self.sst.meta_blocks.len()
            || self.current_kv.is_none()
        {
            return None;
        }
        let current_key = self.current_kv.clone()?.key;
        let current_meta_block = &self.sst.meta_blocks[self.block_index];
        if current_key.get_key() < current_meta_block.get_last_key().get_key() {
            let res = self.block_iterator.next();
            self.current_kv = self.block_iterator.peek();
            res
        } else {
            let res = self.current_kv.clone();
            self.block_index += 1;
            if self.block_index >= self.sst.meta_blocks.len() {
                self.current_kv = None;
                return res;
            }
            // load new block
            let block = self.sst.read_block_cached(self.block_index);
            if block.is_err() {
                self.is_valid = false;
                return res;
            }
            self.block_iterator = BlockIterator::create_and_seek_to_first(block.unwrap());
            self.current_kv = self.block_iterator.next();
            res
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::{
        iterator::StorageIterator,
        kv::timestamped_key::TimestampedKey,
        table::{iterator::SSTIterator, test_utils::build_sst},
    };

    #[test]
    fn test_create_and_seek_to_first() {
        let sst = build_sst();
        // create iterator
        let mut iterator: SSTIterator =
            SSTIterator::create_and_seek_to_first(Arc::new(sst)).unwrap();
        assert!(iterator.peek().is_some());
        assert_eq!(
            iterator.peek().expect("checked for none").key.get_key(),
            "k1".as_bytes()
        );
        for (i, kv) in iterator.enumerate() {
            assert_eq!(kv.key.get_key(), format!("k{}", i + 1));
        }
    }

    #[test]
    fn test_seek_to_key() {
        let sst = Arc::new(build_sst());
        // create iterator
        let mut iterator = SSTIterator::create_and_seek_to_first(sst.clone()).unwrap();
        let mut i = 0;
        while iterator.peek().is_some() {
            let expected_key = format!("k{}", i + 1);
            assert_eq!(iterator.next().unwrap().key.get_key(), expected_key);
            i += 1;
        }
        let key = TimestampedKey::new("k2".as_bytes().into());
        assert!(iterator.seek_to_key(key.clone()).is_ok());
        assert_eq!(iterator.peek().unwrap().key, key);

        // create and seek to key
        iterator = SSTIterator::create_and_seek_to_key(sst.clone(), key.clone()).unwrap();
        assert_eq!(iterator.peek().unwrap().key, key);
        for (i, kv) in iterator.enumerate() {
            // iteration should start from k2
            assert_eq!(kv.key.get_key(), format!("k{}", i + 2));
        }
    }
}
