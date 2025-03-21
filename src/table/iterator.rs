use std::{cmp::Ordering, sync::Arc};

use anyhow::Result;

use crate::{
    block::{iterator::BlockIterator, metadata::BlockMetadata, Block},
    iterator::StorageIterator,
    kv::{kv_pair::KeyValuePair, timestamped_key::TimestampedKey},
};

use super::SST;

pub struct SSTIterator {
    sst: Arc<SST>,
    block_index: usize,
    block_iterator: BlockIterator,
    current_kv: Option<KeyValuePair>,
    is_valid: bool,
}

impl SSTIterator {
    pub fn create_and_seek_to_first(sst: Arc<SST>) -> Result<Self> {
        // load the first block
        let block = Self::load_block_to_mem(sst.clone(), 0)?;
        let mut block_iterator = BlockIterator::create_and_seek_to_first(Arc::new(block));
        let current_kv = block_iterator.peek();
        Ok(Self {
            sst,
            block_index: 0,
            block_iterator,
            current_kv,
            is_valid: true,
        })
    }

    pub fn create_and_seek_to_key(sst: Arc<SST>, key: TimestampedKey) -> Result<Self> {
        let block_index = Self::get_block_index_for_key(sst.clone(), &key);
        let block = Self::load_block_to_mem(sst.clone(), block_index)?;
        let mut block_iterator = BlockIterator::create_and_seek_to_key(Arc::new(block), key);
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
        self.block_index = Self::get_block_index_for_key(self.sst.clone(), &key);
        let block = Self::load_block_to_mem(self.sst.clone(), self.block_index)?;
        self.block_iterator = BlockIterator::create_and_seek_to_key(Arc::new(block), key);
        self.current_kv = self.block_iterator.peek();
        Ok(())
    }

    fn get_block_index_for_key(sst: Arc<SST>, key: &TimestampedKey) -> usize {
        let (mut lo, mut hi) = (0, sst.meta_blocks.len() - 1);
        // seek to last block with first_key less than or equal to key
        while lo < hi {
            let mid = (lo + hi + 1) / 2; // use right mid to avoid infinite loop
            let first_key = sst.meta_blocks[mid].get_first_key();
            match first_key.cmp(&key.get_key()) {
                Ordering::Less => lo = mid,
                Ordering::Greater => hi = mid - 1,
                Ordering::Equal => return mid,
            }
        }
        (lo + hi + 1) / 2
    }

    fn get_current_meta_block(&self) -> &BlockMetadata {
        &self.sst.meta_blocks[self.block_index]
    }

    fn load_block_to_mem(sst: Arc<SST>, block_index: usize) -> Result<Block> {
        let offset = sst.meta_blocks[block_index].get_offset();
        let next_block_index = block_index + 1;
        let next_offset = if sst.meta_blocks.len() < next_block_index + 1 {
            sst.meta_block_offset
        } else {
            sst.meta_blocks[next_block_index].get_offset()
        };
        let block_size = next_offset - offset;
        sst.file.load_block_to_mem(offset, block_size)
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
        if current_key.get_key() < self.get_current_meta_block().get_last_key() {
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
            let block = Self::load_block_to_mem(self.sst.clone(), self.block_index);
            if block.is_err() {
                self.is_valid = false;
                return res;
            }
            self.block_iterator = BlockIterator::create_and_seek_to_first(Arc::new(
                block.expect("just checked for error"),
            ));
            self.current_kv = self.block_iterator.next();
            res
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tempfile::tempdir;

    use crate::{
        iterator::StorageIterator,
        kv::{kv_pair::KeyValuePair, timestamped_key::TimestampedKey},
        table::{builder::SSTBuilder, iterator::SSTIterator, SST},
    };

    fn build_sst() -> SST {
        let mut builder: SSTBuilder = SSTBuilder::new(25);
        // add three key-value pairs
        assert!(builder
            .add(KeyValuePair {
                key: TimestampedKey::new("k1".as_bytes().into()),
                value: "v1".as_bytes().into(),
            })
            .is_ok());
        assert!(builder
            .add(KeyValuePair {
                key: TimestampedKey::new("k2".as_bytes().into()),
                value: "v2".as_bytes().into(),
            })
            .is_ok());
        assert!(builder
            .add(KeyValuePair {
                key: TimestampedKey::new("k3".as_bytes().into()),
                value: "v3".as_bytes().into(),
            })
            .is_ok());
        // build
        let dir = tempdir().unwrap();
        let path = dir.path().join("test_sst_iterate.sst");
        let sst = builder.build(0, path).unwrap();
        sst
    }

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
