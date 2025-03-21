use std::sync::Arc;

use anyhow::Result;

use crate::{block::{iterator::BlockIterator, metadata::BlockMetadata, Block}, iterator::StorageIterator, kv::{kv_pair::KeyValuePair, timestamped_key::TimestampedKey}};

use super::SST;

pub struct SSTIterator {
    sst: Arc<SST>,
    block_index: usize,
    block_iterator: BlockIterator,
    is_valid: bool,
}

impl SSTIterator {
    pub fn create_and_seek_to_first(sst: Arc<SST>) -> Result<Self> {
        // load the first block
        let block = Self::load_block_to_mem(sst.clone(), 0)?;
        let block_iterator = BlockIterator::create_and_seek_to_first(Arc::new(block));
        Ok(Self {
            sst,
            block_index: 0,
            block_iterator,
            is_valid: true
        })
    }

    pub fn create_and_seek_to_key(sst: Arc<SST>, key: TimestampedKey) -> Result<Self> {
        let block_index = Self::get_block_index_for_key(sst.clone(), &key);
        let block = Self::load_block_to_mem(sst.clone(), block_index)?;
        let block_iterator = BlockIterator::create_and_seek_to_key(Arc::new(block), key);
        Ok(Self {
            sst,
            block_index,
            block_iterator,
            is_valid: true,
        })
    }

    pub fn seek_to_key(&mut self, key: TimestampedKey) -> Result<()>{
        self.block_index = Self::get_block_index_for_key(self.sst.clone(), &key);
        let block = Self::load_block_to_mem(self.sst.clone(), self.block_index)?;
        self.block_iterator = BlockIterator::create_and_seek_to_key(Arc::new(block), key);
        Ok(())
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

    fn load_block_to_mem(sst: Arc<SST>, block_index: usize) -> Result<Block> {
        let next_block_index = block_index + 1;
        let next_offset = if sst.meta_blocks.len() < next_block_index + 1 { 
            sst.meta_block_offset 
        } else { 
            sst.meta_blocks[next_block_index].get_offset()
        };
        let block_size = next_offset - 4;  // remove 4 bytes for block offset
        sst.file.load_block_to_mem(0, block_size)
    }
}

impl StorageIterator for SSTIterator {
    fn peek(&mut self) -> Option<KeyValuePair> {
        self.block_iterator.peek()
    }

    fn is_valid(&self) -> bool {
        self.is_valid
    }
}

impl Iterator for SSTIterator {
    type Item = KeyValuePair;
    
    fn next(&mut self) -> Option<KeyValuePair> {
        if !self.is_valid {
            return None;
        }
        if self.block_index >= self.sst.meta_blocks.len() {
            self.is_valid = false;
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
                self.is_valid = false;
                return None;
            }
            // load new block
            let block = Self::load_block_to_mem(self.sst.clone(), self.block_index);
            if block.is_err() {
                self.is_valid = false;
                return None;
            }
            self.block_iterator = BlockIterator::create_and_seek_to_first(Arc::new(block.expect("just checked for error")));
            self.block_iterator.peek()  // first element in new block
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tempfile::tempdir;

    use crate::{iterator::StorageIterator, kv::{kv_pair::KeyValuePair, timestamped_key::TimestampedKey}, table::{builder::SSTBuilder, iterator::SSTIterator, SST}};

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
        let mut iterator = SSTIterator::create_and_seek_to_first(Arc::new(sst)).unwrap();
        assert!(iterator.peek().is_some());
        assert_eq!(
            iterator
                .peek()
                .expect("checked for none")
                .key
                .get_key(),
            "k1".as_bytes()
        );
        for (i, kv) in iterator.enumerate() {
            assert_eq!(kv.key.get_key(), format!("k{}", i+1).as_bytes());
        }
    }
}