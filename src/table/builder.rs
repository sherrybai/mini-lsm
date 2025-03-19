use anyhow::Result;
use bytes::Bytes;

use crate::{
    block::{builder::BlockBuilder, metadata::BlockMetadata},
    kv::kv_pair::KeyValuePair,
};

use super::SST;

pub struct SSTBuilder {
    block_builder: BlockBuilder,
    // assume all metadata blocks can fit in memory
    block_meta_list: Vec<BlockMetadata>,
    block_size: usize,
    block_data: Vec<u8>,
    offset: u32,
    first_key: Bytes,
    last_key: Bytes,
}

impl SSTBuilder {
    pub fn new(block_size: usize) -> Self {
        Self {
            block_builder: BlockBuilder::new(block_size),
            block_meta_list: Vec::new(),
            block_size,
            block_data: Vec::new(),
            offset: 0,
            // junk values before we add keys
            first_key: "".as_bytes().into(),
            last_key: "".as_bytes().into(),
        }
    }

    pub fn add(&mut self, kv: KeyValuePair) -> Result<()> {
        // check if block is full
        if self.block_builder.get_block_size_with_kv(&kv) >= self.block_size {
            self.finalize_block();
            // create new block
            self.block_builder = BlockBuilder::new(self.block_size);
            // update metadata
            self.offset =
                u32::try_from(self.block_data.len()).expect("size of SST must fit in 4 bytes");
            self.first_key = kv.key.get_key();
        }
        // handle first key in SST
        if self.first_key.is_empty() {
            self.first_key = kv.key.get_key();
        }
        self.last_key = kv.key.get_key();
        self.block_builder.add(kv)?;
        Ok(())
    }

    pub fn finalize_block(&mut self) {
        // build block metadata
        let block_meta =
            BlockMetadata::new(self.offset, self.first_key.clone(), self.last_key.clone());
        self.block_meta_list.push(block_meta);
        // build block
        let block = self.block_builder.build();
        self.block_data.extend(block.encode());
    }

    pub fn build(&mut self) -> SST {
        // finalize last block
        self.finalize_block();
        todo!();
    }

    pub fn get_estimated_size(&self) -> usize {
        // just return size of block data in bytes
        // (metadata size is negligible)
        self.block_data.len()
    }
}

#[cfg(test)]
mod tests {
    use crate::kv::{kv_pair::KeyValuePair, timestamped_key::TimestampedKey};

    use super::SSTBuilder;

    #[test]
    fn test_add() {
        let mut builder = SSTBuilder::new(25);
        assert!(builder
            .add(KeyValuePair {
                key: TimestampedKey::new("k1".as_bytes().into()),
                value: "v1".as_bytes().into(),
            })
            .is_ok());
        assert_eq!(builder.block_meta_list.len(), 0);
        assert!(builder
            .add(KeyValuePair {
                key: TimestampedKey::new("k2".as_bytes().into()),
                value: "v2".as_bytes().into(),
            })
            .is_ok());
        assert_eq!(builder.block_meta_list.len(), 0);
        assert!(builder
            .add(KeyValuePair {
                key: TimestampedKey::new("k3".as_bytes().into()),
                value: "v3".as_bytes().into(),
            })
            .is_ok());
        // new block started
        assert_eq!(builder.block_meta_list.len(), 1);
        assert!(builder.block_data.len() > 0);
    }
}
