use std::path::Path;
use std::sync::Arc;

use anyhow::Result;

use crate::{
    block::{builder::BlockBuilder, metadata::BlockMetadata},
    kv::{kv_pair::KeyValuePair, timestamped_key::TimestampedKey},
    table::File,
};

use super::{block_cache::BlockCache, bloom::BloomFilter, Sst};

pub struct SSTBuilder {
    block_builder: BlockBuilder,
    // assume all metadata blocks can fit in memory
    block_meta_list: Vec<BlockMetadata>,
    block_size: usize,
    block_data: Vec<u8>,
    meta_block_offset: u32,
    first_key: TimestampedKey,
    last_key: TimestampedKey,
    all_keys: Vec<TimestampedKey>,
}

impl SSTBuilder {
    pub fn new(block_size: usize) -> Self {
        Self {
            block_builder: BlockBuilder::new(block_size),
            block_meta_list: Vec::new(),
            block_size,
            block_data: Vec::new(),
            meta_block_offset: 0,
            // junk values before we add keys
            first_key: TimestampedKey::new("".as_bytes().into()),
            last_key: TimestampedKey::new("".as_bytes().into()),
            all_keys: Vec::new(),
        }
    }

    pub fn add(&mut self, kv: KeyValuePair) -> Result<()> {
        // check if block is full
        if !self.block_builder.is_empty() && self.block_builder.get_block_size_with_kv(&kv) >= self.block_size {
            self.finalize_block();
            // update metadata
            self.meta_block_offset =
                u32::try_from(self.block_data.len()).expect("size of SST must fit in 4 bytes");
            self.first_key = kv.key.clone();
        }
        // handle first key in SST
        if self.first_key.get_key().is_empty() {
            self.first_key = kv.key.clone();
        }
        self.last_key = kv.key.clone();
        self.all_keys.push(kv.key.clone());
        self.block_builder.add(kv)?;
        Ok(())
    }

    pub fn finalize_block(&mut self) {
        // build block metadata
        let block_meta =
            BlockMetadata::new(self.meta_block_offset, self.first_key.clone(), self.last_key.clone());
        self.block_meta_list.push(block_meta);
        // build block
        let old_block_builder =
            std::mem::replace(&mut self.block_builder, BlockBuilder::new(self.block_size));
        let block = old_block_builder.build();
        self.block_data.extend(block.encode());
    }

    pub fn build(mut self, id: usize, path: impl AsRef<Path>, block_cache: Option<Arc<BlockCache>>) -> Result<Sst> {
        // finalize last block
        self.finalize_block();

        // encode SST
        let mut buffer: Vec<u8> = Vec::new();
        buffer.extend(self.block_data);

        self.meta_block_offset = u32::try_from(buffer.len()).expect("size of SST must fit in 4 bytes");
        for block_meta in self.block_meta_list.iter() {
            buffer.extend(block_meta.encode());
        }
        buffer.extend(self.meta_block_offset.to_be_bytes());

        // build bloom filter
        let mut bloom_filter = BloomFilter::from_keys(self.all_keys);
        let encoded_bloom = bloom_filter.encode();
        let bloom_filter_offset = u32::try_from(buffer.len()).expect("bloom offset must fit in 4 bytes");
        
        buffer.extend(encoded_bloom);
        buffer.extend(bloom_filter_offset.to_be_bytes());

        // dump to file
        let file = File::create(path, buffer)?;
        Ok(
            Sst::new(
                id, 
                file, 
                self.block_meta_list,
                self.meta_block_offset,
                block_cache,
                bloom_filter,
            )
        )
    }

    pub fn get_estimated_size(&self) -> usize {
        // just return size of block data in bytes
        // (metadata size is negligible)
        self.block_data.len()
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use crate::kv::{kv_pair::KeyValuePair, timestamped_key::TimestampedKey};

    use super::SSTBuilder;

    #[test]
    fn test_build() {
        let mut builder: SSTBuilder = SSTBuilder::new(25);
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

        // try build
        let dir = tempdir().unwrap();
        let path = dir.path().join("test_sst_build.sst");
        let mut sst = builder.build(0, path, None).unwrap();
        let file_contents: Vec<u8> = sst.file.get_contents_as_bytes().unwrap();

        // check that data size, meta size, and offset value are correct
        let bloom_offset = u32::from_be_bytes(file_contents[file_contents.len()-4..].try_into().expect("chunk of size 4"));
        let meta_offset = u32::from_be_bytes(file_contents[bloom_offset as usize-4..bloom_offset as usize].try_into().expect("chunk of size 4"));

        let expected_data_size = file_contents.len() 
        - (file_contents.len() - bloom_offset as usize) // size of bloom filter + offset
        - 4 // size of meta_offset
        - 2 * 12; // two metadata blocks of 12 bytes each (4 for offset, 4 each for first and last key)
        // start index of meta blocks should be equal to data size in bytes
        assert_eq!(meta_offset, u32::try_from(expected_data_size).expect("must fit in 4 bytes"));

        // assert correctness of meta offset field in sst struct
        assert_eq!(meta_offset, sst.meta_block_offset);
    }
}
