use std::os::unix::prelude::FileExt;
use std::{io::Read, path::Path};

use anyhow::Result;

use crate::block::metadata::BlockMetadata;
use crate::block::Block;

use super::bloom::BloomFilter;
pub struct File {
    file: std::fs::File,
    size: u64,
}

impl File {
    pub fn create(path: impl AsRef<Path>, data: Vec<u8>) -> Result<Self> {
        std::fs::write(&path, &data)?;
        let file = std::fs::File::open(path)?; // read-only mode
        let size = file.metadata()?.len();
        Ok(Self { file, size })
    }

    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let file = std::fs::File::open(path)?;
        let size = file.metadata()?.len();
        Ok(Self { file, size })
    }

    pub fn get_contents_as_bytes(&mut self) -> Result<Vec<u8>> {
        let mut bytes: Vec<u8> = Vec::new();
        self.file.read_to_end(&mut bytes)?;
        Ok(bytes)
    }

    pub fn get_size(&self) -> u64 {
        self.size
    }

    pub fn load_block_to_mem(&self, offset: u32, block_size: u32) -> Result<Block> {
        let mut buffer = vec![0; block_size.try_into()?];
        self.file.read_exact_at(&mut buffer, offset.into())?;
        let block = Block::decode(buffer);
        Ok(block)
    }

    pub fn get_meta_block_offset(&mut self, bloom_filter_offset: u32) -> Result<u32> {
        // last 4 bytes of file
        let mut buffer = [0; 4];
        self.file.read_exact_at(&mut buffer, bloom_filter_offset as u64 - 4)?;
        Ok(u32::from_be_bytes(buffer))
    }

    pub fn load_meta_blocks(&mut self, meta_block_offset: u32, bloom_filter_offset: u32) -> Result<Vec<BlockMetadata>> {
        // start of bloom filter - start of meta blocks - 4 bytes for meta_block_offset
        let meta_encoded_length =
            usize::try_from(bloom_filter_offset)? - usize::try_from(meta_block_offset)? - 4;
        let mut buffer: Vec<u8> = vec![0; meta_encoded_length];
        self.file
            .read_exact_at(&mut buffer, meta_block_offset.into())?;
        let block_metadata = BlockMetadata::decode_to_list(&buffer);
        Ok(block_metadata)
    }

    pub fn get_bloom_filter_offset(&mut self) -> Result<u32> {
        // last 4 bytes of file
        let mut buffer = [0; 4];
        self.file.read_exact_at(&mut buffer, self.get_size() - 4)?;
        Ok(u32::from_be_bytes(buffer))
    }

    pub fn load_bloom_filter(&mut self, bloom_filter_offset: u32) -> Result<BloomFilter> {
        // size of encoded file - size of data - 4 bytes for bloom_filter_offset
        let bloom_encoded_length =
            usize::try_from(self.size)? - usize::try_from(bloom_filter_offset)? - 4;
        let mut buffer: Vec<u8> = vec![0; bloom_encoded_length];
        self.file
            .read_exact_at(&mut buffer, bloom_filter_offset.into())?;
        Ok(BloomFilter::decode(buffer))
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use crate::{
        block::{builder::BlockBuilder, metadata::BlockMetadata},
        kv::{kv_pair::KeyValuePair, timestamped_key::TimestampedKey},
        table::{file::File, test_utils::build_sst},
    };

    #[test]
    fn test_load_block_to_mem() {
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
        // 8 bytes for first kv pair; 9 bytes for subsequent kv pairs
        // 2 * 2 bytes per offset
        // 2 bytes for end of data offset
        let expected_block_size = 8 + 9 + 2 * 2 + 2;
        assert_eq!(block_builder.get_block_size(), expected_block_size);
        let block = block_builder.build();
        let data = block.encode();

        let dir = tempdir().unwrap();
        let path = dir.path().join("test_sst_build.sst");
        let file = File::create(path, data);
        assert!(file.is_ok());

        let loaded_block = file
            .unwrap()
            .load_block_to_mem(0, expected_block_size.try_into().unwrap());
        assert!(loaded_block.is_ok());
        assert_eq!(loaded_block.unwrap(), block);
    }

    #[test]
    fn test_load_meta_blocks() {
        let sst = build_sst();
        let mut file = sst.file;
        let bloom_filter_offset = file.get_bloom_filter_offset().unwrap();
        let meta_block_offset = file.get_meta_block_offset(bloom_filter_offset).unwrap();
        assert_eq!(meta_block_offset, 35);

        let meta_blocks = file.load_meta_blocks(meta_block_offset, bloom_filter_offset).unwrap();
        let expected_meta_1 = BlockMetadata::new(
            0,
            TimestampedKey::new("k1".as_bytes().into()),
            TimestampedKey::new("k2".as_bytes().into()),
        );
        let expected_meta_2 = BlockMetadata::new(
            23,
            TimestampedKey::new("k3".as_bytes().into()),
            TimestampedKey::new("k3".as_bytes().into()),
        );

        assert_eq!(meta_blocks.len(), 2);
        assert_eq!(meta_blocks[0], expected_meta_1);
        assert_eq!(meta_blocks[1], expected_meta_2);
    }
}
