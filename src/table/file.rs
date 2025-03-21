use std::os::unix::prelude::FileExt;
use std::{io::Read, path::Path};

use anyhow::Result;

use crate::block::Block;
pub struct File {
    file: std::fs::File,
    size: usize,
}

impl File {
    pub fn create(path: impl AsRef<Path>, data: Vec<u8>) -> Result<Self> {
        std::fs::write(&path, &data)?;
        Ok(Self {
            file: std::fs::File::open(path)?, // read-only mode
            size: data.len(),
        })
    }

    pub fn get_contents_as_bytes(&mut self) -> Result<Vec<u8>> {
        let mut bytes: Vec<u8> = Vec::new();
        self.file.read_to_end(&mut bytes)?;
        Ok(bytes)
    }

    pub fn get_size(&self) -> usize {
        self.size
    }

    pub fn load_block_to_mem(&self, offset: u32, block_size: u32) -> Result<Block> {
        let mut buffer = vec![0; block_size.try_into()?];
        self.file.read_exact_at(&mut buffer, offset.into())?;
        let block = Block::decode(buffer);
        Ok(block)
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use crate::{block::builder::BlockBuilder, kv::{kv_pair::KeyValuePair, timestamped_key::TimestampedKey}, table::file::File};

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
        // 2 * 8 bytes per kv pair
        // 2 * 2 bytes per offset
        // 2 bytes for end of data offset
        let expected_block_size = 2 * 8 + 2 * 2 + 2;
        assert_eq!(block_builder.get_block_size(), expected_block_size);
        let block = block_builder.build();
        let data = block.encode();

        let dir = tempdir().unwrap();
        let path = dir.path().join("test_sst_build.sst");
        let file = File::create(path, data);
        assert!(file.is_ok());

        let loaded_block = file.unwrap().load_block_to_mem(0, expected_block_size.try_into().unwrap());
        assert!(loaded_block.is_ok());
        assert_eq!(loaded_block.unwrap(), block);
    }
}