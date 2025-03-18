use anyhow::Result;

use crate::kv::kv_pair::KeyValuePair;

use super::Block;

pub struct BlockBuilder {
    data: Vec<u8>,
    offsets: Vec<u16>,
    current_offset: u16,
}

impl BlockBuilder {
    pub fn new() -> Self {
        Self {
            data: Vec::new(),
            offsets: Vec::new(),
            current_offset: 0,
        }
    }

    pub fn add(&mut self, kv_pair: KeyValuePair) -> Result<()> {
        let kv_as_bytes: Vec<u8> = vec![kv_pair.key.get_key(), kv_pair.value]
            .iter()
            .map(|bytes| bytes.to_vec())
            .flatten()
            .collect();
        self.offsets.push(self.current_offset);
        self.current_offset += u16::try_from(kv_as_bytes.len())?;
        self.data.extend(kv_as_bytes);

        Ok(())
    }

    pub fn build(&self) -> Block {
        Block::new(
            self.data.clone(),
            self.offsets.clone(),
            self.current_offset.clone()
        )
    }
}

#[cfg(test)]
mod tests {
    use crate::kv::{kv_pair::KeyValuePair, timestamped_key::TimestampedKey};

    use super::{Block, BlockBuilder};

    #[test]
    fn test_blockbuilder_build() {
        let mut block_builder = BlockBuilder::new();
        assert!(block_builder.add(KeyValuePair { key: TimestampedKey::new("k1".as_bytes().into()), value: "v1".as_bytes().into() }).is_ok());
        assert!(block_builder.add(KeyValuePair { key: TimestampedKey::new("k2".as_bytes().into()), value: "v2".as_bytes().into() }).is_ok());

        let actual = block_builder.build();
        let expected = Block::new("k1v1k2v2".as_bytes().to_vec(), vec![0, 4], 8);
        assert_eq!(actual, expected);
    }
}