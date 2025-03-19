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
        let key_len_bytes = u16::try_from(kv_pair.key.get_key().len())?.to_be_bytes();
        let value_len_bytes = u16::try_from(kv_pair.value.len())?.to_be_bytes();
        let kv_as_bytes: Vec<u8> = vec![
            key_len_bytes.to_vec(),
            kv_pair.key.get_key().to_vec(),
            value_len_bytes.to_vec(),
            kv_pair.value.to_vec(),
        ]
        .into_iter()
        .flatten()
        .collect();
        self.offsets.push(self.current_offset);
        self.current_offset += u16::try_from(kv_as_bytes.len())?;
        self.data.extend(kv_as_bytes);

        Ok(())
    }

    pub fn build(self) -> Block {
        Block::new(self.data, self.offsets, self.current_offset)
    }

    pub fn get_block_size(&self) -> usize {
        self.data.len() // data in bytes
        + 2 * self.offsets.len() // each offset is 2 bytes
        + 2 // end of data offset is 2 bytes
    }
}

#[cfg(test)]
mod tests {
    use crate::kv::{kv_pair::KeyValuePair, timestamped_key::TimestampedKey};

    use super::{Block, BlockBuilder};

    #[test]
    fn test_blockbuilder_build() {
        let mut block_builder = BlockBuilder::new();
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

        let actual = block_builder.build();

        let mut expected_data = vec![0, 2];
        expected_data.extend("k1".as_bytes());
        expected_data.extend(vec![0, 2]);
        expected_data.extend("v1".as_bytes());
        expected_data.extend(vec![0, 2]);
        expected_data.extend("k2".as_bytes());
        expected_data.extend(vec![0, 2]);
        expected_data.extend("v2".as_bytes());
        let expected = Block::new(expected_data, vec![0, 8], 16);
        assert_eq!(actual, expected);
    }
}
