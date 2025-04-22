use anyhow::{anyhow, Result};

use crate::kv::kv_pair::KeyValuePair;

use super::Block;

pub struct BlockBuilder {
    data: Vec<u8>,
    offsets: Vec<u16>,
    current_offset: u16,
    block_size: usize,
    first_key: Vec<u8>,
}

impl BlockBuilder {
    pub fn new(block_size: usize) -> Self {
        Self {
            data: Vec::new(),
            offsets: Vec::new(),
            current_offset: 0,
            block_size,
            first_key: Vec::new(),
        }
    }

    pub fn add(&mut self, kv_pair: KeyValuePair) -> Result<()> {
        if !self.is_empty() && self.get_block_size_with_kv(&kv_pair) > self.block_size {
            return Err(anyhow!("max block size reached"));
        }

        let key_as_bytes: Vec<u8>;
        if self.first_key.is_empty() {
            self.first_key = kv_pair.key.get_key().to_vec();
            let key_len_bytes = u16::try_from(kv_pair.key.get_key().len())?.to_be_bytes();
            key_as_bytes = vec![key_len_bytes.to_vec(), kv_pair.key.get_key().to_vec()]
                .into_iter()
                .flatten()
                .collect();
        } else {
            let key_overlap_len = kv_pair
                .key
                .get_key()
                .iter()
                .zip(self.first_key.clone())
                .take_while(|(x, y)| *x == y)
                .count();
            let rest_key_len = kv_pair.key.get_key().len() - key_overlap_len;
            key_as_bytes = vec![
                u16::try_from(key_overlap_len)?.to_be_bytes().to_vec(),
                u16::try_from(rest_key_len)?.to_be_bytes().to_vec(),
                kv_pair.key.get_key()[key_overlap_len..].to_vec(),
            ].into_iter()
            .flatten()
            .collect();
        }
        let value_len_bytes = u16::try_from(kv_pair.value.len())?.to_be_bytes();
        let kv_as_bytes: Vec<u8> = vec![
            key_as_bytes,
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

    pub fn is_empty(&self) -> bool {
        self.data.len() == 0
    }

    pub fn get_block_size(&self) -> usize {
        self.data.len() // data in bytes
        + 2 * self.offsets.len() // each offset is 2 bytes
        + 2 // end of data offset is 2 bytes
    }

    pub fn get_block_size_with_kv(&self, kv: &KeyValuePair) -> usize {
        let block_size = self.get_block_size();
        if block_size == 0 {
            2 + kv.key.get_key().len() + 2 + kv.value.len() + 2
        } else {
            block_size
            + 4 // key_overlap + rest_key_len
            + kv.key.get_key().len()
            + 2 // value length
            + kv.value.len()
            + 2 // length of new offset
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::kv::{kv_pair::KeyValuePair, timestamped_key::TimestampedKey};

    use super::{Block, BlockBuilder};

    #[test]
    fn test_blockbuilder_build() {
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
        let estimated_size = block_builder.get_block_size();

        let actual = block_builder.build();

        let mut expected_data = vec![0, 2];
        expected_data.extend("k1".as_bytes());
        expected_data.extend(vec![0, 2]);
        expected_data.extend("v1".as_bytes());
        expected_data.extend(vec![0, 1, 0, 1]);
        expected_data.extend("2".as_bytes());
        expected_data.extend(vec![0, 2]);
        expected_data.extend("v2".as_bytes());
        let expected = Block::new(expected_data, vec![0, 8], 17);
        assert_eq!(actual, expected);

        // check that our calculated size is correct
        assert_eq!(estimated_size, actual.encode().len())
    }

    #[test]
    fn test_blockbuilder_check_block_size() {
        let mut block_builder = BlockBuilder::new(12);
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
            .is_err());
    }
}
