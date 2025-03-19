pub mod builder;
pub mod iterator;
pub mod metadata;

#[derive(Debug, PartialEq)]
pub struct Block {
    data: Vec<u8>,
    // offsets for each key-value pair. allows for binary search over the block
    offsets: Vec<u16>,
    end_of_data_offset: u16,
}

impl Block {
    pub fn new(data: Vec<u8>, offsets: Vec<u16>, end_of_data_offset: u16) -> Self {
        Self {
            data,
            offsets,
            end_of_data_offset,
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut encoded: Vec<u8> = Vec::new();
        encoded.extend(self.data.clone());
        // u16 offsets are stored in big-endian order
        encoded.extend(
            self.offsets
                .iter()
                .map(|offset| offset.to_be_bytes())
                .flatten(),
        );
        encoded.extend(self.end_of_data_offset.to_be_bytes());
        encoded
    }

    pub fn decode(encoded_block: Vec<u8>) -> Self {
        let encoded_block_size = encoded_block.len();
        let end_of_data_offset_le_bytes = [
            encoded_block[encoded_block_size - 2],
            encoded_block[encoded_block_size - 1],
        ];
        let end_of_data_offset = u16::from_be_bytes(end_of_data_offset_le_bytes);

        let data = encoded_block[..end_of_data_offset.into()].to_vec();
        let offsets_bytes = &encoded_block[end_of_data_offset.into()..encoded_block_size - 2];
        let offsets: Vec<u16> = offsets_bytes
            .chunks_exact(2)
            .map(|chunk| u16::from_be_bytes(chunk.try_into().expect("chunk of size 2")))
            .collect();
        Self {
            data,
            offsets,
            end_of_data_offset,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Block;

    #[test]
    fn test_encode_decode() {
        let mut data = vec![0,2];
        data.extend("k1".as_bytes());
        data.extend(vec![0,2]);
        data.extend("v1".as_bytes());
        data.extend(vec![0,2]);
        data.extend("k2".as_bytes());
        data.extend(vec![0,2]);
        data.extend("v2".as_bytes());
        let block = Block::new(
            data.clone(), 
            vec![0, 8], 
            16
        );
        let mut expected = data.clone();  // data block
        expected.extend(vec![0,0,0,8,0,16]);  // offset block

        let actual = block.encode();
        assert_eq!(actual, expected);

        let decoded_block = Block::decode(actual);
        assert_eq!(block, decoded_block);
    }
}