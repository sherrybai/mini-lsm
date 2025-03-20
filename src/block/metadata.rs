use bytes::Bytes;

#[derive(Debug, PartialEq)]
pub struct BlockMetadata {
    offset: u32,
    first_key: Bytes,
    last_key: Bytes,
}

impl BlockMetadata {
    pub fn new(offset: u32, first_key: Bytes, last_key: Bytes) -> Self {
        Self {
            offset,
            first_key,
            last_key,
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut encoded: Vec<u8> = Vec::new();
        encoded.extend(self.offset.to_be_bytes());
        // size of first key
        let first_key_size: u16 = self
            .first_key
            .len()
            .try_into()
            .expect("size must fit in 2 bytes");
        encoded.extend(first_key_size.to_be_bytes());
        encoded.extend(&self.first_key);
        // size of last key
        let last_key_size: u16 = self
            .last_key
            .len()
            .try_into()
            .expect("size must fit in 2 bytes");
        encoded.extend(last_key_size.to_be_bytes());
        encoded.extend(&self.last_key);
        encoded
    }

    pub fn decode(encoded_block_meta: &[u8], start_index: usize) -> (Self, usize) {
        let mut current_index = start_index;
        let offset: u32 = u32::from_be_bytes(
            encoded_block_meta[current_index..current_index + 4]
                .try_into()
                .expect("chunk of size 4"),
        );
        current_index += 4;
        let first_key_size: usize = u16::from_be_bytes(
            encoded_block_meta[current_index..current_index + 2]
                .try_into()
                .expect("chunk of size 2"),
        )
        .into();
        current_index += 2;
        let first_key = Bytes::copy_from_slice(
            &encoded_block_meta[current_index..current_index + first_key_size],
        );
        current_index += first_key_size;
        let last_key_size: usize = u16::from_be_bytes(
            encoded_block_meta[current_index..current_index + 2]
                .try_into()
                .expect("chunk of size 2"),
        )
        .into();
        current_index += 2;
        let last_key = Bytes::copy_from_slice(
            &encoded_block_meta[current_index..current_index + last_key_size],
        );
        current_index += last_key_size;

        // return block meta and size of the encoded meta in bytes
        (
            Self {
                offset,
                first_key,
                last_key,
            },
            current_index,
        )
    }

    pub fn decode_to_list(encoded_block_meta: &[u8]) -> Vec<Self> {
        let mut current_index = 0;
        let mut res: Vec<Self> = Vec::new();
        let encoded_size = encoded_block_meta.len();
        while current_index < encoded_size {
            let (decoded_block_meta, next_index) = Self::decode(&encoded_block_meta, current_index);
            res.push(decoded_block_meta);
            current_index = next_index;
        }
        res
    }

    pub fn get_first_key(&self) -> &Bytes {
        &self.first_key
    }

    pub fn get_last_key(&self) -> &Bytes {
        &self.last_key
    }
}

#[cfg(test)]
mod tests {
    use crate::block::metadata::BlockMetadata;

    #[test]
    fn test_encode_decode() {
        let block_meta = BlockMetadata::new(4, "k1".as_bytes().into(), "k2".as_bytes().into());
        let mut expected = vec![0, 0, 0, 4];
        expected.extend(vec![0, 2]);
        expected.extend("k1".as_bytes());
        expected.extend(vec![0, 2]);
        expected.extend("k2".as_bytes());

        let actual = block_meta.encode();
        let encoded_size = actual.len();
        assert_eq!(actual, expected);

        let (decoded_block_meta, block_meta_size) = BlockMetadata::decode(&actual, 0);
        assert_eq!(block_meta, decoded_block_meta);
        assert_eq!(block_meta_size, encoded_size);
    }

    #[test]
    fn test_decode_to_list() {
        let block_meta_1 = BlockMetadata::new(4, "k1".as_bytes().into(), "k2".as_bytes().into());
        let block_meta_2 = BlockMetadata::new(4, "k3".as_bytes().into(), "k4".as_bytes().into());
        let mut encoded = block_meta_1.encode();
        encoded.extend(block_meta_2.encode());

        let decoded_list = BlockMetadata::decode_to_list(&encoded);
        assert_eq!(decoded_list.len(), 2);
        assert_eq!(decoded_list[0], block_meta_1);
        assert_eq!(decoded_list[1], block_meta_2);
    }
}
