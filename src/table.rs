use std::cmp::Ordering;

use anyhow::Result;
use block_cache::BlockCache;

use crate::block::metadata::BlockMetadata;
use crate::block::Block;
use crate::kv::timestamped_key::TimestampedKey;
use crate::table::file::File;

#[cfg(test)]
mod test_utils;

pub mod block_cache;
pub mod builder;
pub mod file;
pub mod iterator;

// in-memory representation of a single SST file on disk
pub struct SST {
    id: usize,
    file: File,
    meta_blocks: Vec<BlockMetadata>,
    meta_block_offset: u32,
    block_cache: Option<BlockCache>,
}

impl SST {
    pub fn new(
        id: usize,
        file: File,
        meta_blocks: Vec<BlockMetadata>,
        meta_block_offset: u32,
        block_cache: Option<BlockCache>,
    ) -> Self {
        Self {
            id,
            file,
            meta_blocks,
            meta_block_offset,
            block_cache,
        }
    }

    fn load_block_to_mem(&self, block_index: usize) -> Result<Block> {
        let offset = self.meta_blocks[block_index].get_offset();
        let next_block_index = block_index + 1;
        let next_offset = if self.meta_blocks.len() < next_block_index + 1 {
            self.meta_block_offset
        } else {
            self.meta_blocks[next_block_index].get_offset()
        };
        let block_size = next_offset - offset;
        self.file.load_block_to_mem(offset, block_size)
    }

    fn get_block_index_for_key(&self, key: &TimestampedKey) -> usize {
        let (mut lo, mut hi) = (0, self.meta_blocks.len() - 1);
        // seek to last block with first_key less than or equal to key
        while lo < hi {
            let mid = (lo + hi + 1) / 2; // use right mid to avoid infinite loop
            let first_key = self.meta_blocks[mid].get_first_key();
            match first_key.cmp(&key.get_key()) {
                Ordering::Less => lo = mid,
                Ordering::Greater => hi = mid - 1,
                Ordering::Equal => return mid,
            }
        }
        (lo + hi + 1) / 2
    }
}

#[cfg(test)]
mod tests {
    use crate::kv::timestamped_key::TimestampedKey;

    use super::test_utils::build_sst;

    #[test]
    fn test_load_block_to_mem() {
        let mut sst = build_sst();
        let mut expected_block_data = vec![];
        expected_block_data.extend(sst.load_block_to_mem(0).unwrap().encode());
        expected_block_data.extend(sst.load_block_to_mem(1).unwrap().encode());
        let actual_block_data =
            &sst.file.get_contents_as_bytes().unwrap()[..expected_block_data.len()];
        assert_eq!(actual_block_data, expected_block_data);
    }

    #[test]
    fn test_get_block_index_for_key() {
        let sst = build_sst();
        assert_eq!(
            sst.get_block_index_for_key(&TimestampedKey::new("k1".as_bytes().into())),
            0
        );
        assert_eq!(
            sst.get_block_index_for_key(&TimestampedKey::new("k2".as_bytes().into())),
            0
        );
        assert_eq!(
            sst.get_block_index_for_key(&TimestampedKey::new("k3".as_bytes().into())),
            1
        );
    }
}
