use std::cmp::Ordering;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use block_cache::BlockCache;
use bloom::BloomFilter;

use crate::block::metadata::BlockMetadata;
use crate::block::Block;
use crate::kv::timestamped_key::TimestampedKey;
use crate::table::file::File;

#[cfg(test)]
mod test_utils;

pub mod block_cache;
pub mod bloom;
pub mod builder;
pub mod file;
pub mod iterator;

// in-memory representation of a single SST file on disk
pub struct Sst {
    id: usize,
    file: File,
    meta_blocks: Vec<BlockMetadata>,
    meta_block_offset: u32,
    block_cache: Option<Arc<BlockCache>>,
    bloom_filter: BloomFilter,
}

impl Sst {
    pub fn new(
        id: usize,
        file: File,
        meta_blocks: Vec<BlockMetadata>,
        meta_block_offset: u32,
        block_cache: Option<Arc<BlockCache>>,
        bloom_filter: BloomFilter,
    ) -> Self {
        Self {
            id,
            file,
            meta_blocks,
            meta_block_offset,
            block_cache,
            bloom_filter,
        }
    }

    // create from file
    pub fn open(id: usize, path: PathBuf, block_cache: Option<Arc<BlockCache>>) -> Result<Self> {
        let mut file = File::open(path)?;
        let bloom_filter_offset = file.get_bloom_filter_offset()?;
        let bloom_filter = file.load_bloom_filter(bloom_filter_offset)?;
        let meta_block_offset = file.get_meta_block_offset(bloom_filter_offset)?;
        let meta_blocks = file.load_meta_blocks(meta_block_offset, bloom_filter_offset)?;
        Ok(Self::new(
            id,
            file,
            meta_blocks,
            meta_block_offset,
            block_cache,
            bloom_filter,
        ))
    }

    pub fn read_block(&self, block_index: usize) -> Result<Arc<Block>> {
        let offset = self.meta_blocks[block_index].get_offset();
        let next_block_index = block_index + 1;
        let next_offset = if self.meta_blocks.len() < next_block_index + 1 {
            self.meta_block_offset
        } else {
            self.meta_blocks[next_block_index].get_offset()
        };
        let block_size = next_offset - offset;
        let res = self.file.load_block_to_mem(offset, block_size)?;
        Ok(Arc::new(res))
    }

    fn read_block_cached(&self, block_index: usize) -> Result<Arc<Block>> {
        // attempt to read from cache first
        if let Some(cache) = &self.block_cache {
            let cache_res =
                cache.try_get_with((self.id, block_index), || self.read_block(block_index));
            match cache_res {
                Ok(res) => Ok(res),
                Err(err) => Err(anyhow!(err)),
            }
        } else {
            self.read_block(block_index)
        }
    }

    fn get_block_index_for_key(&self, key: &TimestampedKey) -> usize {
        let (mut lo, mut hi) = (0, self.meta_blocks.len() - 1);
        // seek to last block with first_key less than or equal to key
        while lo < hi {
            let mid = (lo + hi).div_ceil(2); // use right mid to avoid infinite loop
            let first_key = self.meta_blocks[mid].get_first_key();
            match first_key.cmp(key) {
                Ordering::Less => lo = mid,
                Ordering::Greater => hi = mid - 1,
                Ordering::Equal => return mid,
            }
        }
        (lo + hi).div_ceil(2)
    }

    pub fn get_id(&self) -> usize {
        self.id
    }

    pub fn get_first_key(&self) -> TimestampedKey {
        self.meta_blocks
            .first()
            .expect("sst must contain at least one block")
            .get_first_key()
    }

    pub fn get_last_key(&self) -> TimestampedKey {
        self.meta_blocks
            .last()
            .expect("sst must contain at least one block")
            .get_last_key()
    }

    pub fn maybe_contains_key(&self, key: &[u8]) -> bool {
        self.bloom_filter.maybe_contains(key)
            && self.get_first_key().get_key() <= key
            && key <= self.get_last_key().get_key()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::{
        block::Block, kv::timestamped_key::TimestampedKey, table::test_utils::build_sst_with_cache,
    };

    use super::test_utils::build_sst;

    #[test]
    fn test_read_block() {
        let mut sst = build_sst();
        let mut expected_block_data = vec![];
        expected_block_data.extend(sst.read_block(0).unwrap().encode());
        expected_block_data.extend(sst.read_block(1).unwrap().encode());
        let actual_block_data =
            &sst.file.get_contents_as_bytes().unwrap()[..expected_block_data.len()];
        assert_eq!(actual_block_data, expected_block_data);
    }

    #[test]
    fn test_read_block_cached() {
        let (sst, cache) = build_sst_with_cache();
        let cached_block = Arc::new(Block::new(vec![], vec![], 0));
        cache.insert((0, 0), cached_block.clone());

        let read_uncached = sst.read_block(0).unwrap();
        let read_cached = sst.read_block_cached(0).unwrap();
        assert_ne!(read_uncached, read_cached);
        assert_eq!(read_cached, cached_block);
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
