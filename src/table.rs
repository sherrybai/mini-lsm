use crate::block::metadata::BlockMetadata;
use crate::table::file::File;

pub mod builder;
pub mod file;
pub mod iterator;

// in-memory representation of a single SST file on disk
pub struct SST {
    id: usize,
    file: File,
    meta_blocks: Vec<BlockMetadata>,
    meta_block_offset: u32,
}

impl SST {
    pub fn new(
        id: usize,
        file: File,
        meta_blocks: Vec<BlockMetadata>,
        meta_block_offset: u32,
    ) -> Self {
        Self {
            id,
            file,
            meta_blocks,
            meta_block_offset,
        }
    }
}