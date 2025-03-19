use crate::block::metadata::BlockMetadata;
use crate::table::file::File;

pub mod builder;
pub mod iterator;
pub mod file;

// in-memory representation of a single SST file on disk
pub struct SST {
    id: usize,
    file: File,
    meta_blocks: Vec<BlockMetadata>,
    meta_block_offset: u32,
}

impl SST {
    pub fn new(id: usize, file: File) -> Self {
        Self {
            id,
            file,
            meta_blocks: Vec::new(),
            meta_block_offset: 0,
        }
    }
}

#[cfg(test)]
mod tests {
}