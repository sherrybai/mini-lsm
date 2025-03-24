use std::path::PathBuf;

pub struct StorageStateOptions {
    pub sst_max_size_bytes: usize,
    pub block_max_size_bytes: usize,
    pub block_cache_size_bytes: u64,
    pub path: PathBuf,
}