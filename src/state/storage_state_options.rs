use std::{path::PathBuf, str::FromStr};
use anyhow::Result;

pub struct StorageStateOptions {
    pub sst_max_size_bytes: usize,
    pub block_max_size_bytes: usize,
    pub block_cache_size_bytes: u64,
    pub path: PathBuf,
    pub num_memtables_limit: usize,
}

impl StorageStateOptions {
    pub fn new_with_defaults() -> Result<StorageStateOptions> {
        Ok(StorageStateOptions { 
            sst_max_size_bytes: 2 << 20,  // 2MB
            block_max_size_bytes: 4096, 
            block_cache_size_bytes: 1 << 20,  // 1MB 
            path: PathBuf::from_str("lsm.db")?,
            num_memtables_limit: 3
        })
    }
}