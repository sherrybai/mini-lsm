use std::sync::Arc;

use crate::kv::kv_pair::KeyValuePair;
use crate::kv::timestamped_key::TimestampedKey;
use crate::table::builder::SSTBuilder;
use crate::table::Sst;

use tempfile::tempdir;

use super::block_cache::BlockCache;

pub fn set_up_builder() -> SSTBuilder {
    // build a test SST with two blocks
    // - block 0 contains k1 and k2
    // - block 1 contains k3
    let mut builder: SSTBuilder = SSTBuilder::new(25);
    // add three key-value pairs
    assert!(builder
        .add(KeyValuePair {
            key: TimestampedKey::new("k1".as_bytes().into()),
            value: "v1".as_bytes().into(),
        })
        .is_ok());
    assert!(builder
        .add(KeyValuePair {
            key: TimestampedKey::new("k2".as_bytes().into()),
            value: "v2".as_bytes().into(),
        })
        .is_ok());
    assert!(builder
        .add(KeyValuePair {
            key: TimestampedKey::new("k3".as_bytes().into()),
            value: "v3".as_bytes().into(),
        })
        .is_ok());
    builder
}

pub fn build_sst() -> Sst {
    let builder: SSTBuilder = set_up_builder();
    // build
    let dir = tempdir().unwrap();
    let path = dir.path().join("test_sst.sst");
    let sst = builder.build(0, path, None).unwrap();
    sst
}

pub fn build_sst_with_cache() -> (Sst, Arc<BlockCache>) {
    let builder: SSTBuilder = set_up_builder();
    let cache = Arc::new(BlockCache::new(50));
    // build
    let dir = tempdir().unwrap();
    let path = dir.path().join("test_sst.sst");
    let sst = builder.build(0, path, Some(cache.clone())).unwrap();
    (sst, cache)
}