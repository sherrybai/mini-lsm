use crate::kv::kv_pair::KeyValuePair;
use crate::kv::timestamped_key::TimestampedKey;
use crate::table::builder::SSTBuilder;
use crate::table::SST;

use tempfile::tempdir;

pub fn build_sst() -> SST {
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
    // build
    let dir = tempdir().unwrap();
    let path = dir.path().join("test_sst_iterate.sst");
    let sst = builder.build(0, path, None).unwrap();
    sst
}