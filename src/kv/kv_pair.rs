use bytes::Bytes;

use super::timestamped_key::TimestampedKey;

#[derive(Eq, Ord, PartialEq, PartialOrd, Clone, Debug)]
pub struct KeyValuePair {
    pub key: TimestampedKey,
    pub value: Bytes,
}