use bytes::Bytes;

#[derive(Eq, Ord, PartialEq, PartialOrd, Clone, Debug)]
pub struct TimestampedKey {
    key: Bytes,
    timestamp_ms: usize,
}

impl TimestampedKey {
    pub fn new(key: Bytes) -> Self {
        TimestampedKey {
            key,
            timestamp_ms: 0,  // TODO: set timestamp later
        }
    }
}