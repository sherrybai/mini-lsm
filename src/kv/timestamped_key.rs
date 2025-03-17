use bytes::Bytes;

#[derive(Eq, PartialEq, Clone, Debug)]
pub struct TimestampedKey {
    key: Bytes,
    timestamp_ms: usize,
}

impl TimestampedKey {
    pub fn new(key: Bytes) -> Self {
        TimestampedKey {
            key,
            timestamp_ms: 0, // TODO: set timestamp later
        }
    }

    pub fn get_key(&self) -> Bytes {
        self.key.clone()
    }
}

impl Ord for TimestampedKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // first compare keys lexicographically
        // if two keys are equal, then latest timestamp is smaller
        self.key
            .cmp(&other.key)
            .then(other.timestamp_ms.cmp(&self.timestamp_ms))
    }
}

impl PartialOrd for TimestampedKey {
    fn partial_cmp(&self, other: &TimestampedKey) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[cfg(test)]
mod tests {
    use super::TimestampedKey;

    #[test]
    fn test_ord() {
        let tk1 = TimestampedKey{key: "k1".into(), timestamp_ms: 100};
        let tk2 = TimestampedKey{key: "k1".into(), timestamp_ms: 0};
        let tk3 = TimestampedKey{key: "k2".into(), timestamp_ms: 100};

        assert!(tk1 < tk2);
        assert!(tk1 < tk3);
        assert!(tk2 < tk3);
    }
}
