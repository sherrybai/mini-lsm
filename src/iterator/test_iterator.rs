use bytes::Bytes;

use crate::kv::{kv_pair::KeyValuePair, timestamped_key::TimestampedKey};

use super::StorageIterator;

pub struct TestIterator {
    is_valid: bool,
    is_valid_count: usize,
    kv: KeyValuePair,
}

impl TestIterator {
    pub fn new(id: usize, is_valid_count: usize) -> Self {
        let key = Bytes::copy_from_slice(format!("k{}", id).as_bytes());
        let value = Bytes::copy_from_slice(format!("v{}", id).as_bytes());
        let kv = KeyValuePair { key: TimestampedKey::new(key), value };
        Self {
            is_valid: is_valid_count > 0,
            is_valid_count,
            kv
        }
    }
}

impl StorageIterator for TestIterator {
    fn peek(&mut self) -> Option<KeyValuePair> {
        Some(self.kv.clone())
    }

    fn is_valid(&self) -> bool {
        self.is_valid
    }
}

impl Iterator for TestIterator {
    type Item = KeyValuePair;

    fn next(&mut self) -> Option<KeyValuePair> {
        if !self.is_valid {
            return None;
        }
        self.is_valid_count -= 1;
        if self.is_valid_count == 0 {
            self.is_valid = false;
        }
        Some(self.kv.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::TestIterator;

    #[test]
    fn test_test_iterator() {
        let test_iterator = TestIterator::new(0, 0);
        assert!(!test_iterator.is_valid);

        let mut test_iterator = TestIterator::new(0, 2);
        assert!(test_iterator.is_valid);
        assert_eq!(test_iterator.next().unwrap().key.get_key(), "k0".as_bytes());
        assert!(test_iterator.is_valid);
        assert_eq!(test_iterator.next().unwrap().key.get_key(), "k0".as_bytes());
        assert!(!test_iterator.is_valid);
    }
}
