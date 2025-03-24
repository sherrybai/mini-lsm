use std::ops::Bound;

use crate::kv::timestamped_key::TimestampedKey;

pub fn range_overlap(
    query_lower: Bound<&[u8]>,
    query_upper: Bound<&[u8]>,
    target_lower: TimestampedKey,
    target_upper: TimestampedKey,
) -> bool {
    let disjoint_lesser = match query_upper {
        Bound::Included(upper) => { upper < target_lower.get_key() },
        Bound::Excluded(upper) => { upper <= target_lower.get_key() },
        Bound::Unbounded => { false }
    };
    let disjoint_greater = match query_lower {
        Bound::Included(lower) => { lower >= target_upper.get_key() },
        Bound::Excluded(lower) => { lower > target_upper.get_key() },
        Bound::Unbounded => { false }
    };
    !disjoint_lesser && !disjoint_greater
}

#[cfg(test)]
mod tests {
    use std::ops::Bound::{Excluded, Included};

    use crate::kv::timestamped_key::TimestampedKey;

    use super::range_overlap;

    #[test]
    fn test_range_overlap() {
        // partial overlap
        assert!(range_overlap(
            Included("k1".as_bytes()), 
            Included("k3".as_bytes()), 
            TimestampedKey::new("k0".as_bytes().into()), 
            TimestampedKey::new("k2".as_bytes().into())
        ));
        assert!(range_overlap(
            Included("k0".as_bytes()), 
            Included("k2".as_bytes()), 
            TimestampedKey::new("k1".as_bytes().into()), 
            TimestampedKey::new("k3".as_bytes().into())
        ));
        // complete overlap
        assert!(range_overlap(
            Included("k1".as_bytes()), 
            Included("k2".as_bytes()), 
            TimestampedKey::new("k0".as_bytes().into()), 
            TimestampedKey::new("k3".as_bytes().into())
        ));
        assert!(range_overlap(
            Included("k0".as_bytes()), 
            Included("k3".as_bytes()), 
            TimestampedKey::new("k1".as_bytes().into()), 
            TimestampedKey::new("k2".as_bytes().into())
        ));
        // disjoint ranges don't overlap
        assert!(!range_overlap(
            Included("k0".as_bytes()), 
            Included("k1".as_bytes()), 
            TimestampedKey::new("k2".as_bytes().into()), 
            TimestampedKey::new("k3".as_bytes().into())
        ));
        // excluded/included edge cases
        assert!(range_overlap(
            Included("k0".as_bytes()), 
            Included("k1".as_bytes()), 
            TimestampedKey::new("k1".as_bytes().into()), 
            TimestampedKey::new("k2".as_bytes().into())
        ));
        assert!(!range_overlap(
            Included("k0".as_bytes()), 
            Excluded("k1".as_bytes()), 
            TimestampedKey::new("k1".as_bytes().into()), 
            TimestampedKey::new("k2".as_bytes().into())
        ));
    }

}