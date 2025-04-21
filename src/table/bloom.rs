use bitvec::{bitvec, vec::BitVec};
use xxhash_rust::xxh3::xxh3_64;

use crate::kv::timestamped_key::TimestampedKey;

const FALSE_POSITIVE_RATE: f64 = 0.01;

pub struct BloomFilter {
    bit_vec: BitVec,
    m: usize,
    k: usize
}

impl BloomFilter {
    pub fn from_keys(keys: Vec<&TimestampedKey>) -> Self {
        let n = keys.len();
        let m = Self::get_bit_arr_len(n);
        let k = Self::get_num_hash_functions(m, n);

        let mut bit_vec = bitvec![0; m];

        // set bits for each key
        for key in keys {
            let indices = Self::get_indices_for_key(key, m, k);
            for i in indices {
                bit_vec.set(i, true);
            }
        }
        Self { bit_vec, m, k }
    }

    fn get_bit_arr_len(n: usize) -> usize {
        (
            -1.0 * (n as f64) * FALSE_POSITIVE_RATE.ln() / 
            std::f64::consts::LN_2.powi(2)
        ).ceil() as usize
    }

    fn get_num_hash_functions(m: usize, n: usize) -> usize {
        (
            (m as f64) / (n as f64) * std::f64::consts::LN_2
        ).ceil() as usize
    }

    fn get_indices_for_key(key: &TimestampedKey, m: usize, k: usize) -> Vec<usize> {
        // hash the key
        let hash64 = xxh3_64(&key.get_key());
        let (h1, h2) = ((hash64 >> 32) as u32, hash64 as u32); 

        let mut indices: Vec<usize> = vec![];
        let mut km_hash = h1;
        for _ in 0..k {
            let index = km_hash % (m as u32);
            indices.push(index as usize);
            // Kirsch-Mitzenmacher optimization: hash_i = hash1 + i * hash2
            km_hash = km_hash.wrapping_add(h2);
        }
        indices
    }

    pub fn maybe_contains(&self, key: &TimestampedKey) -> bool {
        let indices = Self::get_indices_for_key(key, self.m, self.k);
        for i in indices {
            if !self.bit_vec[i] {
                return false;
            }
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use crate::kv::timestamped_key::TimestampedKey;

    use super::BloomFilter;

    #[test]
    fn test_build_from_keys() {
        let k1 = TimestampedKey::new("hello".as_bytes().into());
        let k2 = TimestampedKey::new("world".as_bytes().into());
        let bloom_filter = BloomFilter::from_keys(
            vec![&k1, &k2],
        );

        // verify with https://hur.st/bloomfilter/?n=2&p=0.01&m=&k=
        assert_eq!(bloom_filter.m, 20);
        assert_eq!(bloom_filter.k, 7);
        assert_eq!(bloom_filter.bit_vec.len(), 20);

        assert!(bloom_filter.maybe_contains(&k1));
        assert!(bloom_filter.maybe_contains(&k2));

        let k3 = TimestampedKey::new("not here".as_bytes().into());
        assert!(!bloom_filter.maybe_contains(&k3));
    }
}