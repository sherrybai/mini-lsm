use bitvec::{bitvec, field::BitField, order::Lsb0, vec::BitVec};
use bytes::Bytes;
use xxhash_rust::xxh3::xxh3_64;

use crate::kv::timestamped_key::TimestampedKey;

const FALSE_POSITIVE_RATE: f64 = 0.01;

pub struct BloomFilter {
    bit_vec: BitVec<u8>,
    k: u8
}

impl BloomFilter {
    pub fn from_keys(keys: Vec<TimestampedKey>) -> Self {
        let n = keys.len();
        let m = Self::get_bit_arr_len(n);
        let k = Self::get_num_hash_functions(m, n);

        let mut bit_vec = bitvec![u8, Lsb0; 0; m];

        // set bits for each key
        for key in keys {
            let indices = Self::get_indices_for_key(&key.get_key(), m, k);
            for i in indices {
                bit_vec.set(i, true);
            }
        }
        Self { bit_vec, k }
    }

    fn get_bit_arr_len(n: usize) -> usize {
        let m = (
            -1.0 * (n as f64) * FALSE_POSITIVE_RATE.ln() / 
            std::f64::consts::LN_2.powi(2)
        ).ceil() as usize;
        // pad to byte length
        8 * ((m as f64 / 8.0).ceil() as usize)
    }

    fn get_num_hash_functions(m: usize, n: usize) -> u8 {
        (
            (m as f64) / (n as f64) * std::f64::consts::LN_2
        ).round() as u8
    }

    fn get_indices_for_key(key: &[u8], m: usize, k: u8) -> Vec<usize> {
        // hash the key
        let hash64 = xxh3_64(key);
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

    pub fn maybe_contains(&self, key: &[u8]) -> bool {
        let indices = Self::get_indices_for_key(key, self.bit_vec.len(), self.k);
        for i in indices {
            if !self.bit_vec[i] {
                return false;
            }
        }
        true
    }

    pub fn encode(&mut self) -> Bytes {
        let mut bit_vec_bytes: Vec<u8> = self.bit_vec.chunks(8).map(
            |v| v.load::<u8>()
        ).collect();
        bit_vec_bytes.push(self.k);
        Bytes::from(bit_vec_bytes)
    }

    pub fn decode(encoded: Vec<u8>) -> Self {
        Self {
            bit_vec: BitVec::from_slice(&encoded[..encoded.len()-1]),
            k: *encoded.last().unwrap()
        }
    }
}

#[cfg(test)]
mod tests {
    use bitvec::{order::Lsb0, vec::BitVec};

    use crate::kv::timestamped_key::TimestampedKey;

    use super::BloomFilter;

    #[test]
    fn test_build_from_keys() {
        let k1 = TimestampedKey::new("hello".as_bytes().into());
        let k2 = TimestampedKey::new("world".as_bytes().into());
        let bloom_filter = BloomFilter::from_keys(
            vec![k1.clone(), k2.clone()],
        );

        // verify with 
        // https://hur.st/bloomfilter/?n=2&p=0.01&m=&k= -> optimal m is 20
        assert_eq!(bloom_filter.bit_vec.len(), 24); // 8 * ceil(20 / 8)
        // https://hur.st/bloomfilter/?n=2&p=&m=24&k=
        assert_eq!(bloom_filter.k, 8);

        assert!(bloom_filter.maybe_contains(&k1.get_key()));
        assert!(bloom_filter.maybe_contains(&k2.get_key()));
        assert!(!bloom_filter.maybe_contains("not here".as_bytes()));
    }

    #[test]
    fn test_encode_decode() {
        let k1 = TimestampedKey::new("hello".as_bytes().into());
        let k2 = TimestampedKey::new("world".as_bytes().into());
        let mut bloom_filter = BloomFilter::from_keys(
            vec![k1, k2],
        );
        let encoded = bloom_filter.encode();
        let k = *encoded.last().unwrap();
        assert_eq!(k, bloom_filter.k);
        assert_eq!(BitVec::<u8, Lsb0>::from_slice(&encoded[..encoded.len()-1]), bloom_filter.bit_vec);   

        let decoded = BloomFilter::decode(encoded.into());
        assert_eq!(decoded.bit_vec, bloom_filter.bit_vec);
        assert_eq!(decoded.k, bloom_filter.k); 
    }
}