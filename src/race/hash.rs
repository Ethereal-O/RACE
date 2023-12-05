use crate::cfg::config::CONFIG;
pub struct Hash {}

impl Hash {
    pub fn hash_1(key: &String, capicity: usize) -> u64 {
        let mut hash: u64 = 0;
        for i in 0..key.len() {
            hash = hash * 31 + key.as_bytes()[i] as u64;
        }
        hash = hash % capicity as u64;
        hash
    }

    pub fn hash_2(key: &String, capicity: usize) -> u64 {
        let mut hash: u64 = 0;
        for i in 0..key.len() {
            hash = hash * 131 + key.as_bytes()[i] as u64;
        }
        hash = hash % capicity as u64;
        hash
    }

    pub fn hash_3(key: &String, capicity: usize) -> u64 {
        let mut hash: u64 = 0;
        for i in 0..key.len() {
            hash = hash * 1313 + key.as_bytes()[i] as u64;
        }
        hash = hash % capicity as u64;
        hash
    }

    pub fn hash(key: &String, method: u8) -> u64 {
        match method {
            1 => Hash::hash_1(key, CONFIG.bucket_group_num),
            2 => Hash::hash_2(key, CONFIG.bucket_group_num),
            3 => Hash::hash_3(key, CONFIG.bits_of_byte * CONFIG.fp_size),
            _ => panic!("Invalid hash method!")
        }
    }
}
