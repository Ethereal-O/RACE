use crate::cfg::config::CONFIG;

pub enum HashMethod {
    CombinedBucket1,
    CombinedBucket2,
    FingerPrint,
    Directory,
}
pub struct Hash {}

impl Hash {
    pub fn hash_1(key: &String, capicity: usize) -> u64 {
        let mut hash: u64 = 0;
        for i in 0..key.len() {
            hash = hash % capicity as u64 * 31 + key.as_bytes()[i] as u64;
        }
        hash = hash % capicity as u64;
        hash
    }

    pub fn hash_2(key: &String, capicity: usize) -> u64 {
        let mut hash: u64 = 0;
        for i in 0..key.len() {
            hash = hash % capicity as u64 * 131 + key.as_bytes()[i] as u64;
        }
        hash = hash % capicity as u64;
        hash
    }

    pub fn hash_3(key: &String, capicity: usize) -> u64 {
        let mut hash: u64 = 0;
        for i in 0..key.len() {
            hash = hash % capicity as u64 * 1313 + key.as_bytes()[i] as u64;
        }
        hash = hash % capicity as u64;
        hash
    }

    pub fn hash_4(key: &String, capicity: usize) -> u64 {
        let mut hash: u64 = 0;
        for i in 0..key.len() {
            hash = hash % capicity as u64 * 13131 + key.as_bytes()[i] as u64;
        }
        hash = hash % capicity as u64;
        hash
    }

    pub fn hash(key: &String, method: HashMethod) -> u64 {
        match method {
            HashMethod::CombinedBucket1 => Hash::hash_1(key, CONFIG.bucket_group_num),
            HashMethod::CombinedBucket2 => Hash::hash_2(key, CONFIG.bucket_group_num),
            HashMethod::FingerPrint => Hash::hash_3(key, CONFIG.bits_of_byte * CONFIG.fp_size),
            HashMethod::Directory => Hash::hash_4(key, CONFIG.max_entry_num),
            _ => panic!("Invalid hash method!"),
        }
    }
}
