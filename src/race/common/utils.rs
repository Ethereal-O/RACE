use crate::hash::{Hash, HashMethod};
use crate::race::mempool::subtable::{CombinedBucket, SlotPos};
use crate::CONFIG;
use crate::{Bucket, KVBlockMem};
use crc::{Crc, CRC_64_REDIS};
use std::mem::size_of;

pub struct RaceUtils {}

impl RaceUtils {
    pub fn get_suffix(key: &String, depth: u8) -> u64 {
        let hash_key = Hash::hash(key, HashMethod::Directory);
        let mask = (1 << (depth)) - 1;
        hash_key & mask
    }
    pub fn restrict_suffix_to(key: u64, local_depth: u8) -> u64 {
        let mask = (1 << (local_depth)) - 1;
        key & mask
    }

    pub fn add_bit_to_suffix(suffix: u64, index: u8) -> u64 {
        suffix | (1 << (index - 1))
    }

    pub fn plus_bit_to_suffix(suffix: u64, index: u8) -> u64 {
        suffix + (1 << (index - 1))
    }

    pub fn depth_to_size(depth: u8) -> usize {
        1 << depth
    }

    pub fn check_is_locked(data: u64) -> bool {
        (data
            >> CONFIG.bits_of_byte
                * (size_of::<u64>() - size_of::<u8>() - CONFIG.directory_lock_offset))
            > 0
    }

    pub fn get_new_suffix_from_old(old_index: u64, old_local_depth: u8) -> u64 {
        RaceUtils::restrict_suffix_to(
            RaceUtils::add_bit_to_suffix(old_index as u64, old_local_depth + 1),
            old_local_depth + 1,
        )
    }

    pub fn set_data(key: &String, val: &String, ptr: u64) -> u64 {
        let fp = Hash::hash(&key, HashMethod::FingerPrint) as u8;
        let mut len = size_of::<KVBlockMem>() + key.len() + val.len();
        if len > u8::MAX as usize {
            assert!(true, "size of kv is too big");
        }
        let mut data = 0 as u64;
        data = (data
            & (0xFF
                << CONFIG.bits_of_byte
                    * (size_of::<u64>() - size_of::<u8>() - CONFIG.slot_fp_offset))
            & (0xFF
                << CONFIG.bits_of_byte
                    * (size_of::<u64>() - size_of::<u8>() - CONFIG.slot_len_offset)))
            | ptr;

        data = (data
            & !(0xFF
                << CONFIG.bits_of_byte
                    * (size_of::<u64>() - size_of::<u8>() - CONFIG.slot_fp_offset)))
            | ((fp as u64)
                << CONFIG.bits_of_byte
                    * (size_of::<u64>() - size_of::<u8>() - CONFIG.slot_fp_offset));

        data = (data
            & !(0xFF
                << CONFIG.bits_of_byte
                    * (size_of::<u64>() - size_of::<u8>() - CONFIG.slot_len_offset)))
            | ((len as u64)
                << CONFIG.bits_of_byte
                    * (size_of::<u64>() - size_of::<u8>() - CONFIG.slot_len_offset));
        data
    }

    pub fn check_crc(key: &String, value: &String, checksum: u64) -> bool {
        let combined_string = key.clone() + value;
        checksum == Crc::<u64>::new(&CRC_64_REDIS).checksum(combined_string.as_bytes())
    }
}
