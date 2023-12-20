use crate::CONFIG;
use crate::{Bucket, KVBlockMem};
use std::mem::size_of;

pub struct RaceUtils {}

impl RaceUtils {
    pub fn restrict_suffix_to(suffix: u64, local_depth: u8) -> u64 {
        let mask = (1 << (local_depth)) - 1;
        suffix & mask
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

    pub fn set_data(fp: u8, len: u8, ptr: u64) -> u64 {
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

    pub fn check_bucket(bucket: &Bucket, fp: u8, key: &String) -> bool {
        for slot in bucket.slots.iter() {
            if slot.data == 0 {
                break;
            } else {
                if slot.get_fingerprint() == fp {
                    let kv_pointer = slot.get_kv_pointer();
                    let kv = unsafe { (*(kv_pointer as *mut KVBlockMem)).get() };
                    if kv.key == *key {
                        println!("{:?}", Some(kv.value));
                        return true;
                    }
                }
            }
        }
        false
    }
}
