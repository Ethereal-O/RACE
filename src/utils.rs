use crate::CONFIG;
use crate::{Bucket, KVBlockMem};
use std::mem::size_of;

pub fn set_data(fp: u8, len: u8, ptr: u64) -> u64 {
    let mut data = 0 as u64;
    data = (data
        & (0xFF
            << CONFIG.bits_of_byte * (size_of::<u64>() - size_of::<u8>() - CONFIG.slot_fp_offset))
        & (0xFF
            << CONFIG.bits_of_byte
                * (size_of::<u64>() - size_of::<u8>() - CONFIG.slot_len_offset)))
        | ptr;

    data = (data
        & !(0xFF
            << CONFIG.bits_of_byte * (size_of::<u64>() - size_of::<u8>() - CONFIG.slot_fp_offset)))
        | ((fp as u64)
            << CONFIG.bits_of_byte * (size_of::<u64>() - size_of::<u8>() - CONFIG.slot_fp_offset));

    data = (data
        & !(0xFF
            << CONFIG.bits_of_byte
                * (size_of::<u64>() - size_of::<u8>() - CONFIG.slot_len_offset)))
        | ((len as u64)
            << CONFIG.bits_of_byte * (size_of::<u64>() - size_of::<u8>() - CONFIG.slot_len_offset));
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
