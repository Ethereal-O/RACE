use crate::cfg::config::CONFIG;
use crate::numa::mm::memcpy;
use crate::numa::mm::MemoryManager;
use crate::race::common::hash::Hash;
use crate::race::common::kvblock::{KVBlock, KVBlockMem};
use std::mem::size_of;
use std::sync::{atomic, Arc, Mutex};

pub struct Slot {
    pub data: u64,
}

impl Clone for Slot {
    fn clone(&self) -> Self {
        Slot {
            data: unsafe {
                std::mem::transmute::<&u64, &atomic::AtomicU64>(&self.data)
                    .load(atomic::Ordering::SeqCst)
            },
        }
    }
}

impl Slot {
    pub fn compare_and_swap(&mut self, data: u64, old: u64) -> bool {
        unsafe {
            match std::mem::transmute::<&u64, &atomic::AtomicU64>(&self.data).compare_exchange(
                old,
                data,
                atomic::Ordering::SeqCst,
                atomic::Ordering::SeqCst,
            ) {
                Ok(_) => true,
                Err(_) => false,
            }
        }
    }

    pub fn get_fingerprint(&self) -> u8 {
        (self.data
            >> CONFIG.bits_of_byte * (size_of::<u64>() - size_of::<u8>() - CONFIG.slot_fp_offset))
            as u8
    }

    pub fn get_length(&self) -> u8 {
        (self.data
            >> CONFIG.bits_of_byte * (size_of::<u64>() - size_of::<u8>() - CONFIG.slot_len_offset))
            as u8
    }

    pub fn get_kv(&self) -> KVBlock {
        let kv_pointer = self.get_kv_pointer();
        unsafe { (*(kv_pointer as *mut KVBlockMem)).get() }
    }

    pub fn get_by_key(&self, key: &String) -> Option<String> {
        if self.get_length() == 0 {
            return None;
        }
        let kv_pointer = self.get_kv_pointer();
        let kv = unsafe { (*(kv_pointer as *mut KVBlockMem)).get() };
        if kv.key == *key {
            return Some(kv.value);
        }
        return None;
    }

    pub fn judge_empty(&self) -> bool {
        self.get_length() == 0
    }

    pub fn get_kv_pointer(&self) -> u64 {
        (self.data
            & !(0xFF
                << CONFIG.bits_of_byte
                    * (size_of::<u64>() - size_of::<u8>() - CONFIG.slot_fp_offset))
            & !(0xFF
                << CONFIG.bits_of_byte
                    * (size_of::<u64>() - size_of::<u8>() - CONFIG.slot_len_offset))) as u64
    }
}

#[derive(Clone)]
pub struct Header {
    pub data: u64,
}

impl Header {
    pub fn get_local_depth(&self) -> u8 {
        (self.data
            >> CONFIG.bits_of_byte
                * (size_of::<u64>() - size_of::<u8>() - CONFIG.header_local_depth_offset))
            as u8
    }

    pub fn set_local_depth(&mut self, depth: u8) {
        self.data = (self.data
            & !(0xFF
                << CONFIG.bits_of_byte
                    * (size_of::<u64>() - size_of::<u8>() - CONFIG.header_local_depth_offset)))
            | ((depth as u64)
                << CONFIG.bits_of_byte
                    * (size_of::<u64>() - size_of::<u8>() - CONFIG.header_local_depth_offset));
    }

    pub fn get_suffix(&self) -> u64 {
        (self.data
            & !(0xFF
                << CONFIG.bits_of_byte
                    * (size_of::<u64>() - size_of::<u8>() - CONFIG.header_local_depth_offset)))
            as u64
    }

    pub fn set_suffix(&mut self, suffix: u64) {
        self.data = (self.data
            & (0xFF
                << CONFIG.bits_of_byte
                    * (size_of::<u64>() - size_of::<u8>() - CONFIG.header_local_depth_offset)))
            | suffix;
    }
}

pub struct Bucket {
    pub header: Header,
    pub slots: [Slot; CONFIG.slot_num],
}

impl Bucket {
    pub fn get_used_slot_num(&self) -> usize {
        let mut used_slot_num = 0;
        for slot in self.slots.iter() {
            if !slot.judge_empty() {
                used_slot_num += 1;
            }
        }
        used_slot_num
    }

    pub fn set(&mut self, slot: usize, data: u64, old: u64) -> bool {
        self.slots[slot].compare_and_swap(data, old)
    }

    pub fn get_by_key(&self, key: &String) -> Option<String> {
        for slot in self.slots.iter() {
            let value = slot.get_by_key(key);
            if value.is_some() {
                return value;
            }
        }
        None
    }

    pub fn set_header(&mut self, local_depth: u8, suffix: u64) {
        self.header.set_local_depth(local_depth);
        self.header.set_suffix(suffix);
    }

    pub fn clone(&self) -> Bucket {
        Bucket {
            header: self.header.clone(),
            slots: self.slots.clone(),
        }
    }
}

pub struct CombinedBucket {
    pub main_bucket: Bucket,
    pub overflow_bucket: Bucket,
}

impl CombinedBucket {
    pub fn check_and_count(&self, key: &String, fp: u8) -> (bool, usize, u64) {
        let mut pos = 0 as usize;
        for slot in self.main_bucket.slots.iter() {
            if slot.data == 0 {
                return (false, pos, 0);
            } else {
                if slot.get_fingerprint() == fp {
                    let kv_pointer = slot.get_kv_pointer();
                    let kv = unsafe { (*(kv_pointer as *mut KVBlockMem)).get() };
                    if kv.key == *key {
                        return (true, pos, slot.data);
                    }
                }
                pos += 1;
            }
        }
        for slot in self.overflow_bucket.slots.iter() {
            if slot.data == 0 {
                return (false, pos, 0);
            } else {
                if slot.get_fingerprint() == fp {
                    let kv_pointer = slot.get_kv_pointer();
                    let kv = unsafe { (*(kv_pointer as *mut KVBlockMem)).get() };
                    if kv.key == *key {
                        return (true, pos, slot.data);
                    }
                }
                pos += 1;
            }
        }
        (false, pos, 0)
    }
}

pub struct BucketGroup {
    pub buckets: [Bucket; CONFIG.bucket_num],
}

impl BucketGroup {
    pub fn set(&mut self, bucket: usize, slot: usize, data: u64, old: u64) -> bool {
        self.buckets[bucket].set(slot, data, old)
    }

    pub fn get_by_key(&self, key: &String, method: u8) -> Option<String> {
        let mut value = None;
        match method {
            1 => value = self.buckets[0].get_by_key(key),
            2 => value = self.buckets[2].get_by_key(key),
            _ => panic!("method error"),
        }
        if value.is_none() {
            value = self.buckets[1].get_by_key(key);
        }
        value
    }

    pub fn set_header(&mut self, local_depth: u8, suffix: u64) {
        for bucket in self.buckets.iter_mut() {
            bucket.set_header(local_depth, suffix);
        }
    }
}

pub struct Subtable {
    pub bucket_groups: [BucketGroup; CONFIG.bucket_group_num],
}

impl Subtable {
    pub fn set(
        &mut self,
        bucket_group: usize,
        bucket: usize,
        slot: usize,
        data: u64,
        old: u64,
    ) -> bool {
        self.bucket_groups[bucket_group].set(bucket, slot, data, old)
    }

    pub fn get_by_bucket_ids(&self, bucket1: usize, bucket2: usize) -> Option<[CombinedBucket; 2]> {
        if bucket1 >= 2 * CONFIG.bucket_group_num || bucket2 >= 2 * CONFIG.bucket_group_num {
            return None;
        }
        let cb1 = CombinedBucket {
            main_bucket: self.bucket_groups[bucket1].buckets[0].clone(),
            overflow_bucket: self.bucket_groups[bucket1].buckets[1].clone(),
        };
        let cb2 = CombinedBucket {
            main_bucket: self.bucket_groups[bucket2].buckets[2].clone(),
            overflow_bucket: self.bucket_groups[bucket2].buckets[1].clone(),
        };
        Some([cb1, cb2])
    }

    pub fn set_header(&mut self, local_depth: u8, suffix: u64) {
        for bucket_group in self.bucket_groups.iter_mut() {
            bucket_group.set_header(local_depth, suffix);
        }
    }
}
