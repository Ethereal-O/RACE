use crate::cfg::config::CONFIG;
use crate::numa::mm::memcpy;
use crate::numa::mm::MemoryManager;
use crate::race::common::hash::{Hash, HashMethod};
use crate::race::common::kvblock::KVBlock;
use crate::race::common::kvblock::KVBlockMem;
use std::clone;
use std::mem::size_of;
use std::sync::{atomic, Arc, Mutex};

pub struct SlotPos {
    pub subtable: *const Subtable,
    pub bucket_group: usize,
    pub bucket: usize,
    pub header: u64,
    pub slot: usize,
}

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

    pub fn get_by_key(&self, key: &String, fp: u8) -> Option<KVBlock> {
        if self.get_length() == 0 {
            return None;
        }
        if self.get_fingerprint() == fp {
            let kv_pointer = self.get_kv_pointer();
            let kv = unsafe { (*(kv_pointer as *mut KVBlockMem)).get() };
            if kv.key == *key {
                return Some(KVBlock {
                    klen: kv.klen,
                    vlen: kv.vlen,
                    key: kv.key,
                    value: kv.value,
                    crc64: kv.crc64,
                });
            }
        }
        return None;
    }

    pub fn judge_empty(&self) -> bool {
        self.get_length() == 0
    }

    pub fn get_kv_pointer(&self) -> *const KVBlockMem {
        (self.data
            & !(0xFF
                << CONFIG.bits_of_byte
                    * (size_of::<u64>() - size_of::<u8>() - CONFIG.slot_fp_offset))
            & !(0xFF
                << CONFIG.bits_of_byte
                    * (size_of::<u64>() - size_of::<u8>() - CONFIG.slot_len_offset)))
            as *const KVBlockMem
    }

    pub fn get_data(&self) -> u64 {
        self.data
    }

    pub fn get_atomic(&self) -> u64 {
        unsafe {
            std::mem::transmute::<&u64, &atomic::AtomicU64>(&self.data)
                .load(atomic::Ordering::SeqCst)
        }
    }

    pub fn get_kv(&self) -> Option<KVBlock> {
        let kv_pointer = self.get_kv_pointer();
        if kv_pointer == std::ptr::null_mut() {
            return None;
        }
        let kv = unsafe { (*(kv_pointer as *mut KVBlockMem)).get() };
        return Some(KVBlock {
            klen: kv.klen,
            vlen: kv.vlen,
            key: kv.key,
            value: kv.value,
            crc64: kv.crc64,
        });
    }
}

pub struct Header {
    pub data: u64,
}

impl Clone for Header {
    fn clone(&self) -> Self {
        Header {
            data: unsafe {
                std::mem::transmute::<&u64, &atomic::AtomicU64>(&self.data)
                    .load(atomic::Ordering::SeqCst)
            },
        }
    }
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

    pub fn get_data(&self) -> u64 {
        self.data
    }
}

#[derive(Clone)]
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
            } else {
                break;
            }
        }
        used_slot_num
    }

    pub fn set(&mut self, slot: usize, data: u64, old: u64) -> bool {
        self.slots[slot].compare_and_swap(data, old)
    }

    pub fn get(&self, slot: usize) -> u64 {
        self.slots[slot].get_atomic()
    }

    pub fn get_kv(&self, slot: usize) -> Option<KVBlock> {
        self.slots[slot].get_kv()
    }

    pub fn get_by_key(&self, key: &String, fp: u8) -> Option<KVBlock> {
        for slot in self.slots.iter() {
            if let Some(v) = slot.get_by_key(key, fp) {
                return Some(v);
            }
        }
        None
    }

    pub fn get_slot_and_data(&self, key: &String, fp: u8) -> Option<(usize, u64)> {
        let mut i = 0;
        for slot in self.slots.iter() {
            if let Some(v) = slot.get_by_key(key, fp) {
                return Some((i, slot.get_data()));
            }
            i += 1;
        }
        None
    }

    pub fn get_header(&self) -> u64 {
        self.header.get_data()
    }

    pub fn get_header_atomic(&self) -> Header {
        self.header.clone()
    }

    pub fn set_header(&mut self, local_depth: u8, suffix: u64) {
        self.header.set_local_depth(local_depth);
        self.header.set_suffix(suffix);
    }
}

pub struct CombinedBucket {
    pub subtable: *const Subtable,
    pub bucket_group: usize,
    pub main_bucket: Bucket,
    pub overflow_bucket: Bucket,
}

impl CombinedBucket {
    pub fn count(&self) -> usize {
        let main_bucket_size = self.main_bucket.get_used_slot_num();
        if main_bucket_size < CONFIG.slot_num {
            return main_bucket_size;
        } else {
            return main_bucket_size + self.overflow_bucket.get_used_slot_num();
        }
    }

    pub fn get_by_key(&self, key: &String) -> Option<KVBlock> {
        let string_to_key = Hash::hash(key, HashMethod::Directory);
        let fp = Hash::hash(key, HashMethod::FingerPrint) as u8;
        match self.main_bucket.get_by_key(key, fp) {
            Some(v) => Some(v),
            None => match self.overflow_bucket.get_by_key(key, fp) {
                Some(v) => Some(v),
                None => None,
            },
        }
    }

    pub fn get_slot_pos_and_data(&self, key: &String, hash_type: usize) -> Option<(SlotPos, u64)> {
        let string_to_key = Hash::hash(key, HashMethod::Directory);
        let fp = Hash::hash(key, HashMethod::FingerPrint) as u8;
        match self.main_bucket.get_slot_and_data(key, fp) {
            Some(sd) => {
                return Some((
                    SlotPos {
                        subtable: self.subtable,
                        bucket_group: self.bucket_group,
                        bucket: hash_type * 2,
                        header: self.main_bucket.get_header(),
                        slot: sd.0,
                    },
                    sd.1,
                ))
            }
            None => match self.overflow_bucket.get_slot_and_data(key, fp) {
                Some(sd) => {
                    return Some((
                        SlotPos {
                            subtable: self.subtable,
                            bucket_group: self.bucket_group,
                            bucket: 1,
                            header: self.overflow_bucket.get_header(),
                            slot: sd.0,
                        },
                        sd.1,
                    ))
                }
                None => None,
            },
        }
    }
}

pub struct BucketGroup {
    pub buckets: [Bucket; CONFIG.bucket_num],
}

impl BucketGroup {
    pub fn set(&mut self, bucket: usize, slot: usize, data: u64, old: u64) -> bool {
        self.buckets[bucket].set(slot, data, old)
    }

    pub fn get(&self, bucket: usize, slot: usize) -> u64 {
        self.buckets[bucket].get(slot)
    }

    pub fn get_kv(&self, bucket: usize, slot: usize) -> Option<KVBlock> {
        self.buckets[bucket].get_kv(slot)
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
    pub fn set(&mut self, slot_pos: &SlotPos, data: u64, old: u64) -> bool {
        self.bucket_groups[slot_pos.bucket_group].set(slot_pos.bucket, slot_pos.slot, data, old)
    }

    pub fn get(&self, slot_pos: &SlotPos) -> u64 {
        self.bucket_groups[slot_pos.bucket_group].get(slot_pos.bucket, slot_pos.slot)
    }

    pub fn get_kv(&self, slot_pos: &SlotPos) -> Option<KVBlock> {
        self.bucket_groups[slot_pos.bucket_group].get_kv(slot_pos.bucket, slot_pos.slot)
    }

    pub fn get_by_bucket_group_ids(
        &self,
        bucket_group1: usize,
        bucket_group2: usize,
    ) -> Option<[CombinedBucket; 2]> {
        if bucket_group1 >= CONFIG.bucket_group_num || bucket_group2 >= CONFIG.bucket_group_num {
            return None;
        }
        let cb1 = CombinedBucket {
            subtable: self as *const Subtable,
            bucket_group: bucket_group1,
            main_bucket: self.bucket_groups[bucket_group1].buckets[0].clone(),
            overflow_bucket: self.bucket_groups[bucket_group1].buckets[1].clone(),
        };
        let cb2 = CombinedBucket {
            subtable: self as *const Subtable,
            bucket_group: bucket_group2,
            main_bucket: self.bucket_groups[bucket_group2].buckets[2].clone(),
            overflow_bucket: self.bucket_groups[bucket_group2].buckets[1].clone(),
        };
        Some([cb1, cb2])
    }

    pub fn set_header(&mut self, local_depth: u8, suffix: u64) {
        for bucket_group in self.bucket_groups.iter_mut() {
            bucket_group.set_header(local_depth, suffix);
        }
    }

    pub fn get_bucket_header_atomic(&self, bucket_group: usize, bucket: usize) -> Header {
        self.bucket_groups[bucket_group].buckets[bucket].get_header_atomic()
    }
}
