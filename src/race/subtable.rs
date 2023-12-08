use crate::cfg::config::CONFIG;
use crate::numa::mm::memcpy;
use crate::numa::mm::MemoryManager;
use crate::race::hash::Hash;
use crate::race::kvblock::{KVBlock, KVBlockMem};
use std::mem::size_of;
use std::sync::{atomic, Arc, Mutex};

#[derive(Clone)]
pub struct Slot {
    pub data: u64,
}

impl Slot {
    pub fn compare_and_swap(&mut self, data: u64) -> bool {
        unsafe {
            match std::mem::transmute::<&u64, &atomic::AtomicU64>(&self.data).compare_exchange(
                0,
                data,
                atomic::Ordering::SeqCst,
                atomic::Ordering::SeqCst,
            ) {
                Ok(_) => true,
                Err(_) => false,
            }
        }
    }
    // pub fn set_all(
    //     &mut self,
    //     memory_manager: Arc<Mutex<MemoryManager>>,
    //     key: &String,
    //     value: &String,
    // ) {
    //     let kvblock_pointer = KVBlockMem::new(key, value, memory_manager);
    //     self.set_kv_pointer(kvblock_pointer as u64);
    //     self.set_fingerprint(Hash::hash(&key, 3) as u8);
    //     unsafe {
    //         self.set_length((*(kvblock_pointer as *mut KVBlockMem)).get_total_length() as u8);
    //     }
    // }

    pub fn get_fingerprint(&self) -> u8 {
        (self.data
            >> CONFIG.bits_of_byte * (size_of::<u64>() - size_of::<u8>() - CONFIG.slot_fp_offset))
            as u8
    }

    // pub fn set_fingerprint(&mut self, fp: u8) {
    //     self.data = (self.data
    //         & !(0xFF
    //             << CONFIG.bits_of_byte
    //                 * (size_of::<u64>() - size_of::<u8>() - CONFIG.slot_fp_offset)))
    //         | ((fp as u64)
    //             << CONFIG.bits_of_byte
    //                 * (size_of::<u64>() - size_of::<u8>() - CONFIG.slot_fp_offset));
    // }

    pub fn get_length(&self) -> u8 {
        (self.data
            >> CONFIG.bits_of_byte * (size_of::<u64>() - size_of::<u8>() - CONFIG.slot_len_offset))
            as u8
    }

    // pub fn set_length(&mut self, len: u8) {
    //     self.data = (self.data
    //         & !(0xFF
    //             << CONFIG.bits_of_byte
    //                 * (size_of::<u64>() - size_of::<u8>() - CONFIG.slot_len_offset)))
    //         | ((len as u64)
    //             << CONFIG.bits_of_byte
    //                 * (size_of::<u64>() - size_of::<u8>() - CONFIG.slot_len_offset));
    // }

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

    // pub fn set_kv_pointer(&mut self, ptr: u64) {
    //     self.data = (self.data
    //         & (0xFF
    //             << CONFIG.bits_of_byte
    //                 * (size_of::<u64>() - size_of::<u8>() - CONFIG.slot_fp_offset))
    //         & (0xFF
    //             << CONFIG.bits_of_byte
    //                 * (size_of::<u64>() - size_of::<u8>() - CONFIG.slot_len_offset)))
    //         | ptr;
    // }
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

    pub fn set(&mut self, slot: usize, data: u64) -> bool {
        // let used_slot_num = self.get_used_slot_num();
        // if used_slot_num == CONFIG.slot_num {
        //     return false;
        // }
        // self.slots[used_slot_num].set_all(memory_manager, key, value);
        // true
        self.slots[slot].compare_and_swap(data)
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

pub struct BucketGroup {
    pub buckets: [Bucket; CONFIG.bucket_num],
}

impl BucketGroup {
    pub fn set(&mut self, bucket: usize, slot: usize, data: u64) -> bool {
        // let mut try_add_result = false;
        // match method {
        //     1 => try_add_result = self.buckets[0].add_slot(memory_manager.clone(), key, value),
        //     2 => try_add_result = self.buckets[2].add_slot(memory_manager.clone(), key, value),
        //     _ => panic!("method error"),
        // }
        // if !try_add_result {
        //     try_add_result = self.buckets[1].add_slot(memory_manager.clone(), key, value);
        // }
        // try_add_result
        self.buckets[bucket].set(slot, data)
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
    pub fn set(&mut self, bucket_group: usize, bucket: usize, slot: usize, data: u64) -> bool {
        // let mut try_add_result = false;
        // try_add_result = self.bucket_groups[Hash::hash(key, 1) as usize].add_slot(
        //     1,
        //     memory_manager.clone(),
        //     key,
        //     value,
        // );
        // if !try_add_result {
        //     try_add_result = self.bucket_groups[Hash::hash(key, 2) as usize].add_slot(
        //         2,
        //         memory_manager.clone(),
        //         key,
        //         value,
        //     );
        // }
        // try_add_result
        self.bucket_groups[bucket_group].set(bucket, slot, data)
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
