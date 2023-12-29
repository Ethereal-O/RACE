use std::mem::size_of;

use crate::{
    cfg::config::CONFIG,
    race::mempool::subtable::{self, CombinedBucket, Subtable},
};

#[derive(Copy, Clone)]
pub struct ClientEntry {
    pub data: u64,
}

impl ClientEntry {
    pub fn get_data(&self) -> u64 {
        self.data
    }

    pub fn set_data(&mut self, data: u64) {
        self.data = data;
    }

    pub fn set_subtable_and_localdepth(&mut self, subtable: u64, local_depth: u8) {
        self.set_subtable_pointer(subtable);
        self.set_local_depth(local_depth);
    }

    pub fn clear_lock_status(&mut self) {
        self.data = (self.data
            & !(0xFF
                << CONFIG.bits_of_byte
                    * (size_of::<u64>() - size_of::<u8>() - CONFIG.directory_lock_offset)))
    }

    pub fn get_locked_data(&self) -> u64 {
        (self.data
            & !(0xFF
                << CONFIG.bits_of_byte
                    * (size_of::<u64>() - size_of::<u8>() - CONFIG.directory_lock_offset)))
            | (1 << CONFIG.bits_of_byte
                * (size_of::<u64>() - size_of::<u8>() - CONFIG.directory_lock_offset))
    }

    pub fn get_local_depth(&self) -> u8 {
        (self.data
            >> CONFIG.bits_of_byte
                * (size_of::<u64>() - size_of::<u8>() - CONFIG.directory_localdepth_offset))
            as u8
    }

    pub fn get_subtable_pointer(&self) -> u64 {
        (self.data
            & !(0xFF
                << CONFIG.bits_of_byte
                    * (size_of::<u64>() - size_of::<u8>() - CONFIG.directory_lock_offset))
            & !(0xFF
                << CONFIG.bits_of_byte
                    * (size_of::<u64>() - size_of::<u8>() - CONFIG.directory_localdepth_offset)))
            as u64
    }

    pub fn set_local_depth(&mut self, depth: u8) {
        self.data = (self.data
            & !(0xFF
                << CONFIG.bits_of_byte
                    * (size_of::<u64>() - size_of::<u8>() - CONFIG.directory_localdepth_offset)))
            | ((depth as u64)
                << CONFIG.bits_of_byte
                    * (size_of::<u64>() - size_of::<u8>() - CONFIG.directory_localdepth_offset));
    }

    pub fn set_subtable_pointer(&mut self, subtable: u64) {
        self.data = (self.data
            & (0xFF
                << CONFIG.bits_of_byte
                    * (size_of::<u64>() - size_of::<u8>() - CONFIG.directory_lock_offset))
            & (0xFF
                << CONFIG.bits_of_byte
                    * (size_of::<u64>() - size_of::<u8>() - CONFIG.directory_localdepth_offset)))
            | subtable;
    }

    pub fn get_combined_buckets(
        &self,
        bucket_group1: usize,
        bucket_group2: usize,
    ) -> Option<[CombinedBucket; 2]> {
        if bucket_group1 >= CONFIG.bucket_group_num || bucket_group2 >= CONFIG.bucket_group_num {
            return None;
        }
        let subtable_pointer = self.get_subtable_pointer() as *const Subtable;
        unsafe {
            let cb1 = CombinedBucket {
                subtable: subtable_pointer,
                bucket_group: bucket_group1,
                main_bucket: (*subtable_pointer).bucket_groups[bucket_group1].buckets[0].clone(),
                overflow_bucket: (*subtable_pointer).bucket_groups[bucket_group1].buckets[1]
                    .clone(),
            };
            let cb2 = CombinedBucket {
                subtable: subtable_pointer,
                bucket_group: bucket_group2,
                main_bucket: (*subtable_pointer).bucket_groups[bucket_group2].buckets[2].clone(),
                overflow_bucket: (*subtable_pointer).bucket_groups[bucket_group2].buckets[1]
                    .clone(),
            };
            Some([cb1, cb2])
        }
    }
}

pub struct ClientDirectory {
    pub global_depth: u8,
    pub entries: [ClientEntry; CONFIG.max_entry_num],
}

impl ClientDirectory {
    pub fn new() -> Self {
        ClientDirectory {
            global_depth: 0,
            entries: [ClientEntry { data: 0 }; CONFIG.max_entry_num],
        }
    }

    pub fn get_entry(&mut self, index: usize) -> &mut ClientEntry {
        &mut self.entries[index]
    }

    pub fn get_entry_const(&self, index: usize) -> &ClientEntry {
        &self.entries[index]
    }
}
