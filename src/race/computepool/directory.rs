use std::mem::size_of;

use crate::cfg::config::CONFIG;

#[derive(Copy, Clone)]
pub struct ComputePoolEntry {
    pub data: u64,
}

impl ComputePoolEntry {
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
}

pub struct ComputePoolDirectory {
    pub global_depth: usize,
    pub entries: [ComputePoolEntry; CONFIG.max_entry_num],
}

impl ComputePoolDirectory {
    pub fn new() -> Self {
        ComputePoolDirectory {
            global_depth: 0,
            entries: [ComputePoolEntry { data: 0 }; CONFIG.max_entry_num],
        }
    }

    pub fn get_entry(&mut self, index: usize) -> &mut ComputePoolEntry {
        &mut self.entries[index]
    }

    pub fn get_entry_const(&self, index: usize) -> &ComputePoolEntry {
        &self.entries[index]
    }
}
