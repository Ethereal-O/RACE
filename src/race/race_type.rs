use super::super::cfg::config::CONFIG;
use super::super::numa::mm::MEMORY_MANAGER;
use std::mem::size_of;

pub struct Slot {
    data: u64,
}

impl Slot {
    pub fn get_fingerprint(&self) -> u8 {
        (self.data >> size_of::<u64>() - size_of::<u8>() - CONFIG.slot_fp_offset) as u8
    }

    pub fn set_fingerprint(&mut self, fp: u8) {
        self.data = (self.data
            & !(0xFF << size_of::<u64>() - size_of::<u8>() - CONFIG.slot_fp_offset))
            | ((fp as u64) << size_of::<u64>() - size_of::<u8>() - CONFIG.slot_fp_offset);
    }

    pub fn get_length(&self) -> u8 {
        (self.data >> size_of::<u64>() - size_of::<u8>() - CONFIG.slot_len_offset) as u8
    }

    pub fn set_length(&mut self, len: u8) {
        self.data = (self.data
            & !(0xFF << size_of::<u64>() - size_of::<u8>() - CONFIG.slot_len_offset))
            | ((len as u64) << size_of::<u64>() - size_of::<u8>() - CONFIG.slot_len_offset);
    }

    pub fn get_pointer(&self) -> u64 {
        (self.data
            & !(0xFF << size_of::<u64>() - size_of::<u8>() - CONFIG.slot_fp_offset)
            & !(0xFF << size_of::<u64>() - size_of::<u8>() - CONFIG.slot_len_offset)) as u64
    }

    pub fn set_pointer(&mut self, ptr: u64) {
        self.data = (self.data
            & (0xFF << size_of::<u64>() - size_of::<u8>() - CONFIG.slot_fp_offset)
            & (0xFF << size_of::<u64>() - size_of::<u8>() - CONFIG.slot_len_offset))
            | ptr;
    }
}

pub struct Header {
    data: u64,
}

impl Header {
    pub fn get_local_depth(&self) -> u8 {
        (self.data >> size_of::<u64>() - size_of::<u8>() - CONFIG.header_local_depth_offset) as u8
    }

    pub fn set_local_depth(&mut self, depth: u8) {
        self.data = (self.data
            & !(0xFF << size_of::<u64>() - size_of::<u8>() - CONFIG.header_local_depth_offset))
            | ((depth as u64)
                << size_of::<u64>() - size_of::<u8>() - CONFIG.header_local_depth_offset);
    }

    pub fn get_suffix(&self) -> u8 {
        (self.data >> size_of::<u64>() - size_of::<u8>() - CONFIG.header_suffix_offset) as u8
    }

    pub fn set_suffix(&mut self, suffix: u8) {
        self.data = (self.data
            & !(0xFF << size_of::<u64>() - size_of::<u8>() - CONFIG.header_suffix_offset))
            | ((suffix as u64) << size_of::<u64>() - size_of::<u8>() - CONFIG.header_suffix_offset);
    }
}

pub struct Bucket {
    pub header: Slot,
    pub slots: [Slot; CONFIG.slot_num],
}

pub struct BucketGroup {
    pub buckets: [Bucket; CONFIG.bucket_num],
}

pub struct Subtable {
    pub bucket_groups: [BucketGroup; CONFIG.bucket_group_num],
}

pub struct Directory {
    pub data: u64,
}

impl Directory {
    pub fn new() -> Self {
        let pointer = MEMORY_MANAGER.lock().unwrap().malloc(size_of::<Subtable>());
        let directory = Directory { data: 0 };
        directory.set_subtable(pointer as u64);
        directory
    }

    pub fn get_lock(&self) -> u8 {
        (self.data >> size_of::<u64>() - size_of::<u8>() - CONFIG.directory_lock_offset) as u8
    }

    pub fn set_lock(&mut self, lock: u8) {
        self.data = (self.data
            & !(0xFF << size_of::<u64>() - size_of::<u8>() - CONFIG.directory_lock_offset))
            | ((lock as u64) << size_of::<u64>() - size_of::<u8>() - CONFIG.directory_lock_offset);
    }

    pub fn get_local_depth(&self) -> u8 {
        (self.data >> size_of::<u64>() - size_of::<u8>() - CONFIG.directory_localdepth_offset) as u8
    }

    pub fn set_local_depth(&mut self, depth: u8) {
        self.data = (self.data
            & !(0xFF << size_of::<u64>() - size_of::<u8>() - CONFIG.directory_localdepth_offset))
            | ((depth as u64)
                << size_of::<u64>() - size_of::<u8>() - CONFIG.directory_localdepth_offset);
    }

    pub fn get_subtable(&self) -> u64 {
        (self.data
            & !(0xFF << size_of::<u64>() - size_of::<u8>() - CONFIG.directory_lock_offset)
            & !(0xFF << size_of::<u64>() - size_of::<u8>() - CONFIG.directory_localdepth_offset))
            as u64
    }

    pub fn set_subtable(&mut self, subtable: u64) {
        self.data = (self.data
            & (0xFF << size_of::<u64>() - size_of::<u8>() - CONFIG.directory_lock_offset)
            & (0xFF << size_of::<u64>() - size_of::<u8>() - CONFIG.directory_localdepth_offset))
            | subtable;
    }
}

pub struct Directories {
    pub sub_dirs: Vec<Directory>,
}

impl Directories {
    pub fn new() -> Self {
        let mut sub_dirs = Vec::new();
        Directories { sub_dirs }
    }

    pub fn get_directory(&self, index: usize) -> &Directory {
        &self.sub_dirs[index]
    }
}
