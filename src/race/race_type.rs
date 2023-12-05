use crate::numa::mm::memcpy;

use super::super::cfg::config::CONFIG;
use super::super::numa::mm::MemoryManager;
use crc::{Crc, CRC_64_REDIS};
use std::mem::size_of;
use std::sync::{Arc, Mutex};
use std::time::Instant;

pub struct Slot {
    pub data: u64,
}

impl Slot {
    pub fn get_fingerprint(&self) -> u8 {
        (self.data
            >> CONFIG.bits_of_byte * (size_of::<u64>() - size_of::<u8>() - CONFIG.slot_fp_offset))
            as u8
    }

    pub fn set_fingerprint(&mut self, fp: u8) {
        self.data = (self.data
            & !(0xFF
                << CONFIG.bits_of_byte
                    * (size_of::<u64>() - size_of::<u8>() - CONFIG.slot_fp_offset)))
            | ((fp as u64)
                << CONFIG.bits_of_byte
                    * (size_of::<u64>() - size_of::<u8>() - CONFIG.slot_fp_offset));
    }

    pub fn get_length(&self) -> u8 {
        (self.data
            >> CONFIG.bits_of_byte * (size_of::<u64>() - size_of::<u8>() - CONFIG.slot_len_offset))
            as u8
    }

    pub fn set_length(&mut self, len: u8) {
        self.data = (self.data
            & !(0xFF
                << CONFIG.bits_of_byte
                    * (size_of::<u64>() - size_of::<u8>() - CONFIG.slot_len_offset)))
            | ((len as u64)
                << CONFIG.bits_of_byte
                    * (size_of::<u64>() - size_of::<u8>() - CONFIG.slot_len_offset));
    }

    pub fn get_pointer(&self) -> u64 {
        (self.data
            & !(0xFF
                << CONFIG.bits_of_byte
                    * (size_of::<u64>() - size_of::<u8>() - CONFIG.slot_fp_offset))
            & !(0xFF
                << CONFIG.bits_of_byte
                    * (size_of::<u64>() - size_of::<u8>() - CONFIG.slot_len_offset))) as u64
    }

    pub fn set_pointer(&mut self, ptr: u64) {
        self.data = (self.data
            & (0xFF
                << CONFIG.bits_of_byte
                    * (size_of::<u64>() - size_of::<u8>() - CONFIG.slot_fp_offset))
            & (0xFF
                << CONFIG.bits_of_byte
                    * (size_of::<u64>() - size_of::<u8>() - CONFIG.slot_len_offset)))
            | ptr;
    }
}

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

    pub fn get_suffix(&self) -> u8 {
        (self.data
            >> CONFIG.bits_of_byte
                * (size_of::<u64>() - size_of::<u8>() - CONFIG.header_suffix_offset)) as u8
    }

    pub fn set_suffix(&mut self, suffix: u8) {
        self.data = (self.data
            & !(0xFF
                << CONFIG.bits_of_byte
                    * (size_of::<u64>() - size_of::<u8>() - CONFIG.header_suffix_offset)))
            | ((suffix as u64)
                << CONFIG.bits_of_byte
                    * (size_of::<u64>() - size_of::<u8>() - CONFIG.header_suffix_offset));
    }
}

pub struct Bucket {
    pub header: Header,
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
    pub fn new(memory_manager: Arc<Mutex<MemoryManager>>) -> *mut Self {
        let subtable_pointer = memory_manager.lock().unwrap().malloc(size_of::<Subtable>());
        if subtable_pointer == std::ptr::null_mut() {
            panic!("malloc failed");
        }
        let directory_pointer = memory_manager
            .lock()
            .unwrap()
            .malloc(size_of::<Directory>());
        if directory_pointer == std::ptr::null_mut() {
            panic!("malloc failed");
        }
        unsafe {
            (*(directory_pointer as *mut Self)).set_subtable_pointer(subtable_pointer as u64);
        }
        directory_pointer as *mut Self
    }

    pub fn get_lock(&self) -> u8 {
        (self.data
            >> CONFIG.bits_of_byte
                * (size_of::<u64>() - size_of::<u8>() - CONFIG.directory_lock_offset)) as u8
    }

    pub fn set_lock(&mut self, lock: u8) {
        self.data = (self.data
            & !(0xFF
                << CONFIG.bits_of_byte
                    * (size_of::<u64>() - size_of::<u8>() - CONFIG.directory_lock_offset)))
            | ((lock as u64)
                << CONFIG.bits_of_byte
                    * (size_of::<u64>() - size_of::<u8>() - CONFIG.directory_lock_offset));
    }

    pub fn get_local_depth(&self) -> u8 {
        (self.data
            >> CONFIG.bits_of_byte
                * (size_of::<u64>() - size_of::<u8>() - CONFIG.directory_localdepth_offset))
            as u8
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

    pub fn get_subtable(&self) -> &mut Subtable {
        let subtable_pointer = self.get_subtable_pointer();
        unsafe { &mut *(subtable_pointer as *mut Subtable) }
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

pub struct Directories {
    pub sub_dirs: Vec<*mut Directory>,
}

impl Directories {
    pub fn new() -> Self {
        let sub_dirs = Vec::new();
        Directories { sub_dirs }
    }

    pub fn add_directory(&mut self, memory_manager: Arc<Mutex<MemoryManager>>) {
        self.sub_dirs.push(Directory::new(memory_manager));
    }

    pub fn deref_directories(&mut self) -> Vec<Directory> {
        let mut new_sub_dirs = Vec::new();
        for sub_dir in self.sub_dirs.iter() {
            new_sub_dirs.push(Directory {
                data: unsafe { (*(*sub_dir)).data },
            });
        }
        new_sub_dirs
    }

    pub fn get(&mut self, index: usize) -> &mut Directory {
        unsafe { &mut *(self.sub_dirs[index]) }
    }
}
#[derive(Debug)]
pub struct KVBlock {
    pub klen: u16,
    pub vlen: u16,
    pub key: String,
    pub value: String,
    pub crc64: u64,
}

pub struct KVBlockMem {
    klen: u16,
    vlen: u16,
    crc64: u64,
}

impl KVBlockMem {
    pub fn new(
        kl: u16,
        vl: u16,
        key: String,
        value: String,
        memory_manager: Arc<Mutex<MemoryManager>>,
    ) -> *const Self {
        let kvblock_pointer = memory_manager
            .lock()
            .unwrap()
            .malloc(size_of::<KVBlockMem>() + key.len() + value.len());
        if kvblock_pointer == std::ptr::null_mut() {
            panic!("kvblock malloc failed");
        }
        unsafe {
            (*(kvblock_pointer as *mut Self)).klen = key.len() as u16;
            (*(kvblock_pointer as *mut Self)).vlen = value.len() as u16;
        }
        let combined_string = key + &value;
        let checksum = Crc::<u64>::new(&CRC_64_REDIS).checksum(combined_string.as_bytes());
        unsafe {
            (*(kvblock_pointer as *mut Self)).crc64 = checksum;
        }
        let kv_pointer = kvblock_pointer.wrapping_add(size_of::<KVBlockMem>());
        unsafe {
            memcpy(kv_pointer, combined_string.as_ptr(), combined_string.len());
        }
        kvblock_pointer as *const Self
    }

    pub fn get(&self) -> KVBlock {
        let kl = self.klen;
        let vl = self.vlen;
        let checksum = self.crc64;
        let data_pointer = unsafe { std::mem::transmute::<&KVBlockMem, *mut u8>(self) }
            .wrapping_add(size_of::<KVBlockMem>());
        let key = unsafe {
            std::mem::ManuallyDrop::new(String::from_raw_parts(
                data_pointer,
                kl as usize,
                kl as usize,
            ))
        };
        let value = unsafe {
            std::mem::ManuallyDrop::new(String::from_raw_parts(
                data_pointer.wrapping_add(kl as usize),
                vl as usize,
                vl as usize,
            ))
        };
        KVBlock {
            klen: kl,
            vlen: vl,
            key: key.to_string(),
            value: value.to_string(),
            crc64: checksum,
        }
    }
}
