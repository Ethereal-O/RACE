use crate::cfg::config::CONFIG;
use crate::numa::mm::memcpy;
use crate::numa::mm::MemoryManager;
use crc::{Crc, CRC_64_REDIS};
use std::mem::size_of;
use std::sync::{Arc, Mutex};

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
        key: &String,
        value: &String,
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
        let combined_string = key.to_owned() + value.to_owned().as_str();
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

    pub fn get(&self) -> Option<KVBlock> {
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
            .to_string()
        };
        let value = unsafe {
            std::mem::ManuallyDrop::new(String::from_raw_parts(
                data_pointer.wrapping_add(kl as usize),
                vl as usize,
                vl as usize,
            ))
            .to_string()
        };
        if checksum == Crc::<u64>::new(&CRC_64_REDIS).checksum(value.as_bytes()) {
            Some(KVBlock {
                klen: kl,
                vlen: vl,
                key,
                value,
                crc64: checksum,
            })
        } else {
            None
        }
    }

    pub fn get_total_length(&self) -> usize {
        size_of::<KVBlockMem>() + self.klen as usize + self.vlen as usize
    }
}
