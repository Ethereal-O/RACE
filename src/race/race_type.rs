use crate::numa::mm::memcpy;

use crate::cfg::config::CONFIG;
use crate::numa::mm::MemoryManager;
use crate::race::hash::Hash;
use crc::{Crc, CRC_64_REDIS};
use std::mem::size_of;
use std::sync::{Arc, Mutex};
use std::time::Instant;

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

    pub fn get_total_length(&self) -> usize {
        size_of::<KVBlockMem>() + self.klen as usize + self.vlen as usize
    }
}

pub struct Slot {
    pub data: u64,
}

impl Slot {
    pub fn set_all(
        &mut self,
        memory_manager: Arc<Mutex<MemoryManager>>,
        key: &String,
        value: &String,
    ) {
        let kvblock_pointer = KVBlockMem::new(key, value, memory_manager);
        self.set_kv_pointer(kvblock_pointer as u64);
        self.set_fingerprint(Hash::hash(&key, 3) as u8);
        unsafe {
            self.set_length((*(kvblock_pointer as *mut KVBlockMem)).get_total_length() as u8);
        }
    }

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

    pub fn get_kv(&self) -> KVBlock {
        let kv_pointer = self.get_kv_pointer();
        unsafe { (*(kv_pointer as *mut KVBlockMem)).get() }
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

    pub fn set_kv_pointer(&mut self, ptr: u64) {
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

    pub fn add_slot(
        &mut self,
        memory_manager: Arc<Mutex<MemoryManager>>,
        key: &String,
        value: &String,
    ) -> bool {
        let used_slot_num = self.get_used_slot_num();
        if used_slot_num == CONFIG.slot_num {
            return false;
        }
        self.slots[used_slot_num].set_all(memory_manager, key, value);
        true
    }

    pub fn set_header(&mut self, local_depth: u8, suffix: u64) {
        self.header.set_local_depth(local_depth);
        self.header.set_suffix(suffix);
    }
}

pub struct BucketGroup {
    pub buckets: [Bucket; CONFIG.bucket_num],
}

impl BucketGroup {
    pub fn add_slot(
        &mut self,
        method: u8,
        memory_manager: Arc<Mutex<MemoryManager>>,
        key: &String,
        value: &String,
    ) -> bool {
        let mut try_add_result = false;
        match method {
            1 => try_add_result = self.buckets[0].add_slot(memory_manager.clone(), key, value),
            2 => try_add_result = self.buckets[2].add_slot(memory_manager.clone(), key, value),
            _ => panic!("method error"),
        }
        if !try_add_result {
            try_add_result = self.buckets[1].add_slot(memory_manager.clone(), key, value);
        }
        try_add_result
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
    pub fn add_slot(
        &mut self,
        memory_manager: Arc<Mutex<MemoryManager>>,
        key: &String,
        value: &String,
    ) -> bool {
        let mut try_add_result = false;
        try_add_result = self.bucket_groups[Hash::hash(key, 1) as usize].add_slot(
            1,
            memory_manager.clone(),
            key,
            value,
        );
        if !try_add_result {
            try_add_result = self.bucket_groups[Hash::hash(key, 2) as usize].add_slot(
                2,
                memory_manager.clone(),
                key,
                value,
            );
        }
        try_add_result
    }

    pub fn set_header(&mut self, local_depth: u8, suffix: u64) {
        for bucket_group in self.bucket_groups.iter_mut() {
            bucket_group.set_header(local_depth, suffix);
        }
    }
}

pub struct Directory {
    pub data: u64,
}

impl Directory {
    pub fn new(
        memory_manager: Arc<Mutex<MemoryManager>>,
        local_depth: u8,
        suffix: u64,
    ) -> *mut Self {
        let directory_pointer = memory_manager
            .lock()
            .unwrap()
            .malloc(size_of::<Directory>());
        if directory_pointer == std::ptr::null_mut() {
            panic!("malloc failed");
        }
        unsafe {
            (*(directory_pointer as *mut Self)).new_subtable(memory_manager, local_depth, suffix);
            (*(directory_pointer as *mut Self)).set_local_depth(local_depth);
        }
        directory_pointer as *mut Self
    }

    pub fn new_subtable(
        &mut self,
        memory_manager: Arc<Mutex<MemoryManager>>,
        local_depth: u8,
        suffix: u64,
    ) {
        let subtable_pointer = memory_manager.lock().unwrap().malloc(size_of::<Subtable>());
        if subtable_pointer == std::ptr::null_mut() {
            panic!("malloc failed");
        }
        unsafe {
            (*(self as *mut Self)).set_subtable_pointer(subtable_pointer as u64);
        }
        self.set_header(local_depth, suffix);
    }

    pub fn set_header_and_localdepth(&mut self, local_depth: u8, suffix: u64) {
        self.set_header(local_depth, suffix);
        self.set_local_depth(local_depth);
    }

    pub fn set_header(&mut self, local_depth: u8, suffix: u64) {
        self.get_subtable().set_header(local_depth, suffix);
    }

    pub fn copy_from(
        memory_manager: Arc<Mutex<MemoryManager>>,
        directory: &Directory,
    ) -> *mut Self {
        let directory_pointer = memory_manager
            .lock()
            .unwrap()
            .malloc(size_of::<Directory>());
        if directory_pointer == std::ptr::null_mut() {
            panic!("malloc failed");
        }
        unsafe {
            (*(directory_pointer as *mut Self))
                .set_subtable_pointer(directory.get_subtable_pointer());
            (*(directory_pointer as *mut Self)).set_local_depth(directory.get_local_depth());
        }
        directory_pointer as *mut Self
    }

    pub fn add_slot(
        &mut self,
        memory_manager: Arc<Mutex<MemoryManager>>,
        key: &String,
        value: &String,
    ) -> bool {
        self.get_subtable().add_slot(memory_manager, key, value)
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
    pub fn restrict_suffix_to(suffix: u64, local_depth: u8) -> u64 {
        let mask = (1 << (local_depth)) - 1;
        suffix & mask
    }

    pub fn add_bit_to_suffix(suffix: u64, local_depth: u8) -> u64 {
        suffix | 1 << (local_depth - 1)
    }

    pub fn get_new_suffix_from_old(old_index: u64, old_local_depth: u8) -> u64 {
        Directories::restrict_suffix_to(
            Directories::add_bit_to_suffix(old_index as u64, old_local_depth + 1),
            old_local_depth + 1,
        )
    }

    pub fn new(memory_manager: Arc<Mutex<MemoryManager>>) -> Self {
        let mut sub_dirs = Vec::new();
        sub_dirs.push(Directory::new(memory_manager, 0, 0));
        Directories { sub_dirs }
    }

    pub fn double_size(&mut self, memory_manager: Arc<Mutex<MemoryManager>>) {
        let old_size = self.sub_dirs.len();
        for index in 0..old_size {
            self.sub_dirs
                .push(Directory::copy_from(memory_manager.clone(), unsafe {
                    &*self.sub_dirs[index]
                }));
        }
    }

    pub fn double_size_with_new(
        &mut self,
        memory_manager: Arc<Mutex<MemoryManager>>,
        need_index: usize,
    ) {
        self.double_size(memory_manager.clone());
        // get real old index
        let old_index = Directories::restrict_suffix_to(
            need_index as u64,
            self.get(need_index).get_local_depth(),
        ) as usize;
        self.split_dir(memory_manager.clone(), old_index);
    }

    pub fn move_items(&mut self, old_index: usize, new_index: usize) {}

    pub fn change_dir_suffix_subtable(&mut self, local_depth: u8, suffix: u64) {
        let mut index = suffix as usize;
        while index < self.sub_dirs.len() {
            if Directories::restrict_suffix_to(index as u64, local_depth) == suffix {
                let new_pointer = self.get(suffix as usize).get_subtable_pointer();
                self.get(index).set_subtable_pointer(new_pointer);
                self.get(index).set_local_depth(local_depth);
            }
            index += 1;
        }
    }

    pub fn split_dir(&mut self, memory_manager: Arc<Mutex<MemoryManager>>, old_index: usize) {
        // get new index
        let new_index = Directories::get_new_suffix_from_old(
            old_index as u64,
            self.get(old_index).get_local_depth(),
        ) as usize;

        if self.sub_dirs.len() <= new_index {
            panic!("new_index error");
        }

        // get old depth from old index
        let old_depth = self.get(old_index).get_local_depth();

        // create new subtable
        self.get(new_index)
            .new_subtable(memory_manager, old_depth + 1, new_index as u64);

        // change old subtable's local depth and suffix
        self.get(old_index)
            .set_header_and_localdepth(old_depth + 1, old_index as u64);

        //  change all subtables with old suffix to new subtable
        self.change_dir_suffix_subtable(old_depth + 1, old_index as u64);
        self.change_dir_suffix_subtable(old_depth + 1, new_index as u64);

        // move items from old subtable to new subtable
        self.move_items(old_index, new_index);
    }

    pub fn rehash(&mut self, memory_manager: Arc<Mutex<MemoryManager>>, rehash_index: usize) {
        // get real old index
        let old_index = Directories::restrict_suffix_to(
            rehash_index as u64,
            self.get(rehash_index).get_local_depth(),
        ) as usize;

        let new_index = Directories::get_new_suffix_from_old(
            old_index as u64,
            self.get(old_index).get_local_depth(),
        ) as usize;
        if self.sub_dirs.len() <= new_index {
            self.double_size(memory_manager.clone());
        }

        self.split_dir(memory_manager.clone(), old_index);
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
