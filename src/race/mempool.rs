use crate::directory::Directory;
use crate::race::subtable::CombinedBucket;
use crate::MemoryManager;
use std::mem::size_of;
use std::sync::{Arc, Mutex};

use super::{kvblock::KVBlockMem, subtable::Subtable};
pub struct MemPool {
    memory_manager: Arc<Mutex<MemoryManager>>,
    dir: Directory,
}

impl MemPool {
    pub fn new(memory_manager: Arc<Mutex<MemoryManager>>) -> Self {
        MemPool {
            memory_manager: memory_manager.clone(),
            dir: Directory::new(memory_manager),
        }
    }

    pub fn read(
        &mut self,
        index: usize,
        bucket1: usize,
        bucket2: usize,
    ) -> Option<[CombinedBucket; 2]> {
        self.dir.get(index, bucket1, bucket2)
    }

    pub fn write_kv(&mut self, key: String, value: String) -> *const KVBlockMem {
        KVBlockMem::new(&key, &value, self.memory_manager.clone())
    }

    pub fn write_slot(
        &mut self,
        index: usize,
        bucket_group: usize,
        bucket: usize,
        slot: usize,
        data: u64,
        old: u64,
    ) -> bool {
        self.dir.set(index, bucket_group, bucket, slot, data, old)
    }

    pub fn write_new_entry(&mut self, index: usize, data: u64) -> bool {
        self.dir.write_new_entry(index, data)
    }

    pub fn update_entry(&mut self, index: usize, old_data: u64, new_data: u64) -> bool {
        self.dir.update_entry(index, old_data, new_data)
    }

    pub fn new_subtable(&mut self, local_depth: u8, suffix: u64) -> *const Subtable {
        let subtable_pointer = self
            .memory_manager
            .clone()
            .lock()
            .unwrap()
            .malloc(size_of::<Subtable>());
        if subtable_pointer == std::ptr::null_mut() {
            panic!("malloc failed");
        }
        unsafe {
            (*(subtable_pointer as *mut Subtable)).set_header(local_depth, suffix);
        }
        subtable_pointer as *const Subtable
    }

    pub fn get_global_length(&mut self) -> usize {
        let size = self.dir.get_size();
        let mut i: usize = 1;
        while size / (1 << i) != 0 {
            i += 1;
        }
        i
    }

    pub fn lock_entry(&mut self, index: usize, old_data: u64, lock: u8) -> bool {
        self.dir.lock_entry(index, old_data, lock)
    }

    pub fn unlock_entry(&mut self, index: usize, old_data: u64) -> bool {
        self.dir.unlock_entry(index, old_data)
    }

    //pub fn get_directory_entries(&mut self) -> Vec<Entry> {}
}
