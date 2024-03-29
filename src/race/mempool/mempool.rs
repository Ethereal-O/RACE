use crate::directory::MemPoolDirectory;
use crate::numa::mm::memset;
use crate::race::common::kvblock::{KVBlockMem, KVBlock};
use crate::race::computepool::directory::ClientDirectory;
use crate::race::mempool::subtable::CombinedBucket;
use crate::MemoryManager;
use std::mem::size_of;
use std::sync::{Arc, Mutex};

use super::directory::MemPoolEntry;
use super::subtable::{SlotPos, Subtable};
pub struct MemPool {
    memory_manager: Arc<Mutex<MemoryManager>>,
    dir: MemPoolDirectory,
}

impl MemPool {
    pub fn new() -> Self {
        let memory_manager = Arc::new(Mutex::new(MemoryManager::new()));
        MemPool {
            memory_manager: memory_manager.clone(),
            dir: MemPoolDirectory::new(memory_manager),
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

    pub fn write_kv(&self, key: String, value: String) -> *const KVBlockMem {
        KVBlockMem::new(&key, &value, self.memory_manager.clone())
    }

    pub fn free_kv(&self, kv_block: *const KVBlockMem, size: usize) {
        unsafe {
            memset(kv_block as *mut u8, 0, size as u32);
        }
        self.memory_manager
            .lock()
            .unwrap()
            .free(kv_block as *const u8, size);
    }

    pub fn read_slot(&self, slot_pos: &SlotPos) -> u64 {
        unsafe { (*(slot_pos.subtable as *mut Subtable)).get(slot_pos) }
    }

    pub fn read_slot_kv(&self, slot_pos: &SlotPos) -> Option<KVBlock> {
        unsafe { (*(slot_pos.subtable as *mut Subtable)).get_kv(slot_pos) }
    }

    pub fn write_slot(&self, slot_pos: &SlotPos, data: u64, old: u64) -> bool {
        unsafe { (*(slot_pos.subtable as *mut Subtable)).set(slot_pos, data, old) }
    }

    pub fn write_new_entry(&self, index: usize, data: u64) -> bool {
        self.dir.write_new_entry(index, data)
    }

    pub fn update_entry(&self, index: usize, old_data: u64, new_data: u64) -> bool {
        self.dir.update_entry(index, old_data, new_data)
    }

    pub fn new_subtable(&self, local_depth: u8, suffix: u64) -> *const Subtable {
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

    pub fn set_subtable_header(&self, index: usize, local_depth: u8, suffix: u64) {
        self.dir.set_subtable_header(index, local_depth, suffix);
    }

    pub fn get_global_length(&mut self) -> u8 {
        self.dir.get_global_depth()
    }

    pub fn increase_global_depth(&self) {
        self.dir.atomic_add_global_depth(1)
    }

    pub fn try_lock_entry(&self, index: usize, old_data: u64) -> Result<u64, u64> {
        self.dir.try_lock_entry(index, old_data, 1)
    }

    pub fn unlock_entry(&self, index: usize, old_data: u64) -> Result<u64, u64> {
        self.dir.try_unlock_entry(index, old_data)
    }

    pub fn get_directory(&self) -> ClientDirectory {
        self.dir.get_directory()
    }

    // only for test
    pub fn get_entry(&self, index: usize) -> &MemPoolEntry {
        self.dir.get_entry_const(index)
    }
}
