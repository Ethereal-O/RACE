use crate::cfg::config::CONFIG;
use crate::numa::mm::memcpy;
use crate::numa::mm::MemoryManager;
use crate::race::common::hash::Hash;
use crate::race::common::utils::RaceUtils;
use crate::race::computepool::directory::ClientDirectory;
use crate::race::computepool::directory::ClientEntry;
use crate::race::mempool::subtable::Subtable;
use std::f32::consts::E;
use std::mem::size_of;
use std::sync::atomic::AtomicU64;
use std::sync::{atomic, Arc, Mutex};
use std::vec;

use super::subtable::BucketGroup;
use super::subtable::CombinedBucket;
pub struct MemPoolEntry {
    pub data: u64,
}

impl MemPoolEntry {
    pub fn init(
        &mut self,
        memory_manager: Arc<Mutex<MemoryManager>>,
        local_depth: u8,
        suffix: u64,
    ) {
        self.new_subtable(memory_manager, local_depth, suffix);
        self.set_local_depth(local_depth);
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

    pub fn copy_from(&mut self, entry: &MemPoolEntry) {
        self.set_subtable_and_localdepth(entry.get_subtable_pointer(), entry.get_local_depth());
    }

    pub fn set(
        &mut self,
        bucket_group: usize,
        bucket: usize,
        slot: usize,
        data: u64,
        old: u64,
    ) -> bool {
        self.get_subtable()
            .set(bucket_group, bucket, slot, data, old)
    }

    pub fn get_by_bucket_ids(&self, bucket1: usize, bucket2: usize) -> Option<[CombinedBucket; 2]> {
        self.get_subtable().get_by_bucket_ids(bucket1, bucket2)
    }

    pub fn set_subtable_and_localdepth(&mut self, subtable: u64, local_depth: u8) {
        self.set_subtable_pointer(subtable);
        self.set_local_depth(local_depth);
    }

    pub fn set_subtable_and_header_and_depth(
        &mut self,
        subtable: u64,
        local_depth: u8,
        suffix: u64,
    ) {
        self.set_subtable_pointer(subtable);
        self.set_header_and_localdepth(local_depth, suffix);
    }

    pub fn set_header_and_localdepth(&mut self, local_depth: u8, suffix: u64) {
        self.set_header(local_depth, suffix);
        self.set_local_depth(local_depth);
    }

    pub fn set_header(&mut self, local_depth: u8, suffix: u64) {
        self.get_subtable().set_header(local_depth, suffix);
    }

    pub fn get_lock(&self) -> u8 {
        (self.data
            >> CONFIG.bits_of_byte
                * (size_of::<u64>() - size_of::<u8>() - CONFIG.directory_lock_offset)) as u8
    }

    fn set_lock_data(data: u64, lock: u8) -> u64 {
        (data
            & !(0xFF
                << CONFIG.bits_of_byte
                    * (size_of::<u64>() - size_of::<u8>() - CONFIG.directory_lock_offset)))
            | ((lock as u64)
                << CONFIG.bits_of_byte
                    * (size_of::<u64>() - size_of::<u8>() - CONFIG.directory_lock_offset))
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

    pub fn try_lock(&mut self, old_data: u64, lock: u8) -> Result<u64, u64> {
        let new_data = MemPoolEntry::set_lock_data(old_data, lock);
        unsafe {
            std::mem::transmute::<&u64, &atomic::AtomicU64>(&self.data).compare_exchange(
                old_data,
                new_data,
                atomic::Ordering::SeqCst,
                atomic::Ordering::SeqCst,
            )
        }
    }

    pub fn try_unlock(&mut self, old_data: u64) -> Result<u64, u64> {
        let new_data = MemPoolEntry::set_lock_data(old_data, 0);
        unsafe {
            std::mem::transmute::<&u64, &atomic::AtomicU64>(&self.data).compare_exchange(
                old_data,
                new_data,
                atomic::Ordering::SeqCst,
                atomic::Ordering::SeqCst,
            )
        }
    }

    pub fn initial(&mut self, data: u64) -> bool {
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

    pub fn update(&mut self, old_data: u64, new_data: u64) -> bool {
        unsafe {
            match std::mem::transmute::<&u64, &atomic::AtomicU64>(&self.data).compare_exchange(
                old_data,
                new_data,
                atomic::Ordering::SeqCst,
                atomic::Ordering::SeqCst,
            ) {
                Ok(_) => true,
                Err(_) => false,
            }
        }
    }
}

pub struct MemPoolDirectory {
    pub global_depth: *mut usize,
    pub entries: *mut [MemPoolEntry; CONFIG.max_entry_num],
}

impl MemPoolDirectory {
    pub fn new(memory_manager: Arc<Mutex<MemoryManager>>) -> Self {
        let vec_pointer = memory_manager
            .lock()
            .unwrap()
            .malloc(CONFIG.entry_size * CONFIG.max_entry_num);
        let gd_pointer = memory_manager.lock().unwrap().malloc(CONFIG.ptr_size);
        unsafe {
            (*(vec_pointer as *mut [MemPoolEntry; CONFIG.max_entry_num]))[0].init(
                memory_manager.clone(),
                1,
                0,
            );
            (*(vec_pointer as *mut [MemPoolEntry; CONFIG.max_entry_num]))[1].init(
                memory_manager,
                1,
                1,
            );
            *(gd_pointer as *mut usize) = 1;
            MemPoolDirectory {
                global_depth: gd_pointer as *mut usize,
                entries: vec_pointer as *mut [MemPoolEntry; CONFIG.max_entry_num],
            }
        }
    }

    pub fn set_subtable_header(&self, index: usize, local_depth: u8, suffix: u64) {
        self.get_entry(index)
            .get_subtable()
            .set_header(local_depth, suffix);
    }

    pub fn set(
        &self,
        index: usize,
        bucket_group: usize,
        bucket: usize,
        slot: usize,
        data: u64,
        old: u64,
    ) -> bool {
        self.get_entry(index)
            .set(bucket_group, bucket, slot, data, old)
    }

    pub fn get(&self, index: usize, bucket1: usize, bucket2: usize) -> Option<[CombinedBucket; 2]> {
        self.get_entry(index).get_by_bucket_ids(bucket1, bucket2)
    }

    pub fn get_directory(&self) -> ClientDirectory {
        let mut new_dir_entries = [ClientEntry { data: 0 }; CONFIG.max_entry_num];
        let new_global_depth = self.get_global_depth();
        let new_size = RaceUtils::depth_to_size(new_global_depth as u8);
        for index in 0..new_size {
            new_dir_entries[index].data = self.get_entry_const(index).data;
        }
        ClientDirectory {
            global_depth: new_global_depth,
            entries: new_dir_entries,
        }
    }

    pub fn get_entry(&self, index: usize) -> &mut MemPoolEntry {
        unsafe { &mut (*(self.entries))[index] }
    }

    pub fn get_entry_const(&self, index: usize) -> &MemPoolEntry {
        unsafe { &mut (*(self.entries))[index] }
    }

    pub fn get_global_depth(&self) -> usize {
        unsafe {
            (*(self.global_depth as *mut AtomicU64)).load(std::sync::atomic::Ordering::SeqCst)
                as usize
        }
    }

    pub fn atomic_add_global_depth(&self, add: usize) -> bool {
        unsafe {
            match (*(self.global_depth as *mut AtomicU64)).compare_exchange(
                self.get_global_depth() as u64,
                (self.get_global_depth() + add) as u64,
                atomic::Ordering::SeqCst,
                atomic::Ordering::SeqCst,
            ) {
                Ok(_) => true,
                Err(_) => false,
            }
        }
    }

    pub fn try_lock_entry(&self, index: usize, old_data: u64, lock: u8) -> Result<u64, u64> {
        self.get_entry(index).try_lock(old_data, lock)
    }

    pub fn try_unlock_entry(&self, index: usize, old_data: u64) -> Result<u64, u64> {
        self.get_entry(index).try_unlock(old_data)
    }

    pub fn write_new_entry(&self, index: usize, data: u64) -> bool {
        self.get_entry(index).initial(data)
    }

    pub fn update_entry(&self, index: usize, old_data: u64, new_data: u64) -> bool {
        self.get_entry(index).update(old_data, new_data)
    }
}
