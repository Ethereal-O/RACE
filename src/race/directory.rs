use crate::cfg::config::CONFIG;
use crate::numa::mm::memcpy;
use crate::numa::mm::MemoryManager;
use crate::race::hash::Hash;
use crate::race::subtable::Subtable;
use std::mem::size_of;
use std::sync::{Arc, Mutex};
pub struct Entry {
    pub data: u64,
}

impl Entry {
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

    pub fn copy_from(memory_manager: Arc<Mutex<MemoryManager>>, entry: &Entry) -> *mut Self {
        let entry_pointer = memory_manager.lock().unwrap().malloc(size_of::<Entry>());
        if entry_pointer == std::ptr::null_mut() {
            panic!("malloc failed");
        }
        unsafe {
            (*(entry_pointer as *mut Self))
                .set_subtable_and_localdepth(entry.get_subtable_pointer(), entry.get_local_depth());
        }
        entry_pointer as *mut Self
    }

    pub fn add_slot(
        &mut self,
        memory_manager: Arc<Mutex<MemoryManager>>,
        key: &String,
        value: &String,
    ) -> bool {
        self.get_subtable().add_slot(memory_manager, key, value)
    }

    pub fn get_by_key(&self, key: &String) -> Option<String> {
        self.get_subtable().get_by_key(key)
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

pub struct Directory {
    pub entries: Vec<*mut Entry>,
}

impl Directory {
    pub fn restrict_suffix_to(suffix: u64, local_depth: u8) -> u64 {
        let mask = (1 << (local_depth)) - 1;
        suffix & mask
    }

    pub fn add_bit_to_suffix(suffix: u64, index: u8) -> u64 {
        suffix | (1 << (index - 1))
    }

    pub fn plus_bit_to_suffix(suffix: u64, index: u8) -> u64 {
        suffix + (1 << (index - 1))
    }

    pub fn get_new_suffix_from_old(old_index: u64, old_local_depth: u8) -> u64 {
        Directory::restrict_suffix_to(
            Directory::add_bit_to_suffix(old_index as u64, old_local_depth + 1),
            old_local_depth + 1,
        )
    }

    pub fn get_subtable_index(&mut self, key: &String) -> usize {
        ((self.entries.len() as u64 - 1) & Hash::hash(key, 1)) as usize
    }

    pub fn new(memory_manager: Arc<Mutex<MemoryManager>>) -> Self {
        let mut entries = Vec::new();
        entries.push(Entry::new(memory_manager, 0, 0));
        Directory { entries }
    }

    pub fn double_size(&mut self, memory_manager: Arc<Mutex<MemoryManager>>) {
        let old_size = self.entries.len();
        for index in 0..old_size {
            self.entries
                .push(Entry::copy_from(memory_manager.clone(), unsafe {
                    &*self.entries[index]
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
        let old_index = Directory::restrict_suffix_to(
            need_index as u64,
            self.get_entry(need_index).get_local_depth(),
        ) as usize;
        self.split_entry(memory_manager.clone(), old_index);
    }

    pub fn move_items(&mut self, old_index: usize, new_index: usize) {
        
    }

    pub fn change_entry_suffix_subtable(&mut self, local_depth: u8, suffix: u64) {
        let new_pointer = self.get_entry(suffix as usize).get_subtable_pointer();
        let mut index = suffix as usize;
        while index < self.entries.len() {
            // if Directory::restrict_suffix_to(index as u64, local_depth) == suffix {}
            self.get_entry(index)
                .set_subtable_and_localdepth(new_pointer, local_depth);
            index = Directory::plus_bit_to_suffix(index as u64, local_depth + 1) as usize;
        }
    }

    pub fn split_entry(&mut self, memory_manager: Arc<Mutex<MemoryManager>>, old_index: usize) {
        // get new index
        let new_index = Directory::get_new_suffix_from_old(
            old_index as u64,
            self.get_entry(old_index).get_local_depth(),
        ) as usize;

        if self.entries.len() <= new_index {
            panic!("new_index error");
        }

        // get old depth from old index
        let old_depth = self.get_entry(old_index).get_local_depth();

        // create new subtable
        self.get_entry(new_index)
            .new_subtable(memory_manager, old_depth + 1, new_index as u64);

        // change old subtable's local depth and suffix
        self.get_entry(old_index)
            .set_header(old_depth + 1, old_index as u64);

        //  change all subtables with old suffix to new subtable
        self.change_entry_suffix_subtable(old_depth + 1, old_index as u64);
        self.change_entry_suffix_subtable(old_depth + 1, new_index as u64);

        // move items from old subtable to new subtable
        self.move_items(old_index, new_index);
    }

    pub fn rehash(&mut self, memory_manager: Arc<Mutex<MemoryManager>>, rehash_index: usize) {
        // get real old index
        let old_index = Directory::restrict_suffix_to(
            rehash_index as u64,
            self.get_entry(rehash_index).get_local_depth(),
        ) as usize;

        let new_index = Directory::get_new_suffix_from_old(
            old_index as u64,
            self.get_entry(old_index).get_local_depth(),
        ) as usize;
        if self.entries.len() <= new_index {
            self.double_size(memory_manager.clone());
        }

        self.split_entry(memory_manager.clone(), old_index);
    }

    pub fn add(&mut self, memory_manager: Arc<Mutex<MemoryManager>>, key: &String, value: &String) {
        let index = self.get_subtable_index(key);
        if !self
            .get_entry(index)
            .add_slot(memory_manager.clone(), key, value)
        {
            self.rehash(memory_manager.clone(), index);
            self.get_entry(index)
                .add_slot(memory_manager.clone(), key, value);
        }
    }

    pub fn get(&mut self, key: &String) -> Option<String> {
        let index = self.get_subtable_index(key);
        self.get_entry(index).get_by_key(key)
    }

    pub fn deref_directory(&mut self) -> Vec<Entry> {
        let mut new_entries = Vec::new();
        for entry in self.entries.iter() {
            new_entries.push(Entry {
                data: unsafe { (*(*entry)).data },
            });
        }
        new_entries
    }

    pub fn get_entry(&mut self, index: usize) -> &mut Entry {
        unsafe { &mut *(self.entries[index]) }
    }
}
