use std::mem::size_of;

use super::directory::{self, ComputePoolDirectory};
use crate::cfg::config::CONFIG;
use crate::race::common::hash::{Hash, HashMethod};
use crate::race::common::utils::RaceUtils;
use crate::race::mempool::{self, mempool::MemPool};

pub struct ComputePool {
    mempool: MemPool,
    directory: ComputePoolDirectory,
}

impl ComputePool {
    pub fn new() -> Self {
        let mempool = MemPool::new();
        let directory = mempool.get_directory();
        ComputePool { mempool, directory }
    }

    fn get_size(&self) -> usize {
        RaceUtils::depth_to_size(self.directory.global_depth as u8)
    }

    // pub fn read(
    //     &mut self,
    //     key: &String,
    // ) -> Option<String> {
    //     let index = Hash::hash(key, HashMethod::Directory) as usize;
    //     let hash_1 = Hash::hash(key, HashMethod::CombinedBucket1) as usize;
    //     let hash_2 = Hash::hash(key, HashMethod::CombinedBucket2) as usize;
    //     let combined_bucket = self.mempool.read(index, hash_1, hash_2);
    // }

    /**
     * Inner Remote part
     */
    fn init_local_entry_and_push(&mut self, index: usize, pointer: u64, local_depth: u8) {
        self.directory
            .get_entry(index as usize)
            .set_subtable_and_localdepth(pointer, local_depth);
        let locked_data = self.directory.get_entry(index as usize).get_locked_data();
        self.mempool.write_new_entry(index, locked_data);
    }

    fn set_local_entry_and_push(&mut self, index: usize, pointer: u64, local_depth: u8) {
        let old_data = self.directory.get_entry(index as usize).get_data();
        let old_locked_data = self.directory.get_entry(index as usize).get_locked_data();
        self.directory
            .get_entry(index as usize)
            .set_subtable_and_localdepth(pointer, local_depth);
        let new_locked_data = self.directory.get_entry(index as usize).get_locked_data();
        self.mempool
            .update_entry(index, old_locked_data, new_locked_data);
    }

    fn set_local_entries_and_push(&mut self, suffix: u64, pointer: u64, local_depth: u8) {
        let mut index = suffix as usize;
        let old_size = self.get_size();
        while index < old_size {
            self.set_local_entry_and_push(index, pointer, local_depth);
            index = RaceUtils::plus_bit_to_suffix(index as u64, local_depth + 1) as usize;
        }
    }

    fn refresh_directory(&mut self) {
        self.directory = self.mempool.get_directory();
        self.clear_all_lock_status();
    }

    /**
     * Lock part
     */

    fn clear_all_lock_status(&mut self) {
        for index in 0..self.get_size() {
            self.directory.get_entry(index).clear_lock_status();
        }
    }

    fn lock_base(&mut self, index: usize, is_try: bool) -> bool {
        let old_data = self.directory.get_entry(index).get_data();
        let try_times = 0;
        loop {
            let result = self.mempool.try_lock_entry(index, old_data);
            match result {
                Ok(_) => return true,
                Err(new_data) => {
                    if is_try && RaceUtils::check_is_locked(new_data) {
                        return false;
                    }
                }
            }
            self.refresh_directory();
            if is_try && try_times >= CONFIG.max_try_lock_times {
                return false;
            }
        }
        false
    }

    fn try_lock(&mut self, index: usize) -> bool {
        self.lock_base(index, true)
    }

    fn lock(&mut self, index: usize) {
        self.lock_base(index, false);
    }

    fn unlock(&mut self, index: usize) {
        let locked_data = self.directory.get_entry(index).get_locked_data();
        self.mempool.unlock_entry(index, locked_data);
    }

    fn lock_all(&mut self) {
        let mut now_index = 0;
        loop {
            self.lock(now_index);
            now_index += 1;
            if now_index >= self.get_size() {
                break;
            }
        }
    }

    fn unlock_all(&mut self) {
        let mut now_index = 0;
        loop {
            self.unlock(now_index);
            now_index += 1;
            if now_index >= self.get_size() {
                break;
            }
        }
    }

    fn lock_suffix(&mut self, suffix: u64, local_depth: u8) {
        let mut index = suffix as usize;
        loop {
            self.lock(index);
            index = RaceUtils::plus_bit_to_suffix(index as u64, local_depth + 1) as usize;
            if index >= self.get_size() {
                break;
            }
        }
    }

    fn unlock_suffix(&mut self, suffix: u64) {
        let local_depth = self.directory.get_entry(suffix as usize).get_local_depth();
        let mut index = suffix as usize;
        loop {
            self.unlock(index);
            index = RaceUtils::plus_bit_to_suffix(index as u64, local_depth + 1) as usize;
            if index >= self.get_size() {
                break;
            }
        }
    }

    fn double_size(&mut self) {
        // lock all
        let old_size = self.get_size();
        self.lock_all();
        let new_size = self.get_size();
        if old_size != new_size {
            // shows someone has update the directory
            self.unlock_all();
            return;
        }
        // begin double size now!
        // set directory
        for index in old_size..old_size * 2 {
            let pointer = self
                .directory
                .get_entry((index - old_size) as usize)
                .get_subtable_pointer();
            let local_depth = self
                .directory
                .get_entry((index - old_size) as usize)
                .get_local_depth();
            self.init_local_entry_and_push(index, pointer, local_depth);
        }

        // set global depth
        self.directory.global_depth += 1;
        self.mempool.increase_global_depth();

        // unlock all
        self.unlock_all();
    }

    fn move_items(&mut self, old_index: usize, new_index: usize) {}

    fn split_entry(&mut self, old_index: usize) {
        // get old depth from old index
        let old_depth = self.directory.get_entry(old_index).get_local_depth();

        // get new index
        let new_index = RaceUtils::get_new_suffix_from_old(old_index as u64, old_depth) as usize;

        let old_size = self.get_size();

        if old_size <= new_index {
            panic!("new_index error");
        }

        // create new subtable
        let new_pointer = self.mempool.new_subtable(old_depth + 1, new_index as u64) as u64;

        // get old pointer
        let old_pointer = self.directory.get_entry(old_index).get_subtable_pointer() as u64;

        // set entry
        self.set_local_entries_and_push(old_index as u64, old_pointer, old_depth + 1);
        self.set_local_entries_and_push(new_index as u64, new_pointer, old_depth + 1);

        // do not forget to change old subtable
        self.mempool
            .set_subtable_header(old_index, old_depth + 1, old_index as u64);

        // move items from old subtable to new subtable
        self.move_items(old_index, new_index);
    }

    fn rehash(&mut self, rehash_index: usize) {
        // get real old index
        let old_index = RaceUtils::restrict_suffix_to(
            rehash_index as u64,
            self.directory.get_entry(rehash_index).get_local_depth(),
        ) as usize;

        let new_index = RaceUtils::get_new_suffix_from_old(
            old_index as u64,
            self.directory.get_entry(old_index).get_local_depth(),
        ) as usize;

        let old_size = self.get_size();
        if old_size <= new_index {
            self.double_size();
        }

        // we must get local depth first
        let old_depth = self.directory.get_entry(old_index).get_local_depth();

        // lock suffix
        self.lock_suffix(old_index as u64, old_depth + 1);
        self.lock_suffix(new_index as u64, old_depth + 1);

        // get local depth again
        let new_depth = self.directory.get_entry(old_index).get_local_depth();
        if old_depth != new_depth {
            // someone has changed the directory
            self.unlock_suffix(old_index as u64);
            self.unlock_suffix(new_index as u64);
            return;
        }

        // split now!
        self.split_entry(old_index);

        // unlock suffix
        self.unlock_suffix(old_index as u64);
        self.unlock_suffix(new_index as u64);
    }

    // only for test
    pub fn get_mempool(&self) -> &MemPool {
        &self.mempool
    }

    pub fn get_directory(&self) -> &ComputePoolDirectory {
        &self.directory
    }

    pub fn pub_rehash(&mut self, rehash_index: usize) {
        self.rehash(rehash_index);
    }

    pub fn pub_get_size(&self) -> usize {
        self.get_size()
    }
}
