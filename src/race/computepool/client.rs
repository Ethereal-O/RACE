use super::directory::{self, ClientDirectory};
use crate::cfg::config::CONFIG;
use crate::race::common::hash::{Hash, HashMethod};
use crate::race::common::kvblock::KVBlock;
use crate::race::common::utils::{self, RaceUtils};
use crate::race::mempool::subtable::{CombinedBucket, Slot, SlotPos, Subtable};
use crate::race::mempool::{self, mempool::MemPool};
use crate::KVBlockMem;
use std::mem::size_of;
use std::process::exit;
use std::sync::{Arc, Mutex, RwLock};

pub struct Client {
    mempool: Arc<RwLock<MemPool>>,
    directory: ClientDirectory,
}

impl Client {
    pub fn new(mempool: Arc<RwLock<MemPool>>) -> Self {
        let directory = mempool.read().unwrap().get_directory();
        Client { mempool, directory }
    }

    fn get_size(&self) -> usize {
        RaceUtils::depth_to_size(self.directory.global_depth as u8)
    }

    fn get_combined_buckets(&self, key: &String) -> Option<[CombinedBucket; 2]> {
        let index = RaceUtils::get_suffix(key, self.directory.global_depth as u8) as usize;
        let hash_1 = Hash::hash(key, HashMethod::CombinedBucket1) as usize;
        let hash_2 = Hash::hash(key, HashMethod::CombinedBucket2) as usize;
        self.directory
            .get_entry_const(index)
            .get_combined_buckets(hash_1, hash_2)
    }

    fn get_slot(&self, key: &String) -> Option<SlotPos> {
        match self.get_combined_buckets(key) {
            Some(cbs) => {
                let mut result = None;
                for i in 0..2 {
                    if let Some(v) = cbs[i].get_by_key(key) {
                        result = Some(v);
                        break;
                    }
                }
                if result.is_some() {
                    return None;
                }
                let cb1_count = cbs[0].count();
                let cb2_count = cbs[1].count();
                if cb1_count < cb2_count {
                    Some(SlotPos {
                        subtable: cbs[0].subtable,
                        bucket_group: cbs[0].bucket_group,
                        bucket: if cb1_count < CONFIG.slot_num { 0 } else { 1 },
                        header: if cb1_count < CONFIG.slot_num {
                            cbs[0].main_bucket.get_header()
                        } else {
                            cbs[0].overflow_bucket.get_header()
                        },
                        slot: cb1_count % CONFIG.slot_num,
                    })
                } else if cb1_count > cb2_count {
                    Some(SlotPos {
                        subtable: cbs[1].subtable,
                        bucket_group: cbs[1].bucket_group,
                        bucket: if cb2_count < CONFIG.slot_num { 2 } else { 1 },
                        header: if cb2_count < CONFIG.slot_num {
                            cbs[1].main_bucket.get_header()
                        } else {
                            cbs[1].overflow_bucket.get_header()
                        },
                        slot: cb2_count % CONFIG.slot_num,
                    })
                } else {
                    if cb1_count == 2 * CONFIG.slot_num {
                        None
                    } else {
                        Some(SlotPos {
                            subtable: cbs[0].subtable,
                            bucket_group: cbs[0].bucket_group,
                            bucket: if cb1_count < CONFIG.slot_num { 0 } else { 1 },
                            header: if cb1_count < CONFIG.slot_num {
                                cbs[0].main_bucket.get_header()
                            } else {
                                cbs[0].overflow_bucket.get_header()
                            },
                            slot: cb1_count % CONFIG.slot_num,
                        })
                    }
                }
            }
            None => panic!("metadata error"),
        }
    }

    fn write_slot(
        &mut self,
        slot_pos: &SlotPos,
        key: &String,
        val: &String,
        kv_block: *const KVBlockMem,
    ) -> bool {
        let data = RaceUtils::set_data(key, val, kv_block as u64);
        if self.mempool.read().unwrap().write_slot(&slot_pos, data, 0) {
            // Reread and check whether the insert is correct
            let current_header = unsafe {
                (*(slot_pos.subtable))
                    .get_bucket_header_atomic(slot_pos.bucket_group, slot_pos.bucket)
            };
            if current_header.get_data() == slot_pos.header
                || RaceUtils::get_suffix(key, current_header.get_local_depth())
                    == current_header.get_suffix()
            {
                true
            } else {
                // Insert during resizing, delete the wrong insertion and reinsert
                self.mempool.read().unwrap().write_slot(&slot_pos, 0, data);
                self.insert(key, val)
            }
        } else {
            self.insert(key, val)
        }
    }

    fn update_slot(
        &mut self,
        slot_pos: &SlotPos,
        key: &String,
        val: &String,
        kv_block: *const KVBlockMem,
        old: u64,
    ) -> bool {
        let data = RaceUtils::set_data(key, val, kv_block as u64);
        if self
            .mempool
            .read()
            .unwrap()
            .write_slot(&slot_pos, data, old)
        {
            true
        } else {
            self._update(key, val, kv_block)
        }
    }

    fn _search(&mut self, key: &String, cbs: &[CombinedBucket; 2]) -> Option<String> {
        let remote_local_depth1 = cbs[0].main_bucket.header.get_local_depth();
        let remote_suffix1 = cbs[0].main_bucket.header.get_suffix();
        let suffix1 = RaceUtils::get_suffix(key, remote_local_depth1);
        let remote_local_depth2 = cbs[1].main_bucket.header.get_local_depth();
        let remote_suffix2 = cbs[1].main_bucket.header.get_suffix();
        let suffix2 = RaceUtils::get_suffix(key, remote_local_depth2);

        let mut result = None;
        for i in 0..2 {
            if let Some(v) = cbs[i].get_by_key(key) {
                if RaceUtils::check_crc(&v.key, &v.value, v.crc64) {
                    result = Some(v.value);
                    break;
                } else {
                    // TODO: consider putting current thread to sleep
                    return self.search(key);
                }
            }
        }
        if remote_suffix1 == suffix1 && remote_suffix2 == suffix2 {
            result
        } else {
            self.refresh_directory();
            if result.is_some() {
                result
            } else {
                self.search(key)
            }
        }
    }

    pub fn search(&mut self, key: &String) -> Option<String> {
        match self.get_combined_buckets(key) {
            Some(cbs) => self._search(key, &cbs),
            None => None,
        }
    }

    fn _insert(&mut self, key: &String, val: &String, kv_block: *const KVBlockMem) -> bool {
        match self.get_combined_buckets(key) {
            Some(cbs) => match self._search(key, &cbs) {
                Some(_) => false,
                None => match self.get_slot(key) {
                    Some(sp) => self.write_slot(&sp, key, val, kv_block),
                    None => {
                        self.rehash(
                            RaceUtils::get_suffix(key, self.directory.global_depth) as usize
                        );
                        self._insert(key, val, kv_block)
                    }
                },
            },
            None => panic!("get candidate postion error"),
        }
    }

    pub fn insert(&mut self, key: &String, val: &String) -> bool {
        let kv_block = self
            .mempool
            .read()
            .unwrap()
            .write_kv(key.clone(), val.clone());
        if self._insert(key, val, kv_block) {
            true
        } else {
            self.mempool
                .read()
                .unwrap()
                .free_kv(kv_block, unsafe { (*kv_block).get_total_length() });
            false
        }
    }

    fn _delete(&mut self, key: &String, cbs: &[CombinedBucket; 2]) -> bool {
        let remote_local_depth1 = cbs[0].main_bucket.header.get_local_depth();
        let remote_suffix1 = cbs[0].main_bucket.header.get_suffix();
        let suffix1 = RaceUtils::get_suffix(key, remote_local_depth1);
        let remote_local_depth2 = cbs[1].main_bucket.header.get_local_depth();
        let remote_suffix2 = cbs[1].main_bucket.header.get_suffix();
        let suffix2 = RaceUtils::get_suffix(key, remote_local_depth2);

        // Both local depth and suffix bits mismatch, refresh directory and redo!
        if remote_suffix1 != suffix1 || remote_suffix2 != suffix2 {
            self.refresh_directory();
            return self.delete(key);
        }

        let mut op_spd = None;
        for i in 0..2 {
            op_spd = cbs[i].get_slot_pos_and_data(key, i);
            if op_spd.is_some() {
                break;
            }
        }
        if let Some(spd) = op_spd {
            if self.mempool.read().unwrap().write_slot(&spd.0, 0, spd.1) {
                self.mempool
                    .read()
                    .unwrap()
                    .free_kv((Slot { data: spd.1 }).get_kv_pointer(), unsafe {
                        (*(Slot { data: spd.1 }).get_kv_pointer()).get_total_length()
                    });
                true
            } else {
                // CAS happens after "moving items" in resizing, refresh and redo!
                self.refresh_directory();
                self.delete(key)
            }
        } else {
            // At least suffix bits match, which means the key does not exist, then do nothing!
            false
        }
    }

    pub fn delete(&mut self, key: &String) -> bool {
        match self.get_combined_buckets(key) {
            Some(cbs) => self._delete(key, &cbs),
            None => panic!("get candidate position error"),
        }
    }

    fn _update(&mut self, key: &String, val: &String, kv_block: *const KVBlockMem) -> bool {
        match self.get_combined_buckets(key) {
            Some(cbs) => {
                let remote_local_depth1 = cbs[0].main_bucket.header.get_local_depth();
                let remote_suffix1 = cbs[0].main_bucket.header.get_suffix();
                let suffix1 = RaceUtils::get_suffix(key, remote_local_depth1);
                let remote_local_depth2 = cbs[1].main_bucket.header.get_local_depth();
                let remote_suffix2 = cbs[1].main_bucket.header.get_suffix();
                let suffix2 = RaceUtils::get_suffix(key, remote_local_depth2);

                // Both local depth and suffix bits mismatch, refresh directory and redo!
                if remote_suffix1 != suffix1 || remote_suffix2 != suffix2 {
                    self.refresh_directory();
                    return self._update(key, val, kv_block);
                }

                let mut op_spd = None;
                for i in 0..2 {
                    op_spd = cbs[i].get_slot_pos_and_data(key, i);
                    if op_spd.is_some() {
                        break;
                    }
                }

                if let Some(spd) = op_spd {
                    if self.update_slot(&spd.0, key, val, kv_block, spd.1) {
                        self.mempool.read().unwrap().free_kv(
                            (Slot { data: spd.1 }).get_kv_pointer(),
                            unsafe {
                                (*(Slot { data: spd.1 }).get_kv_pointer()).get_total_length()
                            },
                        );
                        true
                    } else {
                        // CAS happens after "moving items" in resizing, refresh and redo!
                        self.refresh_directory();
                        self._update(key, val, kv_block)
                    }
                } else {
                    // At least suffix bits match, which means the key does not exist, then do nothing!
                    false
                }
            }
            None => panic!("get candidate postion error"),
        }
    }

    pub fn update(&mut self, key: &String, val: &String) -> bool {
        let kv_block = self
            .mempool
            .read()
            .unwrap()
            .write_kv(key.clone(), val.clone());
        if self._update(key, val, kv_block) {
            true
        } else {
            self.mempool
                .read()
                .unwrap()
                .free_kv(kv_block, unsafe { (*kv_block).get_total_length() });
            false
        }
    }

    /**
     * Inner Remote part
     */
    fn init_local_entry_and_push(&mut self, index: usize, pointer: u64, local_depth: u8) {
        self.directory
            .get_entry(index as usize)
            .set_subtable_and_localdepth(pointer, local_depth);
        let locked_data = self.directory.get_entry(index as usize).get_locked_data();
        self.mempool
            .read()
            .unwrap()
            .write_new_entry(index, locked_data);
    }

    fn set_local_entry_and_push(&mut self, index: usize, pointer: u64, local_depth: u8) {
        let old_data = self.directory.get_entry(index as usize).get_data();
        let old_locked_data = self.directory.get_entry(index as usize).get_locked_data();
        self.directory
            .get_entry(index as usize)
            .set_subtable_and_localdepth(pointer, local_depth);
        let new_locked_data = self.directory.get_entry(index as usize).get_locked_data();
        self.mempool
            .read()
            .unwrap()
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
        loop {
            self.directory = self.mempool.read().unwrap().get_directory();
            let mut all_free = true;
            for index in 0..self.get_size() {
                if self.directory.get_entry_const(index).check_is_locked() {
                    all_free = false;
                    break;
                }
            }
            if all_free {
                break;
            }
        }
        self.clear_all_lock_status();
    }

    fn refresh_directory_without_wait(&mut self) {
        self.directory = self.mempool.read().unwrap().get_directory();
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
            let result = self.mempool.read().unwrap().try_lock_entry(index, old_data);
            match result {
                Ok(_) => return true,
                Err(new_data) => {
                    if is_try && RaceUtils::check_is_locked(new_data) {
                        return false;
                    }
                }
            }
            self.refresh_directory_without_wait();
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
        self.mempool
            .read()
            .unwrap()
            .unlock_entry(index, locked_data);
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

    fn lock_suffix_without_self(&mut self, suffix: u64, local_depth: u8) {
        let mut index = suffix as usize;
        index = RaceUtils::plus_bit_to_suffix(index as u64, local_depth + 1) as usize;
        loop {
            if index >= self.get_size() {
                break;
            }
            self.lock(index);
            index = RaceUtils::plus_bit_to_suffix(index as u64, local_depth + 1) as usize;
        }
    }

    fn lock_suffix_helper(
        &mut self,
        old_index: usize,
        new_index: usize,
        local_depth: u8,
        is_first: bool,
    ) {
        if is_first {
            self.lock(old_index);
            self.lock(new_index);
        } else {
            self.lock_suffix_without_self(old_index as u64, local_depth);
            self.lock_suffix_without_self(new_index as u64, local_depth);
        }
    }

    fn lock_suffix_and_flush(&mut self, old_index: usize, new_index: usize, local_depth: u8) {
        self.lock_suffix_helper(old_index, new_index, local_depth, true);
        self.get_directory();
        self.lock_suffix_helper(old_index, new_index, local_depth, false);
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
        self.mempool.read().unwrap().increase_global_depth();

        // unlock all
        self.unlock_all();
    }

    fn read_from_slot_pos_with_crc_check(&mut self, slot_pos: &SlotPos) -> (u64, Option<KVBlock>) {
        let mut data = self.mempool.read().unwrap().read_slot(&slot_pos);
        let mut kv_data_op = Slot { data }.get_kv();
        loop {
            if let Some(kv_data) = &kv_data_op {
                if kv_data.klen == 0 {
                    // there is no data in this slot, we can skip it
                    return (data, kv_data_op);
                }
                if RaceUtils::check_crc(&kv_data.key, &kv_data.value, kv_data.crc64) {
                    return (data, kv_data_op);
                }
            } else {
                return (data, kv_data_op);
            }
            data = self.mempool.read().unwrap().read_slot(&slot_pos);
            kv_data_op = Slot { data }.get_kv();
        }
        return (data, kv_data_op);
    }

    fn move_items(&mut self, old_index: usize, new_index: usize, local_depth: u8) {
        for bucket_group_index in 0..CONFIG.bucket_group_num {
            for bucket_index in 0..CONFIG.bucket_num {
                for slot_index in 0..CONFIG.slot_num {
                    let slot_pos = SlotPos {
                        subtable: self.directory.get_entry(old_index).get_subtable_pointer()
                            as *const Subtable,
                        bucket_group: bucket_group_index,
                        bucket: bucket_index,
                        header: 0,
                        slot: slot_index,
                    };

                    // read from this slot
                    let (mut data, mut kv_data_op) = self.read_from_slot_pos_with_crc_check(&slot_pos);

                    if let Some(kv_data) = &kv_data_op {
                        if kv_data.klen == 0 {
                            // there is no data in this slot, we can skip it
                            continue;
                        }

                        let hash_key = Hash::hash(&kv_data.key, HashMethod::Directory);
                        let desired_new_index =
                            RaceUtils::get_suffix(&kv_data.key, self.directory.global_depth as u8)
                                as usize;

                        if self
                            .directory
                            .get_entry(desired_new_index)
                            .get_subtable_pointer()
                            != self.directory.get_entry(old_index).get_subtable_pointer()
                            && self
                                .directory
                                .get_entry(desired_new_index)
                                .get_subtable_pointer()
                                != self.directory.get_entry(new_index).get_subtable_pointer()
                        {
                            print!(
                                "desired_new_index: {}, old_index: {}, new_index: {}, hash_key: {}, key: {}, global_depth: {}",
                                desired_new_index, old_index, new_index, hash_key, kv_data.key, self.directory.global_depth
                            );
                            print!(
                                "old_pointer: {}, new_pointer: {}, desired_new_pointer: {}",
                                self.directory.get_entry(old_index).get_subtable_pointer(),
                                self.directory.get_entry(new_index).get_subtable_pointer(),
                                self.directory
                                    .get_entry(desired_new_index)
                                    .get_subtable_pointer()
                            );
                            panic!("move items error")
                        }

                        if self
                            .directory
                            .get_entry(desired_new_index)
                            .get_subtable_pointer()
                            != self.directory.get_entry(new_index).get_subtable_pointer()
                        {
                            // don't need to move
                            continue;
                        }

                        // insert to new subtable
                        let new_slot_pos = SlotPos {
                            subtable: self.directory.get_entry(new_index).get_subtable_pointer()
                                as *const Subtable,
                            bucket_group: bucket_group_index,
                            bucket: bucket_index,
                            header: 0,
                            slot: slot_index,
                        };
                        let mut new_kv_block = self
                            .mempool
                            .read()
                            .unwrap()
                            .write_kv(kv_data.key.clone(), kv_data.value.clone());

                        let mut new_data =
                            RaceUtils::set_data(&kv_data.key, &kv_data.value, new_kv_block as u64);

                        self.mempool
                            .read()
                            .unwrap()
                            .write_slot(&new_slot_pos, new_data, 0);

                        // free old data
                        loop {
                            let write_res =
                                self.mempool.read().unwrap().write_slot(&slot_pos, 0, data);
                            if write_res {
                                self.mempool.read().unwrap().free_kv(
                                    (Slot { data }).get_kv_pointer(),
                                    unsafe {
                                        (*(Slot { data }).get_kv_pointer()).get_total_length()
                                    },
                                );
                                break;
                            }
                            (data, kv_data_op) = self.read_from_slot_pos_with_crc_check(&slot_pos);
                            if let Some(kv_data) = kv_data_op {
                                if kv_data.klen == 0 {
                                    // there is no data in this slot, someone has deleted it
                                    self.mempool.read().unwrap().write_slot(
                                        &new_slot_pos,
                                        0,
                                        new_data,
                                    );
                                    self.mempool.read().unwrap().free_kv(
                                        (Slot { data: new_data }).get_kv_pointer(),
                                        unsafe {
                                            (*(Slot { data: new_data }).get_kv_pointer())
                                                .get_total_length()
                                        },
                                    );
                                    break;
                                }
                                // someone update it
                                new_kv_block = self
                                    .mempool
                                    .read()
                                    .unwrap()
                                    .write_kv(kv_data.key.clone(), kv_data.value.clone());

                                new_data = RaceUtils::set_data(
                                    &kv_data.key,
                                    &kv_data.value,
                                    new_kv_block as u64,
                                );

                                self.mempool
                                    .read()
                                    .unwrap()
                                    .write_slot(&new_slot_pos, new_data, 0);
                            } else {
                                panic!("get kv error");
                            }
                        }
                    }
                }
            }
        }
    }

    fn split_entry(&mut self, old_index: usize) {
        // get old depth from old index
        let old_depth = self.directory.get_entry(old_index).get_local_depth();

        // get new index
        let new_index = RaceUtils::get_new_suffix_from_old(old_index as u64, old_depth) as usize;

        let old_size = self.get_size();

        if old_size <= new_index {
            panic!("new_index out of range");
        }

        // create new subtable
        let new_pointer = self
            .mempool
            .read()
            .unwrap()
            .new_subtable(old_depth + 1, new_index as u64) as u64;

        // get old pointer
        let old_pointer = self.directory.get_entry(old_index).get_subtable_pointer() as u64;

        // set entry
        self.set_local_entries_and_push(old_index as u64, old_pointer, old_depth + 1);
        self.set_local_entries_and_push(new_index as u64, new_pointer, old_depth + 1);

        // do not forget to change old subtable
        self.mempool.read().unwrap().set_subtable_header(
            old_index,
            old_depth + 1,
            old_index as u64,
        );
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
        if old_size >= CONFIG.max_entry_num {
            panic!("rehash error");
        }

        if old_size <= new_index {
            self.double_size();
        }

        // we must get local depth first
        let old_depth = self.directory.get_entry(old_index).get_local_depth();

        // we must try lock and get newest global depth
        self.lock_suffix_and_flush(old_index, new_index, old_depth + 1);

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

        // move items from old subtable to new subtable
        // we must have lock, to prevent other thread from changing the directory
        // self.move_items(old_index);
        self.move_items(old_index, new_index, old_depth + 1);

        // unlock suffix
        self.unlock_suffix(old_index as u64);
        self.unlock_suffix(new_index as u64);
    }

    // only for test
    pub fn get_mempool(&self) -> &Arc<RwLock<MemPool>> {
        &self.mempool
    }

    pub fn get_directory(&self) -> &ClientDirectory {
        &self.directory
    }

    pub fn pub_rehash(&mut self, rehash_index: usize) {
        self.rehash(rehash_index);
    }

    pub fn pub_get_size(&self) -> usize {
        self.get_size()
    }
}
