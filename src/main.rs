#![allow(unused)]
mod cfg;
mod numa;
mod race;

use numa::mm::MemoryManager;
use race::{directory, kvblock};
use std::{
    ops::Deref,
    sync::{Arc, Mutex},
};

use crate::numa::numa::numa_free;

fn print_dir_depth(directory: &mut directory::Directory, dir_index: usize) {
    print!(
        "{} dir depth: {}\n",
        dir_index,
        directory.get_entry(dir_index).get_local_depth()
    );
}

fn print_slot_depth(directory: &mut directory::Directory, dir_index: usize) {
    print!(
        "{} slot depth: {}\n",
        dir_index,
        directory.get_entry(dir_index).get_subtable().bucket_groups[0].buckets[0]
            .header
            .get_local_depth()
    );
}

fn print_slot_suffix(directory: &mut directory::Directory, dir_index: usize) {
    print!(
        "{} slot suffix: {}\n",
        dir_index,
        directory.get_entry(dir_index).get_subtable().bucket_groups[0].buckets[0]
            .header
            .get_suffix()
    );
}

fn print_all(directory: &mut directory::Directory) {
    let dir_num = directory.entries.len();
    for i in 0..dir_num {
        print_dir_depth(directory, i);
        print_slot_depth(directory, i);
        print_slot_suffix(directory, i);
    }
}

fn main() {
    let memory_manager = Arc::new(Mutex::new(MemoryManager::new()));
    // let key = String::from("sentence");
    // let value = String::from("Hello World!");
    // let kvbm = KVBlockMem::new(
    //     &key,
    //     &value,
    //     memory_manager,
    // );
    // let kvb = unsafe { (*kvbm).get() };
    // println!("{:?}", kvb);
    let mut directories = directory::Directory::new(memory_manager.clone());

    // // init value
    // print_all(&mut directories);

    // // rehash first subtable
    // directories.rehash(memory_manager.clone(), 0);
    // print_all(&mut directories);

    // // rehash first subtable
    // directories.rehash(memory_manager.clone(), 0);
    // print_all(&mut directories);

    // // rehash second subtable
    // directories.rehash(memory_manager.clone(), 1);
    // print_all(&mut directories);

    // // rehash third subtable
    // directories.rehash(memory_manager.clone(), 2);
    // print_all(&mut directories);

    // // rehash third subtable
    // directories.rehash(memory_manager.clone(), 2);
    // print_all(&mut directories);

    // // rehash first subtable
    // directories.rehash(memory_manager.clone(), 0);
    // print_all(&mut directories);

    // // 0 1 2 3 4 1 6 3 0 1 10 3 4 1 6 3

    for i in 0..100 {
        directories.add(
            memory_manager.clone(),
            &(String::from("key") + &i.to_string()),
            &(String::from("value") + &i.to_string()),
        );
    }
    for i in 0..100 {
        let value = directories.get(&(String::from("key") + &i.to_string()));
        println!("{:?}", value);
    }
}
