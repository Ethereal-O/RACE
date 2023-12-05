#![allow(unused)]
mod cfg;
mod numa;
mod race;

use numa::mm::MemoryManager;
use race::race_type::{Directories, KVBlockMem};
use std::{
    ops::Deref,
    sync::{Arc, Mutex},
};

use crate::numa::numa::numa_free;
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
    let mut directories = Directories::new(memory_manager.clone());

    print!("{}\n", directories.get(0).get_local_depth());
    print!("{}\n", directories.get(0).get_subtable().bucket_groups[0].buckets[0].header.get_local_depth());
    print!("{}\n", directories.get(0).get_subtable().bucket_groups[0].buckets[0].header.get_suffix());

    directories.rehash(memory_manager, 0);

    print!("{}\n", directories.get(0).get_local_depth());
    print!("{}\n", directories.get(1).get_local_depth());
    print!("{}\n", directories.get(0).get_subtable().bucket_groups[0].buckets[0].header.get_local_depth());
    print!("{}\n", directories.get(1).get_subtable().bucket_groups[0].buckets[0].header.get_local_depth());
    print!("{}\n", directories.get(0).get_subtable().bucket_groups[0].buckets[0].header.get_suffix());
    print!("{}\n", directories.get(1).get_subtable().bucket_groups[0].buckets[0].header.get_suffix());

    // directories.add_directory(memory_manager.clone());

    // directories.add_directory(memory_manager.clone());

    // // directories.deref_directories();

    // directories.get(0).get_subtable().bucket_groups[0].buckets[0].slots[0].set_length(10);

    // print!(
    //     "{}",
    //     directories.get(0).get_subtable().bucket_groups[0].buckets[0].slots[0].get_length()
    // );

    // // print!("{}", directories.get(0).get_subtable().bucket_groups[0].buckets[0].header.data);
}
