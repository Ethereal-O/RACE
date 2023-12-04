mod numa;
mod race;
mod cfg;

use race::race_type::Directories;
use numa::mm::MemoryManager;
use std::sync::{Arc, Mutex};

fn main() {
    let memory_manager = Arc::new(Mutex::new(MemoryManager::new()));
    let mut directories = Directories::new();

    directories.add_directory(memory_manager.clone());

    directories.add_directory(memory_manager.clone());

    // directories.deref_directories();

    directories.get(0).get_subtable().bucket_groups[0].buckets[0].slots[0].set_length(10);

    print!("{}", directories.get(0).get_subtable().bucket_groups[0].buckets[0].slots[0].get_length());

    // print!("{}", directories.get(0).get_subtable().bucket_groups[0].buckets[0].header.data);
}
