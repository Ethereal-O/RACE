#![allow(unused)]
mod cfg;
mod numa;
mod race;

use numa::mm::MemoryManager;
use race::kvblock::KVBlockMem;
use race::{directory, kvblock};
use std::mem::size_of;
use std::{
    ops::Deref,
    sync::{Arc, Mutex},
};

use crate::{numa::numa::numa_free, race::hash};

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
    // let mut directory = directory::Directory::new(memory_manager.clone());

    // // init value
    // print_all(&mut directory);

    // // rehash first subtable
    // directory.rehash(memory_manager.clone(), 0);
    // print_all(&mut directory);

    // // rehash first subtable
    // directory.rehash(memory_manager.clone(), 0);
    // print_all(&mut directory);

    // // rehash second subtable
    // directory.rehash(memory_manager.clone(), 1);
    // print_all(&mut directory);

    // // rehash third subtable
    // directory.rehash(memory_manager.clone(), 2);
    // print_all(&mut directory);

    // // rehash third subtable
    // directory.rehash(memory_manager.clone(), 2);
    // print_all(&mut directory);

    // // rehash first subtable
    // directory.rehash(memory_manager.clone(), 0);
    // print_all(&mut directory);

    // // 0 1 2 3 4 1 6 3 0 1 10 3 4 1 6 3

    // for i in 0..100 {
    //     directory.add(
    //         memory_manager.clone(),
    //         &(String::from("key") + &i.to_string()),
    //         &(String::from("value") + &i.to_string()),
    //     );
    // }
    // for i in 0..100 {
    //     let key = String::from("key") + &i.to_string();
    //     let fp = hash::Hash::hash(&key, 3) as u8;
    //     match directory.get(
    //         0, // test only
    //         hash::Hash::hash(&key, 1) as usize,
    //         hash::Hash::hash(&key, 2) as usize,
    //     ) {
    //         Some(v) => {
    //             let mut flag = false;
    //             for slot in v[0].main_bucket.slots.iter() {
    //                 //println!("{} {}", slot.get_fingerprint(), fp);
    //                 if slot.data == 0 {
    //                     break;
    //                 } else {
    //                     if slot.get_fingerprint() == fp {
    //                         let kv_pointer = slot.get_kv_pointer();
    //                         let kv = unsafe { (*(kv_pointer as *mut KVBlockMem)).get() };
    //                         if kv.key == key {
    //                             println!("{:?}", Some(kv.value));
    //                             flag = true;
    //                             break;
    //                         }
    //                     }
    //                 }
    //             }
    //             if flag == true {
    //                 continue;
    //             }

    //             for slot in v[0].overflow_bucket.slots.iter() {
    //                 //println!("{} {}", slot.get_fingerprint(), fp);
    //                 if slot.data == 0 {
    //                     break;
    //                 } else {
    //                     if slot.get_fingerprint() == fp {
    //                         let kv_pointer = slot.get_kv_pointer();
    //                         let kv = unsafe { (*(kv_pointer as *mut KVBlockMem)).get() };
    //                         if kv.key == key {
    //                             println!("{:?}", Some(kv.value));
    //                             flag = true;
    //                             break;
    //                         }
    //                     }
    //                 }
    //             }
    //             if flag == true {
    //                 continue;
    //             }

    //             for slot in v[1].main_bucket.slots.iter() {
    //                 //println!("{} {}", slot.get_fingerprint(), fp);
    //                 if slot.data == 0 {
    //                     break;
    //                 } else {
    //                     if slot.get_fingerprint() == fp {
    //                         let kv_pointer = slot.get_kv_pointer();
    //                         let kv = unsafe { (*(kv_pointer as *mut KVBlockMem)).get() };
    //                         if kv.key == key {
    //                             println!("{:?}", Some(kv.value));
    //                             flag = true;
    //                             break;
    //                         }
    //                     }
    //                 }
    //             }
    //             if flag == true {
    //                 continue;
    //             }

    //             for slot in v[1].overflow_bucket.slots.iter() {
    //                 //println!("{} {}", slot.get_fingerprint(), fp);
    //                 if slot.data == 0 {
    //                     break;
    //                 } else {
    //                     if slot.get_fingerprint() == fp {
    //                         let kv_pointer = slot.get_kv_pointer();
    //                         let kv = unsafe { (*(kv_pointer as *mut KVBlockMem)).get() };
    //                         if kv.key == key {
    //                             println!("{:?}", Some(kv.value));
    //                             flag = true;
    //                             break;
    //                         }
    //                     }
    //                 }
    //             }
    //             if flag == false {
    //                 panic!("Not Found!");
    //             }
    //         }
    //         None => panic!("Not Found!"),
    //     }
    // }

    let mut vec: Vec<*mut u8> = Vec::new();

    for i in 0..256 {
        let ptr = memory_manager
            .lock()
            .unwrap()
            .malloc(size_of::<KVBlockMem>());
        vec.push(ptr);
    }

    for i in 0..vec.len() {
        let ptr = vec[(i * 31) % vec.len()];
        memory_manager
            .lock()
            .unwrap()
            .free(ptr, size_of::<KVBlockMem>());
    }
    let ptr = memory_manager.lock().unwrap().malloc(4096);

    // print!("{}\n", memory_manager.lock().unwrap().pages.len());
}
