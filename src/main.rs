#![allow(unused)]
mod cfg;
mod numa;
mod race;

use cfg::config::CONFIG;
use numa::mm::MemoryManager;
use race::common::kvblock::KVBlockMem;
use race::computepool::client::Client;
use race::computepool::directory::{ClientDirectory, ClientEntry};
use race::mempool;
use race::mempool::mempool::MemPool;
use race::mempool::{directory, subtable::Bucket};
use std::mem::size_of;
use std::sync::RwLock;
use std::vec;
use std::{
    ops::Deref,
    sync::{Arc, Mutex},
};

use crate::{numa::numa::numa_free, race::common::hash};

fn print_dir_depth(directory: &Client, dir_index: usize) {
    print!(
        "{} dir depth: {}\n",
        dir_index,
        directory
            .get_directory()
            .get_entry_const(dir_index)
            .get_local_depth()
    );
}

fn print_slot_depth(directory: &Client, dir_index: usize) {
    print!(
        "{} slot depth: {}\n",
        dir_index,
        directory
            .get_mempool()
            .read()
            .unwrap()
            .get_entry(dir_index)
            .get_subtable()
            .bucket_groups[0]
            .buckets[0]
            .header
            .get_local_depth()
    );
}

fn print_slot_suffix(directory: &Client, dir_index: usize) {
    print!(
        "{} slot suffix: {}\n",
        dir_index,
        directory
            .get_mempool()
            .read()
            .unwrap()
            .get_entry(dir_index)
            .get_subtable()
            .bucket_groups[0]
            .buckets[0]
            .header
            .get_suffix()
    );
}

fn print_all(directory: &Client) {
    print!("##########################\n");
    let dir_num = directory.pub_get_size();
    for i in 0..dir_num {
        print_dir_depth(directory, i);
        print_slot_depth(directory, i);
        print_slot_suffix(directory, i);
    }
}

// // This function can be used to implement "Insert, Update and Delete"!
// fn test_insert(
//     directory: &mut directory::Directory,
//     bias: i32,
//     memory_manager: &Arc<Mutex<MemoryManager>>,
// ) {
//     for i in 0..100 {
//         let key = String::from("key") + &i.to_string();
//         let value = String::from("value") + &(i + bias).to_string();
//         let kv_block = KVBlockMem::new(&key, &value, memory_manager.clone());
//         let fp = hash::Hash::hash(&key, 3) as u8;
//         match directory.get(
//             0,
//             hash::Hash::hash(&key, 1) as usize,
//             hash::Hash::hash(&key, 2) as usize,
//         ) {
//             Some(v) => {
//                 let (flag1, pos1, data1) = v[0].check_and_count(&key, fp);
//                 if flag1 || pos1 / CONFIG.slot_num < 2 {
//                     if !directory.set(
//                         0,
//                         hash::Hash::hash(&key, 1) as usize,
//                         pos1 / CONFIG.slot_num,
//                         pos1 % CONFIG.slot_num,
//                         utils::set_data(
//                             fp,
//                             (size_of::<KVBlockMem>() + key.len() + value.len()) as u8,
//                             kv_block as u64,
//                         ),
//                         data1,
//                     ) {
//                         panic!("Insert Error!");
//                     } else {
//                         continue;
//                     }
//                 }

//                 let (flag2, pos2, data2) = v[1].check_and_count(&key, fp);
//                 if flag2 || pos1 / CONFIG.slot_num < 2 {
//                     if !directory.set(
//                         0,
//                         hash::Hash::hash(&key, 2) as usize,
//                         pos2 / CONFIG.slot_num,
//                         pos2 % CONFIG.slot_num,
//                         utils::set_data(
//                             fp,
//                             (size_of::<KVBlockMem>() + key.len() + value.len()) as u8,
//                             kv_block as u64,
//                         ),
//                         data2,
//                     ) {
//                         panic!("Insert Error!");
//                     }
//                 } else {
//                     panic!("No more slot for insert!");
//                 }
//             }
//             None => panic!("Should not be here!"),
//         }
//     }
// }

fn test_mm() {
    let memory_manager = Arc::new(Mutex::new(MemoryManager::new()));

    let mut vec: Vec<*mut u8> = Vec::new();

    for i in 0..256 {
        let ptr = memory_manager
            .lock()
            .unwrap()
            .malloc(size_of::<KVBlockMem>());
        vec.push(ptr);
    }

    for i in 0..128 {
        let ptr = vec[(i * 31) % vec.len()];
        memory_manager
            .lock()
            .unwrap()
            .free(ptr, size_of::<KVBlockMem>());
        vec.remove((i * 31) % vec.len());
    }

    for i in 0..128 {
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

pub fn test_client() {
    let mempool = Arc::new(RwLock::new(MemPool::new()));
    let mut client = Client::new(mempool.clone());

    // init value
    print_all(&mut client);

    // // rehash first subtable
    // directory.rehash(memory_manager.clone(), 0);
    // print_all(&mut directory);

    // rehash first subtable
    client.pub_rehash(0);
    print_all(&mut client);

    // rehash second subtable
    client.pub_rehash(1);
    print_all(&mut client);

    // rehash third subtable
    client.pub_rehash(2);
    print_all(&mut client);

    // rehash third subtable
    client.pub_rehash(2);
    print_all(&mut client);

    // rehash first subtable
    client.pub_rehash(0);
    print_all(&mut client);

    // 0 1 2 3 4 1 6 3 0 1 10 3 4 1 6 3
}

pub fn test_id() {
    let mempool = Arc::new(RwLock::new(MemPool::new()));
    let mut client = Client::new(mempool.clone());
    let mut vec: Vec<i32> = Vec::new();
    for i in 0..60000 {
        let random_v = rand::random::<i32>() % 60000;
        vec.push(random_v);
        client.insert(
            &(String::from("key") + &random_v.to_string()),
            &(String::from("val") + &random_v.to_string()),
        );
    }
    // let mut i = 0;
    // while i < 100 {
    //     client.delete(&(String::from("key") + &i.to_string()));
    //     i += 2;
    // }
    for i in 0..60000 {
        if let Some(v) = client.search(&(String::from("key") + &vec[i].to_string())) {
            assert_eq!(v, String::from("val") + &vec[i].to_string());
        }
    }
    // i = 0;
    // while i < 100 {
    //     client.insert(
    //         &(String::from("key") + &i.to_string()),
    //         &(String::from("val") + &i.to_string()),
    //     );
    //     i += 2;
    // }
    // for i in 0..100 {
    //     if let Some(v) = client.search(&(String::from("key") + &i.to_string())) {
    //         println!("{}", v);
    //     }
    // }
}

fn main() {
    test_id();
}
