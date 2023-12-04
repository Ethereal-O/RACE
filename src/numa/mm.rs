use super::numa::Numa;
use std::sync::{Arc, Mutex};
struct WrapperU8Ptr(*mut u8);

struct Page {
    ptr: WrapperU8Ptr,
    used_size: usize,
}

pub struct MemoryManager {
    // pages : Vec<Page>
}

impl MemoryManager {
    pub fn new() -> MemoryManager {
        unsafe {
            MemoryManager {
                // pages: Vec::new(),
            }
        }
    }

    fn alloc_new_page(&mut self) {
        let ptr = Numa::numa_alloc_onnode(4096, 0);
    }

    pub fn malloc(&mut self, size: usize) -> *mut u8 {
        if size > 4096 {
            return std::ptr::null_mut();
        }
        let ptr = Numa::numa_alloc_onnode(size, 0);
        for i in 0..size {
            unsafe {
                (*ptr.offset(i as isize)) = 0;
            }
        }
        print!("malloc {}: {:p}\n", size, ptr);
        ptr as *mut u8
    }
}
