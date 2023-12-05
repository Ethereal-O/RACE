use super::numa::Numa;
use std::sync::{Arc, Mutex};
struct WrapperU8Ptr(*mut u8);

extern "C" {
    pub fn memcpy(dst: *mut u8, src: *const u8, bytes: usize);
}

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
