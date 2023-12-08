use crate::cfg::config::CONFIG;

use super::numa::Numa;
use std::{
    borrow::BorrowMut,
    ptr::null,
    sync::{atomic::fence, Arc, Mutex},
};

extern "C" {
    pub fn memcpy(dst: *mut u8, src: *const u8, bytes: usize);
}

struct Free {
    ptr: *mut u8,
    size: usize,
    next: Option<Arc<Mutex<Free>>>,
}

pub struct Page {
    used_size: usize,
    tot_size: usize,
    start_ptr: *mut u8,
    free_list: Option<Arc<Mutex<Free>>>,
}

pub struct MemoryManager {
    pub pages: Vec<Page>,
}

impl MemoryManager {
    pub fn new() -> MemoryManager {
        MemoryManager { pages: Vec::new() }
    }

    fn alloc_new_page(&mut self, num: usize) {
        let ptr = Numa::numa_alloc_onnode(num * CONFIG.page_size, 0);
        if CONFIG.enable_mm_debug {
            print!("alloc new page: {}\n", num);
        }
        for i in 0..num * CONFIG.page_size {
            unsafe {
                (*ptr.offset(i as isize)) = 0;
            }
        }
        self.pages.push(Page {
            used_size: 0,
            tot_size: num * CONFIG.page_size,
            start_ptr: ptr,
            free_list: Some(Arc::new(Mutex::new(Free {
                ptr,
                size: num * CONFIG.page_size,
                next: None,
            }))),
        });
    }

    pub fn find_from_alloced_pages(&mut self, mut size: usize) -> Option<*mut u8> {
        size = (size & (!(CONFIG.align_bytes - 1)))
            + ((size & (CONFIG.align_bytes - 1)) != 0) as usize * CONFIG.align_bytes;
        for page in self.pages.iter_mut() {
            if page.tot_size - page.used_size >= size {
                if CONFIG.enable_mm_debug {
                    print!(
                        "find from alloced pages: size: {}, tot_size: {}, used_size: {}\n",
                        size, page.tot_size, page.used_size
                    );
                }
                let mut free_list = page.free_list.clone();
                let mut free = free_list.unwrap().clone();
                let mut prev = free.clone();
                while free.lock().unwrap().size < size {
                    prev = free.clone();
                    if free.lock().unwrap().next.is_none() {
                        break;
                    }
                    let tmp = free.clone();
                    free = tmp.lock().unwrap().next.clone().unwrap();
                }
                if CONFIG.enable_mm_debug {
                    print!("get free: {}\n", free.lock().unwrap().size);
                }
                if free.lock().unwrap().size < size {
                    continue;
                }
                if free.lock().unwrap().size == size {
                    let new_next = free.lock().unwrap().next.clone();
                    prev.lock().unwrap().next = new_next;
                    page.used_size += size;
                    return Some(free.lock().unwrap().ptr);
                } else {
                    let new_ptr = unsafe { free.clone().lock().unwrap().ptr.offset(size as isize) };
                    let new_size = free.clone().lock().unwrap().size - size;
                    let new_next = free.clone().lock().unwrap().next.clone();
                    let new_free = Arc::new(Mutex::new(Free {
                        ptr: new_ptr,
                        size: new_size,
                        next: new_next,
                    }));
                    page.free_list = Some(new_free.clone());
                    prev.lock().unwrap().next = Some(new_free);
                    page.used_size += size;
                    return Some(free.lock().unwrap().ptr);
                }
            }
        }
        None
    }

    pub fn merge(page: &mut Page) {
        let mut free_list = page.free_list.clone();
        let mut free = free_list.unwrap().clone();
        let mut prev = free.clone();
        while free.lock().unwrap().next.is_some() {
            let tmp = free.clone();
            free = tmp.lock().unwrap().next.clone().unwrap();
            let prev_size = prev.lock().unwrap().size;
            if free.lock().unwrap().ptr
                == unsafe { prev.lock().unwrap().ptr.offset(prev_size as isize) }
            {
                prev.lock().unwrap().size += free.lock().unwrap().size;
                prev.lock().unwrap().next = free.lock().unwrap().next.clone();
            } else {
                prev = free.clone();
            }
        }
    }

    pub fn insert(&mut self, ptr: *mut u8, size: usize) {
        for page in self.pages.iter_mut() {
            if page.start_ptr <= ptr
                && ptr < unsafe { page.start_ptr.offset(page.tot_size as isize) }
            {
                if CONFIG.enable_mm_debug {
                    print!(
                        "find chunk: size: {}, tot_size: {}, used_size: {}\n",
                        size, page.tot_size, page.used_size
                    );
                }
                let mut free_list = page.free_list.clone();
                let mut free = free_list.unwrap().clone();
                let mut prev = free.clone();
                let mut is_head = true;
                while free.lock().unwrap().ptr < ptr {
                    is_head = false;
                    prev = free.clone();
                    if free.lock().unwrap().next.is_none() {
                        break;
                    }
                    let tmp = free.clone();
                    free = tmp.lock().unwrap().next.clone().unwrap();
                }
                // insert new free
                if is_head {
                    let new_next = free.clone();
                    let new_free = Arc::new(Mutex::new(Free {
                        ptr,
                        size,
                        next: Some(new_next),
                    }));
                    page.free_list = Some(new_free.clone());
                } else {
                    let new_next = prev.lock().unwrap().next.clone();
                    let new_free = Arc::new(Mutex::new(Free {
                        ptr,
                        size,
                        next: new_next,
                    }));
                    prev.lock().unwrap().next = Some(new_free.clone());
                }
                page.used_size -= size;
                // merge
                MemoryManager::merge(page);
                return;
            }
        }
    }

    pub fn malloc(&mut self, size: usize) -> *mut u8 {
        let mut ptr = self.find_from_alloced_pages(size);
        if CONFIG.enable_mm_debug {
            print!("malloc: {} find: {}\n", size, ptr.is_some());
        }
        if ptr.is_none() {
            self.alloc_new_page(size / CONFIG.page_size + (size % CONFIG.page_size > 0) as usize);
            ptr = self.find_from_alloced_pages(size);
        }
        ptr.unwrap()
    }

    pub fn free(&mut self, ptr: *mut u8, size: usize) {
        self.insert(ptr, size);
    }
}
