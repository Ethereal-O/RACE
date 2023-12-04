#[link(name = "numa")]
extern "C" {
    pub fn numa_available() -> i32;
    pub fn numa_alloc_onnode(size: usize, node: i32) -> *mut u8;
    pub fn numa_free(ptr: *mut u8, size: usize);
}

pub struct Numa {}

impl Numa {
    pub fn numa_available() -> i32 {
        unsafe {
            numa_available()
        }
    }

    pub fn numa_alloc_onnode(size: usize, node: i32) -> *mut u8 {
        unsafe {
            numa_alloc_onnode(size, node)
        }
    }

    pub fn numa_free(ptr: *mut u8, size: usize) {
        unsafe {
            numa_free(ptr, size)
        }
    }

    
}