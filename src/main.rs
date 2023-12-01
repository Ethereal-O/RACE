mod numa;
mod race;
mod cfg;

use numa::numa::Numa;

fn main() {
    unsafe {
        // alloc a array on node 0 by numa_alloc_onnode
        let ptr = Numa::numa_alloc_onnode(10, 0);
        if ptr.is_null() {
            println!("alloc failed");
        } else {
            println!("alloc success");
        }
        // assign value to array
        for i in 0..4096 {
            *ptr.offset(i as isize) = (i % 128) as u8;
        }
        // print the value
        for i in 0..4096 {
            assert_eq!(*ptr.offset(i as isize), (i % 128) as u8);
        }
        // free the array
        Numa::numa_free(ptr, 10);
    }
}
