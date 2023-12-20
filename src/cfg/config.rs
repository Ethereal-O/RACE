pub struct RaceConfig {
    pub enable_mm_debug: bool,
    pub bits_of_byte: usize,
    pub page_size: usize,
    pub align_bytes: usize,
    pub bucket_group_num: usize,
    pub bucket_num: usize,
    pub slot_num: usize,
    pub ptr_size: usize,
    pub fp_size: usize,
    pub slot_fp_offset: usize,
    pub slot_len_offset: usize,
    pub header_local_depth_offset: usize,
    pub header_suffix_offset: usize,
    pub directory_lock_offset: usize,
    pub directory_localdepth_offset: usize,
    pub max_entry_num: usize,
    pub entry_size: usize,
    pub max_try_lock_times: usize,
}

pub const CONFIG: RaceConfig = RaceConfig {
    enable_mm_debug: false,
    bits_of_byte: 8,
    page_size: 4096,
    align_bytes: 8,
    bucket_group_num: 1024,
    bucket_num: 3,
    slot_num: 7,
    ptr_size: 8,
    fp_size: 1,
    slot_fp_offset: 0,
    slot_len_offset: 1,
    header_local_depth_offset: 0,
    header_suffix_offset: 1,
    directory_lock_offset: 0,
    directory_localdepth_offset: 1,
    max_entry_num: 1 << 16,
    entry_size: 64,
    max_try_lock_times: 5,
};
