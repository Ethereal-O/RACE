pub struct RaceConfig {
    pub bits_of_byte: usize,
    pub bucket_group_num: usize,
    pub bucket_num: usize,
    pub slot_num: usize,
    pub ptr_size: usize,
    pub slot_fp_offset: usize,
    pub slot_len_offset: usize,
    pub header_local_depth_offset: usize,
    pub header_suffix_offset: usize,
    pub directory_lock_offset: usize,
    pub directory_localdepth_offset: usize,
}

pub const CONFIG: RaceConfig = RaceConfig {
    bits_of_byte: 8,
    bucket_group_num: 4,
    bucket_num: 3,
    slot_num: 7,
    ptr_size: 8,
    slot_fp_offset: 0,
    slot_len_offset: 1,
    header_local_depth_offset: 0,
    header_suffix_offset: 1,
    directory_lock_offset: 0,
    directory_localdepth_offset: 1,
};