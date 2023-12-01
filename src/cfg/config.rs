pub struct RaceConfig {
    pub bucket_group_num: usize,
    pub bucket_num: usize,
    pub slot_num: usize,
    pub ptr_size: usize,
    pub slot_fp_offset: usize,
    pub slot_len_offset: usize,
}

pub const CONFIG: RaceConfig = RaceConfig {
    bucket_group_num: 4,
    bucket_num: 3,
    slot_num: 7,
    ptr_size: 8,
    slot_fp_offset: 0,
    slot_len_offset: 1,
};