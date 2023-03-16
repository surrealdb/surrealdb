pub(super) mod docids;
pub(super) mod ft;
mod kvsim;

const MAX_PARTITION_SIZE: usize = 5 * 1024 * 1024; // 5MB
const _HALF_PARTITION_SIZE: usize = MAX_PARTITION_SIZE / 2;
