pub const NORMAL_BATCH_SIZE: u32 = 500;
pub const INDEXING_BATCH_SIZE: u32 = 250;
pub const COUNT_BATCH_SIZE: u32 = 50_000;

/// The estimated bytes per key.
pub const ESTIMATED_BYTES_PER_KEY: u32 = 128;
/// The estimated bytes per key-value entry.
pub const ESTIMATED_BYTES_PER_KV: u32 = 512;
