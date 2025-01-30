use std::sync::LazyLock;

pub static SYNC_DATA: LazyLock<bool> = lazy_env_parse!("SURREAL_SYNC_DATA", bool, false);

pub static SURREALKV_MAX_VALUE_THRESHOLD: LazyLock<usize> =
	lazy_env_parse!("SURREAL_SURREALKV_MAX_VALUE_THRESHOLD", usize, 64);

pub static SURREALKV_MAX_SEGMENT_SIZE: LazyLock<u64> =
	lazy_env_parse!("SURREAL_SURREALKV_MAX_SEGMENT_SIZE", u64, 1 << 29);

pub static SURREALKV_MAX_VALUE_CACHE_SIZE: LazyLock<u64> =
	lazy_env_parse!("SURREAL_SURREALKV_MAX_VALUE_CACHE_SIZE", u64, 10000);
