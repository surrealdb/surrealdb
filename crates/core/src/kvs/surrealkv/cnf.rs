use std::sync::LazyLock;

pub static SURREALKV_MAX_VALUE_SIZE: LazyLock<u64> =
	lazy_env_parse!("SURREAL_SURREALKV_MAX_VALUE_SIZE", u64, 1024 * 1024);
