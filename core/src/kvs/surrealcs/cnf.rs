use std::sync::LazyLock;

pub static SURREALCS_CONNECTION_POOL_SIZE: LazyLock<i32> =
	lazy_env_parse_or_else!("SURREAL_SURREALCS_CONNECTION_POOL_SIZE", i32, |_| num_cpus::get()
		as i32);
