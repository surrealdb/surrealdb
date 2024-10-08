use std::sync::LazyLock;

pub static SURREALCS_CONNECTION_POOL_SIZE: LazyLock<usize> =
	lazy_env_parse_or_else!("SURREAL_SURREALCS_CONNECTION_POOL_SIZE", usize, |_| num_cpus::get());
