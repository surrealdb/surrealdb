use std::sync::LazyLock;

pub static SURREALCS_CONNECTION_POOL_SIZE: LazyLock<usize> =
	lazy_env_parse!("SURREAL_SURREALCS_CONNECTION_POOL_SIZE", usize, || num_cpus::get());
