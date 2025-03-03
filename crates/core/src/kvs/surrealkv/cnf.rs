use std::sync::{LazyLock, OnceLock};

pub static SYNC_DATA: LazyLock<bool> = lazy_env_parse!("SURREAL_SYNC_DATA", bool, false);

pub static SURREALKV_MAX_VALUE_THRESHOLD: LazyLock<usize> =
	lazy_env_parse!("SURREAL_SURREALKV_MAX_VALUE_THRESHOLD", usize, 64);

pub static SURREALKV_MAX_SEGMENT_SIZE: LazyLock<u64> =
	lazy_env_parse!("SURREAL_SURREALKV_MAX_SEGMENT_SIZE", u64, 1 << 29);

pub static SURREALKV_MAX_VALUE_CACHE_SIZE: LazyLock<u64> =
	lazy_env_parse!("SURREAL_SURREALKV_MAX_VALUE_CACHE_SIZE", u64, 10000);

pub(crate) static SKV_COMMIT_POOL: OnceLock<affinitypool::Threadpool> = OnceLock::new();

pub(crate) fn commit_pool() -> &'static affinitypool::Threadpool {
	SKV_COMMIT_POOL.get_or_init(|| {
		affinitypool::Builder::new()
			.thread_name("surrealkv-commitpool")
			.thread_stack_size(5 * 1024 * 1024)
			.thread_per_core(false)
			.worker_threads(1)
			.build()
	})
}
