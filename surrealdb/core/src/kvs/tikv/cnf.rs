use std::sync::LazyLock;

/// Which TiKV cluster API version to use
pub(super) static TIKV_API_VERSION: LazyLock<u8> =
	lazy_env_parse!("SURREAL_TIKV_API_VERSION", u8, 1);

/// The keyspace identifier for data isolation
pub(super) static TIKV_KEYSPACE: LazyLock<Option<String>> =
	lazy_env_parse!("SURREAL_TIKV_KEYSPACE", Option<String>);

/// The duration for requests before they timeout in seconds
pub(super) static TIKV_REQUEST_TIMEOUT: LazyLock<u64> =
	lazy_env_parse!("SURREAL_TIKV_REQUEST_TIMEOUT", u64, 10);

/// Whether to use asynchronous transaction commit
pub(super) static TIKV_ASYNC_COMMIT: LazyLock<bool> =
	lazy_env_parse!("SURREAL_TIKV_ASYNC_COMMIT", bool, true);

/// Whether to use one-phase transaction commit
pub(super) static TIKV_ONE_PHASE_COMMIT: LazyLock<bool> =
	lazy_env_parse!("SURREAL_TIKV_ONE_PHASE_COMMIT", bool, true);

/// Limits the maximum size of a decoded message - default value to 4MB
pub(super) static TIKV_GRPC_MAX_DECODING_MESSAGE_SIZE: LazyLock<usize> =
	lazy_env_parse!("SURREAL_TIKV_GRPC_MAX_DECODING_MESSAGE_SIZE", usize, 4 * 1024 * 1024);
