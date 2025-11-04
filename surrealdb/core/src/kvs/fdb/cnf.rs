use std::sync::LazyLock;

/// The maximum transaction timeout in milliseconds
pub(super) static FOUNDATIONDB_TRANSACTION_TIMEOUT: LazyLock<i32> =
	lazy_env_parse!("SURREAL_FOUNDATIONDB_TRANSACTION_TIMEOUT", i32, 5000);

/// The maximum number of times a transaction can be retried
pub(super) static FOUNDATIONDB_TRANSACTION_RETRY_LIMIT: LazyLock<i32> =
	lazy_env_parse!("SURREAL_FOUNDATIONDB_TRANSACTION_RETRY_LIMIT", i32, 5);

/// The maximum delay between transaction retries in milliseconds
pub(super) static FOUNDATIONDB_TRANSACTION_MAX_RETRY_DELAY: LazyLock<i32> =
	lazy_env_parse!("SURREAL_FOUNDATIONDB_TRANSACTION_MAX_RETRY_DELAY", i32, 500);
