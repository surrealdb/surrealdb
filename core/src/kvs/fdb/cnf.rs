use std::sync::LazyLock;

pub static FOUNDATIONDB_TRANSACTION_TIMEOUT: LazyLock<i32> =
	lazy_env_parse_or_else!("SURREAL_FOUNDATIONDB_TRANSACTION_TIMEOUT", i32, |_| { 5000 });

pub static FOUNDATIONDB_TRANSACTION_RETRY_LIMIT: LazyLock<i32> =
	lazy_env_parse_or_else!("SURREAL_FOUNDATIONDB_TRANSACTION_RETRY_LIMIT", i32, |_| { 5 });

pub static FOUNDATIONDB_TRANSACTION_MAX_RETRY_DELAY: LazyLock<i32> =
	lazy_env_parse_or_else!("SURREAL_FOUNDATIONDB_TRANSACTION_MAX_RETRY_DELAY", i32, |_| { 500 });
