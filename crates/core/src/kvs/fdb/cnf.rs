use once_cell::sync::Lazy;

pub static FOUNDATIONDB_TRANSACTION_TIMEOUT: Lazy<i32> =
	lazy_env_parse_or_else!("SURREAL_FOUNDATIONDB_TRANSACTION_TIMEOUT", i32, |_| { 5000 });

pub static FOUNDATIONDB_TRANSACTION_RETRY_LIMIT: Lazy<i32> =
	lazy_env_parse_or_else!("SURREAL_FOUNDATIONDB_TRANSACTION_RETRY_LIMIT", i32, |_| { 5 });

pub static FOUNDATIONDB_TRANSACTION_MAX_RETRY_DELAY: Lazy<i32> =
	lazy_env_parse_or_else!("SURREAL_FOUNDATIONDB_TRANSACTION_MAX_RETRY_DELAY", i32, |_| { 500 });
