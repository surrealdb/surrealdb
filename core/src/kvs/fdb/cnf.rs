use foundationdb::options::DatabaseOption;
use once_cell::sync::Lazy;

pub static FOUNDATIONDB_TRANSACTION_TIMEOUT: Lazy<DatabaseOption::TransactionTimeout> =
	lazy_env_parse_or_else!("SURREAL_FOUNDATIONDB_TRANSACTION_TIMEOUT", DatabaseOption, |_| {
		"5000"
	});

pub static FOUNDATIONDB_TRANSACTION_RETRY_LIMIT: Lazy<DatabaseOption::TransactionRetryLimit> =
	lazy_env_parse_or_else!("SURREAL_FOUNDATIONDB_TRANSACTION_RETRY_LIMIT", DatabaseOption, |_| {
		"5"
	});

pub static FOUNDATIONDB_TRANSACTION_MAX_RETRY_DELAY: Lazy<
	DatabaseOption::TransactionMaxRetryDelay,
> = lazy_env_parse_or_else!(
	"SURREAL_FOUNDATIONDB_TRANSACTION_MAX_RETRY_DELAY",
	DatabaseOption,
	|_| { "500" }
);
