#[cfg(feature = "cli")]
use crate::parsers::parse_bytes_usize;

const DEFAULT_API_VERSION: u8 = 1;
const DEFAULT_REQUEST_TIMEOUT: u64 = 10;
const DEFAULT_ASYNC_COMMIT: bool = true;
const DEFAULT_ONE_PHASE_COMMIT: bool = true;
const DEFAULT_GRPC_MAX_DECODING_MESSAGE_SIZE: usize = 4 * 1024 * 1024;

#[derive(Debug, Clone)]
#[cfg_attr(feature = "cli", derive(clap::Args))]
pub struct TiKvEngineConfig {
	/// Which TiKV cluster API version to use
	#[cfg_attr(feature = "cli", arg(
		env = "SURREAL_TIKV_API_VERSION",
		long = "tikv-api-version",
		default_value_t = DEFAULT_API_VERSION,
		hide = true,
	))]
	pub api_version: u8,
	/// The keyspace identifier for data isolation
	#[cfg_attr(
		feature = "cli",
		arg(env = "SURREAL_TIKV_KEYSPACE", long = "tikv-keyspace", hide = true,)
	)]
	pub keyspace: Option<String>,
	/// The duration for requests before they timeout in seconds
	#[cfg_attr(feature = "cli", arg(
		env = "SURREAL_TIKV_REQUEST_TIMEOUT",
		long = "tikv-request-timeout",
		default_value_t = DEFAULT_REQUEST_TIMEOUT,
		hide = true,
	))]
	pub request_timeout: u64,
	/// Whether to use asynchronous transaction commit
	#[cfg_attr(feature = "cli", arg(
		env = "SURREAL_TIKV_ASYNC_COMMIT",
		long = "tikv-async-commit",
		default_value_t = DEFAULT_ASYNC_COMMIT,
		hide = true,
	))]
	pub async_commit: bool,
	/// Whether to use one-phase transaction commit
	#[cfg_attr(feature = "cli", arg(
		env = "SURREAL_TIKV_ONE_PHASE_COMMIT",
		long = "tikv-one-phase-commit",
		default_value_t = DEFAULT_ONE_PHASE_COMMIT,
		hide = true,
	))]
	pub one_phase_commit: bool,
	/// Limits the maximum size of a decoded gRPC message
	#[cfg_attr(feature = "cli", arg(
		env = "SURREAL_TIKV_GRPC_MAX_DECODING_MESSAGE_SIZE",
		long = "tikv-grpc-max-decoding-message-size",
		default_value_t = DEFAULT_GRPC_MAX_DECODING_MESSAGE_SIZE,
		hide = true,
		value_parser = parse_bytes_usize,
	))]
	pub grpc_max_decoding_message_size: usize,
}

impl Default for TiKvEngineConfig {
	fn default() -> Self {
		Self {
			api_version: DEFAULT_API_VERSION,
			keyspace: None,
			request_timeout: DEFAULT_REQUEST_TIMEOUT,
			async_commit: DEFAULT_ASYNC_COMMIT,
			one_phase_commit: DEFAULT_ONE_PHASE_COMMIT,
			grpc_max_decoding_message_size: DEFAULT_GRPC_MAX_DECODING_MESSAGE_SIZE,
		}
	}
}
