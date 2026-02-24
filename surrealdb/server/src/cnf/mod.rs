use std::env;
use std::sync::LazyLock;
use std::time::Duration;

use surrealdb_core::lazy_env_parse;

/// The logo of the SurrealDB server
pub const LOGO: &str = "
 .d8888b.                                             888 8888888b.  888888b.
d88P  Y88b                                            888 888  'Y88b 888  '88b
Y88b.                                                 888 888    888 888  .88P
 'Y888b.   888  888 888d888 888d888  .d88b.   8888b.  888 888    888 8888888K.
    'Y88b. 888  888 888P'   888P'   d8P  Y8b     '88b 888 888    888 888  'Y88b
      '888 888  888 888     888     88888888 .d888888 888 888    888 888    888
Y88b  d88P Y88b 888 888     888     Y8b.     888  888 888 888  .d88P 888   d88P
 'Y8888P'   'Y88888 888     888      'Y8888  'Y888888 888 8888888P'  8888888P'

";

/// The development build command-line warning
#[cfg(debug_assertions)]
pub const DEBUG_BUILD_WARNING: &str = "\
┌─────────────────────────────────────────────────────────────────────────────┐
│                     !!! THIS IS A DEVELOPMENT BUILD !!!                     │
│     Development builds are not intended for production use and include      │
│    tooling and features that may affect the performance of the database.    │
└─────────────────────────────────────────────────────────────────────────────┘";

/// The publicly visible name of the server
pub const PKG_NAME: &str = "surrealdb";

/// The public endpoint for the administration interface
pub const APP_ENDPOINT: &str = "https://surrealdb.com/surrealist";

/// How many concurrent network requests can be handled at once (default:
/// 1,048,576)
pub static NET_MAX_CONCURRENT_REQUESTS: LazyLock<usize> =
	lazy_env_parse!("SURREAL_NET_MAX_CONCURRENT_REQUESTS", usize, 1 << 20);

/// The maximum HTTP body size of the HTTP /ml endpoints (default: 4 GiB)
pub static HTTP_MAX_ML_BODY_SIZE: LazyLock<usize> =
	lazy_env_parse!(bytes, "SURREAL_HTTP_MAX_ML_BODY_SIZE", usize, 4 << 30);

/// The maximum HTTP body size of the HTTP /sql endpoint (default: 1 MiB)
pub static HTTP_MAX_SQL_BODY_SIZE: LazyLock<usize> =
	lazy_env_parse!(bytes, "SURREAL_HTTP_MAX_SQL_BODY_SIZE", usize, 1 << 20);

/// The maximum HTTP body size of the HTTP /api endpoint (default: 1 MiB)
pub static HTTP_MAX_API_BODY_SIZE: LazyLock<usize> =
	lazy_env_parse!(bytes, "SURREAL_HTTP_MAX_API_BODY_SIZE", usize, 4 << 20);

/// The maximum HTTP body size of the HTTP /rpc endpoint (default: 4 MiB)
pub static HTTP_MAX_RPC_BODY_SIZE: LazyLock<usize> =
	lazy_env_parse!(bytes, "SURREAL_HTTP_MAX_RPC_BODY_SIZE", usize, 4 << 20);

/// The maximum HTTP body size of the HTTP /key endpoints (default: 16 KiB)
pub static HTTP_MAX_KEY_BODY_SIZE: LazyLock<usize> =
	lazy_env_parse!(bytes, "SURREAL_HTTP_MAX_KEY_BODY_SIZE", usize, 16 << 10);

/// The maximum HTTP body size of the HTTP /signup endpoint (default: 1 KiB)
pub static HTTP_MAX_SIGNUP_BODY_SIZE: LazyLock<usize> =
	lazy_env_parse!(bytes, "SURREAL_HTTP_MAX_SIGNUP_BODY_SIZE", usize, 1 << 10);

/// The maximum HTTP body size of the HTTP /signin endpoint (default: 1 KiB)
pub static HTTP_MAX_SIGNIN_BODY_SIZE: LazyLock<usize> =
	lazy_env_parse!(bytes, "SURREAL_HTTP_MAX_SIGNIN_BODY_SIZE", usize, 1 << 10);

/// The maximum HTTP body size of the HTTP /import endpoint (default: 4 GiB)
pub static HTTP_MAX_IMPORT_BODY_SIZE: LazyLock<usize> =
	lazy_env_parse!(bytes, "SURREAL_HTTP_MAX_IMPORT_BODY_SIZE", usize, 4 << 30);

/// Specifies the frequency with which ping messages are sent to the client
pub const WEBSOCKET_PING_FREQUENCY: Duration = Duration::from_secs(5);

/// What is the maximum WebSocket message size (default: 128 MiB)
pub static WEBSOCKET_MAX_MESSAGE_SIZE: LazyLock<usize> =
	lazy_env_parse!(bytes, "SURREAL_WEBSOCKET_MAX_MESSAGE_SIZE", usize, 128 << 20);

/// The size of the read buffer for WebSocket connections (default: 128 KiB)
///
/// This controls how much data can be buffered when reading from WebSocket connections.
/// Larger values can improve performance for high-throughput connections but consume
/// more memory per connection. The value can be configured via the
/// `SURREAL_WEBSOCKET_READ_BUFFER_SIZE` environment variable.
pub static WEBSOCKET_READ_BUFFER_SIZE: LazyLock<usize> =
	lazy_env_parse!(bytes, "SURREAL_WEBSOCKET_READ_BUFFER_SIZE", usize, 128 * 1024);

/// The size of the write buffer for WebSocket connections (default: 128 KiB)
///
/// This controls how much data can be buffered when writing to WebSocket connections.
/// Larger values can improve performance for high-throughput connections but consume
/// more memory per connection. The value can be configured via the
/// `SURREAL_WEBSOCKET_WRITE_BUFFER_SIZE` environment variable.
pub static WEBSOCKET_WRITE_BUFFER_SIZE: LazyLock<usize> =
	lazy_env_parse!(bytes, "SURREAL_WEBSOCKET_WRITE_BUFFER_SIZE", usize, 128 * 1024);

/// The maximum write buffer size before backpressure is applied (default: unlimited)
///
/// When the write buffer reaches this size, the WebSocket connection will apply
/// backpressure to prevent memory exhaustion. By default, this is set to unlimited
/// (`usize::MAX`), but it can be configured via the
/// `SURREAL_WEBSOCKET_MAX_WRITE_BUFFER_SIZE` environment variable.
///
/// # Environment Variable
///
/// Set `SURREAL_WEBSOCKET_MAX_WRITE_BUFFER_SIZE` to configure this value. The value
/// must be greater than `WEBSOCKET_WRITE_BUFFER_SIZE` to be effective. If not set
/// or if the value is invalid, unlimited buffering is used.
pub static WEBSOCKET_MAX_WRITE_BUFFER_SIZE: LazyLock<usize> = LazyLock::new(|| {
	let buffer_size = || {
		let var = env::var("SURREAL_WEBSOCKET_MAX_WRITE_BUFFER_SIZE").ok()?;
		let size = var.parse().ok()?;
		if size > *WEBSOCKET_WRITE_BUFFER_SIZE {
			Some(size)
		} else {
			None
		}
	};
	buffer_size().unwrap_or(usize::MAX)
});

/// How many messages can be queued for sending down the WebSocket (default:
/// 100)
pub static WEBSOCKET_RESPONSE_CHANNEL_SIZE: LazyLock<usize> =
	lazy_env_parse!("SURREAL_WEBSOCKET_RESPONSE_CHANNEL_SIZE", usize, 100);

/// How many responses can be buffered when delivering to the client (default:
/// 0)
pub static WEBSOCKET_RESPONSE_BUFFER_SIZE: LazyLock<usize> =
	lazy_env_parse!("SURREAL_WEBSOCKET_RESPONSE_BUFFER_SIZE", usize, 0);

/// How often are any buffered responses flushed to the WebSocket client
/// (default: 3 ms)
pub static WEBSOCKET_RESPONSE_FLUSH_PERIOD: LazyLock<u64> =
	lazy_env_parse!("SURREAL_WEBSOCKET_RESPONSE_FLUSH_PERIOD", u64, 3);

/// The number of runtime worker threads to start (default: the number of CPU
/// cores, minimum 4)
pub static RUNTIME_WORKER_THREADS: LazyLock<usize> =
	lazy_env_parse!("SURREAL_RUNTIME_WORKER_THREADS", usize, || {
		std::cmp::max(4, num_cpus::get())
	});

/// What is the runtime thread memory stack size (default: 10 MiB)
pub static RUNTIME_STACK_SIZE: LazyLock<usize> =
	lazy_env_parse!("SURREAL_RUNTIME_STACK_SIZE", usize, || {
		// Stack frames are generally larger in debug mode.
		if cfg!(debug_assertions) {
			20 * 1024 * 1024 // 20 MiB in debug mode
		} else {
			10 * 1024 * 1024 // 10 MiB in release mode
		}
	});

/// How many threads which can be started for blocking operations (default: 512)
pub static RUNTIME_MAX_BLOCKING_THREADS: LazyLock<usize> =
	lazy_env_parse!("SURREAL_RUNTIME_MAX_BLOCKING_THREADS", usize, 512);

/// If set to "otlp" then telemetry is sent to the GRPC OpenTelemetry collector
pub static TELEMETRY_PROVIDER: LazyLock<String> =
	lazy_env_parse!("SURREAL_TELEMETRY_PROVIDER", String);

/// If set then use this as value for the namespace label when sending telemetry
pub static TELEMETRY_NAMESPACE: LazyLock<Option<String>> =
	lazy_env_parse!("SURREAL_TELEMETRY_NAMESPACE", Option<String>);

/// Whether to disable sending traces to the OpenTelemetry collector (default:
/// false)
pub static TELEMETRY_DISABLE_TRACING: LazyLock<bool> =
	lazy_env_parse!("SURREAL_TELEMETRY_DISABLE_TRACING", bool);

/// Whether to disable sending metrics to the OpenTelemetry collector (default:
/// false)
pub static TELEMETRY_DISABLE_METRICS: LazyLock<bool> =
	lazy_env_parse!("SURREAL_TELEMETRY_DISABLE_METRICS", bool);

/// The version identifier of this build
pub static PKG_VERSION: LazyLock<String> = LazyLock::new(|| {
	// Use SURREAL_BUILD_VERSION if set, otherwise fall back to CARGO_PKG_VERSION
	let version = option_env!("SURREAL_BUILD_VERSION")
		.filter(|v| !v.trim().is_empty())
		.unwrap_or(env!("CARGO_PKG_VERSION"));
	// Append build metadata if set
	match option_env!("SURREAL_BUILD_METADATA") {
		Some(metadata) if !metadata.trim().is_empty() => {
			format!("{version}+{metadata}")
		}
		_ => version.to_owned(),
	}
});

/// Whether to enable Tokio Console
pub static ENABLE_TOKIO_CONSOLE: LazyLock<bool> =
	lazy_env_parse!("SURREAL_TOKIO_CONSOLE_ENABLED", bool, false);

/// The socket address that Tokio Console will bind on
pub static TOKIO_CONSOLE_SOCKET_ADDR: LazyLock<Option<String>> =
	lazy_env_parse!("SURREAL_TOKIO_CONSOLE_SOCKET_ADDR", Option<String>);

/// How long, in seconds, to retain data for completed events (default: 60)
pub static TOKIO_CONSOLE_RETENTION: LazyLock<u64> =
	lazy_env_parse!("SURREAL_TOKIO_CONSOLE_RETENTION", u64, 60);
