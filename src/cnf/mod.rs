use std::sync::LazyLock;
use std::time::Duration;
use surrealdb::{lazy_env_parse, lazy_env_parse_or_else};

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

/// The maximum HTTP body size of the HTTP /ml endpoints (defaults to 4 GiB)
pub static HTTP_MAX_ML_BODY_SIZE: LazyLock<usize> =
	lazy_env_parse!("SURREAL_HTTP_MAX_ML_BODY_SIZE", usize, 4 << 30);

/// The maximum HTTP body size of the HTTP /sql endpoint (defaults to 1 MiB)
pub static HTTP_MAX_SQL_BODY_SIZE: LazyLock<usize> =
	lazy_env_parse!("SURREAL_HTTP_MAX_SQL_BODY_SIZE", usize, 1 << 20);

/// The maximum HTTP body size of the HTTP /rpc endpoint (defaults to 4 MiB)
pub static HTTP_MAX_RPC_BODY_SIZE: LazyLock<usize> =
	lazy_env_parse!("SURREAL_HTTP_MAX_RPC_BODY_SIZE", usize, 4 << 20);

/// The maximum HTTP body size of the HTTP /key endpoints (defaults to 16 KiB)
pub static HTTP_MAX_KEY_BODY_SIZE: LazyLock<usize> =
	lazy_env_parse!("SURREAL_HTTP_MAX_KEY_BODY_SIZE", usize, 16 << 10);

/// The maximum HTTP body size of the HTTP /signup endpoint (defaults to 1 KiB)
pub static HTTP_MAX_SIGNUP_BODY_SIZE: LazyLock<usize> =
	lazy_env_parse!("SURREAL_HTTP_MAX_SIGNUP_BODY_SIZE", usize, 1 << 10);

/// The maximum HTTP body size of the HTTP /signin endpoint (defaults to 1 KiB)
pub static HTTP_MAX_SIGNIN_BODY_SIZE: LazyLock<usize> =
	lazy_env_parse!("SURREAL_HTTP_MAX_SIGNIN_BODY_SIZE", usize, 1 << 10);

/// The maximum HTTP body size of the HTTP /import endpoint (defaults to 4 GiB)
pub static HTTP_MAX_IMPORT_BODY_SIZE: LazyLock<usize> =
	lazy_env_parse!("SURREAL_HTTP_MAX_IMPORT_BODY_SIZE", usize, 4 << 30);

/// Specifies the frequency with which ping messages should be sent to the client
pub const WEBSOCKET_PING_FREQUENCY: Duration = Duration::from_secs(5);

/// What is the maximum WebSocket frame size (defaults to 16 MiB)
pub static WEBSOCKET_MAX_FRAME_SIZE: LazyLock<usize> =
	lazy_env_parse!("SURREAL_WEBSOCKET_MAX_FRAME_SIZE", usize, 16 << 20);

/// What is the maximum WebSocket message size (defaults to 128 MiB)
pub static WEBSOCKET_MAX_MESSAGE_SIZE: LazyLock<usize> =
	lazy_env_parse!("SURREAL_WEBSOCKET_MAX_MESSAGE_SIZE", usize, 128 << 20);

/// How many concurrent tasks can be handled on each WebSocket (defaults to 24)
pub static WEBSOCKET_MAX_CONCURRENT_REQUESTS: LazyLock<usize> =
	lazy_env_parse!("SURREAL_WEBSOCKET_MAX_CONCURRENT_REQUESTS", usize, 24);

/// What is the number of runtime worker threads to start (defaults to the number of CPU cores)
pub static RUNTIME_WORKER_THREADS: LazyLock<usize> =
	lazy_env_parse_or_else!("SURREAL_RUNTIME_WORKER_THREADS", usize, |_| {
		std::cmp::max(4, num_cpus::get())
	});

/// What is the runtime thread memory stack size (defaults to 10MiB)
pub static RUNTIME_STACK_SIZE: LazyLock<usize> =
	lazy_env_parse_or_else!("SURREAL_RUNTIME_STACK_SIZE", usize, |_| {
		// Stack frames are generally larger in debug mode.
		if cfg!(debug_assertions) {
			20 * 1024 * 1024 // 20MiB in debug mode
		} else {
			10 * 1024 * 1024 // 10MiB in release mode
		}
	});

/// How many threads which can be started for blocking operations (defaults to 512)
pub static RUNTIME_MAX_BLOCKING_THREADS: LazyLock<usize> =
	lazy_env_parse!("SURREAL_RUNTIME_MAX_BLOCKING_THREADS", usize, 512);

/// If set to "otlp" then telemetry is sent to the GRPC OTEL collector
pub static TELEMETRY_PROVIDER: LazyLock<String> =
	lazy_env_parse!("SURREAL_TELEMETRY_PROVIDER", String);

/// If set to "true" then no traces are sent to the GRPC OTEL collector
pub static TELEMETRY_DISABLE_TRACING: LazyLock<bool> =
	lazy_env_parse!("SURREAL_TELEMETRY_DISABLE_TRACING", bool);

/// If set to "true" then no metrics are sent to the GRPC OTEL collector
pub static TELEMETRY_DISABLE_METRICS: LazyLock<bool> =
	lazy_env_parse!("SURREAL_TELEMETRY_DISABLE_METRICS", bool);

/// If set then use this as value for the namespace label when sending telemetry
pub static TELEMETRY_NAMESPACE: LazyLock<String> =
	lazy_env_parse!("SURREAL_TELEMETRY_NAMESPACE", String);

/// The version identifier of this build
pub static PKG_VERSION: LazyLock<String> =
	LazyLock::new(|| match option_env!("SURREAL_BUILD_METADATA") {
		Some(metadata) if !metadata.trim().is_empty() => {
			let version = env!("CARGO_PKG_VERSION");
			format!("{version}+{metadata}")
		}
		_ => env!("CARGO_PKG_VERSION").to_owned(),
	});

pub static GRAPHQL_ENABLE: LazyLock<bool> =
	lazy_env_parse!("SURREAL_EXPERIMENTAL_GRAPHQL", bool, false);
