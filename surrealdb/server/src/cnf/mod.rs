use std::sync::LazyLock;
use std::time::Duration;

use surrealdb_core::str::ParseBytes;

// ---------------------------------------------------------------------------
// True constants
// ---------------------------------------------------------------------------

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

/// Specifies the frequency with which ping messages are sent to the client
pub const WEBSOCKET_PING_FREQUENCY: Duration = Duration::from_secs(5);

// ---------------------------------------------------------------------------
// Statics that stay global (compile-time / build metadata)
// ---------------------------------------------------------------------------

/// The version identifier of this build
pub static PKG_VERSION: LazyLock<String> = LazyLock::new(|| {
	let version = option_env!("SURREAL_BUILD_VERSION")
		.filter(|v| !v.trim().is_empty())
		.unwrap_or(env!("CARGO_PKG_VERSION"));
	match option_env!("SURREAL_BUILD_METADATA") {
		Some(metadata) if !metadata.trim().is_empty() => {
			format!("{version}+{metadata}")
		}
		_ => version.to_owned(),
	}
});

// ---------------------------------------------------------------------------
// Env-var parsing helpers
// ---------------------------------------------------------------------------

fn env_parse<T: std::str::FromStr>(key: &str, default: T) -> T {
	std::env::var(key).ok().and_then(|s| s.parse::<T>().ok()).unwrap_or(default)
}

fn env_parse_bytes<T: TryFrom<u128>>(key: &str, default: T) -> T {
	std::env::var(key).ok().and_then(|s| s.as_str().parse_bytes::<T>().ok()).unwrap_or(default)
}

// ---------------------------------------------------------------------------
// ServerConfig – the top-level server config struct
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct ServerConfig {
	pub runtime: RuntimeConfig,
	pub telemetry: TelemetryConfig,
	pub http: HttpServerConfig,
	pub websocket: WebSocketConfig,
}

impl ServerConfig {
	pub fn from_env() -> Self {
		Self {
			runtime: RuntimeConfig::from_env(),
			telemetry: TelemetryConfig::from_env(),
			http: HttpServerConfig::from_env(),
			websocket: WebSocketConfig::from_env(),
		}
	}
}

// ---------------------------------------------------------------------------
// RuntimeConfig (pre-Datastore bootstrap)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct RuntimeConfig {
	/// The number of runtime worker threads to start (default: max(4, num_cpus))
	pub worker_threads: usize,
	/// The runtime thread memory stack size (default: 10 MiB / 20 MiB debug)
	pub stack_size: usize,
	/// How many threads can be started for blocking operations (default: 512)
	pub max_blocking_threads: usize,
}

impl RuntimeConfig {
	pub fn from_env() -> Self {
		Self {
			worker_threads: env_parse(
				"SURREAL_RUNTIME_WORKER_THREADS",
				std::cmp::max(4, num_cpus::get()),
			),
			stack_size: env_parse(
				"SURREAL_RUNTIME_STACK_SIZE",
				if cfg!(debug_assertions) {
					20 * 1024 * 1024
				} else {
					10 * 1024 * 1024
				},
			),
			max_blocking_threads: env_parse("SURREAL_RUNTIME_MAX_BLOCKING_THREADS", 512),
		}
	}
}

// ---------------------------------------------------------------------------
// TelemetryConfig
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct TelemetryConfig {
	/// If set to "otlp" then telemetry is sent to the GRPC OpenTelemetry collector
	pub provider: String,
	/// Namespace label when sending telemetry
	pub namespace: Option<String>,
	/// Whether to disable sending traces to the OpenTelemetry collector (default: false)
	pub disable_tracing: bool,
	/// Whether to disable sending metrics to the OpenTelemetry collector (default: false)
	pub disable_metrics: bool,
	/// Whether to enable Tokio Console (default: false)
	pub tokio_console_enabled: bool,
	/// The socket address that Tokio Console will bind on
	pub tokio_console_socket_addr: Option<String>,
	/// How long, in seconds, to retain data for completed events (default: 60)
	pub tokio_console_retention: u64,
}

impl TelemetryConfig {
	pub fn from_env() -> Self {
		Self {
			provider: env_parse("SURREAL_TELEMETRY_PROVIDER", String::new()),
			namespace: std::env::var("SURREAL_TELEMETRY_NAMESPACE").ok(),
			disable_tracing: env_parse("SURREAL_TELEMETRY_DISABLE_TRACING", false),
			disable_metrics: env_parse("SURREAL_TELEMETRY_DISABLE_METRICS", false),
			tokio_console_enabled: env_parse("SURREAL_TOKIO_CONSOLE_ENABLED", false),
			tokio_console_socket_addr: std::env::var("SURREAL_TOKIO_CONSOLE_SOCKET_ADDR").ok(),
			tokio_console_retention: env_parse("SURREAL_TOKIO_CONSOLE_RETENTION", 60),
		}
	}
}

// ---------------------------------------------------------------------------
// HttpServerConfig (body size limits for inbound HTTP)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct HttpServerConfig {
	/// How many concurrent network requests can be handled at once (default: 1_048_576)
	pub max_concurrent_requests: usize,
	/// The maximum HTTP body size of the /ml endpoints (default: 4 GiB)
	pub max_ml_body_size: usize,
	/// The maximum HTTP body size of the /sql endpoint (default: 1 MiB)
	pub max_sql_body_size: usize,
	/// The maximum HTTP body size of the /api endpoint (default: 4 MiB)
	pub max_api_body_size: usize,
	/// The maximum HTTP body size of the /rpc endpoint (default: 4 MiB)
	pub max_rpc_body_size: usize,
	/// The maximum HTTP body size of the /key endpoints (default: 16 KiB)
	pub max_key_body_size: usize,
	/// The maximum HTTP body size of the /signup endpoint (default: 1 KiB)
	pub max_signup_body_size: usize,
	/// The maximum HTTP body size of the /signin endpoint (default: 1 KiB)
	pub max_signin_body_size: usize,
	/// The maximum HTTP body size of the /import endpoint (default: 4 GiB)
	pub max_import_body_size: usize,
}

impl HttpServerConfig {
	pub fn from_env() -> Self {
		Self {
			max_concurrent_requests: env_parse("SURREAL_NET_MAX_CONCURRENT_REQUESTS", 1 << 20),
			max_ml_body_size: env_parse_bytes("SURREAL_HTTP_MAX_ML_BODY_SIZE", 4 << 30),
			max_sql_body_size: env_parse_bytes("SURREAL_HTTP_MAX_SQL_BODY_SIZE", 1 << 20),
			max_api_body_size: env_parse_bytes("SURREAL_HTTP_MAX_API_BODY_SIZE", 4 << 20),
			max_rpc_body_size: env_parse_bytes("SURREAL_HTTP_MAX_RPC_BODY_SIZE", 4 << 20),
			max_key_body_size: env_parse_bytes("SURREAL_HTTP_MAX_KEY_BODY_SIZE", 16 << 10),
			max_signup_body_size: env_parse_bytes("SURREAL_HTTP_MAX_SIGNUP_BODY_SIZE", 1 << 10),
			max_signin_body_size: env_parse_bytes("SURREAL_HTTP_MAX_SIGNIN_BODY_SIZE", 1 << 10),
			max_import_body_size: env_parse_bytes("SURREAL_HTTP_MAX_IMPORT_BODY_SIZE", 4 << 30),
		}
	}
}

// ---------------------------------------------------------------------------
// WebSocketConfig
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct WebSocketConfig {
	/// The maximum WebSocket message size (default: 128 MiB)
	pub max_message_size: usize,
	/// The size of the read buffer for WebSocket connections (default: 128 KiB)
	pub read_buffer_size: usize,
	/// The size of the write buffer for WebSocket connections (default: 128 KiB)
	pub write_buffer_size: usize,
	/// The maximum write buffer size before backpressure is applied (default: usize::MAX)
	pub max_write_buffer_size: usize,
	/// How many messages can be queued for sending down the WebSocket (default: 100)
	pub response_channel_size: usize,
	/// How many responses can be buffered when delivering to the client (default: 0)
	pub response_buffer_size: usize,
	/// How often buffered responses are flushed to the WebSocket client in ms (default: 3)
	pub response_flush_period: u64,
}

impl WebSocketConfig {
	pub fn from_env() -> Self {
		let write_buffer_size = env_parse_bytes("SURREAL_WEBSOCKET_WRITE_BUFFER_SIZE", 128 * 1024);
		let max_write_buffer_size = std::env::var("SURREAL_WEBSOCKET_MAX_WRITE_BUFFER_SIZE")
			.ok()
			.and_then(|s| s.parse::<usize>().ok())
			.filter(|&size| size > write_buffer_size)
			.unwrap_or(usize::MAX);
		Self {
			max_message_size: env_parse_bytes("SURREAL_WEBSOCKET_MAX_MESSAGE_SIZE", 128 << 20),
			read_buffer_size: env_parse_bytes("SURREAL_WEBSOCKET_READ_BUFFER_SIZE", 128 * 1024),
			write_buffer_size,
			max_write_buffer_size,
			response_channel_size: env_parse("SURREAL_WEBSOCKET_RESPONSE_CHANNEL_SIZE", 100),
			response_buffer_size: env_parse("SURREAL_WEBSOCKET_RESPONSE_BUFFER_SIZE", 0),
			response_flush_period: env_parse("SURREAL_WEBSOCKET_RESPONSE_FLUSH_PERIOD", 3),
		}
	}
}
