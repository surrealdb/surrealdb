use once_cell::sync::Lazy;
#[cfg(feature = "has-storage")]
use std::time::Duration;

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

/// The publicly visible name of the server
#[cfg(feature = "has-storage")]
pub const PKG_NAME: &str = "surrealdb";

/// The publicly visible user-agent of the command-line tool
pub const SERVER_AGENT: &str = concat!("SurrealDB ", env!("CARGO_PKG_VERSION"));

/// The public endpoint for the administration interface
#[cfg(feature = "has-storage")]
pub const APP_ENDPOINT: &str = "https://surrealdb.com/app";

/// Specifies the frequency with which ping messages should be sent to the client
#[cfg(feature = "has-storage")]
pub const WEBSOCKET_PING_FREQUENCY: Duration = Duration::from_secs(5);

/// What is the maximum WebSocket frame size (defaults to 16 MiB)
#[cfg(feature = "has-storage")]
pub static WEBSOCKET_MAX_FRAME_SIZE: Lazy<usize> = Lazy::new(|| {
	let default = 16 << 20;
	std::env::var("SURREAL_WEBSOCKET_MAX_FRAME_SIZE")
		.map(|v| v.parse::<usize>().unwrap_or(default))
		.unwrap_or(default)
});

/// What is the maximum WebSocket message size (defaults to 128 MiB)
#[cfg(feature = "has-storage")]
pub static WEBSOCKET_MAX_MESSAGE_SIZE: Lazy<usize> = Lazy::new(|| {
	let default = 128 << 20;
	std::env::var("SURREAL_WEBSOCKET_MAX_MESSAGE_SIZE")
		.map(|v| v.parse::<usize>().unwrap_or(default))
		.unwrap_or(default)
});

/// How many concurrent tasks can be handled on each WebSocket (defaults to 24)
#[cfg(feature = "has-storage")]
pub static WEBSOCKET_MAX_CONCURRENT_REQUESTS: Lazy<usize> = Lazy::new(|| {
	let default = 24;
	std::env::var("SURREAL_WEBSOCKET_MAX_CONCURRENT_REQUESTS")
		.map(|v| v.parse::<usize>().unwrap_or(default))
		.unwrap_or(default)
});

/// What is the runtime thread memory stack size (defaults to 10MiB)
#[cfg(feature = "has-storage")]
pub static RUNTIME_STACK_SIZE: Lazy<usize> = Lazy::new(|| {
	// Stack frames are generally larger in debug mode.
	let default = if cfg!(debug_assertions) {
		20 * 1024 * 1024 // 20MiB in debug mode
	} else {
		10 * 1024 * 1024 // 10MiB in release mode
	};
	std::env::var("SURREAL_RUNTIME_STACK_SIZE")
		.map(|v| v.parse::<usize>().unwrap_or(default))
		.unwrap_or(default)
});

/// How many threads which can be started for blocking operations (defaults to 512)
#[cfg(feature = "has-storage")]
pub static RUNTIME_MAX_BLOCKING_THREADS: Lazy<usize> = Lazy::new(|| {
	let default = 512;
	std::env::var("SURREAL_RUNTIME_MAX_BLOCKING_THREADS")
		.map(|v| v.parse::<usize>().unwrap_or(default))
		.unwrap_or(default)
});

/// The version identifier of this build
pub static PKG_VERSION: Lazy<String> = Lazy::new(|| match option_env!("SURREAL_BUILD_METADATA") {
	Some(metadata) if !metadata.trim().is_empty() => {
		let version = env!("CARGO_PKG_VERSION");
		format!("{version}+{metadata}")
	}
	_ => env!("CARGO_PKG_VERSION").to_owned(),
});
