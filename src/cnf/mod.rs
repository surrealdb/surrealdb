use once_cell::sync::Lazy;

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

// The name and version of this build
pub const PKG_NAME: &str = env!("CARGO_PKG_NAME");
pub static PKG_VERS: Lazy<String> = Lazy::new(|| match option_env!("SURREAL_BUILD_METADATA") {
	Some(metadata) if !metadata.trim().is_empty() => {
		let version = env!("CARGO_PKG_VERSION");
		format!("{version}+{metadata}")
	}
	_ => env!("CARGO_PKG_VERSION").to_owned(),
});

// The publicly visible name of the server
pub const SERVER_NAME: &str = "SurrealDB";

// The public endpoint for the database administration interface
pub const APP_ENDPOINT: &str = "https://surrealdb.com/app";

// Specifies how many concurrent jobs can be buffered in the worker channel.
pub const MAX_CONCURRENT_CALLS: usize = 24;
