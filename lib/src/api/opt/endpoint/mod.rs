#[cfg(feature = "protocol-http")]
mod http;
#[cfg(feature = "protocol-ws")]
mod ws;

#[cfg(feature = "kv-fdb")]
mod fdb;
#[cfg(feature = "kv-indxdb")]
mod indxdb;
#[cfg(feature = "kv-mem")]
mod mem;
#[cfg(feature = "kv-rocksdb")]
mod rocksdb;
#[cfg(feature = "kv-speedb")]
mod speedb;
#[cfg(feature = "kv-tikv")]
mod tikv;

use crate::api::Connection;
use crate::api::Result;
use url::Url;

use super::Config;

/// A server address used to connect to the server
#[derive(Debug)]
#[allow(dead_code)] // used by the embedded and remote connections
pub struct Endpoint {
	pub(crate) url: Url,
	pub(crate) path: String,
	pub(crate) config: Config,
}

/// A trait for converting inputs to a server address object
pub trait IntoEndpoint<Scheme> {
	/// The client implied by this scheme and address combination
	type Client: Connection;
	/// Converts an input into a server address object
	fn into_endpoint(self) -> Result<Endpoint>;
}

pub(crate) fn replace_tilde(path: &str) -> String {
	let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_owned());
	path.replacen("://~", &format!("://{home}"), 1).replacen(":~", &format!(":{home}"), 1)
}

#[allow(dead_code)]
fn path_to_string(protocol: &str, path: impl AsRef<std::path::Path>) -> String {
	use path_clean::PathClean;
	use std::path::Path;

	let path = format!("{protocol}{}", path.as_ref().display());
	let expanded = replace_tilde(&path);
	Path::new(&expanded).clean().display().to_string()
}
