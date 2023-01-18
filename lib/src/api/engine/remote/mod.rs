//! Protocols for communicating with the server

use serde::Deserialize;

#[cfg(feature = "protocol-http")]
#[cfg_attr(docsrs, doc(cfg(feature = "protocol-http")))]
pub mod http;

#[cfg(feature = "protocol-ws")]
#[cfg_attr(docsrs, doc(cfg(feature = "protocol-ws")))]
pub mod ws;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub(crate) enum Status {
	Ok,
	Err,
}
