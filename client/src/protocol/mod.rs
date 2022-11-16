//! Protocols for communicating with the server

#[cfg(feature = "http")]
pub(crate) mod http;
#[cfg(feature = "ws")]
pub(crate) mod ws;

use serde::Deserialize;

/// The HTTP scheme used to connect to `http://` endpoints
#[cfg(feature = "http")]
#[cfg_attr(docsrs, doc(cfg(feature = "http")))]
#[derive(Debug)]
pub struct Http;

/// The HTTPS scheme used to connect to `https://` endpoints
#[cfg(feature = "http")]
#[cfg_attr(docsrs, doc(cfg(feature = "http")))]
#[derive(Debug)]
pub struct Https;

/// The WS scheme used to connect to `ws://` endpoints
#[cfg(feature = "ws")]
#[cfg_attr(docsrs, doc(cfg(feature = "ws")))]
#[derive(Debug)]
pub struct Ws;

/// The WSS scheme used to connect to `wss://` endpoints
#[cfg(feature = "ws")]
#[cfg_attr(docsrs, doc(cfg(feature = "ws")))]
#[derive(Debug)]
pub struct Wss;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub(crate) enum Status {
	Ok,
	Err,
}
