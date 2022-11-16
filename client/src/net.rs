//! Networking clients for communicating with the server

#[cfg(feature = "http")]
#[cfg_attr(docsrs, doc(cfg(feature = "http")))]
pub use crate::protocol::http::Client as HttpClient;

#[cfg(feature = "ws")]
#[cfg_attr(docsrs, doc(cfg(feature = "ws")))]
pub use crate::protocol::ws::Client as WsClient;
