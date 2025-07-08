
mod cmd;
#[cfg(feature = "protocol-http")]
pub(crate) use cmd::RouterRequest;
pub(crate) use cmd::{Command, LiveQueryParams, Request};


#[derive(Debug, Clone)]
pub(crate) struct MlExportConfig {
	#[allow(dead_code, reason = "Used in http and local non-wasm with ml features.")]
	pub(crate) name: String,
	#[allow(dead_code, reason = "Used in http and local non-wasm with ml features.")]
	pub(crate) version: String,
}

// /// Connection trait implemented by supported protocols
// pub trait Sealed: Sized + Send + Sync + 'static {
// 	/// Connect to the server
// 	fn connect(address: Endpoint, capacity: usize) -> BoxFuture<'static, Result<Surreal<Self>>>
// 	where
// 		Self: api::Connection;
// }
