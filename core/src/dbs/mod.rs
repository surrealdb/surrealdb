//! Datastore module which is the core of the database node.
//! In this module we essentially manage the entire lifecycle of a database request acting as the
//! glue between the API and the response. In this module we use channels as a transport layer
//! and executors to process the operations. This module also gives a `context` to the transaction.
mod distinct;
mod executor;
mod group;
mod iterator;
mod notification;
mod options;
mod plan;
mod processor;
mod response;
mod result;
mod session;
mod statement;
mod store;
mod variables;

pub mod capabilities;
pub mod node;

pub use self::capabilities::Capabilities;
pub(crate) use self::executor::*;
pub(crate) use self::iterator::*;
pub use self::notification::*;
pub use self::options::*;
pub use self::response::*;
pub use self::session::*;
pub(crate) use self::statement::*;
pub(crate) use self::variables::*;
#[cfg(not(target_arch = "wasm32"))]
use crate::err::Error;
#[cfg(not(target_arch = "wasm32"))]
use tokio::sync::oneshot::error::RecvError;
#[cfg(not(target_arch = "wasm32"))]
use tokio::task;

#[doc(hidden)]
pub mod fuzzy_eq;
#[cfg(test)]
pub(crate) mod test;

#[cfg(not(target_arch = "wasm32"))]
pub(super) async fn spawn_blocking<T, F, E>(f: F, e: E) -> Result<T, Error>
where
	F: FnOnce() -> Result<T, Error> + Send + 'static,
	E: FnOnce(RecvError) -> Error,
	T: Send + 'static,
{
	let (send, recv) = tokio::sync::oneshot::channel();
	task::spawn_blocking(move || {
		let res = f();
		let _ = send.send(res);
	});
	recv.await.map_err(e)?
}
