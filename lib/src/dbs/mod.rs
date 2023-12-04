//! Datastore module which is the core of the database node.
//! In this module we essentially manage the entire lifecycle of a database request acting as the
//! glue between the API and the response. In this module we use channels as a transport layer
//! and executors to process the operations. This module also gives a `context` to the transaction.
mod distinct;
mod executor;
mod explanation;
mod iterator;
mod notification;
mod options;
mod response;
mod session;
mod statement;
mod transaction;
mod variables;

pub use self::notification::*;
pub use self::options::*;
pub use self::response::*;
pub use self::session::*;

pub(crate) use self::executor::*;
pub(crate) use self::iterator::*;
pub(crate) use self::statement::*;
pub(crate) use self::transaction::*;
pub(crate) use self::variables::*;

pub mod capabilities;
pub use self::capabilities::Capabilities;
pub mod node;

mod processor;

use lazy_static::lazy_static;
use std::sync::Mutex;
use tokio::task::JoinHandle;

lazy_static! {
	pub static ref CLEAR_HANDLES: Mutex<Vec<JoinHandle<()>>> = Mutex::new(Vec::new());
}

pub fn add_handle(h: JoinHandle<()>) {
	#[cfg(debug_assertions)]
	CLEAR_HANDLES.lock().unwrap().push(h);
}

#[cfg(debug_assertions)]
async fn await_handles_async() {
	let mut handles = CLEAR_HANDLES.lock().unwrap();
	println!("Handles: {}", handles.len());
	for h in handles.drain(..) {
		h.await.unwrap();
	}
}

pub fn await_handles() {
	#[cfg(debug_assertions)]
	tokio::task::block_in_place(|| {
		tokio::runtime::Runtime::new().unwrap().block_on(await_handles_async());
	});
}

#[cfg(test)]
pub(crate) mod test;
