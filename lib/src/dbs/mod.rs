//! Datastore module which is the core of the database node.
//! In this module we essentially manage the entire lifecycle of a database request acting as the
//! glue between the API and the response. In this module we use channels as a transport layer
//! and executors to process the operations. This module also gives a `context` to the transaction.
mod auth;
mod executor;
mod iterate;
mod iterator;
mod notification;
mod options;
mod response;
mod session;
mod statement;
mod transaction;
mod variables;

pub use self::auth::*;
pub use self::options::*;
pub use self::response::*;
pub use self::session::*;

pub(crate) use self::executor::*;
pub(crate) use self::iterator::*;
pub(crate) use self::notification::*;
pub(crate) use self::statement::*;
pub(crate) use self::transaction::*;
pub(crate) use self::variables::*;

#[cfg(not(target_arch = "wasm32"))]
mod channel;

#[cfg(not(target_arch = "wasm32"))]
pub use self::channel::*;

pub mod cl;

#[cfg(test)]
pub(crate) mod test;

pub(crate) const LOG: &str = "surrealdb::dbs";
