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

pub mod node;

mod processor;
#[cfg(test)]
pub(crate) mod test;
