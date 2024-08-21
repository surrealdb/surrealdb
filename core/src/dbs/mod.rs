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
pub mod lifecycle;
pub mod node;

pub use self::capabilities::Capabilities;
pub use self::lifecycle::*;
pub use self::notification::*;
pub use self::options::*;
pub use self::response::*;
pub use self::session::*;

pub(crate) use self::executor::*;
pub(crate) use self::iterator::*;
pub(crate) use self::statement::*;
pub(crate) use self::variables::*;

#[doc(hidden)]
pub mod fuzzy_eq;
#[cfg(test)]
pub(crate) mod test;
