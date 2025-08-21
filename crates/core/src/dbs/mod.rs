//! Datastore module which is the core of the database node.
//! In this module we essentially manage the entire lifecycle of a database
//! request acting as the glue between the API and the response. In this module
//! we use channels as a transport layer and executors to process the
//! operations. This module also gives a `context` to the transaction.

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

pub use variables::Variables;

pub use self::capabilities::Capabilities;
pub(crate) use self::executor::Executor;
pub(crate) use self::iterator::{Iterable, Iterator, Operable, Processed, Workable};
pub use self::notification::{Action, Notification};
pub(crate) use self::options::{Force, Options};
pub use self::response::{QueryMethodResponse, QueryType, Response, Status};
pub use self::session::Session;
pub(crate) use self::statement::Statement;

#[cfg(storage)]
mod file;

#[cfg(test)]
pub(crate) mod test;
