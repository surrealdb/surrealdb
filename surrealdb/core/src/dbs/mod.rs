//! Datastore module which is the core of the database node.
//! In this module we essentially manage the entire lifecycle of a database
//! request acting as the glue between the API and the response. In this module
//! we use channels as a transport layer and executors to process the
//! operations. This module also gives a `context` to the transaction.

mod broker;
mod distinct;
pub mod executor;
mod group;
mod iterator;
mod options;
mod plan;
mod processor;
mod response;
mod result;
mod session;
mod statement;
mod statement_counters;
mod store;
mod variables;

pub mod capabilities;
pub mod node;

pub(crate) use variables::{ParameterCapturePass, Variables};

pub use self::broker::{
	BrokerRoutingContext, LocalMessageBroker, MessageBroker, NodeEndpointResolver,
	RoutedNotification,
};
pub use self::capabilities::Capabilities;
pub(crate) use self::executor::Executor;
pub(crate) use self::iterator::{Iterable, Iterator, Operable, Processable};
pub(crate) use self::options::{Force, Options};
pub use self::response::{QueryResult, QueryResultBuilder, QueryType, Status};
pub use self::session::{NewPlannerStrategy, Session};
pub(crate) use self::statement::Statement;
pub(crate) use self::statement_counters::StatementCounters;

#[cfg(storage)]
mod file;

#[cfg(test)]
pub(crate) mod test;
