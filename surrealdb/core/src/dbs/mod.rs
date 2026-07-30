//! Datastore module which is the core of the database node.
//! In this module we essentially manage the entire lifecycle of a database
//! request acting as the glue between the API and the response. In this module
//! we use channels as a transport layer and executors to process the
//! operations. This module also gives a `context` to the transaction.

mod broker;
mod capture;
mod distinct;
pub mod executor;
mod group;
mod iterator;
mod observe_ctx;
mod options;
mod plan;
pub(crate) mod processor;
mod result;
mod session;
mod sort_error;
mod statement;
mod statement_counters;
mod store;

pub mod capabilities;
pub use surrealdb_rpc::{QueryResult, QueryResultBuilder, QueryType, Status};

pub use self::broker::{
	BrokerRoutingContext, LocalMessageBroker, MessageBroker, NodeEndpointResolver,
	RoutedNotification,
};
pub use self::capabilities::Capabilities;
pub(crate) use self::capture::ParameterCapturePass;
pub(crate) use self::executor::Executor;
pub(crate) use self::iterator::{Iterable, Iterator, Operable, Processable};
pub(crate) use self::options::{Force, Options};
pub(crate) use self::session::DurableSession;
pub use self::session::{NewPlannerStrategy, Session};
pub(crate) use self::sort_error::SortError;
pub(crate) use self::statement::Statement;
pub(crate) use self::statement_counters::StatementCounters;
pub use crate::catalog::node;
pub(crate) use crate::expr::variables::Variables;

#[cfg(storage)]
mod file;

#[cfg(test)]
pub(crate) mod test;
