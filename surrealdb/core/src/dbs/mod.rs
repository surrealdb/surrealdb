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
mod stream;

// A session outlives the process that attached it, so its stored form belongs to
// the keyspace; the live session above converts in and out of it.
pub(crate) use surrealdb_datastore::values::session::DurableSession;
pub(crate) use surrealdb_rpc::capabilities::NewPlannerStrategy;
pub(crate) use surrealdb_rpc::{
	QueryResult, QueryResultBuilder, QueryStreamItem, QueryType, capabilities, items_for_result,
};

pub(crate) use self::broker::SendKill;
pub use self::broker::{
	BrokerRoutingContext, LocalMessageBroker, MessageBroker, NodeEndpointResolver,
	RoutedNotification,
};
pub(crate) use self::capabilities::Capabilities;
pub(crate) use self::capture::ParameterCapturePass;
pub(crate) use self::executor::Executor;
pub(crate) use self::iterator::{Iterable, Iterator, Operable, Processable};
pub(crate) use self::options::{Force, NoWriteFrame, Options};
pub use self::session::{AuthPrincipalSnapshot, Session};
pub(crate) use self::session::{durable_session, restore_session};
pub(crate) use self::sort_error::SortError;
pub(crate) use self::statement::Statement;
pub(crate) use self::statement_counters::StatementCounters;
pub use self::stream::{QueryItemStream, QueryStreamJob};
pub use crate::catalog::node;
pub(crate) use crate::expr::variables::Variables;

#[cfg(storage)]
mod file;

#[cfg(all(test, feature = "kv-mem"))]
mod definer_rights_test;
#[cfg(test)]
mod read_only_test;
#[cfg(all(test, feature = "kv-mem"))]
mod stream_test;
#[cfg(test)]
pub(crate) mod test;
