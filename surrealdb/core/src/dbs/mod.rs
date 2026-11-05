//! Datastore module which is the core of the database node.
//! In this module we essentially manage the entire lifecycle of a database
//! request acting as the glue between the API and the response. In this module
//! we use channels as a transport layer and executors to process the
//! operations. This module also gives a `context` to the transaction.

mod broker;
mod capture;
pub mod executor;
pub(crate) mod live_recheck;
mod observe_ctx;
mod options;
mod session;
mod sort_error;
mod statement;
mod statement_counters;
mod stream;
mod top_level;

// A session outlives the process that attached it, so its stored form belongs to
// the keyspace; the live session above converts in and out of it.
pub(crate) use surrealdb_datastore::values::session::DurableSession;
pub(crate) use surrealdb_rpc::{
	QueryResult, QueryResultBuilder, QueryStreamItem, QueryType, capabilities, items_for_result,
};

pub use self::broker::{
	BrokerRoutingContext, LocalMessageBroker, MessageBroker, NodeEndpointResolver,
	RoutedNotification,
};
pub(crate) use self::broker::{DefaultBroker, OrderedBroker, SendKill};
pub(crate) use self::capabilities::Capabilities;
pub(crate) use self::capture::ParameterCapturePass;
pub(crate) use self::executor::Executor;
pub(crate) use self::options::{Force, NoWriteFrame, Options};
pub use self::session::{
	AuthPrincipalSnapshot, Session, SessionData, StoredRpcSession, complete_credential,
};
pub(crate) use self::session::{durable_session, restore_session};
pub(crate) use self::sort_error::SortError;
pub(crate) use self::statement::{
	FOR_UPDATE_GROUP_ERROR, FOR_UPDATE_LIMIT_ERROR, FOR_UPDATE_TARGETS_ERROR,
	FOR_UPDATE_VERSION_ERROR, validate_for_update,
};
pub(crate) use self::statement_counters::StatementCounters;
pub use self::stream::{QueryItemStream, QueryStreamJob};
pub(crate) use self::top_level::{Dispatch, TopLevelStatement};
pub use crate::catalog::node;
pub(crate) use crate::expr::variables::Variables;

#[cfg(all(test, feature = "kv-mem"))]
mod catalog_boundary_test;
#[cfg(all(test, feature = "kv-mem"))]
mod definer_rights_test;
#[cfg(all(test, feature = "kv-mem"))]
mod live_notification_test;
#[cfg(all(test, feature = "kv-mem"))]
mod nested_matches_parity_test;
#[cfg(test)]
mod read_only_test;
#[cfg(all(test, feature = "kv-mem"))]
mod request_test;
#[cfg(all(test, feature = "kv-mem"))]
mod stream_test;
#[cfg(test)]
pub(crate) mod test;
#[cfg(test)]
mod top_level_test;
#[cfg(all(test, feature = "kv-mem"))]
mod trace_test;
