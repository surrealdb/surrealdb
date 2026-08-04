//! Executing a query as a stream of results.
//!
//! [`Datastore::execute`](crate::kvs::Datastore::execute) answers with every
//! statement's result at once, which means holding a whole `SELECT` in memory
//! and sending nothing until the last row is read.
//! [`Datastore::execute_stream`](crate::kvs::Datastore::execute_stream) answers
//! with [`QueryStreamItem`]s instead, so a transport can put rows on the wire as
//! the executor produces them.
//!
//! What a consumer may assume about those items — chiefly that rows are
//! provisional until their statement's terminal item arrives — is documented on
//! [`QueryStreamItem`] itself, since that contract holds wherever they came
//! from.

use std::pin::Pin;

use futures::Stream;
use surrealdb_rpc::{QueryResult, QueryStreamItem};
use surrealdb_types::Error as TypesError;

/// A query that has been parsed and is ready to stream its results.
///
/// Parsing happens before this exists, so a parse error is reported instead of
/// one, and `statement_count` is known before any result is.
pub struct QueryStreamJob {
	/// How many statements the query parsed into.
	///
	/// An upper bound on the statements that produce results, not a count of
	/// them: control flow can skip the tail of a query — a `RETURN` inside a
	/// `BEGIN` block short-circuits the rest — and a skipped statement emits no
	/// items at all.
	pub statement_count: usize,
	/// The execution itself, which sends into the channel it was built with.
	///
	/// Drive this while draining that channel; doing both on one task
	/// deadlocks, because the channel is bounded so that a slow consumer slows
	/// the executor rather than being outrun by it.
	///
	/// The channel closing means the execution has stopped, not that it
	/// succeeded: a failure not attributable to any one statement — the
	/// transaction could not be created, the query timed out — is reported
	/// here, and a consumer needs both signals to tell "finished" from
	/// "gave up".
	///
	/// The per-statement results come back as well, for the callers that have
	/// to act on what a statement *was* rather than what it returned — chiefly
	/// registering the live query a `LIVE SELECT` just created. Holding them
	/// costs nothing: a statement whose rows streamed carries no value here,
	/// because those rows already went to the consumer.
	pub run: Pin<Box<dyn Future<Output = Result<Vec<QueryResult>, TypesError>> + Send + 'static>>,
}

/// A stream of a query's results.
pub type QueryItemStream = Pin<Box<dyn Stream<Item = QueryStreamItem> + Send>>;
