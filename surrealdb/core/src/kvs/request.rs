//! The description of a query handed to [`Datastore::run`] and
//! [`Datastore::run_streaming`].
//!
//! A query varies along five axes that are otherwise independent of one
//! another: the form it arrives in, the language it is written in, whether it
//! runs inside a caller-owned transaction, whether the caller holds a handle
//! to cancel it, and whether its results are buffered or streamed. The first
//! four are described by a [`QueryRequest`]; the fifth is the runner the
//! request is handed to.
//!
//! [`Datastore::run`]: crate::kvs::Datastore::run
//! [`Datastore::run_streaming`]: crate::kvs::Datastore::run_streaming

use std::sync::Arc;

use surrealdb_expr::parser::Query as Ast;

use crate::ctx::CancelHandle;
use crate::dbs::Session;
use crate::expr::logical::LogicalOp;
use crate::kvs::Transaction;
use crate::types::PublicVariables;

/// The language a query's text is written in.
///
/// Only [`QuerySource::Text`] carries one: an [`Ast`](QuerySource::Ast) is
/// SurrealQL by construction, and a [`Plan`](QuerySource::Plan) has already
/// been lowered past the point where the language it came from matters.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum Dialect {
	/// SurrealQL.
	#[default]
	SurrealQl,
	/// GQL, the ISO/IEC 39075 property-graph query language.
	///
	/// A build without the `gql` feature reports that rather than failing to
	/// name the variant, because the dialect is chosen from what a client
	/// sent — a connection parameter, a route, a tool call — and so is a
	/// runtime value on every path that produces one.
	Gql,
	/// PostgreSQL SQL dialect.
	PgSql,
	/// MySQL SQL dialect.
	MySql,
}

/// The form a query arrives in.
///
/// The runner splits every variant into its statements before the session
/// preflight runs, so a caller that submits unparsable text learns that from
/// the call rather than from a later stage. SurrealQL statements carry runtime
/// expressions with inert credentials; execution prepares each reached statement.
pub enum QuerySource<'src> {
	/// Query text, in `dialect`. Parsed with the datastore's capabilities and
	/// parser configuration.
	Text {
		text: &'src str,
		dialect: Dialect,
	},
	/// An already-parsed SurrealQL surface AST.
	Ast(Ast),
	/// Plans from a front end that emits operators rather than statements: a
	/// `PreparedGqlQuery` holds one, so pass its inner plan. There is no
	/// surface syntax behind a plan, so nothing converts it, and each runs on
	/// the engine or fails.
	Plan(Vec<LogicalOp>),
}

impl<'src> QuerySource<'src> {
	/// SurrealQL text.
	pub fn text(text: &'src str) -> Self {
		QuerySource::Text {
			text,
			dialect: Dialect::SurrealQl,
		}
	}

	/// GQL text.
	pub fn gql(text: &'src str) -> Self {
		QuerySource::Text {
			text,
			dialect: Dialect::Gql,
		}
	}

	/// PostgreSQL SQL text.
	pub fn pgsql(text: &'src str) -> Self {
		QuerySource::Text {
			text,
			dialect: Dialect::PgSql,
		}
	}

	/// MySQL SQL text.
	pub fn mysql(text: &'src str) -> Self {
		QuerySource::Text {
			text,
			dialect: Dialect::MySql,
		}
	}

	/// Text in `dialect`, for a caller that reads the dialect from its client
	/// rather than knowing it at the call site.
	pub fn in_dialect(text: &'src str, dialect: Dialect) -> Self {
		QuerySource::Text {
			text,
			dialect,
		}
	}
}

impl From<Ast> for QuerySource<'_> {
	fn from(ast: Ast) -> Self {
		QuerySource::Ast(ast)
	}
}

impl From<Vec<LogicalOp>> for QuerySource<'_> {
	fn from(plans: Vec<LogicalOp>) -> Self {
		QuerySource::Plan(plans)
	}
}

/// Bare text is SurrealQL. Another dialect is named, never inferred.
impl<'src> From<&'src str> for QuerySource<'src> {
	fn from(text: &'src str) -> Self {
		QuerySource::text(text)
	}
}

/// A request to run every statement of a query in one read-only transaction
/// over a snapshot no older than `max_staleness`.
///
/// Exactly the statements wrapped as
/// `BEGIN READ ONLY MAX STALENESS <max_staleness> [STRICT]; …; COMMIT;`, so the
/// results are those of the wrapped query, the `BEGIN` result (which carries
/// the snapshot description) and the `COMMIT` result included. The bound is
/// applied as the options of the wrapping block and never spliced into the
/// text, so anything keyed on the statement text is the same for every bound.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReadOnlyOptions {
	pub max_staleness: std::time::Duration,
	pub strict: bool,
}

/// What a read-only `BEGIN` answers: the snapshot its block reads.
///
/// `position` is the backend's opaque token as bytes, or `NONE` where the
/// backend has no such notion; `age` is how old the snapshot was when the block
/// opened; `stale` is whether it is older than the current state.
pub fn snapshot_description(info: &surrealdb_kvs::SnapshotInfo) -> surrealdb_types::Value {
	let mut object = surrealdb_types::Object::new();
	object.insert(
		"position",
		match &info.position {
			Some(position) => surrealdb_types::Value::Bytes(surrealdb_types::Bytes::from(
				position.as_bytes().to_vec(),
			)),
			None => surrealdb_types::Value::None,
		},
	);
	object.insert("age", surrealdb_types::Duration::from(info.age));
	object.insert("stale", info.stale);
	surrealdb_types::Value::Object(object)
}

/// What a `begin` call asks for: an isolation level, and a read-only mode.
/// A call that asks for neither opens a read-write transaction at `SNAPSHOT`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BeginRequest {
	pub isolation: Option<crate::kvs::Isolation>,
	pub read_only: Option<ReadOnlyOptions>,
}

/// A query and the execution conditions it runs under.
///
/// Build one with [`QueryRequest::new`] and the `with_*` methods, or with a
/// struct literal — the fields are the whole description and none of them
/// interact:
///
/// ```rust,no_run
/// # use std::sync::Arc;
/// # use surrealdb_core::dbs::Session;
/// # use surrealdb_core::kvs::{Datastore, QueryRequest};
/// # async fn example(ds: &Datastore, sess: &Session) -> anyhow::Result<()> {
/// let results = ds.run(QueryRequest::new("SELECT * FROM person", sess)).await?;
/// # let _ = results;
/// # Ok(())
/// # }
/// ```
pub struct QueryRequest<'a> {
	/// What to run.
	pub source: QuerySource<'a>,
	/// The session the query runs as. Supplies the namespace, database,
	/// authentication level and session parameters.
	pub session: &'a Session,
	/// Variables bound for the duration of the query.
	pub variables: Option<PublicVariables>,
	/// A caller-owned transaction to run inside. When present the executor
	/// neither creates nor finalises a transaction: `BEGIN`/`COMMIT`/`CANCEL`
	/// are the caller's to issue, and the write-cardinality guard and tenant
	/// identity are applied to the supplied transaction just as they are to an
	/// executor-created one.
	pub transaction: Option<Arc<Transaction>>,
	/// A handle that stops execution at its next yield point. The executor
	/// checks [`Context::done`](crate::ctx::Context::done) between statements
	/// and inside iterator hot loops, and bare-await sites (`SLEEP`) select
	/// against the handle, so tripping it interrupts the query rather than
	/// letting it run to completion. The caller owns the handle and decides
	/// when to trip it.
	pub cancel: Option<CancelHandle>,
	/// The encoding a streaming execution writes its rows in, for a transport
	/// that answers in it: rows then arrive as
	/// [`QueryStreamItem::Frame`](surrealdb_rpc::QueryStreamItem::Frame)s and
	/// the server builds no public value for them. `None` streams rows as
	/// values. A buffered execution ignores it.
	pub row_format: Option<surrealdb_rpc::RowFormat>,
	/// The trace kinds a streaming execution reports beside its rows and on
	/// each statement's `Finished` (see [`surrealdb_rpc::trace`]). Empty
	/// reports none. A buffered execution ignores it.
	pub trace: surrealdb_rpc::TraceKinds,
	/// Run the whole query as one read-only block. See [`ReadOnlyOptions`].
	/// Refused together with a caller-owned transaction, which has its own
	/// mode.
	pub read_only: Option<ReadOnlyOptions>,
	/// Whether each explicit `BEGIN … COMMIT` transaction's rows are held until
	/// it resolves. Visible to the datastore only, so the public field set is
	/// unchanged.
	pub(super) held_transactions: bool,
	/// Whether the consumer gathers the whole answer before any of it reaches
	/// a client, as a buffered answer does. Visible to the datastore only.
	pub(super) collected: bool,
}

impl<'a> QueryRequest<'a> {
	/// A request for `source`, running as `session`, with no variables, no
	/// caller-owned transaction and no cancellation handle.
	pub fn new(source: impl Into<QuerySource<'a>>, session: &'a Session) -> Self {
		QueryRequest {
			source: source.into(),
			session,
			variables: None,
			transaction: None,
			cancel: None,
			row_format: None,
			trace: surrealdb_rpc::TraceKinds::NONE,
			read_only: None,
			held_transactions: false,
			collected: false,
		}
	}

	/// Run the whole query as one read-only block. See [`Self::read_only`].
	pub fn with_read_only(mut self, read_only: Option<ReadOnlyOptions>) -> Self {
		self.read_only = read_only;
		self
	}

	/// Bind variables for the duration of the query.
	pub fn with_variables(mut self, variables: Option<PublicVariables>) -> Self {
		self.variables = variables;
		self
	}

	/// Run inside a caller-owned transaction. See [`Self::transaction`].
	pub fn with_transaction(mut self, transaction: Arc<Transaction>) -> Self {
		self.transaction = Some(transaction);
		self
	}

	/// Run inside a caller-owned transaction when one was supplied.
	pub fn with_optional_transaction(mut self, transaction: Option<Arc<Transaction>>) -> Self {
		self.transaction = transaction;
		self
	}

	/// Make the query interruptible through `cancel`. See [`Self::cancel`].
	pub fn with_cancel(mut self, cancel: CancelHandle) -> Self {
		self.cancel = Some(cancel);
		self
	}

	/// Make the query interruptible when a handle was supplied. See
	/// [`Self::cancel`].
	pub fn with_optional_cancel(mut self, cancel: Option<CancelHandle>) -> Self {
		self.cancel = cancel;
		self
	}

	/// Report the trace kinds `trace` while streaming. See [`Self::trace`].
	pub fn with_trace(mut self, trace: surrealdb_rpc::TraceKinds) -> Self {
		self.trace = trace;
		self
	}

	/// Stream rows encoded in `format`, or as values when `None`. See
	/// [`Self::row_format`].
	pub fn with_row_format(mut self, format: Option<surrealdb_rpc::RowFormat>) -> Self {
		self.row_format = format;
		self
	}

	/// Hold each explicit `BEGIN … COMMIT` transaction's rows until it has
	/// resolved, rather than streaming them provisionally. Read only by
	/// `Datastore::run_streaming` outside a caller-owned transaction. An
	/// internal knob, not an API.
	#[doc(hidden)]
	pub fn with_held_transactions(mut self) -> Self {
		self.held_transactions = true;
		self
	}

	/// The streamed items are gathered into a whole answer before any of it
	/// reaches a client, so no item counts as delivered while the query runs:
	/// a write that loses a retryable commit conflict is run again however much
	/// it had produced, as it is for a buffered answer. Read only by
	/// `Datastore::run_streaming`. An internal knob, not an API.
	#[doc(hidden)]
	pub fn with_collected_answer(mut self) -> Self {
		self.collected = true;
		self
	}
}

#[cfg(test)]
mod tests {
	use super::{Dialect, QuerySource};

	/// A caller that passes bare text gets SurrealQL, so naming a dialect is
	/// something a caller does deliberately rather than something every call
	/// site has to restate.
	#[test]
	fn bare_text_is_surrealql() {
		let QuerySource::Text {
			dialect,
			..
		} = QuerySource::from("SELECT * FROM person")
		else {
			panic!("text is a `Text` source");
		};
		assert_eq!(dialect, Dialect::SurrealQl);
		assert_eq!(Dialect::default(), Dialect::SurrealQl);
	}
}
