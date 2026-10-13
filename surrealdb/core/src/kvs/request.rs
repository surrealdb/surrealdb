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

use crate::ctx::CancelHandle;
use crate::dbs::Session;
use crate::expr::LogicalPlan;
use crate::kvs::Transaction;
use crate::sql::Ast;
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
}

/// The form a query arrives in.
///
/// The runner splits every variant into its statements before the session
/// preflight runs, so a caller that submits unparsable text learns that from
/// the call rather than from a later stage. SurrealQL keeps its surface form
/// from there on: the executor converts each statement to the expression layer
/// when it is about to run it.
pub enum QuerySource<'src> {
	/// Query text, in `dialect`. Parsed with the datastore's capabilities and
	/// parser configuration.
	Text {
		text: &'src str,
		dialect: Dialect,
	},
	/// An already-parsed SurrealQL surface AST.
	Ast(Ast),
	/// Statements lowered by a caller that builds the expression tree itself:
	/// a `PreparedGqlQuery` wraps one of these, so pass its inner plan; so do
	/// the GraphQL bridge and model upload. There is no surface syntax behind
	/// these.
	Plan(LogicalPlan),
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

impl From<LogicalPlan> for QuerySource<'_> {
	fn from(plan: LogicalPlan) -> Self {
		QuerySource::Plan(plan)
	}
}

/// Bare text is SurrealQL. Another dialect is named, never inferred.
impl<'src> From<&'src str> for QuerySource<'src> {
	fn from(text: &'src str) -> Self {
		QuerySource::text(text)
	}
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
		}
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
