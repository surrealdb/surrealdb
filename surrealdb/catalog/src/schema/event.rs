use std::sync::OnceLock;

use anyhow::Context as _;
use revision::revisioned;
use surrealdb_strand::Strand;
use surrealdb_types::{SqlFormat, ToSql};

use crate::catalog::auth::AuthLimit;
use crate::catalog::{ExprText, FromStored, LogicalBody};
use crate::expr::Expr;
use crate::expr::logical::LogicalExpr;
use crate::expr::statements::info::InfoStructure;
use crate::stored::Stored;
use crate::val::{TableName, Value};
use crate::{expr as runtime, impl_kv_value_catalog};

/// Whether a stored event runs inline with the triggering write or is
/// dispatched asynchronously, and the retry/nesting budget when async.
///
/// The stored twin of [`runtime::EventKind`]. The AST carries the parsed form and
/// the parser's defaults; this carries the persisted form, so it is the one
/// that owns the encoding and must not change shape without a revision bump.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash, Stored)]
#[stored(since = "4.0")]
pub enum EventKind {
	#[stored(n = 1)]
	Sync,
	#[stored(n = 2)]
	Async {
		/// Maximum retry count for async events (0 disables retries; event still runs once).
		#[stored(n = 2)]
		retry: u16,
		/// Maximum async event nesting depth for this event (0 allows top-level only).
		#[stored(n = 4)]
		max_depth: u16,
	},
}

impl From<runtime::EventKind> for EventKind {
	fn from(v: runtime::EventKind) -> Self {
		match v {
			runtime::EventKind::Sync => Self::Sync,
			runtime::EventKind::Async {
				retry,
				max_depth,
			} => Self::Async {
				retry,
				max_depth,
			},
		}
	}
}

impl From<EventKind> for runtime::EventKind {
	fn from(v: EventKind) -> Self {
		match v {
			EventKind::Sync => Self::Sync,
			EventKind::Async {
				retry,
				max_depth,
			} => Self::Async {
				retry,
				max_depth,
			},
		}
	}
}

#[revisioned(revision = 3)]
#[derive(Clone, Debug, Eq, PartialEq, Hash, Stored)]
#[stored(since = "4.0", revision_embedded)]
pub struct StoredEventDefinition {
	#[stored(n = 2)]
	pub name: Strand,
	#[stored(n = 4)]
	pub target_table: Strand,
	/// Canonical SurrealQL text of the `WHEN` clause's expression.
	#[stored(n = 6)]
	pub when: ExprText,
	/// Canonical SurrealQL text of each `THEN` clause expression.
	#[stored(n = 8)]
	pub then: Vec<ExprText>,
	#[stored(
		n = 1,
		ignorable,
		independent = "a comment is text for people and no other field reads it"
	)]
	pub comment: Option<String>,
	/// The auth limit of the API.
	#[revision(start = 2, default_fn = "default_auth_limit")]
	#[stored(n = 10)]
	pub auth_limit: AuthLimit,
	/// Whether this event should be queued for async processing.
	#[revision(start = 3, default_fn = "default_event_kind")]
	#[stored(n = 12)]
	pub kind: EventKind,
}

// This was pushed in after the first beta, so we need to add auth_limit to structs in a
// non-breaking way
impl StoredEventDefinition {
	fn default_auth_limit(_revision: u16) -> Result<AuthLimit, revision::Error> {
		Ok(AuthLimit::new_no_limit())
	}

	fn default_event_kind(_revision: u16) -> Result<EventKind, revision::Error> {
		Ok(EventKind::Sync)
	}
}

impl_kv_value_catalog!(StoredEventDefinition);

impl StoredEventDefinition {
	pub fn retry(&self) -> u16 {
		match self.kind {
			EventKind::Sync => 0,
			EventKind::Async {
				retry,
				..
			} => retry,
		}
	}

	pub fn max_depth(&self) -> u16 {
		match self.kind {
			EventKind::Sync => 0,
			EventKind::Async {
				max_depth,
				..
			} => max_depth,
		}
	}
}

impl InfoStructure for EventDefinition {
	fn structure(self) -> Value {
		let mut map = map! {
			"name" => self.name.into(),
			"what" => Value::Table(self.target_table),
			"when" => Value::from(self.when.to_stored_sql()),
			"then" => self.then.iter().map(|e| Value::from(e.to_stored_sql())).collect(),
			"comment", if let Some(v) = self.comment => v.into(),
		};
		if let EventKind::Async {
			retry,
			max_depth,
		} = &self.kind
		{
			map.insert("async", Value::Bool(true));
			map.insert("retry", (*retry).into());
			map.insert("maxdepth", (*max_depth).into());
		}
		Value::from(map)
	}
}

impl EventDefinition {
	/// Builds the runtime definition used by the canonical schema printer.
	pub fn to_runtime_definition(&self) -> runtime::statements::define::DefineEventStatement {
		runtime::statements::define::DefineEventStatement {
			kind: runtime::statements::define::DefineKind::Default,
			name: runtime::Expr::Idiom(runtime::Idiom::field(self.name.clone())),
			target_table: runtime::Expr::Table(self.target_table.clone()),
			when: self.when.clone(),
			then: self.then.clone(),
			comment: self
				.comment
				.clone()
				.map(|v| runtime::Expr::Literal(runtime::Literal::String(v.into())))
				.unwrap_or(runtime::Expr::Literal(runtime::Literal::None)),
			event_kind: self.kind.clone().into(),
		}
	}
}

impl ToSql for EventDefinition {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		self.to_runtime_definition().fmt_sql(f, fmt)
	}
}

/// Runtime form of [`StoredEventDefinition`].
///
/// The clauses the streaming engine evaluates — `WHEN` and each `THEN` — are
/// carried twice: as the [`Expr`] the legacy engine evaluates and as a lazily
/// compiled logical body. Both come from the stored text, so `when_body` names
/// the same clause as `when` and `then_body` holds one body per entry of
/// `then`, in order. A constructor that sets one half must set the other from
/// the same source; [`LogicalBody::of`] does so from the expression a `DEFINE`
/// statement produced.
#[derive(Debug)]
pub struct EventDefinition {
	pub name: Strand,
	pub target_table: TableName,
	pub when: Expr,
	pub then: Vec<Expr>,
	/// The `WHEN` clause in logical form; see [`Self::when_logical`].
	pub when_body: LogicalBody,
	/// Each `THEN` clause in logical form, in the order they run; see
	/// [`Self::then_logical`].
	pub then_body: Vec<LogicalBody>,
	pub kind: EventKind,
	pub auth_limit: AuthLimit,
	pub comment: Option<String>,
	/// Memoized [`EventDefinition::to_stored`].
	///
	/// An `ASYNC` event persists its stored form into the queue for every row
	/// it fires on, and rendering walks the `WHEN` and every `THEN` to text.
	/// Definitions are read from the transaction-cached list, so rendering
	/// once per definition rather than once per row makes a bulk write pay it
	/// a handful of times instead of a million.
	///
	/// Derived, so it takes no part in equality and is not carried across a
	/// clone; both are safe because the fields it is derived from are never
	/// mutated after construction.
	stored: OnceLock<StoredEventDefinition>,
}

impl Clone for EventDefinition {
	fn clone(&self) -> Self {
		Self {
			name: self.name.clone(),
			target_table: self.target_table.clone(),
			when: self.when.clone(),
			then: self.then.clone(),
			when_body: self.when_body.clone(),
			then_body: self.then_body.clone(),
			kind: self.kind.clone(),
			auth_limit: self.auth_limit.clone(),
			comment: self.comment.clone(),
			stored: OnceLock::new(),
		}
	}
}

/// The logical bodies are derived from `when` and `then`, so they take no part
/// in equality: two definitions with the same clauses are equal whatever their
/// compile state.
impl PartialEq for EventDefinition {
	fn eq(&self, other: &Self) -> bool {
		self.name == other.name
			&& self.target_table == other.target_table
			&& self.when == other.when
			&& self.then == other.then
			&& self.kind == other.kind
			&& self.auth_limit == other.auth_limit
			&& self.comment == other.comment
	}
}

impl Eq for EventDefinition {}

impl EventDefinition {
	/// Returns true if the event is asynchronous.
	pub fn is_async(&self) -> bool {
		matches!(self.kind, EventKind::Async { .. })
	}

	/// Builds a definition from freshly-parsed clauses.
	#[allow(clippy::too_many_arguments)]
	pub fn new(
		name: Strand,
		target_table: TableName,
		when: Expr,
		then: Vec<Expr>,
		kind: EventKind,
		auth_limit: AuthLimit,
		comment: Option<String>,
	) -> Self {
		let when_body = LogicalBody::of(&when);
		let then_body = then.iter().map(LogicalBody::of).collect();
		Self {
			name,
			target_table,
			when,
			then,
			when_body,
			then_body,
			kind,
			auth_limit,
			comment,
			stored: OnceLock::new(),
		}
	}

	/// The `WHEN` clause as the logical scalar the streaming engine evaluates,
	/// compiled on first call.
	///
	/// Errs when the clause has no logical form — a write statement, a mock
	/// record set in scalar position — and the caller then evaluates the
	/// [`Expr`] half instead.
	pub fn when_logical(&self) -> anyhow::Result<&LogicalExpr> {
		self.when_body.logical()
	}

	/// The `THEN` clauses as the logical scalars the streaming engine
	/// evaluates, one per clause in the order they run, each compiled on first
	/// call. An entry errs exactly as [`Self::when_logical`] does.
	pub fn then_logical(&self) -> impl Iterator<Item = anyhow::Result<&LogicalExpr>> {
		self.then_body.iter().map(LogicalBody::logical)
	}

	/// The stored form, rendered once. See [`EventDefinition::stored`].
	pub fn stored(&self) -> &StoredEventDefinition {
		self.stored.get_or_init(|| self.to_stored())
	}
}

impl FromStored for EventDefinition {
	type Stored = StoredEventDefinition;

	fn from_stored(stored: &StoredEventDefinition) -> anyhow::Result<EventDefinition> {
		fn build(stored: &StoredEventDefinition) -> anyhow::Result<EventDefinition> {
			Ok(EventDefinition {
				name: stored.name.clone(),
				target_table: TableName::from(stored.target_table.clone()),
				when: stored.when.compile()?,
				then: stored.then.iter().map(|t| t.compile()).collect::<anyhow::Result<_>>()?,
				when_body: LogicalBody::new(&stored.when),
				then_body: stored.then.iter().map(LogicalBody::new).collect(),
				kind: stored.kind.clone(),
				auth_limit: stored.auth_limit.clone(),
				comment: stored.comment.clone(),
				stored: OnceLock::new(),
			})
		}
		build(stored).with_context(|| {
			format!(
				"the stored definition of event `{}` on table `{}` no longer compiles",
				stored.name, stored.target_table
			)
		})
	}
}

impl EventDefinition {
	pub fn to_stored(&self) -> StoredEventDefinition {
		StoredEventDefinition {
			name: self.name.clone(),
			target_table: self.target_table.clone().into(),
			when: ExprText::new(&self.when),
			then: self.then.iter().map(ExprText::new).collect(),
			comment: self.comment.clone(),
			auth_limit: self.auth_limit.clone(),
			kind: self.kind.clone(),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	/// The bodies are carried twice from one source: a definition a `DEFINE`
	/// built and the same definition read back from storage compile the same
	/// scalars, one per `THEN` in the order they run.
	#[test]
	fn the_when_and_then_clauses_compile_to_their_logical_form() {
		let when: Expr = ExprText::from_raw("$event != NONE").compile().unwrap();
		let then: Vec<Expr> = ["$value.a", "$value.b"]
			.iter()
			.map(|text| ExprText::from_raw(*text).compile().unwrap())
			.collect();
		let definition = EventDefinition::new(
			Strand::new("ev"),
			TableName::from("t"),
			when,
			then,
			EventKind::Sync,
			AuthLimit::new_no_limit(),
			None,
		);

		let loaded = EventDefinition::from_stored(&definition.to_stored()).unwrap();
		assert_eq!(loaded, definition);
		assert_eq!(loaded.when_logical().unwrap().to_sql(), "$event != NONE");
		let then: Vec<String> = loaded.then_logical().map(|body| body.unwrap().to_sql()).collect();
		assert_eq!(then, ["$value.a", "$value.b"]);
	}
}
