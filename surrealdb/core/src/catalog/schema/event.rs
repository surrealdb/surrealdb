use std::sync::OnceLock;

use anyhow::Context as _;
use revision::revisioned;
use surrealdb_strand::Strand;
use surrealdb_types::{SqlFormat, ToSql};

use crate::catalog::auth::AuthLimit;
use crate::catalog::{ExprText, FromStored};
use crate::expr::Expr;
use crate::expr::statements::info::InfoStructure;
use crate::key::impl_kv_value_revisioned;
use crate::sql;
use crate::val::{TableName, Value};

/// Whether a stored event runs inline with the triggering write or is
/// dispatched asynchronously, and the retry/nesting budget when async.
///
/// The stored twin of [`sql::EventKind`]. The AST carries the parsed form and
/// the parser's defaults; this carries the persisted form, so it is the one
/// that owns the encoding and must not change shape without a revision bump.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum EventKind {
	Sync,
	Async {
		/// Maximum retry count for async events (0 disables retries; event still runs once).
		retry: u16,
		/// Maximum async event nesting depth for this event (0 allows top-level only).
		max_depth: u16,
	},
}

impl From<sql::EventKind> for EventKind {
	fn from(v: sql::EventKind) -> Self {
		match v {
			sql::EventKind::Sync => Self::Sync,
			sql::EventKind::Async {
				retry,
				max_depth,
			} => Self::Async {
				retry,
				max_depth,
			},
		}
	}
}

impl From<EventKind> for sql::EventKind {
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
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
#[non_exhaustive]
pub struct StoredEventDefinition {
	pub(crate) name: Strand,
	pub(crate) target_table: Strand,
	/// Canonical SurrealQL text of the `WHEN` clause's expression.
	pub(crate) when: ExprText,
	/// Canonical SurrealQL text of each `THEN` clause expression.
	pub(crate) then: Vec<ExprText>,
	pub(crate) comment: Option<String>,
	/// The auth limit of the API.
	#[revision(start = 2, default_fn = "default_auth_limit")]
	pub(crate) auth_limit: AuthLimit,
	/// Whether this event should be queued for async processing.
	#[revision(start = 3, default_fn = "default_event_kind")]
	pub(crate) kind: EventKind,
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

impl_kv_value_revisioned!(StoredEventDefinition);

impl StoredEventDefinition {
	pub(crate) fn retry(&self) -> u16 {
		match self.kind {
			EventKind::Sync => 0,
			EventKind::Async {
				retry,
				..
			} => retry,
		}
	}

	pub(crate) fn max_depth(&self) -> u16 {
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
	/// Lowers to the sql-side statement, which is what renders. One renderer,
	/// not two: see `FieldDefinition::to_sql_definition`.
	pub(crate) fn to_sql_definition(&self) -> sql::statements::define::DefineEventStatement {
		sql::statements::define::DefineEventStatement {
			kind: sql::statements::define::DefineKind::Default,
			name: sql::Expr::Idiom(sql::Idiom::field(self.name.clone())),
			target_table: sql::Expr::Table(self.target_table.clone().into()),
			when: self.when.clone().into(),
			then: self.then.iter().cloned().map(Into::into).collect(),
			comment: self
				.comment
				.clone()
				.map(|v| sql::Expr::Literal(sql::Literal::String(v.into())))
				.unwrap_or(sql::Expr::Literal(sql::Literal::None)),
			event_kind: self.kind.clone().into(),
		}
	}
}

impl ToSql for EventDefinition {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		self.to_sql_definition().fmt_sql(f, fmt)
	}
}

/// Runtime form of [`StoredEventDefinition`].
#[derive(Debug)]
pub(crate) struct EventDefinition {
	pub name: Strand,
	pub target_table: TableName,
	pub when: Expr,
	pub then: Vec<Expr>,
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
			kind: self.kind.clone(),
			auth_limit: self.auth_limit.clone(),
			comment: self.comment.clone(),
			stored: OnceLock::new(),
		}
	}
}

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
	pub(crate) fn is_async(&self) -> bool {
		matches!(self.kind, EventKind::Async { .. })
	}

	/// Builds a definition from freshly-parsed clauses.
	#[allow(clippy::too_many_arguments)]
	pub(crate) fn new(
		name: Strand,
		target_table: TableName,
		when: Expr,
		then: Vec<Expr>,
		kind: EventKind,
		auth_limit: AuthLimit,
		comment: Option<String>,
	) -> Self {
		Self {
			name,
			target_table,
			when,
			then,
			kind,
			auth_limit,
			comment,
			stored: OnceLock::new(),
		}
	}

	/// The stored form, rendered once. See [`EventDefinition::stored`].
	pub(crate) fn stored(&self) -> &StoredEventDefinition {
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
	pub(crate) fn to_stored(&self) -> StoredEventDefinition {
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
