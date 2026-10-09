//! The logical plan: a tree of operators lowered from the surface AST.
//!
//! [`LogicalOp`] describes *what* a statement computes, as an ordered tree of
//! operators over scalar [`LogicalExpr`]s. The physical planner
//! (`surrealdb_core::exec::planner`) compiles it into `ExecOperator`s, choosing
//! access paths and pushdowns; nothing here names an index, a scan strategy or
//! a transaction.
//!
//! # Scalars are `LogicalExpr`, statements are operators
//!
//! The plan is built for the engine. Its scalars are [`LogicalExpr`]: a type that holds
//! scalars only, sharing the expression-free leaf types (parameters, table
//! names, constants, kinds, operators) with the expression layer and
//! mirroring everything that holds an expression. Every statement kind the
//! engine runs is an operator — a `SELECT` pipeline, an [`LogicalOp::Eval`] of
//! a bare expression, a [`LogicalOp::Sequence`] for a block, a
//! [`LogicalOp::Let`], [`LogicalOp::Return`], [`LogicalOp::Foreach`],
//! [`LogicalOp::If`], [`LogicalOp::Throw`], [`LogicalOp::Break`],
//! [`LogicalOp::Continue`], [`LogicalOp::Info`], [`LogicalOp::Ddl`], [`LogicalOp::Sleep`] or
//! [`LogicalOp::Explain`] — and the one way a statement appears inside a scalar is
//! [`LogicalExpr::Subquery`]. A block in scalar position is
//! `Subquery(Sequence)`; a block as a statement is `Sequence`. One encoding.
//!
//! Schema-derived behaviour reaches the plan by *desugaring* rather than by
//! gaining a node kind:
//!
//! | Declaration | Logical form |
//! |---|---|
//! | `PERMISSIONS FOR select WHERE p` on a table | [`LogicalOp::PermissionFilter`] |
//! | `DEFINE FIELD c TYPE int COMPUTED a + b` | a layer column `a + b AS c : int` in [`EvalMode::Computed`] |
//! | `PERMISSIONS FOR select WHERE p` on field `f` | a layer column `(IF p THEN f ELSE NONE END) AS f` in [`EvalMode::Permission`] |
//! | `PERMISSIONS FOR select WHERE p` on field `a.b` | a layer column `set_field(a, 'b', (IF p THEN a.b ELSE NONE END)) AS a` ([`LogicalExpr::SetField`]) |
//!
//! A *layer* is a [`LogicalOp::Project`] of the shape `*, column, …` whose
//! every column carries a frame that is not the caller's
//! ([`LogicalOp::is_layer`]): the row passes through with the named columns
//! set, each under the field's own name. The row above a stack of layers
//! carries `c` as its computed value and `f` as the value the caller may
//! see, so a user expression reads plain field names and nothing is
//! rewritten. A `Project` that is not a layer is the user's projection.
//!
//! # Rendering
//!
//! A plan renders as an indented operator tree whose scalars are SurrealQL and
//! whose scalar subqueries are nested plans. The text is deterministic, so it
//! is what golden tests compare and what a plan cache keys on. It is not
//! reparseable SurrealQL, and nothing turns a plan back into a statement.
//!
//! # Evaluation frames
//!
//! Every expression in a plan is evaluated in an [`EvalFrame`]: a *mode*, which
//! fixes whether the expression's own reads are permission-checked and whether
//! it may write, and an optional *authority limit* narrowing the actor it runs
//! as. The frame belongs to the operator slot that owns the expression, never
//! to the expression: the same `a + b` is an ordinary projection in one plan
//! and a definer's-rights computed field in another. [`LogicalOp::Filter`]
//! always evaluates as the caller; [`LogicalOp::PermissionFilter`] always in
//! [`EvalMode::Permission`]; a projected field carries its frame on
//! [`LogicalField::Single`], which is the caller's for a field the user wrote
//! and the definer's for a layer column.
//!
//! # Barriers are pass policy, not node state
//!
//! [`LogicalOp::PermissionFilter`] and the layers above it establish the
//! record image the operators above them are entitled to see. The rule a pass
//! must uphold: a user-written expression never moves below a
//! `PermissionFilter`, and moves below a layer only when every field it reads
//! is a passthrough of that layer — a column with a non-caller frame is
//! opaque, since the value under that name differs on the two sides — and
//! neither node is dropped, because that ordering is what stops a user
//! expression observing a value the check is about to refuse. The one
//! permitted crossing is the physical planner's access-path offer beneath the
//! stack: an index lookup reads keys rather than record values, and the nodes
//! above still judge every row it yields, so the predicate and ordering above
//! the stack are offered to index selection less every conjunct and order key
//! whose root field is a column of some layer above the source. A
//! [`LogicalOp::Join`] whose frame is a permission frame was lifted out of a
//! permission predicate and is a barrier of the same standing: no user
//! expression moves below it. Each pass states its own rule by matching
//! `PermissionFilter`, [`LogicalOp::is_layer`] and a join's frame; there is
//! no `is_barrier` predicate to consult, so a pass that forgets them fails to
//! compile when a new variant is added rather than silently hoisting across
//! it.
//!
//! # A relation reached from a row is a join
//!
//! A [`LogicalOp::Join`] pairs each left row with the right rows whose keys
//! match, under an alias the left row does not otherwise carry. It is the
//! form a correlated scalar subquery takes once a pass has lifted it: an
//! `IN (SELECT VALUE k FROM t WHERE …)` becomes a semi join on `k`, its
//! negation an anti join, with the subquery's correlated conjuncts as further
//! keys or as the join's filter. A shape no pass lifts stays a scalar
//! subquery evaluated per row; the two encodings yield the same rows, and a
//! join is preferred where the planner can read the right side once or by
//! key. A `FETCH` stays a [`LogicalOp::Fetch`]: it lands the fetched record
//! in place of the link, leaving a link that is not set unset, which no
//! projection over a join expresses.
//!
//! # Hints ride on the node they constrain
//!
//! `WITH INDEX` / `WITH NOINDEX` on [`LogicalOp::Source`] and `TEMPFILES` on
//! [`LogicalOp::Sort`] are physical-strategy hints the user wrote. They do not
//! change which rows come out, and passes must neither depend on nor remove
//! them. A hint is kept on the logical node it constrains because the enum has
//! no node identity to key a side table on. `WITH` is attached to every
//! `Source` of a multi-source statement, since access paths are chosen per
//! source.
//!
//! # Versions are ambient
//!
//! [`LogicalOp::VersionScope`] wraps the whole pipeline, as `VERSION` does in
//! the statement, and every source beneath it reads as of that version. A pass
//! cannot answer "is this source versioned?" from the source alone; one that
//! needs to know carries the enclosing version down as it descends.
//!
//! # Schemas
//!
//! [`LogicalOp::Source`] carries the schema the analyzer resolved for it, or
//! `None` — open — in an unbound plan. Every other node derives its schema
//! from its inputs through [`LogicalOp::schema`]; see [`schema`] for what a
//! schema says and how a scalar is typed against one.
//!
//! # Traversal
//!
//! [`LogicalOp::children`] and [`LogicalOp::map_children`] reach a node's
//! inputs; [`LogicalOp::visit_scalars`] and [`LogicalOp::visit_scalars_mut`]
//! reach the expressions a node owns. [`LogicalOp::visit`] walks the operator
//! tree and [`LogicalOp::visit_scalars_deep`] that tree's scalars; both stay
//! within it. A scalar's own nesting is walked by [`LogicalExpr::visit`],
//! which stops at a [`LogicalExpr::Subquery`]: the plan inside is a
//! `LogicalOp`, and a walk of the tree alone does not reach it.
//!
//! [`LogicalOp::visit_ops_deep`] is the walk of the whole statement: every
//! operator in pre-order — the node, then every plan nested in a
//! [`LogicalExpr::Subquery`] within one of the node's scalars at any depth,
//! then the node's inputs. It is what a pass whose analysis is about the
//! statement uses, in place of a descent of its own. It stops at a
//! [`LogicalExpr::Closure`] body: a closure body is bound by nothing where it
//! is written and is compiled where the closure is invoked, so a plan inside
//! one is not part of this statement.
//!
//! A [`LogicalOp::Match`] plan is a leaf to the tree traversals: no pass
//! descends into its stages, whose binding-table semantics are not row
//! semantics, but its scalars are [`LogicalExpr`]s like every other node's and
//! [`LogicalOp::visit_scalars`] reaches them, so a walk over a plan's scalars
//! covers every expression the plan evaluates — and a plan one of those
//! scalars holds is reached like any other.
//!
//! A schema statement is a [`LogicalOp::Ddl`], which carries the statement
//! whole. Its clauses stay surface expressions and are not this layer's
//! scalars, so no pass here reads or rewrites them.

// Adding a `LogicalOp` variant must break every pass that has not stated its
// position on it, which a wildcard arm would silently absorb.
#![warn(clippy::wildcard_enum_match_arm)]

mod effects;
mod exits;
mod expr;
mod lower;
pub mod schema;

use std::fmt::{self, Display, Formatter};
use std::ops::ControlFlow;
use std::sync::Arc;

use surrealdb_iam::AuthLimit;
use surrealdb_strand::{Strand, TableName};
use surrealdb_types::{SqlFormat, ToSql, write_sql};

pub use self::effects::{
	EffectOracle, EffectSet, Effects, ITERATION_PARAMS, ROW_PARAMS, effects_of,
	invokes_unseen_body, key_is_generated, may_dereference,
};
pub use self::exits::Exits;
pub use self::expr::{
	LogicalCall, LogicalClosure, LogicalDestructureField, LogicalExpr, LogicalField, LogicalFields,
	LogicalIf, LogicalLiteral, LogicalLookup, LogicalLookupSubject, LogicalOrder, LogicalOrdering,
	LogicalPostfixOperator, LogicalRecordIdKey, LogicalRecordIdKeyRange, LogicalRecordIdLit,
	LogicalRecurse, LogicalRecurseInstruction, LogicalRecursePath, OutputPath, OutputPathPart,
	SourceRef,
};
pub use self::lower::{
	NoLogicalForm, is_value_literal, lower, lower_expr, lower_fields, lower_select,
	lower_value_statement,
};
pub use self::schema::{
	ComputedBody, FieldPath, FieldSchema, FieldSegment, LogicalPermission, LogicalPermissionRef,
	Openness, Schema,
};
use crate::expr::match_plan::MatchPlan;
use crate::expr::with::With;
use crate::expr::{Base, ExplainFormat, Idiom, Kind, Param};

/// How an expression's own reads and writes are treated while it evaluates.
///
/// One variant per evaluation frame the executor distinguishes. Two
/// independent properties vary: whether the reads the expression performs are
/// themselves permission-checked, and whether it may run a writing statement.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum EvalMode {
	/// An expression the user wrote in the statement. Reads are
	/// permission-checked; writes are allowed.
	#[default]
	Caller,
	/// A permission predicate on a read: `PERMISSIONS FOR select` on a table
	/// or a field. Reads are not permission-checked, otherwise a permission
	/// that follows a record link recurses into itself; writes are rejected.
	Permission,
	/// A permission predicate a write triggers: `PERMISSIONS FOR create`,
	/// `update` or `delete`. Reads are not permission-checked, as for
	/// [`EvalMode::Permission`]; writes are allowed, so a predicate that logs
	/// or counts runs rather than failing.
	MutablePermission,
	/// A `COMPUTED` field body. Reads *are* permission-checked, as the
	/// definer; writes are rejected.
	Computed,
}

impl EvalMode {
	/// Whether reads the expression performs are permission-checked.
	pub fn checks_permissions(self) -> bool {
		matches!(self, EvalMode::Caller | EvalMode::Computed)
	}

	/// Whether a writing statement inside the expression is rejected.
	pub fn forbids_writes(self) -> bool {
		matches!(self, EvalMode::Permission | EvalMode::Computed)
	}
}

/// Who evaluates an expression, and under what authority.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct EvalFrame {
	pub mode: EvalMode,
	/// Narrows the authority the expression runs as to that recorded by the
	/// `DEFINE` statement that supplied it (`AUTH LIMIT`). `None` runs as the
	/// caller.
	///
	/// This is the confused-deputy guard: a low-privileged definer supplies a
	/// `COMPUTED`, `VALUE`, `ASSERT` or event body and a high-privileged caller
	/// supplies the privilege, so the body must not run with the caller's
	/// reach. It is independent of `mode`: a `VALUE` clause runs in
	/// [`EvalMode::Caller`] under the definer's limit.
	pub auth: Option<AuthLimit>,
}

impl EvalFrame {
	/// The user's own expression: caller mode, caller's authority.
	pub fn caller() -> Self {
		Self::default()
	}

	/// A `PERMISSIONS FOR select` predicate under the definer's limit.
	pub fn permission(auth: Option<AuthLimit>) -> Self {
		Self {
			mode: EvalMode::Permission,
			auth,
		}
	}

	/// A `PERMISSIONS FOR create/update/delete` predicate under the definer's
	/// limit.
	pub fn mutable_permission(auth: Option<AuthLimit>) -> Self {
		Self {
			mode: EvalMode::MutablePermission,
			auth,
		}
	}

	/// A `COMPUTED` field body under the definer's limit.
	pub fn computed(auth: Option<AuthLimit>) -> Self {
		Self {
			mode: EvalMode::Computed,
			auth,
		}
	}

	/// Whether this is the plain caller frame, which a rendered plan omits.
	pub fn is_caller(&self) -> bool {
		self.mode == EvalMode::Caller && self.auth.is_none()
	}
}

/// An `INFO FOR …` statement. The subjects and the `VERSION` clause are
/// expressions the statement evaluates before reading the catalog.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum LogicalInfo {
	Root {
		structure: bool,
		version: Option<LogicalExpr>,
	},
	Ns {
		structure: bool,
		version: Option<LogicalExpr>,
	},
	Db {
		structure: bool,
		version: Option<LogicalExpr>,
	},
	Table {
		table: LogicalExpr,
		structure: bool,
		version: Option<LogicalExpr>,
	},
	User {
		user: LogicalExpr,
		base: Option<Base>,
		structure: bool,
	},
	Index {
		index: LogicalExpr,
		table: LogicalExpr,
		structure: bool,
	},
}

impl LogicalInfo {
	/// Every expression the statement evaluates.
	pub fn for_each_expr<'a>(&'a self, out: &mut dyn FnMut(&'a LogicalExpr)) {
		match self {
			LogicalInfo::Root {
				version,
				..
			}
			| LogicalInfo::Ns {
				version,
				..
			}
			| LogicalInfo::Db {
				version,
				..
			} => {
				if let Some(e) = version {
					out(e);
				}
			}
			LogicalInfo::Table {
				table,
				version,
				..
			} => {
				out(table);
				if let Some(e) = version {
					out(e);
				};
			}
			LogicalInfo::User {
				user,
				..
			} => out(user),
			LogicalInfo::Index {
				index,
				table,
				..
			} => {
				out(index);
				out(table);
			}
		}
	}

	/// [`Self::for_each_expr`], collected.
	pub fn exprs<'a>(&'a self, out: &mut Vec<&'a LogicalExpr>) {
		self.for_each_expr(&mut |expr| out.push(expr));
	}

	/// [`Self::exprs`], mutably.
	pub fn exprs_mut<'a>(&'a mut self, out: &mut Vec<&'a mut LogicalExpr>) {
		match self {
			LogicalInfo::Root {
				version,
				..
			}
			| LogicalInfo::Ns {
				version,
				..
			}
			| LogicalInfo::Db {
				version,
				..
			} => out.extend(version),
			LogicalInfo::Table {
				table,
				version,
				..
			} => {
				out.push(table);
				out.extend(version);
			}
			LogicalInfo::User {
				user,
				..
			} => out.push(user),
			LogicalInfo::Index {
				index,
				table,
				..
			} => {
				out.push(index);
				out.push(table);
			}
		}
	}

	/// Whether the statement only reads. Catalog metadata is read-only; a
	/// write can reach the transaction only through the subject expressions.
	pub fn read_only(&self) -> bool {
		let mut exprs = Vec::new();
		self.exprs(&mut exprs);
		exprs.into_iter().all(LogicalExpr::read_only)
	}
}

impl ToSql for LogicalInfo {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		let (structure, version) = match self {
			LogicalInfo::Root {
				structure,
				version,
			} => {
				f.push_str("INFO FOR ROOT");
				(*structure, version.as_ref())
			}
			LogicalInfo::Ns {
				structure,
				version,
			} => {
				f.push_str("INFO FOR NAMESPACE");
				(*structure, version.as_ref())
			}
			LogicalInfo::Db {
				structure,
				version,
			} => {
				f.push_str("INFO FOR DATABASE");
				(*structure, version.as_ref())
			}
			LogicalInfo::Table {
				table,
				structure,
				version,
			} => {
				write_sql!(f, fmt, "INFO FOR TABLE {table}");
				(*structure, version.as_ref())
			}
			LogicalInfo::User {
				user,
				base,
				structure,
			} => {
				write_sql!(f, fmt, "INFO FOR USER {user}");
				if let Some(base) = base {
					write_sql!(f, fmt, " ON {base}");
				}
				(*structure, None)
			}
			LogicalInfo::Index {
				index,
				table,
				structure,
			} => {
				write_sql!(f, fmt, "INFO FOR INDEX {index} ON {table}");
				(*structure, None)
			}
		};
		if let Some(version) = version {
			write_sql!(f, fmt, " VERSION {version}");
		}
		if structure {
			f.push_str(" STRUCTURE");
		}
	}
}

/// A node of the logical plan.
///
/// The relational operators are listed in the order a SELECT pipeline applies
/// them; see [`lower_select`] for the correspondence to statement clauses. The
/// statement operators follow. Every node but the leaves holds its inputs
/// inline. Four functions enumerate variants to reach a node's contents, and
/// every traversal is built on them: [`LogicalOp::children`] and
/// [`LogicalOp::map_children`] for the inputs, [`LogicalOp::visit_scalars`] and
/// [`LogicalOp::visit_scalars_mut`] for the expressions the node itself owns.
///
/// Each variant holds one named struct (`LogicalFilter`, `LogicalProject`, …)
/// carrying that operator's fields, so a pass that handles one operator takes
/// and returns the struct by name — its signature says which operator it works
/// on — and adding a field to an operator touches only the code that reads it.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum LogicalOp {
	/// Rows read from storage; see [`LogicalSource`].
	Source(LogicalSource),
	/// Rows produced from a literal value; see [`LogicalValues`].
	Values(LogicalValues),
	/// A GQL `MATCH` query as a leaf plan; see [`LogicalMatch`].
	Match(LogicalMatch),
	/// A statement in relational position; see [`LogicalSubquery`].
	Subquery(LogicalSubquery),
	/// A mutation; see [`LogicalWrite`].
	Write(LogicalWrite),
	/// A schema statement; see [`LogicalDdl`].
	Ddl(LogicalDdl),
	/// The concatenation of several inputs; see [`LogicalUnionAll`].
	UnionAll(LogicalUnionAll),
	/// Each left row paired with the right rows whose keys match; see
	/// [`LogicalJoin`].
	Join(LogicalJoin),
	/// Keeps the rows a user predicate admits; see [`LogicalFilter`].
	Filter(LogicalFilter),
	/// Keeps the rows a table permission admits; see
	/// [`LogicalPermissionFilter`].
	PermissionFilter(LogicalPermissionFilter),
	/// `SPLIT ON`; see [`LogicalSplit`].
	Split(LogicalSplit),
	/// `GROUP BY`; see [`LogicalAggregate`].
	Aggregate(LogicalAggregate),
	/// Keeps one row per distinct value; see [`LogicalDistinct`].
	Distinct(LogicalDistinct),
	/// `ORDER BY`; see [`LogicalSort`].
	Sort(LogicalSort),
	/// `LIMIT` / `START`; see [`LogicalLimit`].
	Limit(LogicalLimit),
	/// The `SELECT` projection, or a resolver layer; see [`LogicalProject`].
	Project(LogicalProject),
	/// `FETCH`; see [`LogicalFetch`].
	Fetch(LogicalFetch),
	/// `TIMEOUT`; see [`LogicalTimeout`].
	Timeout(LogicalTimeout),
	/// `VERSION`; see [`LogicalVersionScope`].
	VersionScope(LogicalVersionScope),
	/// `ONLY`; see [`LogicalOnly`].
	Only(LogicalOnly),
	/// `EXPLAIN`; see [`LogicalExplain`].
	Explain(LogicalExplain),
	/// A bare expression as a statement; see [`LogicalEval`].
	Eval(LogicalEval),
	/// A block of statements; see [`LogicalSequence`].
	Sequence(LogicalSequence),
	/// `LET`; see [`LogicalLet`].
	Let(LogicalLet),
	/// `RETURN`; see [`LogicalReturn`].
	Return(LogicalReturn),
	/// `FOR … IN`; see [`LogicalForeach`].
	Foreach(LogicalForeach),
	/// `IF` in statement position; see [`LogicalIfOp`].
	If(LogicalIfOp),
	/// `THROW`; see [`LogicalThrow`].
	Throw(LogicalThrow),
	/// `BREAK`; see [`LogicalBreak`].
	Break(LogicalBreak),
	/// `CONTINUE`; see [`LogicalContinue`].
	Continue(LogicalContinue),
	/// `INFO FOR …`.
	Info(Box<LogicalInfo>),
	/// `SLEEP`; see [`LogicalSleep`].
	Sleep(LogicalSleep),
}

/// Rows read from storage: a table, a record id or range, a mock set, or
/// a parameter or expression that resolves to one of those when the
/// statement runs. Choosing an access path is the physical planner's job.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct LogicalSource {
	pub source: SourceRef,
	/// What the catalog declares for the rows, once bound. `None` in an
	/// unbound plan and reads as open.
	pub schema: Option<Arc<Schema>>,
	/// The `WITH INDEX` / `WITH NOINDEX` hint, which constrains access-path
	/// selection but not the rows produced.
	pub with: Option<With>,
	/// `FOR UPDATE`: the read locks the records it returns.
	pub for_update: bool,
}

/// Rows produced from a literal value, as in `FROM [1, 2, 3]` or
/// `FROM 'x'`. Reads nothing, so it has no table, permissions or fields;
/// an `ONLY` over a pipeline whose every leaf is `Values` yields `NONE`
/// rather than an error when empty.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct LogicalValues {
	pub expr: LogicalExpr,
	/// `FOR UPDATE` written over the value. The rows are not records, so
	/// there is nothing for the clause to lock; the planner rejects the
	/// statement wherever it appears.
	pub for_update: bool,
	/// Whether `expr` is an array written in `FROM`, which is a list of
	/// sources: an element that evaluates to an array contributes its
	/// elements. A value a pass substituted — a parameter's, a folded call's —
	/// is one source, and is not written.
	pub written: bool,
}

/// A GQL `MATCH` query, as the binding-table plan its lowering produced.
///
/// A leaf: the plan carries its own stages, bindings and projection, and
/// the physical planner compiles it whole. The binding-table semantics
/// inside it are not row semantics, so no pass that rewrites this tree
/// descends into its stages; its scalars are [`LogicalExpr`]s and are
/// reached by [`LogicalOp::visit_scalars`] like any other node's.
///
/// Produced only by the GQL front end, which emits a plan rather than a
/// statement — there is no SurrealQL syntax that lowers to this node.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct LogicalMatch {
	pub plan: Box<MatchPlan>,
}

/// A statement in relational position, as in `FROM (SELECT …)`, lowered
/// eagerly so passes can see through it.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct LogicalSubquery {
	pub input: Box<LogicalOp>,
}

/// A mutation: `source` supplies what to write, and `statement`'s per-record
/// pipeline writes it.
///
/// The per-record write order is deliberately *not* expressed here. It is not
/// relational algebra: it is a fixed sequence whose positions carry security
/// properties, so it belongs to the operator, which holds it as an ordered
/// stage list. See `doc/adr/0001-streaming-write-path.md`.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct LogicalWrite {
	/// Which mutation this is. Carried whole because the write operator
	/// reads what the statement asked for — its form, its `ONLY`, its
	/// `TIMEOUT`.
	pub statement: LogicalWriteStatement,
	/// What the mutation writes to.
	pub source: LogicalWriteSource,
	/// The data clause in logical form: `SET`, `CONTENT`, `MERGE` and the
	/// rest, with every value an expression the rules can rewrite.
	///
	/// The statement carries the same clause as the surface form it was
	/// written in. This is the one the engine compiles, so a rule that rewrites
	/// it changes what the engine does.
	pub data: Option<LogicalData>,
	/// `INSERT … ON DUPLICATE KEY UPDATE`, in logical form.
	pub duplicate_update: Option<LogicalData>,
	/// The statement's `WHERE`, in logical form.
	pub cond: Option<LogicalExpr>,
}

/// A mutation's data clause, with its expressions in logical form.
///
/// Mirrors [`crate::expr::Data`] arm for arm. The places an assignment writes
/// to stay as written: a place is a location rather than a value, and what a
/// path names is settled where the write is compiled.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum LogicalData {
	/// `SET a = 1, b += 2`, and `ON DUPLICATE KEY UPDATE`'s assignments.
	Set(Vec<LogicalAssignment>),
	/// `UNSET a, b`.
	Unset(Vec<crate::expr::Idiom>),
	/// `PATCH [ … ]`.
	Patch(LogicalExpr),
	/// `MERGE { … }`.
	Merge(LogicalExpr),
	/// `REPLACE { … }`.
	Replace(LogicalExpr),
	/// `CONTENT { … }`.
	Content(LogicalExpr),
	/// `INSERT INTO t { … }` — one row rather than a clause applied to one.
	Single(LogicalExpr),
	/// `INSERT INTO t (a, b) VALUES (…)` — one tuple per row.
	Values(Vec<Vec<(crate::expr::Idiom, LogicalExpr)>>),
}

/// One `place = value` of a [`LogicalData::Set`].
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct LogicalAssignment {
	pub place: crate::expr::Idiom,
	pub operator: crate::expr::AssignOperator,
	pub value: LogicalExpr,
}

impl LogicalData {
	/// Every value the clause evaluates, in the order it was written.
	pub fn values(&self) -> Vec<&LogicalExpr> {
		match self {
			LogicalData::Set(assignments) => assignments.iter().map(|a| &a.value).collect(),
			LogicalData::Unset(_) => vec![],
			LogicalData::Patch(expr)
			| LogicalData::Merge(expr)
			| LogicalData::Replace(expr)
			| LogicalData::Content(expr)
			| LogicalData::Single(expr) => vec![expr],
			LogicalData::Values(rows) => {
				rows.iter().flat_map(|row| row.iter().map(|(_, value)| value)).collect()
			}
		}
	}

	/// [`Self::values`], to rewrite in place.
	pub fn values_mut(&mut self) -> Vec<&mut LogicalExpr> {
		match self {
			LogicalData::Set(assignments) => assignments.iter_mut().map(|a| &mut a.value).collect(),
			LogicalData::Unset(_) => vec![],
			LogicalData::Patch(expr)
			| LogicalData::Merge(expr)
			| LogicalData::Replace(expr)
			| LogicalData::Content(expr)
			| LogicalData::Single(expr) => vec![expr],
			LogicalData::Values(rows) => {
				rows.iter_mut().flat_map(|row| row.iter_mut().map(|(_, value)| value)).collect()
			}
		}
	}
}

/// Where a mutation's subjects come from.
///
/// The arms are not interchangeable, and the distinction is why this is an
/// enum rather than a plan subtree in every case: a creating form's subject
/// does not exist yet, so it must not be read. A reading source resolves a
/// record id to its stored document and yields nothing when there is none,
/// which for a target being created is always — the target would be silently
/// dropped rather than written.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum LogicalWriteSource {
	/// The subjects the statement names, each evaluated to a value: a table to
	/// generate an id in, a record id to write at, or — for `RELATE` — the
	/// endpoints the edge joins. Nothing is read.
	///
	/// The list is per form: `CREATE` and `UPSERT` hold the targets they were
	/// written with, in that order; `RELATE` holds exactly `[from, through,
	/// to]`.
	Targets(Vec<LogicalExpr>),
	/// `INSERT`'s rows, which name one record each. Nothing is read: a row is
	/// the record to write, not a claim about one that exists.
	Inserts(LogicalInserts),
	/// Records read from an ordinary plan subtree, for a form whose subjects
	/// already exist. This is what lowering a mutation buys: `UPDATE t WHERE …`
	/// inherits index selection, filter pushdown and the projection-layer
	/// encoding from the read side rather than going through a separate
	/// iterator.
	Rows(Box<LogicalOp>),
}

/// `INSERT`'s subjects: the rows, and the table they land on.
///
/// `INSERT` is the one form whose subjects are spelled as its data. The rows
/// are not a clause — nothing applies them to a subject the statement named
/// elsewhere — so they are the source, and every row produces one record.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct LogicalInserts {
	/// The `INTO` table, evaluated once for the statement to a table name.
	/// `None` where the statement names none, in which case each row names its
	/// table through the record id it carries.
	pub into: Option<LogicalExpr>,
	/// The rows themselves.
	pub rows: LogicalInsertRows,
}

/// How an `INSERT` spells its rows.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum LogicalInsertRows {
	/// `INSERT INTO t (a, b) VALUES (1, 2), (3, 4)` — one tuple per row. Each
	/// tuple builds a fresh empty object by setting every expression into the
	/// path paired with it, so the paths are places rather than field names:
	/// `(a.b)` writes at `a.b`.
	Tuples(Vec<Vec<(Idiom, LogicalExpr)>>),
	/// `INSERT INTO t { … }`, `INSERT INTO t [ … ]`, `INSERT INTO t (SELECT …)`
	/// — one expression, evaluated once for the statement. An object is one
	/// row and an array is one row per element.
	Value(LogicalExpr),
}

/// The statement a [`LogicalWrite`] performs.
///
/// One variant per statement form. The enum exists rather than a single boxed
/// statement so that a pass matching on the form states which forms it handles,
/// and so that adding a form is a compile error everywhere it matters.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum LogicalWriteStatement {
	Create(Box<crate::expr::statements::CreateStatement>),
	Update(Box<crate::expr::statements::UpdateStatement>),
	Upsert(Box<crate::expr::statements::UpsertStatement>),
	Delete(Box<crate::expr::statements::DeleteStatement>),
	Insert(Box<crate::expr::statements::InsertStatement>),
	Relate(Box<crate::expr::statements::RelateStatement>),
}

impl LogicalWriteStatement {
	/// The statement kind, for `EXPLAIN` and diagnostics.
	pub fn kind(&self) -> &'static str {
		match self {
			LogicalWriteStatement::Create(_) => "CREATE",
			LogicalWriteStatement::Update(_) => "UPDATE",
			LogicalWriteStatement::Upsert(_) => "UPSERT",
			LogicalWriteStatement::Delete(_) => "DELETE",
			LogicalWriteStatement::Insert(_) => "INSERT",
			LogicalWriteStatement::Relate(_) => "RELATE",
		}
	}

	/// The statement's `RETURN` clause, which the write operator dispatches on.
	pub fn output(&self) -> Option<&crate::expr::output::Output> {
		match self {
			LogicalWriteStatement::Create(stmt) => stmt.output.as_ref(),
			LogicalWriteStatement::Update(stmt) => stmt.output.as_ref(),
			LogicalWriteStatement::Upsert(stmt) => stmt.output.as_ref(),
			LogicalWriteStatement::Delete(stmt) => stmt.output.as_ref(),
			LogicalWriteStatement::Insert(stmt) => stmt.output.as_ref(),
			LogicalWriteStatement::Relate(stmt) => stmt.output.as_ref(),
		}
	}

	/// The statement's `WHERE`, which the per-record order evaluates in its own
	/// place rather than as a filter beneath the operator — the table's write
	/// permission is judged first, so a condition with side effects runs only
	/// for records the caller may write.
	///
	/// The creating forms have none: `CREATE`, `INSERT` and `RELATE` name their
	/// subjects rather than selecting them.
	pub fn cond(&self) -> Option<&crate::expr::Expr> {
		match self {
			LogicalWriteStatement::Update(stmt) => stmt.cond.as_ref().map(|cond| &cond.0),
			LogicalWriteStatement::Upsert(stmt) => stmt.cond.as_ref().map(|cond| &cond.0),
			LogicalWriteStatement::Delete(stmt) => stmt.cond.as_ref().map(|cond| &cond.0),
			LogicalWriteStatement::Create(_)
			| LogicalWriteStatement::Insert(_)
			| LogicalWriteStatement::Relate(_) => None,
		}
	}

	/// The statement's data clause.
	///
	/// `INSERT`'s is deliberately absent: its `data` produces the statement's
	/// *subjects* — the rows it writes — rather than a clause applied to one,
	/// so it is carried by [`LogicalWriteSource::Inserts`] and not here.
	/// `DELETE` has none at all.
	pub fn data(&self) -> Option<&crate::expr::Data> {
		match self {
			LogicalWriteStatement::Create(stmt) => stmt.data.as_ref(),
			LogicalWriteStatement::Update(stmt) => stmt.data.as_ref(),
			LogicalWriteStatement::Upsert(stmt) => stmt.data.as_ref(),
			LogicalWriteStatement::Relate(stmt) => stmt.data.as_ref(),
			LogicalWriteStatement::Delete(_) | LogicalWriteStatement::Insert(_) => None,
		}
	}

	/// `INSERT … ON DUPLICATE KEY UPDATE`, the clause a conflicting row is
	/// re-run as an update with. No other form has one.
	pub fn duplicate_update(&self) -> Option<&crate::expr::Data> {
		match self {
			LogicalWriteStatement::Insert(stmt) => stmt.update.as_ref(),
			LogicalWriteStatement::Create(_)
			| LogicalWriteStatement::Update(_)
			| LogicalWriteStatement::Upsert(_)
			| LogicalWriteStatement::Delete(_)
			| LogicalWriteStatement::Relate(_) => None,
		}
	}

	/// Whether the statement asked for a single record with `ONLY`.
	///
	/// `INSERT` never does: the grammar gives it no `ONLY` keyword, so the
	/// statement carries no flag to read and the answer is fixed.
	pub fn only(&self) -> bool {
		match self {
			LogicalWriteStatement::Create(stmt) => stmt.only,
			LogicalWriteStatement::Update(stmt) => stmt.only,
			LogicalWriteStatement::Upsert(stmt) => stmt.only,
			LogicalWriteStatement::Delete(stmt) => stmt.only,
			LogicalWriteStatement::Relate(stmt) => stmt.only,
			LogicalWriteStatement::Insert(_) => false,
		}
	}
}

/// A schema statement: the node that writes the catalog.
///
/// The statement is carried whole and unmirrored, as
/// [`LogicalWrite`]'s is. What a schema statement names — the thing being
/// defined, the table it hangs off, the field paths an index covers — is
/// written as an expression, but those expressions are the statement's
/// clauses rather than this node's scalars: they are resolved where the
/// catalog write is formed, against the row the statement runs under.
///
/// The node is therefore a leaf with no inputs and no scalars, and two
/// consequences follow from that. An optimiser rule that reaches expressions
/// through [`LogicalOp::scalars`] sees none here and leaves the statement
/// alone. And a mutation written into one of those clauses — `DEFINE PARAM $p
/// VALUE (CREATE t)`, a `DEFINE EVENT … THEN { … }` body — is invisible to a
/// walk over scalars and children, so a pre-flight that reports the writes a
/// plan holds does not name it. Nothing is lost by that: those writes are
/// never planned, they are performed by the same evaluator that performs the
/// catalog write. The plan still counts as writing, because
/// [`LogicalOp::read_only`] answers `false` for this node whatever it holds.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct LogicalDdl {
	/// Which schema statement this is, carried whole because the catalog
	/// write is formed from its clauses directly.
	pub statement: LogicalDdlStatement,
}

/// The statement a [`LogicalDdl`] performs.
///
/// One variant per statement form, for the same reason
/// [`LogicalWriteStatement`] has one: a pass matching on the form states which
/// forms it handles, and adding a form is a compile error everywhere it
/// matters.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum LogicalDdlStatement {
	Define(Box<crate::expr::statements::DefineStatement>),
	Remove(Box<crate::expr::statements::RemoveStatement>),
	Alter(Box<crate::expr::statements::AlterStatement>),
	Rebuild(Box<crate::expr::statements::RebuildStatement>),
}

impl LogicalDdlStatement {
	/// The statement kind, for `EXPLAIN` and diagnostics.
	pub fn kind(&self) -> &'static str {
		match self {
			LogicalDdlStatement::Define(_) => "DEFINE",
			LogicalDdlStatement::Remove(_) => "REMOVE",
			LogicalDdlStatement::Alter(_) => "ALTER",
			LogicalDdlStatement::Rebuild(_) => "REBUILD",
		}
	}
}

/// The statement as an expression, which is the form the evaluator that owns
/// the catalog writes takes.
impl From<LogicalDdlStatement> for crate::expr::Expr {
	fn from(statement: LogicalDdlStatement) -> Self {
		match statement {
			LogicalDdlStatement::Define(stmt) => crate::expr::Expr::Define(stmt),
			LogicalDdlStatement::Remove(stmt) => crate::expr::Expr::Remove(stmt),
			LogicalDdlStatement::Alter(stmt) => crate::expr::Expr::Alter(stmt),
			LogicalDdlStatement::Rebuild(stmt) => crate::expr::Expr::Rebuild(stmt),
		}
	}
}

/// Renders as the statement itself.
///
/// The statement is held in its executed form, whose rendering is what every
/// surface that prints a schema statement produces — `DEFINE USER` prints the
/// derived `PASSHASH` rather than the `PASSWORD` it was written with, because
/// the plaintext is gone by the time the statement is carried here.
impl ToSql for LogicalDdlStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		crate::expr::Expr::from(self.clone()).fmt_sql(f, fmt);
	}
}

/// The concatenation of several inputs, as in `SELECT … FROM a, b`. Rows
/// are not deduplicated.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct LogicalUnionAll {
	pub inputs: Vec<LogicalOp>,
}

/// Pairs each row of the left input with the rows of the right input whose
/// join keys match.
///
/// `on` lists the equi-join keys as `(left, right)` pairs: the left
/// expression is evaluated against a left row, the right one against a right
/// row read through `alias` (`_join0.id`), and the two match when they are
/// equal by SurrealQL's `=` — a `NULL` key matches a `NULL` key, as `IN` and
/// `CONTAINS` match their elements — and every pair matches. `filter`, when
/// present, is evaluated against the joined row (the left row with the
/// candidate right row under `alias`) and must be truthy for the pair to
/// count as a match.
///
/// The output row depends on `kind`. An `Inner` join emits the left row with
/// the matched right row under the top-level key `alias`, once per match; a
/// `Left` join additionally emits an unmatched left row once with `NULL`
/// under `alias`; a `Semi` join emits each left row with at least one match,
/// unchanged, and an `Anti` join each left row with none. Output order is
/// the left input's; matches of one left row are emitted together.
///
/// Every scalar the join owns is evaluated in `frame`. A join the
/// decorrelation rule lifted out of a `PERMISSIONS FOR select` predicate
/// carries the permission frame and is a barrier like a
/// [`LogicalOp::PermissionFilter`]:
/// a user-written expression never moves below it. The caller's frame marks
/// a join lifted from the user's own predicate.
///
/// The right input's expressions reach its rows only through `alias`, so
/// the `alias` name must not collide with a field of the left row; the
/// passes that build joins mint aliases with [`RelationAlias::new`] and
/// never re-use an index within one plan.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct LogicalJoin {
	/// `[left, right]`, stored contiguously so [`LogicalOp::children`] can
	/// borrow both as a slice.
	pub inputs: Box<[LogicalOp; 2]>,
	pub kind: JoinKind,
	pub on: Vec<(LogicalExpr, LogicalExpr)>,
	pub filter: Option<LogicalExpr>,
	pub alias: RelationAlias,
	pub frame: EvalFrame,
}

impl LogicalJoin {
	pub fn new(
		left: LogicalOp,
		right: LogicalOp,
		kind: JoinKind,
		on: Vec<(LogicalExpr, LogicalExpr)>,
		filter: Option<LogicalExpr>,
		alias: RelationAlias,
		frame: EvalFrame,
	) -> Self {
		Self {
			inputs: Box::new([left, right]),
			kind,
			on,
			filter,
			alias,
			frame,
		}
	}

	pub fn left(&self) -> &LogicalOp {
		&self.inputs[0]
	}

	pub fn right(&self) -> &LogicalOp {
		&self.inputs[1]
	}

	pub fn left_mut(&mut self) -> &mut LogicalOp {
		&mut self.inputs[0]
	}

	pub fn right_mut(&mut self) -> &mut LogicalOp {
		&mut self.inputs[1]
	}

	/// Takes the two inputs, `(left, right)`.
	pub fn into_inputs(self) -> (LogicalOp, LogicalOp) {
		let [left, right] = *self.inputs;
		(left, right)
	}

	/// The table the right input reads, when its pipeline reads exactly one
	/// table: what the joined row under the alias is a record of.
	pub fn right_table(&self) -> Option<&TableName> {
		leaf_table(self.right())
	}
}

/// Which rows a [`LogicalJoin`] emits; see the operator for each kind's
/// output.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum JoinKind {
	Inner,
	Left,
	Semi,
	Anti,
}

impl JoinKind {
	pub fn as_str(self) -> &'static str {
		match self {
			JoinKind::Inner => "INNER",
			JoinKind::Left => "LEFT",
			JoinKind::Semi => "SEMI",
			JoinKind::Anti => "ANTI",
		}
	}

	/// Whether the join adds the matched right row to the left row. A semi
	/// or anti join emits the left row unchanged.
	pub fn adds_right_row(self) -> bool {
		matches!(self, JoinKind::Inner | JoinKind::Left)
	}
}

/// The name a [`LogicalJoin`] gives the matched right row on the joined
/// row: the `n`th alias minted in a plan is `_join<n>`. The right input's
/// expressions read their fields through [`RelationAlias::access`].
///
/// The name is a field of the joined row, so it shadows a field of the left
/// row that carries the same name wherever the join's scalars are evaluated.
/// A caller therefore mints an index that no name of this form anywhere in
/// the plan carries ([`Self::index_of`]).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct RelationAlias(Strand);

impl RelationAlias {
	const PREFIX: &'static str = "_join";

	/// The alias of the `index`th join of a plan. `index` must be one no
	/// name of the minted form in the plan carries; see the type docs.
	pub fn new(index: usize) -> Self {
		Self(Strand::new(format!("{}{index}", Self::PREFIX)))
	}

	/// The index `name` carries as a minted alias, or `None` for a name that
	/// is not of the minted form. A plan that names `_join<n>` anywhere — a
	/// field of a row, a key of an object — reserves that index, since an
	/// alias minted with it would shadow the name.
	pub fn index_of(name: &str) -> Option<usize> {
		name.strip_prefix(Self::PREFIX)?.parse().ok()
	}

	pub fn name(&self) -> &Strand {
		&self.0
	}

	/// The index this alias was minted with, so a pass adding joins to a
	/// plan can continue the numbering. `None` for a name that is not of the
	/// minted form.
	pub fn index(&self) -> Option<usize> {
		Self::index_of(self.0.as_str())
	}

	/// `_join<n>`: the matched right row.
	pub fn field(&self) -> LogicalExpr {
		LogicalExpr::Field(self.0.clone())
	}

	/// `_join<n>.field`: a field of the matched right row.
	pub fn access(&self, field: impl Into<Strand>) -> LogicalExpr {
		LogicalExpr::Access {
			base: Box::new(self.field()),
			field: field.into(),
		}
	}

	/// Whether `expr` is the alias itself or a path rooted in it.
	pub fn is_root_of(&self, expr: &LogicalExpr) -> bool {
		matches!(expr.path_root(), LogicalExpr::Field(name) if name == &self.0)
	}
}

/// Keeps the rows for which the user's `predicate` is truthy. Evaluated
/// as the caller.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct LogicalFilter {
	pub input: Box<LogicalOp>,
	pub predicate: LogicalExpr,
}

/// Keeps the rows the table's `PERMISSIONS FOR select` admits, evaluated in
/// [`EvalMode::Permission`] under the definer's `auth`. A distinct operator
/// from [`LogicalFilter`] so a pass that moves predicates cannot match it by
/// accident; see the module documentation on barriers.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct LogicalPermissionFilter {
	pub input: Box<LogicalOp>,
	pub predicate: LogicalExpr,
	pub auth: Option<AuthLimit>,
	/// Which stored clause the predicate is, which decides the frame it is
	/// evaluated in. See [`GuardKind`].
	pub guard: GuardKind,
}

/// Which stored `PERMISSIONS` clause a guard is, which decides the frame its
/// predicate is evaluated in.
///
/// A read guard is evaluated write-refusing and a write guard is not: the
/// line is drawn by giving a `SELECT` clause
/// `Options::new_for_permission_predicate` and a create/update/delete clause
/// `Options::new_for_mutable_permission_predicate`. A write inside a write
/// guard is therefore performed rather than refused, which is not a nicety —
/// a guard that records an access attempt is written that way.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum GuardKind {
	/// `PERMISSIONS FOR select`, on a table or a field.
	Read,
	/// `PERMISSIONS FOR create`, `update` or `delete`, on a table or a field.
	Write,
}

/// `SPLIT ON`: expands each row into one row per element of each
/// expression's value.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct LogicalSplit {
	pub input: Box<LogicalOp>,
	pub exprs: Vec<LogicalExpr>,
}

/// `GROUP BY`. An empty `group` is `GROUP ALL`.
///
/// Carries `fields` because a grouped query's projection is evaluated by
/// the aggregation itself: the aggregate calls are extracted from these
/// fields, and the [`LogicalOp::Project`] above a grouped query applies
/// only `OMIT`.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct LogicalAggregate {
	pub input: Box<LogicalOp>,
	pub group: Vec<LogicalExpr>,
	pub fields: LogicalFields,
}

/// Keeps one row per distinct value.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct LogicalDistinct {
	pub input: Box<LogicalOp>,
}

/// `ORDER BY`. May be eliminated by the physical planner when the input is
/// already ordered accordingly.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct LogicalSort {
	pub input: Box<LogicalOp>,
	pub ordering: LogicalOrdering,
	/// `TEMPFILES`: prefer a disk-backed sort.
	pub tempfiles: bool,
}

/// `LIMIT` / `START`.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct LogicalLimit {
	pub input: Box<LogicalOp>,
	pub limit: Option<LogicalExpr>,
	pub start: Option<LogicalExpr>,
}

/// The `SELECT` projection and its `OMIT` list, each field evaluated in
/// the frame its [`LogicalField::Single`] carries: the caller's for a
/// field the user wrote. Also the *layer* the resolver puts above a
/// source for the schema's computed fields and field permissions, which
/// is the shape `*, column, …` with every column in a non-caller frame
/// and no `OMIT`; [`LogicalOp::is_layer`] tells the two apart, and a
/// layer's schema is its input's with the columns retyped
/// ([`LogicalOp::schema`]).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct LogicalProject {
	pub input: Box<LogicalOp>,
	pub fields: LogicalFields,
	pub omit: Vec<LogicalExpr>,
}

impl LogicalProject {
	/// Whether this projection is a *layer*: `*` first, then one or more
	/// columns whose every frame is not the caller's — a column's
	/// [`EvalFrame::mode`] is not [`EvalMode::Caller`] — with no `OMIT`. See
	/// [`LogicalOp::is_layer`] for what a layer is; this answers from the
	/// projection's fields and `omit` alone, without touching its input.
	pub fn is_layer(&self) -> bool {
		let LogicalFields::Select(fields) = &self.fields else {
			return false;
		};
		if !self.omit.is_empty() {
			return false;
		}
		let [LogicalField::All, columns @ ..] = fields.as_slice() else {
			return false;
		};
		!columns.is_empty()
			&& columns.iter().all(|column| {
				matches!(
					column,
					LogicalField::Single {
						frame,
						..
					} if frame.mode != EvalMode::Caller
				)
			})
	}
}

/// `FETCH`: replaces record links with the records they point at.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct LogicalFetch {
	pub input: Box<LogicalOp>,
	pub fetches: Vec<LogicalExpr>,
}

/// `TIMEOUT`.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct LogicalTimeout {
	pub input: Box<LogicalOp>,
	pub timeout: LogicalExpr,
}

/// `VERSION`: every source beneath reads the state as of `version`.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct LogicalVersionScope {
	pub input: Box<LogicalOp>,
	pub version: LogicalExpr,
}

/// `ONLY`: requires exactly one row and unwraps it. Whether an empty input
/// yields `NONE` rather than an error follows from the leaves (see
/// [`LogicalValues`]) and is not carried here.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct LogicalOnly {
	pub input: Box<LogicalOp>,
}

/// `EXPLAIN`: the input's rows are replaced by a description of the
/// operator tree the physical planner built for it. `analyze` additionally
/// runs the tree and reports what each operator did; `format` chooses the
/// description's encoding, and the `SELECT … EXPLAIN` clause form uses its
/// default.
///
/// Outermost, because it describes everything below it — including the
/// `ONLY` unwrapping. It changes what a plan returns rather than which
/// rows it computes, so a pass rewrites beneath it and never through it.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct LogicalExplain {
	pub input: Box<LogicalOp>,
	pub analyze: bool,
	pub format: ExplainFormat,
}

/// A bare expression as a statement. Yields the expression's value.
///
/// An `IF`, `THROW`, `BREAK` or `CONTINUE` in statement position is an
/// operator of its own ([`LogicalIfOp`], [`LogicalThrow`], [`LogicalBreak`],
/// [`LogicalContinue`]); an `Eval` carries one only where it stands in value
/// position, as the value of a `RETURN`, or where a pass substituted it.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct LogicalEval {
	pub expr: LogicalExpr,
}

/// A block `{ s1; s2; … }`: the statements in order, yielding the last.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct LogicalSequence {
	pub stmts: Vec<LogicalOp>,
}

/// `LET $name: kind = value`.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct LogicalLet {
	/// The parameter name without its `$`.
	pub name: String,
	/// The declared kind the value is coerced to.
	pub kind: Option<Kind>,
	pub value: Box<LogicalOp>,
}

/// `RETURN what FETCH …`.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct LogicalReturn {
	pub what: Box<LogicalOp>,
	pub fetch: Vec<LogicalExpr>,
}

/// `FOR $param IN range { body }`.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct LogicalForeach {
	pub param: Param,
	pub range: LogicalExpr,
	pub body: Box<LogicalOp>,
}

/// `IF` in statement position: the conditions evaluated in order, and the arm
/// of the first that is truthy run, or the `ELSE` arm; the value is the arm's,
/// `NONE` when no arm runs.
///
/// Produced where an `IF` stands as a statement: at the top level, in a block
/// (a `FOR` body and a function body included), as a `LET` value, and as a
/// statement of another `IF`'s arm. An `IF` in value position stays
/// [`LogicalExpr::If`].
///
/// The arms are held contiguously so they are this node's inputs
/// ([`LogicalOp::children`]): `arms[i]` is the arm of `conditions[i]`, and an
/// `arms` one longer than `conditions` ends with the `ELSE` arm. A block arm
/// is a [`LogicalOp::Sequence`], which runs in a scope of its own; an arm
/// written as an expression (`IF c THEN 1 END`) is the operator [`lower`]
/// gives that expression.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct LogicalIfOp {
	pub conditions: Vec<LogicalExpr>,
	pub arms: Vec<LogicalOp>,
}

impl LogicalIfOp {
	/// An `IF` of `(condition, arm)` pairs and an optional `ELSE` arm.
	pub fn new(branches: Vec<(LogicalExpr, LogicalOp)>, otherwise: Option<LogicalOp>) -> Self {
		let (conditions, mut arms): (Vec<_>, Vec<_>) = branches.into_iter().unzip();
		arms.extend(otherwise);
		Self {
			conditions,
			arms,
		}
	}

	/// Each condition beside its arm, in order.
	pub fn branches(&self) -> impl Iterator<Item = (&LogicalExpr, &LogicalOp)> {
		self.conditions.iter().zip(&self.arms)
	}

	/// The `ELSE` arm, where one is written.
	pub fn otherwise(&self) -> Option<&LogicalOp> {
		self.arms.get(self.conditions.len())
	}

	/// The pairs and the `ELSE` arm, taken apart.
	pub fn into_parts(self) -> (Vec<(LogicalExpr, LogicalOp)>, Option<LogicalOp>) {
		let mut arms = self.arms;
		let otherwise = (arms.len() > self.conditions.len()).then(|| arms.pop()).flatten();
		(self.conditions.into_iter().zip(arms).collect(), otherwise)
	}

	/// The same `IF` in value position: each arm as the scalar it evaluates
	/// to. An arm that is an expression statement is its expression, and any
	/// other arm is [`LogicalExpr::Subquery`] of it. Evaluating the result
	/// yields what running this node yields.
	pub fn to_scalar(&self) -> LogicalIf {
		let (branches, otherwise) = self.clone().into_parts();
		LogicalIf {
			branches: branches
				.into_iter()
				.map(|(condition, arm)| (condition, arm.into_scalar()))
				.collect(),
			otherwise: otherwise.map(LogicalOp::into_scalar),
		}
	}
}

/// `THROW value`: the value evaluated and raised as the statement's error.
/// Control does not continue past it.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct LogicalThrow {
	pub value: LogicalExpr,
}

/// `BREAK`: leave the innermost enclosing loop. Control does not continue past
/// it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct LogicalBreak;

/// `CONTINUE`: move to the innermost enclosing loop's next element. Control
/// does not continue past it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct LogicalContinue;

/// `SLEEP duration`.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct LogicalSleep {
	pub duration: LogicalExpr,
}

/// Declares [`OpKind`] alongside [`OpKind::ALL`] and [`LogicalOp::kind`], so
/// the three cannot drift apart.
///
/// This list is the one place every [`LogicalOp`] variant has to be accounted
/// for by the optimiser. The generated `kind` is an exhaustive match, so a new
/// variant is a compile error here; each rule then names the kinds it fires on
/// and needs no arm for the rest.
macro_rules! op_kinds {
	($($variant:ident),* $(,)?) => {
		/// The kind of a [`LogicalOp`], without its contents.
		#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
		pub enum OpKind {
			$($variant,)*
		}

		impl OpKind {
			/// Every kind, for a rule that fires on any node.
			pub const ALL: &'static [OpKind] = &[$(OpKind::$variant,)*];
		}

		impl LogicalOp {
			/// This node's kind, without its contents.
			pub fn kind(&self) -> OpKind {
				match self {
					$(LogicalOp::$variant(_) => OpKind::$variant,)*
				}
			}
		}
	};
}

op_kinds!(
	Source,
	Values,
	Match,
	Subquery,
	Write,
	Ddl,
	UnionAll,
	Join,
	Filter,
	PermissionFilter,
	Split,
	Aggregate,
	Distinct,
	Sort,
	Limit,
	Project,
	Fetch,
	Timeout,
	VersionScope,
	Only,
	Explain,
	Eval,
	Sequence,
	Let,
	Return,
	Foreach,
	If,
	Throw,
	Break,
	Continue,
	Info,
	Sleep,
);

impl LogicalOp {
	/// The operator's name, as rendered and as used in tests.
	pub fn name(&self) -> &'static str {
		match self {
			LogicalOp::Source(_) => "Source",
			LogicalOp::Values(_) => "Values",
			LogicalOp::Match(_) => "Match",
			LogicalOp::Subquery(_) => "Subquery",
			LogicalOp::Write(_) => "Write",
			LogicalOp::Ddl(_) => "Ddl",
			LogicalOp::UnionAll(_) => "UnionAll",
			LogicalOp::Join(_) => "Join",
			LogicalOp::Filter(_) => "Filter",
			LogicalOp::PermissionFilter(_) => "PermissionFilter",
			LogicalOp::Split(_) => "Split",
			LogicalOp::Aggregate(_) => "Aggregate",
			LogicalOp::Distinct(_) => "Distinct",
			LogicalOp::Sort(_) => "Sort",
			LogicalOp::Limit(_) => "Limit",
			LogicalOp::Project(_) => "Project",
			LogicalOp::Fetch(_) => "Fetch",
			LogicalOp::Timeout(_) => "Timeout",
			LogicalOp::VersionScope(_) => "VersionScope",
			LogicalOp::Only(_) => "Only",
			LogicalOp::Explain(_) => "Explain",
			LogicalOp::Eval(_) => "Eval",
			LogicalOp::Sequence(_) => "Sequence",
			LogicalOp::Let(_) => "Let",
			LogicalOp::Return(_) => "Return",
			LogicalOp::Foreach(_) => "Foreach",
			LogicalOp::If(_) => "If",
			LogicalOp::Throw(_) => "Throw",
			LogicalOp::Break(_) => "Break",
			LogicalOp::Continue(_) => "Continue",
			LogicalOp::Info(_) => "Info",
			LogicalOp::Sleep(_) => "Sleep",
		}
	}

	/// The operator's inputs, in order. Borrows in place; no allocation.
	pub fn children(&self) -> &[LogicalOp] {
		match self {
			LogicalOp::Source(_)
			| LogicalOp::Values(_)
			| LogicalOp::Match(_)
			| LogicalOp::Eval(_)
			| LogicalOp::Throw(_)
			| LogicalOp::Break(_)
			| LogicalOp::Continue(_)
			| LogicalOp::Info(_)
			| LogicalOp::Ddl(_)
			| LogicalOp::Sleep(_) => &[],
			LogicalOp::Write(LogicalWrite {
				source: LogicalWriteSource::Rows(input),
				..
			}) => std::slice::from_ref(input),
			LogicalOp::Write(LogicalWrite {
				source: LogicalWriteSource::Targets(_) | LogicalWriteSource::Inserts(_),
				..
			}) => &[],
			LogicalOp::UnionAll(LogicalUnionAll {
				inputs,
			}) => inputs,
			LogicalOp::Sequence(LogicalSequence {
				stmts,
			}) => stmts,
			LogicalOp::If(LogicalIfOp {
				arms,
				..
			}) => arms,
			LogicalOp::Join(LogicalJoin {
				inputs,
				..
			}) => inputs.as_slice(),
			LogicalOp::Subquery(LogicalSubquery {
				input,
			})
			| LogicalOp::Filter(LogicalFilter {
				input,
				..
			})
			| LogicalOp::PermissionFilter(LogicalPermissionFilter {
				input,
				..
			})
			| LogicalOp::Split(LogicalSplit {
				input,
				..
			})
			| LogicalOp::Aggregate(LogicalAggregate {
				input,
				..
			})
			| LogicalOp::Distinct(LogicalDistinct {
				input,
			})
			| LogicalOp::Sort(LogicalSort {
				input,
				..
			})
			| LogicalOp::Limit(LogicalLimit {
				input,
				..
			})
			| LogicalOp::Project(LogicalProject {
				input,
				..
			})
			| LogicalOp::Fetch(LogicalFetch {
				input,
				..
			})
			| LogicalOp::Timeout(LogicalTimeout {
				input,
				..
			})
			| LogicalOp::VersionScope(LogicalVersionScope {
				input,
				..
			})
			| LogicalOp::Only(LogicalOnly {
				input,
			})
			| LogicalOp::Explain(LogicalExplain {
				input,
				..
			})
			| LogicalOp::Let(LogicalLet {
				value: input,
				..
			})
			| LogicalOp::Return(LogicalReturn {
				what: input,
				..
			})
			| LogicalOp::Foreach(LogicalForeach {
				body: input,
				..
			}) => std::slice::from_ref(input),
		}
	}

	/// [`Self::children`] for rewriting in place. The same inputs, in the same
	/// order.
	pub fn children_mut(&mut self) -> &mut [LogicalOp] {
		match self {
			LogicalOp::Source(_)
			| LogicalOp::Values(_)
			| LogicalOp::Match(_)
			| LogicalOp::Eval(_)
			| LogicalOp::Throw(_)
			| LogicalOp::Break(_)
			| LogicalOp::Continue(_)
			| LogicalOp::Info(_)
			| LogicalOp::Ddl(_)
			| LogicalOp::Sleep(_) => &mut [],
			LogicalOp::Write(LogicalWrite {
				source: LogicalWriteSource::Rows(input),
				..
			}) => std::slice::from_mut(input),
			LogicalOp::Write(LogicalWrite {
				source: LogicalWriteSource::Targets(_) | LogicalWriteSource::Inserts(_),
				..
			}) => &mut [],
			LogicalOp::UnionAll(LogicalUnionAll {
				inputs,
			}) => inputs,
			LogicalOp::Sequence(LogicalSequence {
				stmts,
			}) => stmts,
			LogicalOp::If(LogicalIfOp {
				arms,
				..
			}) => arms,
			LogicalOp::Join(LogicalJoin {
				inputs,
				..
			}) => inputs.as_mut_slice(),
			LogicalOp::Subquery(LogicalSubquery {
				input,
			})
			| LogicalOp::Filter(LogicalFilter {
				input,
				..
			})
			| LogicalOp::PermissionFilter(LogicalPermissionFilter {
				input,
				..
			})
			| LogicalOp::Split(LogicalSplit {
				input,
				..
			})
			| LogicalOp::Aggregate(LogicalAggregate {
				input,
				..
			})
			| LogicalOp::Distinct(LogicalDistinct {
				input,
			})
			| LogicalOp::Sort(LogicalSort {
				input,
				..
			})
			| LogicalOp::Limit(LogicalLimit {
				input,
				..
			})
			| LogicalOp::Project(LogicalProject {
				input,
				..
			})
			| LogicalOp::Fetch(LogicalFetch {
				input,
				..
			})
			| LogicalOp::Timeout(LogicalTimeout {
				input,
				..
			})
			| LogicalOp::VersionScope(LogicalVersionScope {
				input,
				..
			})
			| LogicalOp::Only(LogicalOnly {
				input,
			})
			| LogicalOp::Explain(LogicalExplain {
				input,
				..
			})
			| LogicalOp::Let(LogicalLet {
				value: input,
				..
			})
			| LogicalOp::Return(LogicalReturn {
				what: input,
				..
			})
			| LogicalOp::Foreach(LogicalForeach {
				body: input,
				..
			}) => std::slice::from_mut(input),
		}
	}

	/// Rebuilds this node with each input replaced by `f(input)`, in order.
	/// The node's own attributes are untouched. The first error aborts the
	/// rebuild.
	pub fn map_children<E>(
		self,
		mut f: impl FnMut(LogicalOp) -> Result<LogicalOp, E>,
	) -> Result<Self, E> {
		let boxed = |input: Box<LogicalOp>,
		             f: &mut dyn FnMut(LogicalOp) -> Result<LogicalOp, E>| {
			f(*input).map(Box::new)
		};
		Ok(match self {
			leaf @ (LogicalOp::Source(_)
			| LogicalOp::Values(_)
			| LogicalOp::Match(_)
			| LogicalOp::Eval(_)
			| LogicalOp::Throw(_)
			| LogicalOp::Break(_)
			| LogicalOp::Continue(_)
			| LogicalOp::Info(_)
			| LogicalOp::Ddl(_)
			| LogicalOp::Sleep(_)) => leaf,
			LogicalOp::Write(LogicalWrite {
				statement,
				source,
				data,
				duplicate_update,
				cond,
			}) => LogicalOp::Write(LogicalWrite {
				statement,
				data,
				duplicate_update,
				cond,
				source: match source {
					LogicalWriteSource::Rows(input) => {
						LogicalWriteSource::Rows(Box::new(f(*input)?))
					}
					leaf @ (LogicalWriteSource::Targets(_) | LogicalWriteSource::Inserts(_)) => {
						leaf
					}
				},
			}),
			LogicalOp::UnionAll(LogicalUnionAll {
				inputs,
			}) => LogicalOp::UnionAll(LogicalUnionAll {
				inputs: inputs.into_iter().map(f).collect::<Result<_, E>>()?,
			}),
			LogicalOp::Sequence(LogicalSequence {
				stmts,
			}) => LogicalOp::Sequence(LogicalSequence {
				stmts: stmts.into_iter().map(f).collect::<Result<_, E>>()?,
			}),
			LogicalOp::If(LogicalIfOp {
				conditions,
				arms,
			}) => LogicalOp::If(LogicalIfOp {
				conditions,
				arms: arms.into_iter().map(f).collect::<Result<_, E>>()?,
			}),
			LogicalOp::Join(mut op) => {
				let [left, right] = *op.inputs;
				op.inputs = Box::new([f(left)?, f(right)?]);
				LogicalOp::Join(op)
			}
			LogicalOp::Subquery(mut op) => {
				op.input = boxed(op.input, &mut f)?;
				LogicalOp::Subquery(op)
			}
			LogicalOp::Filter(mut op) => {
				op.input = boxed(op.input, &mut f)?;
				LogicalOp::Filter(op)
			}
			LogicalOp::PermissionFilter(mut op) => {
				op.input = boxed(op.input, &mut f)?;
				LogicalOp::PermissionFilter(op)
			}
			LogicalOp::Split(mut op) => {
				op.input = boxed(op.input, &mut f)?;
				LogicalOp::Split(op)
			}
			LogicalOp::Aggregate(mut op) => {
				op.input = boxed(op.input, &mut f)?;
				LogicalOp::Aggregate(op)
			}
			LogicalOp::Distinct(mut op) => {
				op.input = boxed(op.input, &mut f)?;
				LogicalOp::Distinct(op)
			}
			LogicalOp::Sort(mut op) => {
				op.input = boxed(op.input, &mut f)?;
				LogicalOp::Sort(op)
			}
			LogicalOp::Limit(mut op) => {
				op.input = boxed(op.input, &mut f)?;
				LogicalOp::Limit(op)
			}
			LogicalOp::Project(mut op) => {
				op.input = boxed(op.input, &mut f)?;
				LogicalOp::Project(op)
			}
			LogicalOp::Fetch(mut op) => {
				op.input = boxed(op.input, &mut f)?;
				LogicalOp::Fetch(op)
			}
			LogicalOp::Timeout(mut op) => {
				op.input = boxed(op.input, &mut f)?;
				LogicalOp::Timeout(op)
			}
			LogicalOp::VersionScope(mut op) => {
				op.input = boxed(op.input, &mut f)?;
				LogicalOp::VersionScope(op)
			}
			LogicalOp::Only(mut op) => {
				op.input = boxed(op.input, &mut f)?;
				LogicalOp::Only(op)
			}
			LogicalOp::Explain(mut op) => {
				op.input = boxed(op.input, &mut f)?;
				LogicalOp::Explain(op)
			}
			LogicalOp::Let(mut op) => {
				op.value = boxed(op.value, &mut f)?;
				LogicalOp::Let(op)
			}
			LogicalOp::Return(mut op) => {
				op.what = boxed(op.what, &mut f)?;
				LogicalOp::Return(op)
			}
			LogicalOp::Foreach(mut op) => {
				op.body = boxed(op.body, &mut f)?;
				LogicalOp::Foreach(op)
			}
		})
	}

	/// Rewrites the tree bottom-up: every input is rewritten before `f` sees
	/// the node that holds it, so `f` observes already-rewritten children.
	///
	/// Passes are synchronous. One that needs the catalog or a transaction
	/// gathers what it needs with [`LogicalOp::visit`], fetches it, and then
	/// rewrites with the fetched data in hand.
	pub fn transform_up<E>(
		self,
		f: &mut impl FnMut(LogicalOp) -> Result<LogicalOp, E>,
	) -> Result<Self, E> {
		let node = self.map_children(|child| child.transform_up(f))?;
		f(node)
	}

	/// Rewrites the tree top-down: `f` sees the node first, and the inputs of
	/// whatever it returns are then rewritten in turn.
	pub fn transform_down<E>(
		self,
		f: &mut impl FnMut(LogicalOp) -> Result<LogicalOp, E>,
	) -> Result<Self, E> {
		f(self)?.map_children(|child| child.transform_down(f))
	}

	/// Walks the tree in pre-order, stopping at the first `Break`.
	pub fn visit<'a, B>(
		&'a self,
		f: &mut impl FnMut(&'a LogicalOp) -> ControlFlow<B>,
	) -> ControlFlow<B> {
		f(self)?;
		for child in self.children() {
			child.visit(f)?;
		}
		ControlFlow::Continue(())
	}

	/// Every expression this node owns directly, in order: a predicate, each
	/// projected expression (a layer's columns included), each `OMIT`,
	/// `FETCH`, `LIMIT` and `START`, the timeout, the version, and the
	/// expressions embedded in the node's split keys, grouping keys, ordering
	/// keys and source reference. Inputs are not included; combine with
	/// [`LogicalOp::visit`] to cover a tree, or use
	/// [`LogicalOp::visit_scalars_deep`]. Hints, table names, aliases,
	/// declared kinds, frames and parameter sources hold no expression. A `Match`
	/// plan lists every scalar it evaluates — its predicates, mutation values,
	/// grouping keys, columns, ordering and paging — in execution order.
	pub fn for_each_scalar<'a>(&'a self, out: &mut dyn FnMut(&'a LogicalExpr)) {
		match self {
			// Everything the write evaluates: what its source names — a
			// target, an `INSERT`'s `INTO` and rows — and what its clauses
			// hold. The clauses are here so a rule reaches them: the
			// statement beside them keeps the surface form the per-record
			// engine is given, and is not a scalar of this node.
			LogicalOp::Write(LogicalWrite {
				source,
				data,
				duplicate_update,
				cond,
				..
			}) => {
				match source {
					LogicalWriteSource::Targets(targets) => {
						for target in targets {
							out(target);
						}
					}
					LogicalWriteSource::Inserts(inserts) => {
						if let Some(into) = &inserts.into {
							out(into);
						}
						match &inserts.rows {
							LogicalInsertRows::Tuples(tuples) => {
								for tuple in tuples {
									for (_, value) in tuple {
										out(value);
									}
								}
							}
							LogicalInsertRows::Value(expr) => out(expr),
						}
					}
					LogicalWriteSource::Rows(_) => {}
				}
				for clause in [data, duplicate_update] {
					for value in clause.iter().flat_map(LogicalData::values) {
						out(value);
					}
				}
				if let Some(cond) = cond {
					out(cond);
				}
			}
			// A schema statement's expressions are its clauses, resolved
			// where the catalog write is formed, so the node itself owns none.
			LogicalOp::Ddl(_) => {}
			LogicalOp::Source(LogicalSource {
				source,
				..
			}) => source.for_each_expr(out),
			LogicalOp::Values(LogicalValues {
				expr,
				..
			})
			| LogicalOp::Eval(LogicalEval {
				expr,
			})
			| LogicalOp::Timeout(LogicalTimeout {
				timeout: expr,
				..
			})
			| LogicalOp::VersionScope(LogicalVersionScope {
				version: expr,
				..
			})
			| LogicalOp::Foreach(LogicalForeach {
				range: expr,
				..
			})
			| LogicalOp::Sleep(LogicalSleep {
				duration: expr,
			})
			| LogicalOp::Throw(LogicalThrow {
				value: expr,
			}) => out(expr),
			LogicalOp::If(LogicalIfOp {
				conditions,
				..
			}) => {
				for condition in conditions {
					out(condition);
				}
			}
			LogicalOp::Match(LogicalMatch {
				plan,
			}) => {
				for e in plan.scalars() {
					out(e);
				}
			}
			LogicalOp::Subquery(_)
			| LogicalOp::UnionAll(_)
			| LogicalOp::Distinct(_)
			| LogicalOp::Only(_)
			| LogicalOp::Explain(_)
			| LogicalOp::Sequence(_)
			| LogicalOp::Let(_)
			| LogicalOp::Break(_)
			| LogicalOp::Continue(_) => {}
			LogicalOp::Filter(LogicalFilter {
				predicate,
				..
			})
			| LogicalOp::PermissionFilter(LogicalPermissionFilter {
				predicate,
				..
			}) => out(predicate),
			LogicalOp::Join(LogicalJoin {
				on,
				filter,
				..
			}) => {
				for (left, right) in on {
					out(left);
					out(right);
				}
				if let Some(e) = filter {
					out(e);
				}
			}
			LogicalOp::Split(LogicalSplit {
				exprs,
				..
			}) => {
				for e in exprs {
					out(e);
				}
			}
			LogicalOp::Aggregate(LogicalAggregate {
				group,
				fields,
				..
			}) => {
				for e in group {
					out(e);
				}
				fields.for_each_expr(out);
			}
			LogicalOp::Sort(LogicalSort {
				ordering,
				..
			}) => ordering.for_each_expr(out),
			LogicalOp::Limit(LogicalLimit {
				limit,
				start,
				..
			}) => {
				if let Some(e) = limit {
					out(e);
				}
				if let Some(e) = start {
					out(e);
				}
			}
			LogicalOp::Project(LogicalProject {
				fields,
				omit,
				..
			}) => {
				fields.for_each_expr(out);
				for e in omit {
					out(e);
				}
			}
			LogicalOp::Fetch(LogicalFetch {
				fetches,
				..
			})
			| LogicalOp::Return(LogicalReturn {
				fetch: fetches,
				..
			}) => {
				for e in fetches {
					out(e);
				}
			}
			LogicalOp::Info(info) => info.for_each_expr(out),
		}
	}

	/// [`Self::for_each_scalar`], collected. Allocates once per node, so a
	/// walk of a whole plan takes the callback form.
	pub fn scalars(&self) -> Vec<&LogicalExpr> {
		let mut out = Vec::new();
		self.for_each_scalar(&mut |scalar| out.push(scalar));
		out
	}

	/// [`Self::scalars`], mutably.
	pub fn scalars_mut(&mut self) -> Vec<&mut LogicalExpr> {
		let mut out = Vec::new();
		match self {
			// The mirror of the read walk: the source's own expressions and
			// the clauses' values, so a rule rewrites a data clause as it
			// rewrites anything else.
			LogicalOp::Write(LogicalWrite {
				source,
				data,
				duplicate_update,
				cond,
				..
			}) => {
				match source {
					LogicalWriteSource::Targets(targets) => out.extend(targets.iter_mut()),
					LogicalWriteSource::Inserts(inserts) => {
						out.extend(inserts.into.iter_mut());
						match &mut inserts.rows {
							LogicalInsertRows::Tuples(tuples) => out.extend(
								tuples
									.iter_mut()
									.flat_map(|tuple| tuple.iter_mut().map(|(_, v)| v)),
							),
							LogicalInsertRows::Value(expr) => out.push(expr),
						}
					}
					LogicalWriteSource::Rows(_) => {}
				}
				for clause in [data, duplicate_update] {
					out.extend(clause.iter_mut().flat_map(LogicalData::values_mut));
				}
				out.extend(cond.iter_mut());
			}
			LogicalOp::Ddl(_) => {}
			LogicalOp::Source(LogicalSource {
				source,
				..
			}) => source.exprs_mut(&mut out),
			LogicalOp::Values(LogicalValues {
				expr,
				..
			})
			| LogicalOp::Eval(LogicalEval {
				expr,
			})
			| LogicalOp::Timeout(LogicalTimeout {
				timeout: expr,
				..
			})
			| LogicalOp::VersionScope(LogicalVersionScope {
				version: expr,
				..
			})
			| LogicalOp::Foreach(LogicalForeach {
				range: expr,
				..
			})
			| LogicalOp::Sleep(LogicalSleep {
				duration: expr,
			})
			| LogicalOp::Throw(LogicalThrow {
				value: expr,
			}) => out.push(expr),
			LogicalOp::If(LogicalIfOp {
				conditions,
				..
			}) => out.extend(conditions),
			LogicalOp::Match(LogicalMatch {
				plan,
			}) => out.extend(plan.scalars_mut()),
			LogicalOp::Subquery(_)
			| LogicalOp::UnionAll(_)
			| LogicalOp::Distinct(_)
			| LogicalOp::Only(_)
			| LogicalOp::Explain(_)
			| LogicalOp::Sequence(_)
			| LogicalOp::Let(_)
			| LogicalOp::Break(_)
			| LogicalOp::Continue(_) => {}
			LogicalOp::Filter(LogicalFilter {
				predicate,
				..
			})
			| LogicalOp::PermissionFilter(LogicalPermissionFilter {
				predicate,
				..
			}) => out.push(predicate),
			LogicalOp::Join(LogicalJoin {
				on,
				filter,
				..
			}) => {
				for (left, right) in on {
					out.push(left);
					out.push(right);
				}
				out.extend(filter);
			}
			LogicalOp::Split(LogicalSplit {
				exprs,
				..
			}) => out.extend(exprs),
			LogicalOp::Aggregate(LogicalAggregate {
				group,
				fields,
				..
			}) => {
				out.extend(group);
				fields.exprs_mut(&mut out);
			}
			LogicalOp::Sort(LogicalSort {
				ordering,
				..
			}) => ordering.exprs_mut(&mut out),
			LogicalOp::Limit(LogicalLimit {
				limit,
				start,
				..
			}) => {
				out.extend(limit);
				out.extend(start);
			}
			LogicalOp::Project(LogicalProject {
				fields,
				omit,
				..
			}) => {
				fields.exprs_mut(&mut out);
				out.extend(omit);
			}
			LogicalOp::Fetch(LogicalFetch {
				fetches,
				..
			})
			| LogicalOp::Return(LogicalReturn {
				fetch: fetches,
				..
			}) => out.extend(fetches),
			LogicalOp::Info(info) => info.exprs_mut(&mut out),
		}
		out
	}

	/// Applies `f` to each of [`Self::scalars`], stopping at the first error.
	pub fn visit_scalars<E>(
		&self,
		f: &mut impl FnMut(&LogicalExpr) -> Result<(), E>,
	) -> Result<(), E> {
		let mut result = Ok(());
		self.for_each_scalar(&mut |scalar| {
			if result.is_ok() {
				result = f(scalar);
			}
		});
		result
	}

	/// Applies `f` to each of [`Self::scalars_mut`], stopping at the first
	/// error.
	pub fn visit_scalars_mut<E>(
		&mut self,
		f: &mut impl FnMut(&mut LogicalExpr) -> Result<(), E>,
	) -> Result<(), E> {
		self.scalars_mut().into_iter().try_for_each(f)
	}

	/// Visits every scalar in the tree, each node's own before its inputs'.
	///
	/// [`Self::visit_scalars`] covers one node; this covers the plan. Use it
	/// where an analysis is about the expressions a plan evaluates rather than
	/// the operators that evaluate them. A scalar's own nesting, including any
	/// [`LogicalExpr::Subquery`] inside it, is the callback's to walk.
	pub fn visit_scalars_deep<E>(
		&self,
		f: &mut impl FnMut(&LogicalExpr) -> Result<(), E>,
	) -> Result<(), E> {
		self.visit_scalars(f)?;
		self.children().iter().try_for_each(|child| child.visit_scalars_deep(f))
	}

	/// [`Self::visit_scalars_deep`] over the same scalars, rewritten in place.
	pub fn visit_scalars_deep_mut<E>(
		&mut self,
		f: &mut impl FnMut(&mut LogicalExpr) -> Result<(), E>,
	) -> Result<(), E> {
		self.visit_scalars_mut(f)?;
		self.children_mut().iter_mut().try_for_each(|child| child.visit_scalars_deep_mut(f))
	}

	/// Visits every operator of the statement in pre-order, stopping at the
	/// first `Break`: the node itself, then every plan nested in a
	/// [`LogicalExpr::Subquery`] within one of the node's scalars at any
	/// depth, then the node's inputs.
	///
	/// [`Self::visit`] covers the operator tree; this covers the statement. A
	/// scalar subquery runs in the same transaction as the plan holding it and
	/// as the same principal, so an analysis about what a statement does reads
	/// it here rather than re-descending from the callback. Two stopping
	/// rules:
	///
	/// - A [`LogicalExpr::Closure`] body is not entered. A closure body is bound by nothing where
	///   it is written: it resolves its sources, parameters and calls where the closure is invoked,
	///   so a plan inside one is not a plan of this statement.
	/// - A [`LogicalOp::Match`] plan is a leaf, as it is to [`Self::visit`], since its stages are
	///   not row semantics. Its scalars are walked like any other node's, so a plan one of them
	///   holds is still reached.
	///
	/// The callback short-circuits rather than failing: an analysis that
	/// answers from the first node it accepts stops there, and one that must
	/// see the whole statement returns `Continue` throughout and discards the
	/// result.
	pub fn visit_ops_deep<'a, B>(
		&'a self,
		f: &mut impl FnMut(&'a LogicalOp) -> ControlFlow<B>,
	) -> ControlFlow<B> {
		// The recursion is over `dyn FnMut` rather than the caller's closure
		// type: a generic function that handed itself a fresh closure at every
		// level would have no monomorphic form.
		fn walk_op<'a, B>(
			op: &'a LogicalOp,
			f: &mut dyn FnMut(&'a LogicalOp) -> ControlFlow<B>,
		) -> ControlFlow<B> {
			f(op)?;
			let mut flow = ControlFlow::Continue(());
			op.for_each_scalar(&mut |scalar| {
				if flow.is_continue() {
					flow = walk_scalar(scalar, &mut *f);
				}
			});
			flow?;
			for child in op.children() {
				walk_op(child, &mut *f)?;
			}
			ControlFlow::Continue(())
		}

		/// Walks the plans `expr` holds, in the order it nests them.
		fn walk_scalar<'a, B>(
			expr: &'a LogicalExpr,
			f: &mut dyn FnMut(&'a LogicalOp) -> ControlFlow<B>,
		) -> ControlFlow<B> {
			if let LogicalExpr::Subquery(plan) = expr {
				return walk_op(plan, f);
			}
			if matches!(expr, LogicalExpr::Closure(_)) {
				return ControlFlow::Continue(());
			}
			let mut flow = ControlFlow::Continue(());
			expr.for_each_child(&mut |child| {
				if flow.is_continue() {
					flow = walk_scalar(child, &mut *f);
				}
			});
			flow
		}

		walk_op(self, f)
	}

	/// Whether any node in the tree satisfies `pred`.
	pub fn any(&self, mut pred: impl FnMut(&LogicalOp) -> bool) -> bool {
		self.visit(&mut |node| {
			if pred(node) {
				ControlFlow::Break(())
			} else {
				ControlFlow::Continue(())
			}
		})
		.is_break()
	}

	/// This statement as the scalar that evaluates to its value: an
	/// expression statement is its expression, a statement `IF`, `THROW`,
	/// `BREAK` or `CONTINUE` is its value-position form, and any other
	/// statement is [`LogicalExpr::Subquery`] of it.
	pub fn into_scalar(self) -> LogicalExpr {
		match self {
			LogicalOp::Eval(LogicalEval {
				expr,
			}) => expr,
			LogicalOp::If(op) => LogicalExpr::If(Box::new(op.to_scalar())),
			LogicalOp::Throw(LogicalThrow {
				value,
			}) => LogicalExpr::Throw(Box::new(value)),
			LogicalOp::Break(_) => LogicalExpr::Break,
			LogicalOp::Continue(_) => LogicalExpr::Continue,
			other @ (LogicalOp::Source(_)
			| LogicalOp::Values(_)
			| LogicalOp::Match(_)
			| LogicalOp::Subquery(_)
			| LogicalOp::Write(_)
			| LogicalOp::Ddl(_)
			| LogicalOp::UnionAll(_)
			| LogicalOp::Join(_)
			| LogicalOp::Filter(_)
			| LogicalOp::PermissionFilter(_)
			| LogicalOp::Split(_)
			| LogicalOp::Aggregate(_)
			| LogicalOp::Distinct(_)
			| LogicalOp::Sort(_)
			| LogicalOp::Limit(_)
			| LogicalOp::Project(_)
			| LogicalOp::Fetch(_)
			| LogicalOp::Timeout(_)
			| LogicalOp::VersionScope(_)
			| LogicalOp::Only(_)
			| LogicalOp::Explain(_)
			| LogicalOp::Sequence(_)
			| LogicalOp::Let(_)
			| LogicalOp::Return(_)
			| LogicalOp::Foreach(_)
			| LogicalOp::Info(_)
			| LogicalOp::Sleep(_)) => LogicalExpr::Subquery(Box::new(other)),
		}
	}

	/// Whether the plan only reads.
	///
	/// A plan writes when one of its scalars does, when a source takes locks,
	/// or when a `MATCH` plan carries a write stage. The scalar half carries
	/// the one-sided answer of [`LogicalExpr::read_only`]: a call through a
	/// value whose body is invisible here counts as writing.
	pub fn read_only(&self) -> bool {
		!self.any(|node| !node.node_read_only())
	}

	/// [`Self::read_only`] for this node alone, inputs excluded.
	fn node_read_only(&self) -> bool {
		match self {
			// A mutation writes by construction, and so does a schema
			// statement. Nothing about an input or a statement can make
			// either read-only, so this is the one arm that consults neither
			// — and the one a node owning no scalars must be in, since the
			// scalar arm below answers `true` for a node that has none.
			LogicalOp::Write(_) | LogicalOp::Ddl(_) => false,
			LogicalOp::Source(LogicalSource {
				source,
				for_update,
				..
			}) => !*for_update && source.read_only(),
			LogicalOp::Values(LogicalValues {
				expr,
				for_update,
				..
			}) => !*for_update && expr.read_only(),
			// A write stage targets bindings rather than expressions, so the
			// plan's scalars do not reveal it.
			LogicalOp::Match(LogicalMatch {
				plan,
			}) => !plan.has_mutations() && plan.scalars().into_iter().all(LogicalExpr::read_only),
			// A key or an ordering can name a closure stored on a value
			// through an unregistered method, which the expression's own
			// classification over-approximates to writing.
			LogicalOp::Split(LogicalSplit {
				exprs,
				..
			}) => exprs.iter().all(LogicalExpr::read_only),
			LogicalOp::Aggregate(LogicalAggregate {
				group,
				fields,
				..
			}) => group.iter().all(LogicalExpr::read_only) && fields.read_only(),
			LogicalOp::Sort(LogicalSort {
				ordering,
				..
			}) => ordering.read_only(),
			LogicalOp::Project(LogicalProject {
				fields,
				omit,
				..
			}) => fields.read_only() && omit.iter().all(LogicalExpr::read_only),
			LogicalOp::Subquery(_)
			| LogicalOp::UnionAll(_)
			| LogicalOp::Join(_)
			| LogicalOp::Filter(_)
			| LogicalOp::PermissionFilter(_)
			| LogicalOp::Distinct(_)
			| LogicalOp::Limit(_)
			| LogicalOp::Fetch(_)
			| LogicalOp::Timeout(_)
			| LogicalOp::VersionScope(_)
			| LogicalOp::Only(_)
			| LogicalOp::Explain(_)
			| LogicalOp::Eval(_)
			| LogicalOp::Sequence(_)
			| LogicalOp::Let(_)
			| LogicalOp::Return(_)
			| LogicalOp::Foreach(_)
			| LogicalOp::If(_)
			| LogicalOp::Throw(_)
			| LogicalOp::Break(_)
			| LogicalOp::Continue(_)
			| LogicalOp::Info(_)
			| LogicalOp::Sleep(_) => self.scalars().into_iter().all(LogicalExpr::read_only),
		}
	}

	/// The schema of the rows this node produces, derived from its inputs.
	///
	/// A `Source` answers with what binding resolved for it, or open. The
	/// row-preserving operators pass their input's schema through. `Project`
	/// and `Aggregate` build one field per projected expression, typed by its
	/// declared kind when it has one and else by [`LogicalExpr::kind`] against
	/// the input; `*` carries the input's fields and openness through, and
	/// `VALUE` is open because a bare value is not a field list. A `Project`
	/// that is a layer ([`LogicalOp::is_layer`]) instead keeps its input's
	/// fields and retypes each column — by its declared kind; a guard column
	/// as `option<kind of the field>` from the input's declaration, or by its
	/// expression where the input declares nothing — so a read of the field
	/// above the layer is typed as the column. `Split` replaces
	/// each split field's collection kind with its element kind; a kind that
	/// is not a collection passes through, as the value does. `UnionAll`
	/// merges its inputs' fields by path — the shared kind, `option<kind>`
	/// where a closed input lacks the field, and `Any` where declarations
	/// differ or an open input leaves it undeclared — and is open if any
	/// input is. `Fetch` keeps its input's fields and openness, retyping
	/// every field a fetch target starts from to `Any`: the fetched value is
	/// the record the link points at, whose shape needs the catalog, and a
	/// target that does not start from a named field leaves the whole schema
	/// open. `Join` is its left input's schema plus, for an inner or left
	/// join, the alias field: `record<t>` when the right input reads exactly
	/// one table `t` and `Any` otherwise, `null | …` for a left join whose
	/// unmatched rows hold `NULL` there; a semi or anti join emits left rows
	/// unchanged and has its left input's schema.
	/// `Values`, `Match`, `Explain` and every statement operator are open:
	/// `Explain` replaces the rows with a description of the physical plan,
	/// and a statement's result is not a row set.
	pub fn schema(&self) -> Schema {
		match self {
			// The records a mutation emits are the target table's, not its
			// input's, and this node resolves no table. An open schema is the
			// honest answer rather than a placeholder: nothing above may treat
			// a mutation's output as having declared columns.
			LogicalOp::Write(_) => Schema::open(),
			LogicalOp::Source(LogicalSource {
				schema,
				..
			}) => schema.as_deref().cloned().unwrap_or_else(Schema::open),
			LogicalOp::Fetch(LogicalFetch {
				input,
				fetches,
			}) => {
				let mut schema = input.schema();
				for fetch in fetches {
					let Some(root) = fetch_root(fetch) else {
						return Schema::open();
					};
					for field in &mut schema.fields {
						if field.path.root() == Some(root) {
							field.kind = Kind::Any;
						}
					}
				}
				schema
			}
			LogicalOp::Values(_)
			| LogicalOp::Match(_)
			| LogicalOp::Explain(_)
			| LogicalOp::Eval(_)
			| LogicalOp::Sequence(_)
			| LogicalOp::Let(_)
			| LogicalOp::Return(_)
			| LogicalOp::Foreach(_)
			| LogicalOp::If(_)
			| LogicalOp::Throw(_)
			| LogicalOp::Break(_)
			| LogicalOp::Continue(_)
			| LogicalOp::Info(_)
			| LogicalOp::Ddl(_)
			| LogicalOp::Sleep(_) => Schema::open(),
			LogicalOp::Subquery(LogicalSubquery {
				input,
			})
			| LogicalOp::Filter(LogicalFilter {
				input,
				..
			})
			| LogicalOp::PermissionFilter(LogicalPermissionFilter {
				input,
				..
			})
			| LogicalOp::Distinct(LogicalDistinct {
				input,
			})
			| LogicalOp::Sort(LogicalSort {
				input,
				..
			})
			| LogicalOp::Limit(LogicalLimit {
				input,
				..
			})
			| LogicalOp::Timeout(LogicalTimeout {
				input,
				..
			})
			| LogicalOp::VersionScope(LogicalVersionScope {
				input,
				..
			})
			| LogicalOp::Only(LogicalOnly {
				input,
			}) => input.schema(),
			LogicalOp::Join(join) => {
				let mut schema = join.left().schema();
				if !join.kind.adds_right_row() {
					return schema;
				}
				let matched = match join.right_table() {
					Some(table) => Kind::Record(vec![table.clone()]),
					None => Kind::Any,
				};
				let kind = match join.kind {
					JoinKind::Left if matched != Kind::Any => {
						Kind::either(vec![Kind::Null, matched])
					}
					JoinKind::Inner | JoinKind::Left | JoinKind::Semi | JoinKind::Anti => matched,
				};
				let path = FieldPath::field(join.alias.name().clone());
				schema.fields.retain(|field| field.path != path);
				schema.fields.push(FieldSchema::new(path, kind));
				schema
			}
			LogicalOp::UnionAll(LogicalUnionAll {
				inputs,
			}) => {
				let schemas: Vec<Schema> = inputs.iter().map(LogicalOp::schema).collect();
				let mut out = Schema::closed(Vec::new());
				for schema in &schemas {
					if schema.is_open() {
						out.openness = Openness::Open;
					}
					for field in &schema.fields {
						if out.field(&field.path).is_none() {
							let mut merged = field.clone();
							merged.kind = union_kind(&schemas, &field.path);
							// A read of the union is known to be of the merged
							// kind only where every input proves it of that
							// same kind. One input's coerced column says
							// nothing about the stored field another input
							// reads, and a kind `union_kind` widened — to
							// `option` for a field an input lacks, to `any`
							// for inputs that disagree — is not the kind any
							// input checked.
							merged.checked = schemas.iter().all(|schema| {
								schema
									.field(&field.path)
									.is_some_and(|f| f.checked && f.kind == merged.kind)
							});
							out.fields.push(merged);
						}
					}
				}
				out
			}
			LogicalOp::Split(LogicalSplit {
				input,
				exprs,
			}) => {
				let mut schema = input.schema();
				for path in exprs.iter().filter_map(LogicalExpr::field_path) {
					if let Some(field) = schema.fields.iter_mut().find(|f| f.path == path) {
						field.kind = element_kind(&field.kind);
					}
				}
				schema
			}
			LogicalOp::Aggregate(LogicalAggregate {
				input,
				fields,
				..
			}) => project_schema(fields, &input.schema()),
			LogicalOp::Project(LogicalProject {
				input,
				fields,
				omit,
			}) => {
				let input = input.schema();
				if self.is_layer() {
					return layer_schema(fields, input);
				}
				let mut schema = project_schema(fields, &input);
				schema.fields.retain(|field| {
					!omit.iter().any(|o| o.field_path().as_ref() == Some(&field.path))
				});
				schema
			}
		}
	}

	/// Whether a statement in scalar position evaluates its expressions against
	/// the row of the statement that holds it. A block, `RETURN`, `LET`, `FOR`,
	/// `INFO`, a schema statement, `SLEEP` or `EXPLAIN` does — each is
	/// computed with the enclosing document — while a relational plan or a
	/// GQL `MATCH` supplies its own rows.
	pub fn inherits_row(&self) -> bool {
		match self {
			LogicalOp::Eval(_)
			| LogicalOp::Sequence(_)
			| LogicalOp::Let(_)
			| LogicalOp::Return(_)
			| LogicalOp::Foreach(_)
			| LogicalOp::If(_)
			| LogicalOp::Throw(_)
			| LogicalOp::Break(_)
			| LogicalOp::Continue(_)
			| LogicalOp::Info(_)
			| LogicalOp::Ddl(_)
			| LogicalOp::Sleep(_)
			| LogicalOp::Explain(_) => true,
			LogicalOp::Source(_)
			| LogicalOp::Values(_)
			| LogicalOp::Match(_)
			| LogicalOp::Subquery(_)
			| LogicalOp::Write(_)
			| LogicalOp::UnionAll(_)
			| LogicalOp::Join(_)
			| LogicalOp::Filter(_)
			| LogicalOp::PermissionFilter(_)
			| LogicalOp::Split(_)
			| LogicalOp::Aggregate(_)
			| LogicalOp::Distinct(_)
			| LogicalOp::Sort(_)
			| LogicalOp::Limit(_)
			| LogicalOp::Project(_)
			| LogicalOp::Fetch(_)
			| LogicalOp::Timeout(_)
			| LogicalOp::VersionScope(_)
			| LogicalOp::Only(_) => false,
		}
	}

	/// Whether this is a projection layer: a [`LogicalOp::Project`] of the
	/// shape `*, column, …` — [`LogicalFields::Select`] whose first field is
	/// `*`, followed by at least one [`LogicalField::Single`] and nothing
	/// else, every one of them in a frame whose mode is not
	/// [`EvalMode::Caller`] — with no `OMIT`.
	///
	/// A layer is the resolver's encoding of a table's computed fields and
	/// field permissions: the row passes through with each column set under
	/// its name. Passes and the physical planner tell a layer from the user's
	/// projection by this predicate alone; see the module documentation on
	/// barriers for what a pass may move across one.
	pub fn is_layer(&self) -> bool {
		match self {
			LogicalOp::Project(project) => project.is_layer(),
			LogicalOp::Source(_)
			| LogicalOp::Values(_)
			| LogicalOp::Match(_)
			| LogicalOp::Subquery(_)
			| LogicalOp::Write(_)
			| LogicalOp::UnionAll(_)
			| LogicalOp::Join(_)
			| LogicalOp::Filter(_)
			| LogicalOp::PermissionFilter(_)
			| LogicalOp::Split(_)
			| LogicalOp::Aggregate(_)
			| LogicalOp::Distinct(_)
			| LogicalOp::Sort(_)
			| LogicalOp::Limit(_)
			| LogicalOp::Fetch(_)
			| LogicalOp::Timeout(_)
			| LogicalOp::VersionScope(_)
			| LogicalOp::Only(_)
			| LogicalOp::Explain(_)
			| LogicalOp::Eval(_)
			| LogicalOp::Sequence(_)
			| LogicalOp::Let(_)
			| LogicalOp::Return(_)
			| LogicalOp::Foreach(_)
			| LogicalOp::If(_)
			| LogicalOp::Throw(_)
			| LogicalOp::Break(_)
			| LogicalOp::Continue(_)
			| LogicalOp::Info(_)
			| LogicalOp::Ddl(_)
			| LogicalOp::Sleep(_) => false,
		}
	}

	/// The operator's own attributes, rendered, excluding its inputs. One
	/// entry per attribute of a rendered node.
	fn attrs(&self) -> Vec<String> {
		match self {
			LogicalOp::Write(LogicalWrite {
				statement,
				..
			}) => {
				let mut out = vec![statement.kind().to_string()];
				if statement.only() {
					out.push("ONLY".to_string());
				}
				out
			}
			LogicalOp::Source(LogicalSource {
				source,
				with,
				for_update,
				..
			}) => {
				let mut out = vec![source.to_sql()];
				if let Some(w) = with {
					out.push(w.to_sql());
				}
				if *for_update {
					out.push("FOR UPDATE".to_string());
				}
				out
			}
			LogicalOp::Values(LogicalValues {
				expr,
				for_update,
				..
			}) => {
				let mut out = vec![expr.to_sql()];
				if *for_update {
					out.push("FOR UPDATE".to_string());
				}
				out
			}
			LogicalOp::Eval(LogicalEval {
				expr,
			}) => vec![expr.to_sql()],
			// The plan renders as the GQL query it came from.
			LogicalOp::Match(LogicalMatch {
				plan,
			}) => vec![plan.to_sql()],
			LogicalOp::Subquery(_)
			| LogicalOp::UnionAll(_)
			| LogicalOp::Distinct(_)
			| LogicalOp::Only(_)
			| LogicalOp::Sequence(_) => vec![],
			LogicalOp::Explain(LogicalExplain {
				analyze,
				format,
				..
			}) => {
				let mut out = Vec::new();
				if *analyze {
					out.push("ANALYZE".to_string());
				}
				match format {
					ExplainFormat::Text => {}
					ExplainFormat::Json => out.push("FORMAT JSON".to_string()),
				}
				out
			}
			LogicalOp::Filter(LogicalFilter {
				predicate,
				..
			}) => vec![predicate.to_sql()],
			LogicalOp::PermissionFilter(LogicalPermissionFilter {
				predicate,
				auth,
				..
			}) => {
				let mut out = vec![predicate.to_sql()];
				if let Some(auth) = auth {
					out.push(render_auth(auth));
				}
				out
			}
			LogicalOp::Join(LogicalJoin {
				kind,
				on,
				filter,
				alias,
				frame,
				..
			}) => {
				let mut out = vec![format!("{} AS {}", kind.as_str(), alias.name())];
				if !on.is_empty() {
					let keys = on
						.iter()
						.map(|(left, right)| format!("{} = {}", left.to_sql(), right.to_sql()))
						.collect::<Vec<_>>()
						.join(" AND ");
					out.push(format!("ON {keys}"));
				}
				if let Some(filter) = filter {
					out.push(format!("WHERE {}", filter.to_sql()));
				}
				out.extend(render_frame(frame));
				out
			}
			LogicalOp::Split(LogicalSplit {
				exprs,
				..
			}) => exprs.iter().map(ToSql::to_sql).collect(),
			LogicalOp::Aggregate(LogicalAggregate {
				group,
				fields,
				..
			}) => {
				let by = if group.is_empty() {
					"ALL".to_string()
				} else {
					group.iter().map(ToSql::to_sql).collect::<Vec<_>>().join(", ")
				};
				vec![format!("BY {by}"), fields.to_sql()]
			}
			LogicalOp::Sort(LogicalSort {
				ordering,
				tempfiles,
				..
			}) => {
				let mut out = vec![ordering.to_sql()];
				if *tempfiles {
					out.push("TEMPFILES".to_string());
				}
				out
			}
			LogicalOp::Limit(LogicalLimit {
				limit,
				start,
				..
			}) => {
				let mut out = Vec::new();
				if let Some(l) = limit {
					out.push(format!("LIMIT {}", l.to_sql()));
				}
				if let Some(s) = start {
					out.push(format!("START {}", s.to_sql()));
				}
				out
			}
			LogicalOp::Project(LogicalProject {
				fields,
				omit,
				..
			}) => {
				let mut out = vec![fields.to_sql()];
				if !omit.is_empty() {
					out.push(format!(
						"OMIT {}",
						omit.iter().map(ToSql::to_sql).collect::<Vec<_>>().join(", ")
					));
				}
				out
			}
			LogicalOp::Fetch(LogicalFetch {
				fetches,
				..
			}) => fetches.iter().map(ToSql::to_sql).collect(),
			LogicalOp::Timeout(LogicalTimeout {
				timeout,
				..
			}) => vec![timeout.to_sql()],
			LogicalOp::VersionScope(LogicalVersionScope {
				version,
				..
			}) => vec![version.to_sql()],
			LogicalOp::Let(LogicalLet {
				name,
				kind,
				..
			}) => {
				let mut out = format!("${name}");
				if let Some(kind) = kind {
					out.push_str(&format!(" : {}", kind.to_sql()));
				}
				vec![out]
			}
			LogicalOp::Return(LogicalReturn {
				fetch,
				..
			}) => {
				if fetch.is_empty() {
					vec![]
				} else {
					vec![format!(
						"FETCH {}",
						fetch.iter().map(ToSql::to_sql).collect::<Vec<_>>().join(", ")
					)]
				}
			}
			LogicalOp::Foreach(LogicalForeach {
				param,
				range,
				..
			}) => vec![format!("{} IN {}", param.to_sql(), range.to_sql())],
			// One attribute per condition, and `ELSE` where the last arm is
			// one; the arms render as the inputs, in the same order.
			LogicalOp::If(op) => {
				let mut out: Vec<String> = op.conditions.iter().map(ToSql::to_sql).collect();
				if op.otherwise().is_some() {
					out.push("ELSE".to_string());
				}
				out
			}
			LogicalOp::Throw(LogicalThrow {
				value,
			}) => vec![value.to_sql()],
			LogicalOp::Break(_) | LogicalOp::Continue(_) => vec![],
			LogicalOp::Info(info) => vec![info.to_sql()],
			// The kind alone. A schema statement's text can name a secret the
			// catalog holds — an access method's signing key, a user's
			// password hash — and a plan renders wherever one is printed,
			// `EXPLAIN` and diagnostics alike.
			LogicalOp::Ddl(ddl) => vec![ddl.statement.kind().to_string()],
			LogicalOp::Sleep(LogicalSleep {
				duration,
			}) => vec![duration.to_sql()],
		}
	}

	/// Renders the subtree rooted here, indented by `depth` levels, one
	/// operator per line. A scalar that renders over several lines — a
	/// scalar subquery — has its continuation lines re-indented beneath this
	/// node, so a nested plan reads as a nested source does.
	fn render(&self, f: &mut String, depth: usize) {
		let indent = "  ".repeat(depth);
		f.push_str(&indent);
		f.push_str(self.name());
		let attrs = self.attrs();
		if !attrs.is_empty() {
			f.push('[');
			for (i, line) in attrs.join("    ").split('\n').enumerate() {
				if i > 0 {
					f.push('\n');
					f.push_str(&indent);
				}
				f.push_str(line);
			}
			f.push(']');
		}
		f.push('\n');
		for child in self.children() {
			child.render(f, depth + 1);
		}
	}
}

/// The row field a `FETCH` target starts from: the field a path is rooted in.
/// `None` for a target that is not a path or does not start from a field — a
/// parameter holding a path, a projection function, a path rooted in an
/// expression — whose reach over the row is decided at run time.
fn fetch_root(fetch: &LogicalExpr) -> Option<&surrealdb_strand::Strand> {
	if let LogicalExpr::Field(name) = fetch.path_root() {
		Some(name)
	} else {
		None
	}
}

/// The table a pipeline reads when every node down to its leaf has one input
/// and the leaf is a table source; `None` for any other shape.
fn leaf_table(mut op: &LogicalOp) -> Option<&TableName> {
	loop {
		match op.children() {
			[] => {
				return match op {
					LogicalOp::Source(LogicalSource {
						source: SourceRef::Table(table),
						..
					}) => Some(table),
					LogicalOp::Source(_)
					| LogicalOp::Values(_)
					| LogicalOp::Match(_)
					| LogicalOp::Subquery(_)
					| LogicalOp::Write(_)
					| LogicalOp::UnionAll(_)
					| LogicalOp::Join(_)
					| LogicalOp::Filter(_)
					| LogicalOp::PermissionFilter(_)
					| LogicalOp::Split(_)
					| LogicalOp::Aggregate(_)
					| LogicalOp::Distinct(_)
					| LogicalOp::Sort(_)
					| LogicalOp::Limit(_)
					| LogicalOp::Project(_)
					| LogicalOp::Fetch(_)
					| LogicalOp::Timeout(_)
					| LogicalOp::VersionScope(_)
					| LogicalOp::Only(_)
					| LogicalOp::Explain(_)
					| LogicalOp::Eval(_)
					| LogicalOp::Sequence(_)
					| LogicalOp::Let(_)
					| LogicalOp::Return(_)
					| LogicalOp::Foreach(_)
					| LogicalOp::If(_)
					| LogicalOp::Throw(_)
					| LogicalOp::Break(_)
					| LogicalOp::Continue(_)
					| LogicalOp::Info(_)
					| LogicalOp::Ddl(_)
					| LogicalOp::Sleep(_) => None,
				};
			}
			[input] => op = input,
			_ => return None,
		}
	}
}

/// The kind a `SPLIT` on a field of `kind` leaves in each row: the element
/// kind of an array or set; each alternative's element kind for an `either`;
/// and the kind itself for anything else, since a value that is not a
/// collection passes through a split unchanged. `Any` stays `Any`, and a
/// literal kind is not looked into.
fn element_kind(kind: &Kind) -> Kind {
	match kind {
		Kind::Array(inner, _) | Kind::Set(inner, _) => (**inner).clone(),
		Kind::Either(kinds) => Kind::either(kinds.iter().map(element_kind).collect()),
		Kind::Any | Kind::Literal(_) => Kind::Any,
		Kind::None
		| Kind::Null
		| Kind::Bool
		| Kind::Bytes
		| Kind::Datetime
		| Kind::Decimal
		| Kind::Duration
		| Kind::Float
		| Kind::Int
		| Kind::Number
		| Kind::Object
		| Kind::String
		| Kind::Uuid
		| Kind::Regex
		| Kind::Table(_)
		| Kind::Record(_)
		| Kind::Geometry(_)
		| Kind::Function(..)
		| Kind::Range
		| Kind::File(_) => kind.clone(),
	}
}

/// The kind of `path` across the inputs of a union: the declared kind when
/// every input declares it identically; `option<kind>` when a closed input
/// lacks it, since that input's rows have no such field; `Any` when the
/// declarations differ or an open input leaves it undeclared.
fn union_kind(inputs: &[Schema], path: &FieldPath) -> Kind {
	let mut kind: Option<Kind> = None;
	let mut absent = false;
	for input in inputs {
		match input.field(path) {
			Some(field) => match &kind {
				None => kind = Some(field.kind.clone()),
				Some(seen) if *seen == field.kind => {}
				Some(_) => return Kind::Any,
			},
			None if input.is_open() => return Kind::Any,
			None => absent = true,
		}
	}
	match kind {
		Some(kind) if absent && kind != Kind::Any => Kind::option(kind),
		Some(kind) => kind,
		None => Kind::Any,
	}
}

/// The schema a layer ([`LogicalOp::is_layer`]) produces over `input`: the
/// input's fields, in order and with its openness, each column retyped. A
/// column in an [`EvalMode::Permission`] frame yields its value or cuts the
/// field, so it is `option<…>` of its kind: the declared kind when the column
/// carries one, else the kind the input declares for the field; where the
/// input declares nothing, or declares no `TYPE`, it is typed by its
/// expression. Any other column is its declared kind, or is typed by its
/// expression when it carries none. A column the input does not declare is
/// appended. A column coerced to a kind as it lands is marked checked
/// ([`FieldSchema::checked`]); a guard column, and a column typed by an
/// expression whose kind is not checked, is not. A column's other declared
/// properties (`FLEXIBLE`, the computed body, the select permission) are the
/// catalog's and are kept.
fn layer_schema(fields: &LogicalFields, mut input: Schema) -> Schema {
	let LogicalFields::Select(fields) = fields else {
		return input;
	};
	for field in fields {
		let LogicalField::Single {
			expr,
			alias,
			kind,
			frame,
		} = field
		else {
			continue;
		};
		let path = match alias {
			Some(alias) => FieldPath::from(alias),
			None => FieldPath::from(&expr.output_path()),
		};
		// A guard column passes the stored value through, or `NONE`, so its
		// kind is declared rather than checked; any other column is coerced
		// to its kind as it lands, or is typed by its expression.
		let (kind, checked) = match kind {
			Some(kind) if frame.mode == EvalMode::Permission => (Kind::option(kind.clone()), false),
			Some(kind) => (kind.clone(), true),
			None => match input.field(&path).map(|declared| &declared.kind) {
				Some(declared) if frame.mode == EvalMode::Permission && *declared != Kind::Any => {
					(Kind::option(declared.clone()), false)
				}
				Some(_) | None if frame.mode == EvalMode::Permission => (expr.kind(&input), false),
				Some(_) | None => (expr.kind(&input), expr.checked_kind(&input).is_some()),
			},
		};
		match input.field_mut(&path) {
			Some(declared) => {
				declared.kind = kind;
				declared.checked = checked;
			}
			None => {
				let mut field = FieldSchema::new(path, kind);
				field.checked = checked;
				input.fields.push(field);
			}
		}
	}
	input
}

/// The schema a projection produces over `input`. `VALUE` rows are bare
/// values rather than objects, so that form is open.
fn project_schema(fields: &LogicalFields, input: &Schema) -> Schema {
	let selected = match fields {
		LogicalFields::Value {
			..
		} => return Schema::open(),
		LogicalFields::Select(selected) => selected,
	};
	let mut out = Schema::closed(Vec::new());
	for field in selected {
		match field {
			LogicalField::All => {
				out.openness = input.openness;
				for field in &input.fields {
					if out.field(&field.path).is_none() {
						out.fields.push(field.clone());
					}
				}
			}
			LogicalField::Single {
				expr,
				alias,
				kind,
				..
			} => {
				let path = match alias {
					Some(alias) => FieldPath::from(alias),
					None => FieldPath::from(&expr.output_path()),
				};
				// A column typed by its expression holds that expression's
				// value, so its kind is checked when the expression's is; a
				// declared kind is not.
				let (kind, checked) = match kind {
					Some(kind) => (kind.clone(), false),
					None => (expr.kind(input), expr.checked_kind(input).is_some()),
				};
				out.fields.retain(|f| f.path != path);
				let mut field = FieldSchema::new(path, kind);
				field.checked = checked;
				out.fields.push(field);
			}
		}
	}
	out
}

fn render_auth(auth: &AuthLimit) -> String {
	format!("@auth = {:?}", auth.level)
}

/// Renders a frame's non-default parts, or `None` for the plain caller frame.
fn render_frame(frame: &EvalFrame) -> Option<String> {
	let mut parts = Vec::new();
	match frame.mode {
		EvalMode::Caller => {}
		EvalMode::Permission => parts.push("@permission".to_string()),
		EvalMode::MutablePermission => parts.push("@mutable-permission".to_string()),
		EvalMode::Computed => parts.push("@computed".to_string()),
	}
	if let Some(auth) = &frame.auth {
		parts.push(render_auth(auth));
	}
	if parts.is_empty() {
		None
	} else {
		Some(parts.join(" "))
	}
}

/// Renders the plan as an indented operator tree whose scalars are SurrealQL
/// and whose scalar subqueries are nested plans. Deterministic diagnostic
/// text: a tree is not itself a parseable statement.
impl ToSql for LogicalOp {
	fn fmt_sql(&self, f: &mut String, _fmt: SqlFormat) {
		self.render(f, 0);
	}
}

impl Display for LogicalOp {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		let mut out = String::new();
		self.render(&mut out, 0);
		f.write_str(out.trim_end())
	}
}

#[cfg(test)]
mod tests {
	use std::convert::Infallible;

	use surrealdb_strand::TableName;

	use super::*;
	use crate::expr::match_plan::{
		BindingDef, BindingKind, DetachMode, MatchClausePlan, MatchColumn, MatchOutput,
		MatchPredicate, MatchStage, MutationStage, NodeStep, PatternPlan,
	};
	use crate::expr::operator::BinaryOperator;

	fn parse_expr(sql: &str) -> crate::sql::Expr {
		let ast = surrealdb_syn::parse(&format!("RETURN {sql}")).expect("expression should parse");
		let crate::sql::TopLevelExpr::Expr(crate::sql::Expr::Return(out)) =
			ast.expressions.into_iter().next().expect("one statement")
		else {
			panic!("expected a RETURN");
		};
		out.what
	}

	fn expr(sql: &str) -> LogicalExpr {
		lower_expr(parse_expr(sql)).expect("expression should lower")
	}

	/// Lowers a whole statement, as a pass sees it before any rule runs.
	fn lower_sql(sql: &str) -> LogicalOp {
		let ast = surrealdb_syn::parse(sql).expect("statement should parse");
		let crate::sql::TopLevelExpr::Expr(top) =
			ast.expressions.into_iter().next().expect("one statement")
		else {
			panic!("expected a bare expression");
		};
		lower(top).expect("statement should lower")
	}

	/// Walks `plan` with a callback that breaks at the first table `Source`.
	/// Answers what the walk itself returned and every table the callback was
	/// handed, so a walk that records a `Break` and carries on is visible.
	fn tables_until_first(plan: &LogicalOp) -> (ControlFlow<&str>, Vec<&str>) {
		let mut seen = Vec::new();
		let stopped = plan.visit_ops_deep(&mut |node| {
			if let LogicalOp::Source(LogicalSource {
				source: SourceRef::Table(name),
				..
			}) = node
			{
				seen.push(name.as_str());
				return ControlFlow::Break(name.as_str());
			}
			ControlFlow::Continue(())
		});
		(stopped, seen)
	}

	/// Every table a `Source` of `plan` names, in the order
	/// [`LogicalOp::visit_ops_deep`] reaches it.
	fn tables_reached(plan: &LogicalOp) -> Vec<&str> {
		let mut reached = Vec::new();
		let _ = plan.visit_ops_deep(&mut |node| {
			if let LogicalOp::Source(LogicalSource {
				source: SourceRef::Table(name),
				..
			}) = node
			{
				reached.push(name.as_str());
			}
			ControlFlow::<()>::Continue(())
		});
		reached
	}

	fn source(table: &str) -> LogicalOp {
		LogicalOp::Source(LogicalSource {
			source: SourceRef::Table(TableName::from(table)),
			schema: None,
			with: None,
			for_update: false,
		})
	}

	fn root_limit() -> AuthLimit {
		AuthLimit::new(surrealdb_iam::Level::Root, None)
	}

	fn single(expr: LogicalExpr, alias: Option<&str>) -> LogicalField {
		LogicalField::user(expr, alias.map(OutputPath::field))
	}

	/// A projection layer over `input` setting `columns`.
	fn layer(input: LogicalOp, columns: Vec<LogicalField>) -> LogicalOp {
		let mut fields = vec![LogicalField::All];
		fields.extend(columns);
		LogicalOp::Project(LogicalProject {
			input: Box::new(input),
			fields: LogicalFields::Select(fields),
			omit: vec![],
		})
	}

	/// The shape the field-resolution pass produces for a table carrying a
	/// select permission, a `COMPUTED` field and a permission-guarded field.
	///
	/// Built by hand rather than by a pass, so the test pins that the
	/// vocabulary expresses this shape independently of whatever produces it.
	fn resolved_table_view() -> LogicalOp {
		//   DEFINE TABLE t PERMISSIONS FOR select WHERE published = true;
		//   DEFINE FIELD c      ON t TYPE int COMPUTED a + b;
		//   DEFINE FIELD secret ON t PERMISSIONS FOR select WHERE $auth.id = owner;
		//   SELECT a, c, secret FROM t WHERE c > 10 AND secret = 'x'
		let table_permission = LogicalOp::PermissionFilter(LogicalPermissionFilter {
			input: Box::new(source("t")),
			predicate: expr("published = true"),
			auth: None,
			guard: GuardKind::Read,
		});
		// The computed field: a typed column in the computed frame, under
		// the field's own name.
		let computed = layer(
			table_permission,
			vec![LogicalField::column(
				"c",
				expr("a + b"),
				Some(Kind::Int),
				EvalFrame::computed(None),
			)],
		);
		// The guarded field: a plain IF/THEN/ELSE under the field's own name,
		// evaluated as a permission predicate so its own reads are not
		// re-checked.
		let guarded = layer(
			computed,
			vec![LogicalField::column(
				"secret",
				expr("IF $auth.id = owner THEN secret ELSE NONE END"),
				None,
				EvalFrame::permission(None),
			)],
		);
		// The user's predicate reads plain field names; above the stack they
		// are the derived values.
		let user_filter = LogicalOp::Filter(LogicalFilter {
			input: Box::new(guarded),
			predicate: expr("c > 10 AND secret = 'x'"),
		});
		LogicalOp::Project(LogicalProject {
			input: Box::new(user_filter),
			fields: LogicalFields::Select(vec![
				single(expr("a"), None),
				single(expr("c"), None),
				single(expr("secret"), None),
			]),
			omit: vec![],
		})
	}

	/// A schema statement node. Which statement it carries is immaterial to
	/// the traversal and rendering fixtures: the node is a leaf with no
	/// inputs and no scalars whichever form it holds, and it renders as its
	/// kind alone.
	fn ddl_op() -> LogicalOp {
		LogicalOp::Ddl(LogicalDdl {
			statement: LogicalDdlStatement::Rebuild(Box::new(
				crate::expr::statements::RebuildStatement::Index(
					crate::expr::statements::rebuild::RebuildIndexStatement {
						name: "i".into(),
						table: TableName::from("t"),
						if_exists: false,
						concurrently: false,
					},
				),
			)),
		})
	}

	/// One node of every variant, each over a single `Source` leaf where it
	/// has an input, so traversal tests can cover the whole enum.
	fn one_of_each() -> Vec<LogicalOp> {
		let leaf = || Box::new(source("t"));
		vec![
			source("t"),
			LogicalOp::Values(LogicalValues {
				expr: expr("[1, 2]"),
				for_update: false,
				written: false,
			}),
			LogicalOp::Subquery(LogicalSubquery {
				input: leaf(),
			}),
			LogicalOp::Write(LogicalWrite {
				statement: LogicalWriteStatement::Create(Box::default()),
				source: LogicalWriteSource::Targets(vec![expr("person")]),
				data: None,
				duplicate_update: None,
				cond: None,
			}),
			LogicalOp::UnionAll(LogicalUnionAll {
				inputs: vec![source("a"), source("b")],
			}),
			LogicalOp::Join(LogicalJoin::new(
				source("a"),
				source("b"),
				JoinKind::Semi,
				vec![(expr("x"), RelationAlias::new(0).access("k"))],
				None,
				RelationAlias::new(0),
				EvalFrame::caller(),
			)),
			LogicalOp::Filter(LogicalFilter {
				input: leaf(),
				predicate: expr("a > 1"),
			}),
			LogicalOp::PermissionFilter(LogicalPermissionFilter {
				input: leaf(),
				predicate: expr("published = true"),
				auth: Some(root_limit()),
				guard: GuardKind::Read,
			}),
			LogicalOp::Split(LogicalSplit {
				input: leaf(),
				exprs: vec![expr("a")],
			}),
			LogicalOp::Aggregate(LogicalAggregate {
				input: leaf(),
				group: vec![],
				fields: LogicalFields::all(),
			}),
			LogicalOp::Distinct(LogicalDistinct {
				input: leaf(),
			}),
			LogicalOp::Sort(LogicalSort {
				input: leaf(),
				ordering: LogicalOrdering::Random,
				tempfiles: false,
			}),
			LogicalOp::Limit(LogicalLimit {
				input: leaf(),
				limit: Some(expr("1")),
				start: None,
			}),
			LogicalOp::Project(LogicalProject {
				input: leaf(),
				fields: LogicalFields::all(),
				omit: vec![],
			}),
			LogicalOp::Fetch(LogicalFetch {
				input: leaf(),
				fetches: vec![expr("f")],
			}),
			LogicalOp::Timeout(LogicalTimeout {
				input: leaf(),
				timeout: expr("1s"),
			}),
			LogicalOp::VersionScope(LogicalVersionScope {
				input: leaf(),
				version: expr("d'2024-01-01T00:00:00Z'"),
			}),
			LogicalOp::Only(LogicalOnly {
				input: leaf(),
			}),
			LogicalOp::Explain(LogicalExplain {
				input: leaf(),
				analyze: false,
				format: ExplainFormat::Text,
			}),
			LogicalOp::Eval(LogicalEval {
				expr: expr("1 + 1"),
			}),
			LogicalOp::Sequence(LogicalSequence {
				stmts: vec![source("a"), source("b")],
			}),
			LogicalOp::Let(LogicalLet {
				name: "x".to_string(),
				kind: Some(Kind::Int),
				value: leaf(),
			}),
			LogicalOp::Return(LogicalReturn {
				what: leaf(),
				fetch: vec![expr("f")],
			}),
			LogicalOp::Foreach(LogicalForeach {
				param: Param::from("x".to_string()),
				range: expr("[1, 2]"),
				body: leaf(),
			}),
			LogicalOp::Info(Box::new(LogicalInfo::Db {
				structure: false,
				version: None,
			})),
			LogicalOp::Sleep(LogicalSleep {
				duration: expr("1ms"),
			}),
			ddl_op(),
		]
	}

	#[test]
	fn read_only_covers_the_scalar_kinds_and_the_lock() {
		assert!(
			LogicalOp::Filter(LogicalFilter {
				input: Box::new(source("t")),
				predicate: expr("a > 1"),
			})
			.read_only()
		);
		// A writing statement in a predicate is a scalar subquery.
		assert!(
			!LogicalOp::Filter(LogicalFilter {
				input: Box::new(source("t")),
				predicate: expr("fn::write() != NONE"),
			})
			.read_only()
		);
		// A path whose last step calls an unregistered method names a closure
		// stored on the receiver. Its body is not an expression this walk can
		// descend into, so only the method's own classification
		// over-approximates it to writing.
		assert!(
			!LogicalOp::Split(LogicalSplit {
				input: Box::new(source("t")),
				exprs: vec![expr("a.some_method()")],
			})
			.read_only()
		);
		// A locking read is not a read.
		assert!(
			!LogicalOp::Source(LogicalSource {
				source: SourceRef::Table(TableName::from("t")),
				schema: None,
				with: None,
				for_update: true,
			})
			.read_only()
		);
		// A `Values` row set is its expression: a call that may write makes
		// the plan a write, as it does for a dynamic source expression.
		assert!(
			LogicalOp::Values(LogicalValues {
				expr: expr("[1, 2]"),
				for_update: false,
				written: false,
			})
			.read_only()
		);
		assert!(
			!LogicalOp::Values(LogicalValues {
				expr: expr("fn::write()"),
				for_update: false,
				written: false,
			})
			.read_only()
		);
		// A write inside a nested statement operator.
		assert!(
			!LogicalOp::Sequence(LogicalSequence {
				stmts: vec![LogicalOp::Eval(LogicalEval {
					expr: expr("fn::write()"),
				})],
			})
			.read_only()
		);
	}

	/// `MATCH (a:t) WHERE <predicate> RETURN a.x`, with `extra` stages
	/// appended after the read clause. One node binding and no edges: the
	/// smallest pattern the plan's invariants admit.
	fn match_op(predicate: LogicalExpr, extra: Vec<MatchStage>) -> LogicalOp {
		let mut stages = vec![MatchStage::Read(MatchClausePlan {
			optional_group: None,
			patterns: vec![PatternPlan {
				path_var: None,
				search: None,
				start: NodeStep {
					binding: 0,
					label: Some(TableName::from("t")),
				},
				steps: Vec::new(),
			}],
			predicates: vec![MatchPredicate {
				expr: predicate,
				deps: vec![0],
			}],
		})];
		stages.extend(extra);
		LogicalOp::Match(LogicalMatch {
			plan: Box::new(MatchPlan {
				bindings: vec![BindingDef {
					name: "a".to_string(),
					kind: BindingKind::Node,
					user_named: true,
				}],
				stages,
				output: Some(MatchOutput {
					columns: vec![MatchColumn {
						name: "x".to_string(),
						expr: expr("a.x"),
						hidden: false,
					}],
					distinct: false,
					group_by: None,
					order: Vec::new(),
					skip: None,
					limit: None,
				}),
			}),
		})
	}

	/// A `MATCH` plan is a leaf to the tree traversals — its stages are a
	/// binding table, not rows — but it is not opaque: it answers every
	/// question the vocabulary asks of a node, and its scalars are reached
	/// like any other node's.
	#[test]
	fn a_match_plan_declares_what_it_does_not_expose() {
		let read = match_op(expr("a.n > 1"), Vec::new());
		// A leaf to the tree walk, and a row source of its own: it neither
		// takes rows from an input nor evaluates against the row of a
		// statement holding it.
		assert!(read.children().is_empty());
		assert!(!read.inherits_row());
		// The binding-table shape is not a row schema, so the plan declares
		// nothing about the fields above it.
		assert!(read.schema().is_open());
		// Not a layer: nothing about it is a barrier the rules read.
		assert!(!read.is_layer());
		// Every scalar the plan evaluates is reached from the node, the
		// clause predicates before the output columns.
		let scalars: Vec<String> =
			read.scalars().into_iter().map(|scalar| scalar.to_sql()).collect();
		assert_eq!(scalars, vec!["a.n > 1".to_string(), "a.x".to_string()]);
		// And reached mutably, so a bind pass rewrites through the node
		// rather than around it.
		let mut rewritten = read.clone();
		let Ok(()) = rewritten.visit_scalars_mut(&mut |scalar: &mut LogicalExpr| {
			*scalar = LogicalExpr::Literal(LogicalLiteral::Integer(0));
			Ok::<_, Infallible>(())
		});
		let rewritten: Vec<String> =
			rewritten.scalars().into_iter().map(|scalar| scalar.to_sql()).collect();
		assert_eq!(rewritten, vec!["0".to_string(), "0".to_string()]);
		// A scalar that writes makes the node a write, as it does anywhere
		// else.
		assert!(read.read_only());
		assert!(!match_op(expr("fn::write() != NONE"), Vec::new()).read_only());
		// A write stage targets a binding rather than an expression, so the
		// scalars do not reveal it: a `DELETE` stage carries none at all. The
		// node has to answer from the stages, not from what it exposes.
		let deleting = match_op(
			expr("a.n > 1"),
			vec![MatchStage::Mutate(MutationStage::Delete {
				target: 0,
				detach: DetachMode::NoDetach,
			})],
		);
		assert!(deleting.scalars().into_iter().all(LogicalExpr::read_only));
		assert!(!deleting.read_only());
	}

	#[test]
	fn the_vocabulary_expresses_a_resolved_table_view() {
		let rendered = resolved_table_view().to_string();
		let expected = "\
Project[a, c, secret]
  Filter[c > 10 AND secret = 'x']
    Project[*, (IF $auth.id = owner THEN secret ELSE NONE END) AS secret @permission]
      Project[*, a + b AS c : int @computed]
        PermissionFilter[published = true]
          Source[t]";
		assert_eq!(rendered, expected, "\nrendered:\n{rendered}\n");
	}

	#[test]
	fn a_layer_is_a_project_whose_columns_are_not_the_callers() {
		let guard = || {
			LogicalField::column(
				"secret",
				expr("IF p THEN secret ELSE NONE END"),
				None,
				EvalFrame::permission(None),
			)
		};
		let computed =
			|| LogicalField::column("c", expr("a + b"), Some(Kind::Int), EvalFrame::computed(None));
		let project = |fields: Vec<LogicalField>, omit: Vec<LogicalExpr>| {
			LogicalOp::Project(LogicalProject {
				input: Box::new(source("t")),
				fields: LogicalFields::Select(fields),
				omit,
			})
		};
		assert!(project(vec![LogicalField::All, guard()], vec![]).is_layer());
		assert!(project(vec![LogicalField::All, computed(), guard()], vec![]).is_layer());
		// A column in a caller-mode frame is the user's, even narrowed by an
		// authority limit: the mode decides, not the whole frame.
		let narrowed = LogicalField::Single {
			expr: expr("a + b"),
			alias: Some(OutputPath::field("c")),
			kind: None,
			frame: EvalFrame {
				mode: EvalMode::Caller,
				auth: Some(root_limit()),
			},
		};
		assert!(!project(vec![LogicalField::All, narrowed], vec![]).is_layer());
		// The user's shapes: no `*`, `*` alone, `*` not first, `*, field` as
		// the caller, a second `*`, an `OMIT`, a `VALUE`.
		assert!(!project(vec![single(expr("a"), None)], vec![]).is_layer());
		assert!(!project(vec![LogicalField::All], vec![]).is_layer());
		assert!(!project(vec![guard(), LogicalField::All], vec![]).is_layer());
		assert!(
			!project(vec![LogicalField::All, single(expr("a + b"), Some("c"))], vec![]).is_layer()
		);
		assert!(!project(vec![LogicalField::All, guard(), LogicalField::All], vec![]).is_layer());
		assert!(!project(vec![LogicalField::All, guard()], vec![expr("secret")]).is_layer());
		assert!(
			!LogicalOp::Project(LogicalProject {
				input: Box::new(source("t")),
				fields: LogicalFields::Value {
					expr: expr("a"),
					alias: None,
				},
				omit: vec![],
			})
			.is_layer()
		);
		// Every other operator, whatever it holds.
		for op in one_of_each() {
			if !matches!(op, LogicalOp::Project(_)) {
				assert!(!op.is_layer(), "{} is not a layer", op.name());
			}
		}
	}

	/// A mutation lowers like any other statement, so `engine_for` can offer
	/// it to the engine and the planner can report why it declines.
	#[test]
	fn a_create_lowers_to_a_write() {
		let op = super::lower(parse_expr("CREATE person SET name = 'x'")).expect("should lower");
		let LogicalOp::Write(write) = &op else {
			panic!("expected a Write, got {}", op.name());
		};
		assert_eq!(write.statement.kind(), "CREATE");
		assert!(!write.statement.only());
		// Targets, not rows: a creating form's subject does not exist yet, so
		// nothing may read it.
		let LogicalWriteSource::Targets(targets) = &write.source else {
			panic!("a creating statement must not take a reading source")
		};
		assert_eq!(targets.len(), 1);
	}

	/// Each target is kept separately, and they are the node's own scalars, so
	/// a pass that rewrites expressions reaches them. They are not a plan
	/// subtree: a creating form's target must not be read.
	#[test]
	fn create_keeps_one_target_per_thing_written() {
		let many = super::lower(parse_expr("CREATE person:a, person:b")).expect("should lower");
		let LogicalOp::Write(write) = &many else {
			panic!("expected a Write")
		};
		let LogicalWriteSource::Targets(targets) = &write.source else {
			panic!("expected targets")
		};
		assert_eq!(targets.len(), 2);
		assert_eq!(many.scalars().len(), 2, "the targets are the node's own scalars");
		assert!(many.children().is_empty(), "targets are not a plan subtree");
	}

	/// The one arm of `node_read_only` that consults neither its source nor
	/// its statement. A mutation writes by construction, and `read_only()`
	/// selects the transaction, so a `Write` that answered otherwise would put
	/// a write on a read-only transaction.
	#[test]
	fn a_write_is_never_read_only() {
		let op = super::lower(parse_expr("CREATE person")).expect("should lower");
		assert!(!op.read_only());

		// Not inherited: there is no subtree, and the target expression itself
		// reads nothing.
		assert!(op.children().is_empty());
		assert!(op.scalars().iter().all(|target| target.read_only()));
	}

	/// A mutation emits the target table's records, which this node does not
	/// resolve, so nothing above it may assume declared columns.
	#[test]
	fn a_write_declares_no_columns() {
		let op = super::lower(parse_expr("CREATE person")).expect("should lower");
		assert!(op.schema().is_open());
	}

	#[test]
	fn a_scalar_subquery_renders_as_a_nested_plan() {
		let plan = LogicalOp::Filter(LogicalFilter {
			input: Box::new(source("t")),
			predicate: expr("a IN (SELECT VALUE b FROM u)"),
		});
		let expected = "\
Filter[a INSIDE (
  Project[VALUE b]
    Source[u]
)]
  Source[t]";
		assert_eq!(plan.to_string(), expected, "\nrendered:\n{plan}\n");
	}

	#[test]
	fn a_join_renders_its_kind_alias_keys_filter_and_frame() {
		let alias = RelationAlias::new(0);
		let plan = LogicalOp::Join(LogicalJoin::new(
			source("t"),
			source("u"),
			JoinKind::Semi,
			vec![(expr("a"), alias.access("b"))],
			Some(LogicalExpr::Binary {
				left: Box::new(alias.access("n")),
				op: BinaryOperator::MoreThan,
				right: Box::new(expr("m")),
			}),
			alias,
			EvalFrame::permission(None),
		));
		let expected = "\
Join[SEMI AS _join0    ON a = _join0.b    WHERE _join0.n > m    @permission]
  Source[t]
  Source[u]";
		assert_eq!(plan.to_string(), expected, "\nrendered:\n{plan}\n");
		let plain = LogicalOp::Join(LogicalJoin::new(
			source("t"),
			source("u"),
			JoinKind::Left,
			vec![],
			None,
			RelationAlias::new(3),
			EvalFrame::caller(),
		));
		assert_eq!(plain.to_string(), "Join[LEFT AS _join3]\n  Source[t]\n  Source[u]");
		assert_eq!(RelationAlias::new(3).index(), Some(3));
	}

	#[test]
	fn the_evaluation_modes_cover_every_executor_frame() {
		// Each mode is one Options frame; the two axes must be independent so
		// a computed body (checked, no writes) and a mutable permission
		// (unchecked, writes) are both expressible.
		let table = [
			(EvalMode::Caller, true, false),
			(EvalMode::Permission, false, true),
			(EvalMode::MutablePermission, false, false),
			(EvalMode::Computed, true, true),
		];
		for (mode, checks, forbids) in table {
			assert_eq!(mode.checks_permissions(), checks, "{mode:?} checks_permissions");
			assert_eq!(mode.forbids_writes(), forbids, "{mode:?} forbids_writes");
		}
	}

	#[test]
	fn authority_is_independent_of_mode() {
		// A VALUE or ASSERT body runs as the caller under the definer's limit,
		// which is a caller-mode frame that is not the plain caller frame.
		let frame = EvalFrame {
			mode: EvalMode::Caller,
			auth: Some(root_limit()),
		};
		assert!(!frame.is_caller());
		assert!(EvalFrame::caller().is_caller());
	}

	#[test]
	fn a_layer_column_renders_its_kind_and_frame() {
		// The coercion and the confused-deputy guard have to be visible in a
		// rendered plan, or a reviewer cannot tell which values are coerced
		// and which bodies run narrowed.
		let column = LogicalField::column(
			"c",
			expr("a + b"),
			Some(Kind::Int),
			EvalFrame::computed(Some(root_limit())),
		);
		assert_eq!(column.to_sql(), "a + b AS c : int @computed @auth = Root");
		let guard = LogicalField::column(
			"secret",
			expr("IF p THEN secret ELSE NONE END"),
			None,
			EvalFrame::permission(None),
		);
		assert_eq!(guard.to_sql(), "(IF p THEN secret ELSE NONE END) AS secret @permission");
		// A field the user wrote renders as written.
		assert_eq!(single(expr("a + b"), Some("c")).to_sql(), "a + b AS c");
		assert_eq!(single(expr("a"), None).to_sql(), "a");
	}

	#[test]
	fn map_children_preserves_arity_for_every_variant() {
		for op in one_of_each() {
			let arity = op.children().len();
			let Ok(rebuilt) = op.clone().map_children(Ok::<_, Infallible>);
			assert_eq!(rebuilt, op, "identity rebuild should be a no-op for {}", op.name());
			assert_eq!(rebuilt.children().len(), arity, "{} lost or gained an input", op.name());
		}
	}

	#[test]
	fn map_children_replaces_each_input_in_order() {
		let op = LogicalOp::UnionAll(LogicalUnionAll {
			inputs: vec![source("a"), source("b")],
		});
		let mut seen = Vec::new();
		let Ok(rebuilt) = op.map_children(|child| {
			seen.push(child.to_string());
			Ok::<_, Infallible>(LogicalOp::Values(LogicalValues {
				expr: expr("[]"),
				for_update: false,
				written: false,
			}))
		});
		assert_eq!(seen, ["Source[a]", "Source[b]"]);
		assert_eq!(rebuilt.children().len(), 2);
		assert!(rebuilt.children().iter().all(|c| c.name() == "Values"));
	}

	#[test]
	fn map_children_stops_at_the_first_error() {
		let op = LogicalOp::UnionAll(LogicalUnionAll {
			inputs: vec![source("a"), source("b"), source("c")],
		});
		let mut calls = 0;
		let result = op.map_children(|child| {
			calls += 1;
			if child.to_string() == "Source[b]" {
				Err("b")
			} else {
				Ok(child)
			}
		});
		assert_eq!(result, Err("b"));
		assert_eq!(calls, 2);
	}

	#[test]
	fn transform_up_sees_children_before_parents() {
		let mut order = Vec::new();
		let Ok(_) = resolved_table_view().transform_up(&mut |node| {
			order.push(node.name());
			Ok::<_, Infallible>(node)
		});
		assert_eq!(
			order,
			["Source", "PermissionFilter", "Project", "Project", "Filter", "Project"]
		);
	}

	#[test]
	fn transform_down_sees_parents_before_children() {
		let mut order = Vec::new();
		let Ok(_) = resolved_table_view().transform_down(&mut |node| {
			order.push(node.name());
			Ok::<_, Infallible>(node)
		});
		assert_eq!(
			order,
			["Project", "Filter", "Project", "Project", "PermissionFilter", "Source"]
		);
	}

	#[test]
	fn transform_down_descends_into_the_rewritten_node() {
		// A top-down pass that replaces a node must have the replacement's
		// inputs visited, not the original's.
		let op = LogicalOp::Only(LogicalOnly {
			input: Box::new(source("t")),
		});
		let mut visited = Vec::new();
		let Ok(out) = op.transform_down(&mut |node| {
			visited.push(node.name());
			Ok::<_, Infallible>(
				if let LogicalOp::Only(LogicalOnly {
					input,
				}) = node
				{
					LogicalOp::Distinct(LogicalDistinct {
						input: Box::new(LogicalOp::Filter(LogicalFilter {
							input,
							predicate: expr("true"),
						})),
					})
				} else {
					node
				},
			)
		});
		assert_eq!(visited, ["Only", "Filter", "Source"]);
		assert_eq!(out.name(), "Distinct");
	}

	#[test]
	fn visit_short_circuits_and_any_reports_it() {
		let view = resolved_table_view();
		let mut visited = 0;
		let found = view.visit(&mut |node| {
			visited += 1;
			if matches!(node, LogicalOp::PermissionFilter(_)) {
				ControlFlow::Break(node.name())
			} else {
				ControlFlow::Continue(())
			}
		});
		assert_eq!(found, ControlFlow::Break("PermissionFilter"));
		assert_eq!(visited, 5, "should stop at the permission filter");
		assert!(view.any(LogicalOp::is_layer));
		assert!(view.any(|n| matches!(n, LogicalOp::Project(_)) && !n.is_layer()));
	}

	/// Counts parameter references in a node's own scalars, descending into
	/// each scalar but not into a scalar subquery, the way a plan-time pass
	/// does when it leaves nested plans to their own walk.
	fn count_params(op: &LogicalOp) -> usize {
		let mut count = 0;
		let Ok(()) = op.visit_scalars(&mut |scalar| {
			let _ = scalar.visit(&mut |e| {
				if matches!(e, LogicalExpr::Param(_)) {
					count += 1;
				}
				ControlFlow::<()>::Continue(())
			});
			Ok::<(), Infallible>(())
		});
		count
	}

	/// Replaces every `$p` in a node's own scalars with the literal `1`.
	fn fold_params(op: &mut LogicalOp) {
		let Ok(()) = op.visit_scalars_mut(&mut |scalar| {
			scalar.visit_mut(&mut |e| {
				if matches!(e, LogicalExpr::Param(p) if p.as_str() == "p") {
					*e = LogicalExpr::Literal(LogicalLiteral::Integer(1));
				}
				Ok::<(), Infallible>(())
			})
		});
	}

	/// A node of every scalar-bearing variant with `$p` in each scalar slot,
	/// so a slot the scalar visitors skip shows up as an uncounted parameter.
	fn one_of_each_with_params() -> Vec<(LogicalOp, usize)> {
		let leaf = || Box::new(source("t"));
		let p = || expr("$p");
		let fields_with_param =
			|| LogicalFields::Select(vec![single(p(), Some("a")), LogicalField::All]);
		let idiom_with_param = || expr("a[$p]");
		// The grammar admits no expression inside an ORDER BY idiom, so the
		// ordering is built directly; the type can still hold one.
		let ordering_with_param = || {
			LogicalOrdering::Order(vec![LogicalOrder {
				value: idiom_with_param(),
				collate: false,
				numeric: false,
				direction: true,
			}])
		};
		vec![
			(
				LogicalOp::Source(LogicalSource {
					source: SourceRef::Dynamic(expr("$p + 1")),
					schema: None,
					with: None,
					for_update: false,
				}),
				1,
			),
			(
				LogicalOp::Source(LogicalSource {
					source: SourceRef::classify(expr("t:[$p]")),
					schema: None,
					with: None,
					for_update: false,
				}),
				1,
			),
			(
				LogicalOp::Values(LogicalValues {
					expr: expr("[$p, $p]"),
					for_update: false,
					written: false,
				}),
				2,
			),
			(
				LogicalOp::Filter(LogicalFilter {
					input: leaf(),
					predicate: expr("a > $p"),
				}),
				1,
			),
			(
				LogicalOp::PermissionFilter(LogicalPermissionFilter {
					input: leaf(),
					predicate: expr("owner = $p"),
					auth: None,
					guard: GuardKind::Read,
				}),
				1,
			),
			(
				LogicalOp::Join(LogicalJoin::new(
					source("a"),
					source("b"),
					JoinKind::Left,
					vec![(p(), p())],
					Some(p()),
					RelationAlias::new(0),
					EvalFrame::caller(),
				)),
				3,
			),
			// A layer's columns are scalars of the node like any other.
			(
				LogicalOp::Project(LogicalProject {
					input: leaf(),
					fields: LogicalFields::Select(vec![
						LogicalField::All,
						LogicalField::column("c", p(), Some(Kind::Int), EvalFrame::computed(None)),
					]),
					omit: vec![],
				}),
				1,
			),
			(
				LogicalOp::Split(LogicalSplit {
					input: leaf(),
					exprs: vec![idiom_with_param()],
				}),
				1,
			),
			(
				LogicalOp::Aggregate(LogicalAggregate {
					input: leaf(),
					group: vec![idiom_with_param()],
					fields: fields_with_param(),
				}),
				2,
			),
			(
				LogicalOp::Sort(LogicalSort {
					input: leaf(),
					ordering: ordering_with_param(),
					tempfiles: false,
				}),
				1,
			),
			(
				LogicalOp::Limit(LogicalLimit {
					input: leaf(),
					limit: Some(p()),
					start: Some(p()),
				}),
				2,
			),
			(
				LogicalOp::Project(LogicalProject {
					input: leaf(),
					fields: fields_with_param(),
					omit: vec![p()],
				}),
				2,
			),
			(
				LogicalOp::Fetch(LogicalFetch {
					input: leaf(),
					fetches: vec![p()],
				}),
				1,
			),
			(
				LogicalOp::Timeout(LogicalTimeout {
					input: leaf(),
					timeout: p(),
				}),
				1,
			),
			(
				LogicalOp::VersionScope(LogicalVersionScope {
					input: leaf(),
					version: p(),
				}),
				1,
			),
			(
				LogicalOp::Eval(LogicalEval {
					expr: expr("$p + $p"),
				}),
				2,
			),
			(
				LogicalOp::Return(LogicalReturn {
					what: leaf(),
					fetch: vec![p()],
				}),
				1,
			),
			(
				LogicalOp::Foreach(LogicalForeach {
					param: Param::from("x".to_string()),
					range: p(),
					body: leaf(),
				}),
				1,
			),
			(
				LogicalOp::Info(Box::new(LogicalInfo::Index {
					index: p(),
					table: p(),
					structure: false,
				})),
				2,
			),
			(
				LogicalOp::Sleep(LogicalSleep {
					duration: p(),
				}),
				1,
			),
		]
	}

	#[test]
	fn visit_scalars_reaches_every_expression_slot() {
		for (op, expected) in one_of_each_with_params() {
			assert_eq!(
				count_params(&op),
				expected,
				"{} should expose {expected} params",
				op.name()
			);
		}
	}

	#[test]
	fn visit_scalars_mut_rewrites_the_same_slots() {
		for (mut op, _) in one_of_each_with_params() {
			fold_params(&mut op);
			assert_eq!(count_params(&op), 0, "{} should have no params left: {op}", op.name());
		}
	}

	#[test]
	fn visit_scalars_does_not_descend_into_inputs() {
		let op = LogicalOp::Only(LogicalOnly {
			input: Box::new(LogicalOp::Filter(LogicalFilter {
				input: Box::new(source("t")),
				predicate: expr("a > $p"),
			})),
		});
		assert_eq!(count_params(&op), 0);
	}

	#[test]
	fn a_scalar_subquery_is_walked_by_the_plan_not_the_scalar() {
		// The scalar walk stops at the subquery; the plan inside is reached
		// through the variant with the operator traversal.
		let predicate = expr("a IN (SELECT VALUE b FROM u WHERE c = $p)");
		assert!(!predicate.any(|e| matches!(e, LogicalExpr::Param(_))));
		let mut nested = 0;
		let _ = predicate.visit(&mut |e| {
			if let LogicalExpr::Subquery(op) = e {
				let Ok(()) = op.visit_scalars_deep(&mut |s| {
					let _ = s.visit(&mut |x| {
						if matches!(x, LogicalExpr::Param(_)) {
							nested += 1;
						}
						ControlFlow::<()>::Continue(())
					});
					Ok::<(), Infallible>(())
				});
			}
			ControlFlow::<()>::Continue(())
		});
		assert_eq!(nested, 1);
	}

	#[test]
	fn visit_ops_deep_reaches_every_plan_of_the_statement() {
		// One subquery per position a plan can occupy, each over a table
		// named for where it sits, so what the walk reached is legible from
		// the names alone.
		let plan = lower_sql(
			"SELECT
				(SELECT * FROM in_projection) AS p,
				|$x| (SELECT * FROM in_closure_body) AS f
			 FROM at_root
			 WHERE k IN (
				SELECT VALUE id FROM in_predicate
				WHERE j IN (SELECT VALUE id FROM two_deep)
			 )",
		);

		// Written out, not collected by the walk under test. `at_root` is the
		// statement's own source; the other three sit one, one and two
		// subqueries deep. The sequence is the documented pre-order — a
		// node's own scalars before its inputs — so a projection's subquery
		// comes first and the statement's own source comes last, and the
		// deeper of two nested predicates is reached before the plan holding
		// it. `in_closure_body` is absent: a closure body is bound and
		// compiled where the closure is invoked, so the plan inside it is not
		// a plan of this statement.
		assert_eq!(tables_reached(&plan), ["in_projection", "two_deep", "in_predicate", "at_root"]);

		// `Break` stops the walk where the callback says so, rather than
		// running to the end and reporting.
		assert_eq!(
			tables_until_first(&plan),
			(ControlFlow::Break("in_projection"), vec!["in_projection"])
		);

		// Two plans in one node's scalars: the `Break` in the first leaves the
		// second unwalked, rather than being recorded and walked past.
		let siblings = lower_sql(
			"SELECT (SELECT * FROM sib_first) AS a, (SELECT * FROM sib_second) AS b FROM at_root",
		);
		assert_eq!(tables_reached(&siblings), ["sib_first", "sib_second", "at_root"]);
		assert_eq!(
			tables_until_first(&siblings),
			(ControlFlow::Break("sib_first"), vec!["sib_first"])
		);

		// A `MATCH` plan is a leaf to the operator walk — its stages are a
		// binding table rather than rows — and a plan one of its scalars
		// holds is still reached.
		let matched =
			match_op(LogicalExpr::Subquery(Box::new(source("in_match_scalar"))), Vec::new());
		assert!(matched.children().is_empty());
		assert_eq!(tables_reached(&matched), ["in_match_scalar"]);
	}

	#[test]
	fn a_user_filter_and_a_permission_filter_are_distinct_variants() {
		// The property the pushdown relies on: a pass matching `Filter` to
		// move predicates does not see the table permission at all.
		let view = resolved_table_view();
		let mut user = 0;
		let mut permission = 0;
		let _ = view.visit(&mut |node| {
			if matches!(node, LogicalOp::Filter(_)) {
				user += 1;
			}
			if matches!(node, LogicalOp::PermissionFilter(_)) {
				permission += 1;
			}
			ControlFlow::<()>::Continue(())
		});
		assert_eq!((user, permission), (1, 1));
	}

	#[test]
	fn every_statement_operator_renders_in_the_tree_style() {
		let plan = LogicalOp::Sequence(LogicalSequence {
			stmts: vec![
				LogicalOp::Let(LogicalLet {
					name: "x".to_string(),
					kind: Some(Kind::Int),
					value: Box::new(LogicalOp::Eval(LogicalEval {
						expr: expr("1 + 1"),
					})),
				}),
				LogicalOp::Foreach(LogicalForeach {
					param: Param::from("i".to_string()),
					range: expr("[1, 2]"),
					body: Box::new(LogicalOp::Sequence(LogicalSequence {
						stmts: vec![
							LogicalOp::Sleep(LogicalSleep {
								duration: expr("1ms"),
							}),
							LogicalOp::Eval(LogicalEval {
								expr: expr("IF $i > 1 THEN BREAK END"),
							}),
						],
					})),
				}),
				LogicalOp::Info(Box::new(LogicalInfo::Table {
					table: expr("t"),
					structure: true,
					version: Some(expr("d'2024-01-01T00:00:00Z'")),
				})),
				LogicalOp::Info(Box::new(LogicalInfo::Index {
					index: expr("i"),
					table: expr("t"),
					structure: false,
				})),
				ddl_op(),
				LogicalOp::Return(LogicalReturn {
					what: Box::new(LogicalOp::Eval(LogicalEval {
						expr: expr("$x"),
					})),
					fetch: vec![expr("f")],
				}),
			],
		});
		let expected = "\
Sequence
  Let[$x : int]
    Eval[1 + 1]
  Foreach[$i IN [1, 2]]
    Sequence
      Sleep[1ms]
      Eval[IF $i > 1 THEN BREAK END]
  Info[INFO FOR TABLE t VERSION d'2024-01-01T00:00:00Z' STRUCTURE]
  Info[INFO FOR INDEX i ON t]
  Ddl[REBUILD]
  Return[FETCH f]
    Eval[$x]";
		assert_eq!(plan.to_string(), expected, "\nrendered:\n{plan}\n");
	}

	#[test]
	fn a_scalar_subquery_deep_in_the_tree_is_indented_beneath_its_operator() {
		// The continuation lines of a nested plan are re-indented by the
		// operator holding the scalar, so the nesting reads at any depth.
		let plan = LogicalOp::Project(LogicalProject {
			input: Box::new(LogicalOp::Filter(LogicalFilter {
				input: Box::new(source("t")),
				predicate: expr("a IN (SELECT VALUE b FROM u WHERE c > 1)"),
			})),
			fields: LogicalFields::Select(vec![single(expr("a"), None)]),
			omit: vec![],
		});
		let expected = "\
Project[a]
  Filter[a INSIDE (
    Project[VALUE b]
      Filter[c > 1]
        Source[u]
  )]
    Source[t]";
		assert_eq!(plan.to_string(), expected, "\nrendered:\n{plan}\n");
		// The same under a statement operator, with a nested plan that is
		// itself a statement.
		let plan = LogicalOp::Let(LogicalLet {
			name: "x".to_string(),
			kind: None,
			value: Box::new(LogicalOp::Eval(LogicalEval {
				expr: expr("[1, (SELECT * FROM t), { RETURN 2; }]"),
			})),
		});
		let expected = "\
Let[$x]
  Eval[[1, (
    Project[*]
      Source[t]
  ), (
    Sequence
      Return
        Eval[2]
  )]]";
		assert_eq!(plan.to_string(), expected, "\nrendered:\n{plan}\n");
	}
}
