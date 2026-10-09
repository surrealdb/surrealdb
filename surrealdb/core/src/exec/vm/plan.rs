//! The physical plan: an inert description of how a statement runs.
//!
//! A plan holds no transaction, cursor, future or session value, so it can be
//! compared, printed and shared across callers. Scalars stay in their logical
//! form and are compiled only when the plan is lowered. Every catalog object
//! the plan was made from is listed in [`PhysicalPlan::dependencies`] with the
//! version that was read, which is what a cached plan is keyed by.

use surrealdb_strand::TableName;
use uuid::Uuid;

use crate::exec::field_path::FieldPath;
use crate::expr::logical::LogicalExpr;
use crate::kvs::Direction;

/// The first `ORDER BY` key of a top-k sort, which the table scan beneath it
/// reads from stored bytes to reject a row that cannot enter the top k.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct TopKKey {
	/// The key's field path as pre-encoded wire segments.
	pub(crate) path: std::sync::Arc<[crate::exec::object_extract::PathSegment]>,
	pub(crate) direction: crate::exec::operators::SortDirection,
	/// Whether the order has exactly one key: a tie with the threshold then
	/// rejects too.
	pub(crate) single_key: bool,
	/// The bound on how deep the walk over the stored bytes descends.
	pub(crate) depth_limit: u32,
}

/// A plan and the catalog state it was made from.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PhysicalPlan {
	pub(crate) root: Plan,
	pub(crate) dependencies: Vec<Dependency>,
	/// The statement's `TIMEOUT`, `VERSION` and `ONLY`, applied around the
	/// program by the operators the operator tree applies them with.
	pub(crate) tail: Tail,
	/// The statements a [`Control`] plan runs, as written, each with the names
	/// bound around it, by [`Control::Statement::source`]: what the lowering
	/// reads a statement's footprint and its bound names from. The plan itself
	/// holds none.
	pub(crate) sources: Vec<(crate::expr::logical::LogicalOp, Vec<String>)>,
	/// The statements written inside the plan's expressions, each mapped with
	/// the plan, with how it runs. An expression holding a statement not
	/// listed here runs it as a program of its own.
	pub(crate) nested: Vec<std::sync::Arc<Nested>>,
	/// The `MATCHES` clauses and `KNN` operator the statement's own `WHERE`
	/// declares, which the index functions its expressions call report
	/// against.
	pub(crate) scope: IndexScope,
	/// What the mapping read of the catalog for the plan's lowering
	/// ([`super::plan_catalog`]); `None` for a plan mapped inside another's
	/// mapping, which is lowered with its enclosing plan's.
	pub(crate) catalog: Option<std::sync::Arc<super::plan_catalog::PlanCatalog>>,
}

impl PhysicalPlan {
	/// The error planning the statement raises, where the operator tree
	/// refuses it as it plans it ([`Plan::Refused`]).
	pub(crate) fn refusal(&self) -> Option<&Refusal> {
		match &self.root {
			Plan::Refused {
				refusal,
			} => Some(refusal),
			_ => None,
		}
	}

	/// Whether the operator tree refuses the statement as it plans it.
	pub(crate) fn refused(&self) -> bool {
		self.refusal().is_some()
	}
}

/// What an index function (`search::score`, `search::highlight`,
/// `search::offsets`, `vector::distance::knn`) written in a statement
/// resolves against: the `MATCHES` clauses of the statement's own `WHERE`,
/// by reference, and whether that `WHERE` holds a `KNN` operator. A statement
/// that declares neither has no scope of its own, and an index function it
/// holds is not covered.
#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct IndexScope {
	/// Whether the statement is a `SELECT` whose predicates this scope was
	/// collected from. Only such a statement has a scope to answer for; the
	/// operator an expression of any other statement holds is not covered
	/// when it needs one.
	pub(crate) select: bool,
	/// The table the `MATCHES` clauses are read against.
	pub(crate) table: Option<TableName>,
	/// For each match reference, the field read and the query searched, in
	/// the order the clauses are written.
	pub(crate) matches: Vec<(crate::idx::ft::MatchRef, MatchedField)>,
	/// Whether the `WHERE` holds a `KNN` operator.
	pub(crate) knn: bool,
	/// The `MATCHES` expressions the `WHERE` registers: those reachable from
	/// it through binary nodes and negations. One written elsewhere has no
	/// clause to answer it.
	pub(crate) registered: std::collections::HashSet<LogicalExpr>,
	/// The tables whose rows an index answers a `MATCHES` for.
	pub(crate) executor_tables: crate::exec::physical_expr::ExecutorTables,
}

/// The field and query of one `MATCHES` clause.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct MatchedField {
	pub(crate) field: LogicalExpr,
	pub(crate) query: String,
	/// The parameter the query is read from when the clause runs, where its
	/// value is bound around the statement — by a `FOR`, a closure or a
	/// function — rather than known when the statement is planned. `query`
	/// is empty then.
	pub(crate) query_param: Option<crate::expr::Param>,
}

impl IndexScope {
	/// The clause match reference `rf` names, or the only clause the scope
	/// holds when none carries that reference.
	pub(crate) fn resolve(&self, rf: crate::idx::ft::MatchRef) -> Option<&MatchedField> {
		let only = match self.matches.as_slice() {
			[(_, only)] => Some(only),
			_ => None,
		};
		self.matches.iter().rev().find(|(held, _)| *held == rf).map(|(_, field)| field).or(only)
	}
}

/// A statement written inside an expression: its plan, what the plan's
/// strategy is chosen from, and the strategy.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Nested {
	/// The statement as the expression holding it writes it.
	pub(crate) op: crate::expr::logical::LogicalOp,
	pub(crate) plan: PhysicalPlan,
	pub(crate) facts: NestedFacts,
	/// The equality the statement's rows are joined to the row it is
	/// evaluated for by, where that is the only way it reads the row.
	pub(crate) join: Option<JoinKey>,
	pub(crate) strategy: Strategy,
	/// Whether the statement's only use is as the right operand of an
	/// `INSIDE` or `NOT INSIDE`, so a kept value is held as a membership
	/// index. It takes effect with [`Strategy::Once`] alone.
	pub(crate) membership: bool,
}

/// A correlated read whose only reading of the row it is evaluated for is
/// the first conjunct of its innermost predicate, `field = outer` or
/// `outer = field`: `field` a field of the rows beneath that predicate, and
/// `outer` a path into `$parent`. Beneath the predicate, the statement reads
/// nothing of the row.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct JoinKey {
	pub(crate) field: surrealdb_strand::Strand,
	pub(crate) outer: LogicalExpr,
}

/// What a nested statement's strategy is chosen from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct NestedFacts {
	/// Whether the statement reads the row it is evaluated for: it inherits
	/// the row as its document, or names `$this`, `$self`, `$parent`,
	/// `$value` or a closure parameter of the enclosing statement.
	pub(crate) correlated: bool,
	/// Whether evaluating it once answers as evaluating it for every row
	/// does: every scalar it holds, its permissions and computed fields
	/// included, is deterministic and may be evaluated fewer times than
	/// written, and it writes nothing.
	pub(crate) repeatable: bool,
	/// Whether it is a read that yields rows, not a write, a block or a
	/// schema statement.
	pub(crate) reads_rows: bool,
	/// Whether the statement it is written in may write, so a read of it
	/// may see what an earlier row wrote.
	pub(crate) enclosing_writes: bool,
}

/// How a nested statement runs, chosen by the optimizer from its facts.
///
/// An index nested loop — a per-row run over an index on the correlated key —
/// needs an index access whose key is evaluated per row, which no plan holds:
/// an index access is fixed when the statement is planned, so a correlated
/// read applied per row reads its table whole for every row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Strategy {
	/// Run per evaluation, with the row it is evaluated for bound as its
	/// `$parent` and `$this`: the only strategy for a statement that writes
	/// or reads the row.
	Apply,
	/// Run at the first evaluation that reaches it, and its value kept for
	/// every later one until the statement it is written in runs again.
	Once,
	/// Applied per evaluation over a [`Plan::Probe`]: the rows beneath its
	/// join predicate read once, at the first evaluation that reaches them,
	/// and kept, keyed by the join field, until the statement it is written
	/// in runs again; each evaluation runs the rest of the statement over
	/// the kept rows its key may equal.
	Hash,
}

/// A block's control flow, over physical plans of the statements it runs.
///
/// Every statement of the tree is bound and mapped with the program, with the
/// names the blocks around it bind held as bound around it: the loop
/// parameter and each earlier `LET`, which the statement reads when it runs.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Control {
	/// Each statement in turn: `LET` binding its name for the statements
	/// after it; the value is the last statement's.
	Seq(Vec<Control>),
	/// `LET name = value`, the value coerced to `kind` when it declares one.
	Let {
		name: String,
		kind: Option<crate::expr::Kind>,
		value: Box<Control>,
	},
	/// `FOR param IN range`: `range` evaluated once, then `body` for each of
	/// its elements with `param` bound to it.
	For {
		param: crate::expr::Param,
		range: Box<Control>,
		/// The body the first element runs, where the loop's schema
		/// statements change the catalog on the first iteration and do nothing
		/// after it: `body` is then planned against the catalog the first
		/// iteration leaves, its schema statements folded away.
		first: Option<Box<Control>>,
		body: Box<Control>,
	},
	/// `RETURN value`.
	Return {
		value: Box<Control>,
	},
	/// `SLEEP` for a written duration.
	Sleep {
		duration: crate::val::Duration,
	},
	/// An expression holding no statement, evaluated once where the block
	/// reaches it. The block's expressions that hold one are expression
	/// statements ([`Control::Statement`]).
	Value(LogicalExpr),
	/// `IF` in a statement's place: each condition evaluated in turn, and the
	/// first branch whose condition is truthy run, or `otherwise`; its value
	/// is the branch's, `NONE` when no branch runs. A branch holding
	/// statements runs them as control flow of the block, in a frame of its
	/// own.
	If {
		branches: Vec<(Control, Control)>,
		otherwise: Option<Box<Control>>,
	},
	/// `THROW value`: the value evaluated and raised as the statement's error.
	Throw {
		value: Box<Control>,
	},
	/// A statement, bound and mapped when the query arrives and lowered with
	/// the program as a group of its own. `source` is its entry in
	/// [`PhysicalPlan::sources`]; `changes_catalog` is whether it is a schema
	/// statement.
	Statement {
		plan: Box<PhysicalPlan>,
		source: u16,
		changes_catalog: bool,
		/// The catalog the statement was planned against when the block's
		/// schema statements before it changed it: what their speculative
		/// runs wrote (`vm::speculate`). `None` for the catalog as the block
		/// found it.
		planned_over: Option<std::sync::Arc<crate::kvs::speculation::Overlay>>,
		/// For a schema statement whose effect the statements after it were
		/// planned knowing: its speculative run, whose reserved ids its run
		/// takes.
		modelled: Option<std::sync::Arc<super::speculate::Speculated>>,
		/// For `EXPLAIN ANALYZE` of the statement `plan` maps: what it
		/// renders of the statement's run, whose value is the rendering.
		analysis: Option<Box<super::program::Analysis>>,
		/// Whether the statement runs after a change to the catalog the block
		/// was not planned knowing, and loads each catalog object it names
		/// when it runs ([`crate::exec::planner::Planner::late_binding`]).
		late_bound: bool,
		/// How the statement reaches what code it runs may redefine while it
		/// runs ([`crate::exec::planner::CatalogWithin`]).
		within: crate::exec::planner::CatalogWithin,
		/// Whether `plan` is a refusal the statement raises when the block
		/// reaches it, rather than when the block is planned.
		raises_when_reached: bool,
	},
	/// `BREAK`: leave the innermost loop; where no loop of the program
	/// encloses it, the signal leaves the program for its caller.
	Break,
	/// `CONTINUE`: move to the innermost loop's next element, leaving the
	/// program as [`Control::Break`] does where no loop encloses it.
	Continue,
}

/// An error the operator tree raises while it plans a statement, before any
/// of the statement runs: the statement is answered with the error rather
/// than run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Refusal {
	/// A `LET` of a name no statement may bind.
	ProtectedParam {
		name: String,
	},
	/// A statement the query's shape makes invalid, with the message the
	/// operator tree raises for it.
	Query {
		message: String,
	},
	/// A statement written where no statement of its kind may stand, with the
	/// message the operator tree raises for it.
	InvalidStatement {
		message: String,
	},
	/// An `OMIT` or `FETCH` expression that names no path, as written in
	/// `value`.
	InvalidFetch {
		value: String,
	},
	/// A `CONTEXT` clause that writes.
	AccessContextNotReadOnly,
	/// A `CONTEXT` clause on an access method that authenticates no record.
	AccessContextRequiresRecord,
	/// A `MATCHES`, written as `exp`, that no clause of its statement's `WHERE`
	/// registers and that a full table source would evaluate against no index.
	NoIndexForMatch {
		exp: String,
	},
	/// A recursion bound outside `1..=idiom_recursion_limit`.
	InvalidBound {
		found: String,
		expected: String,
	},
	/// A recursion that carries an instruction beside a trailing body that
	/// re-enters through `.@`.
	RecursionInstructionPlanConflict,
	/// An expression nested past the computation depth the planner allows.
	ComputationDepthExceeded,
	/// A `DEFINE ACCESS` naming the redaction placeholder as a key of the
	/// access `ac`.
	AccessRedactedKey {
		ac: String,
	},
	/// A statement that reads its database's catalog, run by a session that
	/// selects no namespace.
	NamespaceEmpty,
	/// A statement that reads its database's catalog, run by a session that
	/// selects a namespace but no database.
	DatabaseEmpty,
}

impl Refusal {
	/// The error the refusal raises.
	pub(crate) fn error(&self) -> crate::err::Error {
		match self {
			Self::ProtectedParam {
				name,
			} => crate::err::Error::Exec(crate::exec::Error::InvalidParam {
				name: name.clone(),
			}),
			Self::Query {
				message,
			} => crate::err::Error::Exec(crate::exec::Error::Query {
				message: message.clone(),
			}),
			Self::InvalidStatement {
				message,
			} => crate::err::Error::Exec(crate::exec::Error::InvalidStatement(message.clone())),
			Self::InvalidFetch {
				value,
			} => crate::err::Error::Exec(crate::exec::Error::InvalidFetch {
				value: value.clone(),
			}),
			Self::AccessContextNotReadOnly => {
				crate::err::Error::Exec(crate::exec::Error::AccessContextNotReadOnly)
			}
			Self::AccessContextRequiresRecord => {
				crate::err::Error::Exec(crate::exec::Error::AccessContextRequiresRecord)
			}
			Self::NoIndexForMatch {
				exp,
			} => crate::idx::Error::NoIndexFoundForMatch {
				exp: exp.clone(),
			}
			.into(),
			Self::InvalidBound {
				found,
				expected,
			} => crate::err::Error::Exec(crate::exec::Error::InvalidBound {
				found: found.clone(),
				expected: expected.clone(),
			}),
			Self::RecursionInstructionPlanConflict => {
				crate::err::Error::Exec(crate::exec::Error::RecursionInstructionPlanConflict)
			}
			Self::ComputationDepthExceeded => {
				crate::err::Error::Exec(crate::exec::Error::ComputationDepthExceeded)
			}
			Self::AccessRedactedKey {
				ac,
			} => crate::err::Error::Exec(crate::exec::Error::AccessRedactedKey {
				ac: ac.clone(),
			}),
			Self::NamespaceEmpty => crate::err::Error::Exec(crate::exec::Error::NsEmpty),
			Self::DatabaseEmpty => crate::err::Error::Exec(crate::exec::Error::DbEmpty),
		}
	}

	/// The refusal that raises `error`, where it is one a refusal carries.
	pub(crate) fn of(error: &crate::err::Error) -> Option<Self> {
		match error {
			crate::err::Error::Exec(crate::exec::Error::Query {
				message,
			}) => Some(Self::Query {
				message: message.clone(),
			}),
			crate::err::Error::Exec(crate::exec::Error::InvalidParam {
				name,
			}) => Some(Self::ProtectedParam {
				name: name.clone(),
			}),
			crate::err::Error::Exec(crate::exec::Error::InvalidStatement(message)) => {
				Some(Self::InvalidStatement {
					message: message.clone(),
				})
			}
			crate::err::Error::Exec(crate::exec::Error::InvalidFetch {
				value,
			}) => Some(Self::InvalidFetch {
				value: value.clone(),
			}),
			crate::err::Error::Exec(crate::exec::Error::InvalidBound {
				found,
				expected,
			}) => Some(Self::InvalidBound {
				found: found.clone(),
				expected: expected.clone(),
			}),
			crate::err::Error::Exec(crate::exec::Error::RecursionInstructionPlanConflict) => {
				Some(Self::RecursionInstructionPlanConflict)
			}
			crate::err::Error::Exec(crate::exec::Error::ComputationDepthExceeded) => {
				Some(Self::ComputationDepthExceeded)
			}
			crate::err::Error::Exec(crate::exec::Error::AccessContextNotReadOnly) => {
				Some(Self::AccessContextNotReadOnly)
			}
			crate::err::Error::Exec(crate::exec::Error::AccessContextRequiresRecord) => {
				Some(Self::AccessContextRequiresRecord)
			}
			crate::err::Error::Exec(crate::exec::Error::AccessRedactedKey {
				ac,
			}) => Some(Self::AccessRedactedKey {
				ac: ac.clone(),
			}),
			crate::err::Error::Exec(crate::exec::Error::NsEmpty) => Some(Self::NamespaceEmpty),
			crate::err::Error::Exec(crate::exec::Error::DbEmpty) => Some(Self::DatabaseEmpty),
			crate::err::Error::Idx(crate::idx::Error::NoIndexFoundForMatch {
				exp,
			}) => Some(Self::NoIndexForMatch {
				exp: exp.clone(),
			}),
			_ => None,
		}
	}
}

/// The clauses that wrap a `SELECT`'s rows rather than shape them.
#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct Tail {
	pub(crate) timeout: Option<LogicalExpr>,
	/// Every source beneath reads the state as of this stamp.
	pub(crate) version: Option<LogicalExpr>,
	pub(crate) only: bool,
	/// `TEMPFILES`: the statement's sort holds on disk what outgrows the rows
	/// it may keep in memory.
	pub(crate) tempfiles: bool,
}

/// One scan of an index union.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct IndexBranch {
	pub(crate) index: crate::exec::index::access_path::IndexRef,
	pub(crate) read: BranchRead,
	/// The direction a B-tree branch scans in; a full-text branch lists its
	/// hits in the order the index gives them.
	pub(crate) direction: Direction,
}

/// What a branch of an index union reads.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum BranchRead {
	/// The entries of a B-tree index under an access.
	BTree(super::plain::ValueData<crate::exec::index::access_path::BTreeAccess>),
	/// The records a full-text index lists for a query.
	FullText {
		query: String,
		operator: crate::expr::operator::MatchesOperator,
	},
}

/// A catalog object a plan was made from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Dependency {
	/// A table's definition, fields and indexes, at the catalog version read.
	/// `None` is a table whose catalog has never been stamped.
	Table {
		table: TableName,
		version: Option<Uuid>,
	},
}

/// One node of a physical plan.
///
/// Every node delivers its rows in a defined order: a table scan in record-key
/// order along `direction`; a sort in key order, ties in arrival order; every
/// other node in its input's order.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Plan {
	/// Every record of `table`, as stored, in key order along `direction`.
	/// Rows are the records' objects.
	///
	/// `schema` is what the catalog declares for the rows, read by the
	/// lowering to choose typed instructions. A declared kind is a hint, not
	/// a guarantee: a field's kind is enforced on write alone, so rows
	/// written before a `DEFINE FIELD` or `DEFINE FIELD OVERWRITE` hold what
	/// they held. The lowering may use it only to choose an instruction whose
	/// fallback answers every value the field may hold. A declared kind never
	/// removes a check, never licenses moving or merging a fallible
	/// expression, and never selects an unguarded path
	/// ([`FieldSchema::checked`](crate::expr::logical::FieldSchema)). Merging
	/// two evaluations of one pure expression where the first dominates the
	/// second ([`crate::expr::logical::dag`]) needs no kind at all.
	TableScan {
		table: TableName,
		direction: Direction,
		schema: Option<std::sync::Arc<crate::expr::logical::Schema>>,
		topk: Option<TopKKey>,
	},
	/// The records of `table` that B-tree index `index` lists under `access`,
	/// in the index's key order along `direction`: a superset of the rows
	/// the predicate above admits, so every row is still judged there.
	/// `ordered_by` names the record fields that order satisfies, in the
	/// statement's `ORDER BY` directions, when it satisfies the statement's
	/// ordering; a sort over exactly those fields is removed by a rule.
	IndexScan {
		table: TableName,
		index: crate::exec::index::access_path::IndexRef,
		access: crate::exec::index::access_path::BTreeAccess,
		direction: Direction,
		ordered_by: Option<Vec<FieldPath>>,
		schema: Option<std::sync::Arc<crate::expr::logical::Schema>>,
		/// The top-level fields read from each record, when nothing above the
		/// scan reads more of a row than the fields its clauses name; `None`
		/// reads whole records.
		decode: Option<crate::exec::operators::scan::common::DecodeFields>,
	},
	/// The records of `table` the B-tree scans `branches` list, each record
	/// once: branch by branch in branch order, or merged by `merge`'s key
	/// when the union was chosen to deliver the statement's ordering.
	/// `ordered_by` is as for [`Plan::IndexScan`].
	IndexUnion {
		table: TableName,
		branches: Vec<IndexBranch>,
		merge: Option<crate::exec::operators::scan::MergeMode>,
		ordered_by: Option<Vec<FieldPath>>,
		schema: Option<std::sync::Arc<crate::expr::logical::Schema>>,
		/// As for [`Plan::IndexScan`], for every branch.
		decode: Option<crate::exec::operators::scan::common::DecodeFields>,
	},
	/// The records of `table` whose doc-IDs survive fused bitmap plan
	/// `root`, in doc-ID order: a superset of the rows the predicate above
	/// admits. `fallback` is the index union an OR fusion replaced, which
	/// answers in its place when a branch overflows its budget.
	Bitmap {
		table: TableName,
		root: crate::exec::index::access_path::BitmapPlan,
		fallback: Option<Box<Plan>>,
		schema: Option<std::sync::Arc<crate::expr::logical::Schema>>,
		/// As for [`Plan::IndexScan`].
		decode: Option<crate::exec::operators::scan::common::DecodeFields>,
	},
	/// The records of `table` that full-text index `index` lists for `query`
	/// using `operator`.
	FullTextScan {
		table: TableName,
		index: crate::exec::index::access_path::IndexRef,
		query: String,
		operator: crate::expr::operator::MatchesOperator,
		schema: Option<std::sync::Arc<crate::expr::logical::Schema>>,
		/// As for [`Plan::IndexScan`].
		decode: Option<crate::exec::operators::scan::common::DecodeFields>,
	},
	/// The records of `table` that KNN index `index` lists for nearest
	/// neighbors of `vector`.
	KnnScan {
		table: TableName,
		index: crate::exec::index::access_path::IndexRef,
		vector: Vec<crate::val::Number>,
		k: u32,
		ef: u32,
		/// What the search gates each candidate on, where the statement's
		/// conditions or the table's permission narrow the candidates.
		filter: Option<KnnFilter>,
		/// Whether nothing above the scan reads more of a row than its `id`
		/// and every field of the table reads as stored, so a read at the
		/// current version builds its rows from the ids the search returned
		/// and reads no record.
		ids_only: bool,
		schema: Option<std::sync::Arc<crate::expr::logical::Schema>>,
	},
	/// The `k` rows of `input` whose vector at `field` is nearest to `vector`
	/// by `distance`, nearest first, among the rows that carry a vector the
	/// distance applies to: a brute-force search over every row `input`
	/// yields.
	KnnTopK {
		input: Box<Plan>,
		field: LogicalExpr,
		vector: KnnTarget,
		k: u32,
		distance: crate::catalog::Distance,
	},
	/// One row holding the number of records `count` names that the caller
	/// may select, answered as the count fast path answers it: from key
	/// counts or an index where the decision chose one, judging each record
	/// under a conditional permission.
	Count {
		count: crate::exec::planner::CountDecision,
		/// What the counted table's `SELECT` permission does to a caller
		/// whose reads are checked.
		select: CountSelect,
		/// The `COUNT` index whose deltas answer the count, where one does.
		deltas: Option<crate::catalog::IndexId>,
		/// The statement as the ordinary pipeline answers it, for a count
		/// that is not answered from keys or an index: a conditional
		/// permission is judged per record, and a predicate no index covers
		/// is evaluated per record.
		ordinary: Option<Box<PhysicalPlan>>,
	},
	/// The records `record` names: one record for a record id, the records
	/// of its range in key order for a range, none for a record that does not
	/// exist. `record` is evaluated once, when the scan opens.
	///
	/// Under `for_update` the record is read locked, registering its key for
	/// commit-time conflict detection whether or not it exists; a range or a
	/// `VERSION` is refused when the scan opens.
	RecordScan {
		table: TableName,
		record: LogicalExpr,
		schema: Option<std::sync::Arc<crate::expr::logical::Schema>>,
		for_update: bool,
	},
	/// The rows of `input`, a read of `table`'s storage that no node of the
	/// plan enforces the table's permission, computed fields and field
	/// permissions on, with each row put through them as the table's scan puts
	/// the rows it reads through them: a row the table's permission refuses is
	/// no row, and the rest carry the computed fields and the field
	/// permissions the caller is owed.
	Enforced {
		input: Box<Plan>,
		table: TableName,
	},
	/// The rows a value in `FROM` yields: `expr`, evaluated once when the
	/// source opens, read as `read` says. A row may be of any kind, not only
	/// an object.
	Values {
		expr: LogicalExpr,
		read: ValuesRead,
	},
	/// The rows of a statement in `FROM`, as it yields them, run once when
	/// the source opens.
	Subquery {
		plan: Box<PhysicalPlan>,
	},
	/// The rows of `build` whose `field` may equal `outer`, in `build`'s
	/// order: a superset of the rows `field = outer` admits, which the
	/// predicate above still judges. `build` reads nothing of the row a
	/// nested statement is evaluated for; it is run at the first evaluation
	/// that opens the probe, and its rows are kept until the statement the
	/// nested statement is written in runs again. `outer` is evaluated once
	/// per evaluation, and only where `build` has a row. A zero limit opens
	/// no probe, as it opens no scan.
	Probe {
		build: Box<Plan>,
		field: surrealdb_strand::Strand,
		outer: LogicalExpr,
	},
	/// `FROM a, b`: the rows of each input, one input after another in written
	/// order. Each input is a scan, a value or a statement.
	Union {
		inputs: Vec<Plan>,
	},
	/// `left JOIN right ON keys`: each row of `left` in order, paired with the
	/// rows of `right` whose keys equal its own by `=`, in the order `right`
	/// yields them. `right` is read whole when the join opens; `left` streams.
	///
	/// A key pair is `(left, right)`: the left expression is evaluated against
	/// a `left` row, and the right against a `right` row seen through `alias`,
	/// as `{ alias: row }`. No pair at all pairs every row with every row. A
	/// pair of rows is matched when the keys are equal and `residual`, which
	/// reads the row paired (the `left` row with the `right` row under
	/// `alias`), is truthy. A paired row is the `left` row with the `right`
	/// row set under `alias`; a `left` row that is no object is emitted as it
	/// is. A [`JoinMatch::Left`] join emits a `left` row no `right` row
	/// matches once, with `NULL` under `alias`.
	Join {
		left: Box<Plan>,
		right: Box<Plan>,
		kind: JoinMatch,
		keys: Vec<(LogicalExpr, LogicalExpr)>,
		residual: Option<LogicalExpr>,
		alias: String,
	},
	/// The first of each set of equal rows of `input`, in `input`'s order:
	/// two rows are the same row when they compare equal as values.
	Distinct {
		input: Box<Plan>,
	},
	/// The rows of `input` its table's `PERMISSIONS FOR select` admits:
	/// `predicate` evaluated per row in the permission frame, under `auth`.
	/// Every row passes where permissions are not checked.
	PermissionFilter {
		input: Box<Plan>,
		predicate: LogicalExpr,
		auth: Option<surrealdb_iam::AuthLimit>,
		guard: crate::expr::logical::GuardKind,
		/// Whether `predicate` is enforced on the caller the plan is for and
		/// was offered to the access path of the source beneath. That path
		/// may then read at the definer's bound, so `EXPLAIN` prints none of
		/// the access paths beneath the node.
		hides_access_paths: bool,
		/// The `COMPUTED` fields of the table beneath that `predicate` reads,
		/// which it judges each row with derived onto a copy; `None` when it
		/// reads none, and the row is judged as it comes.
		computed: Option<ComputedReads>,
	},
	/// A projection layer over `input`: each row with the table's computed
	/// fields derived and its restricted fields cut to what the caller may
	/// see, as the projection-layer operator sets them. `input_schema` is the
	/// schema of the layer's input.
	Layer {
		input: Box<Plan>,
		fields: crate::expr::logical::LogicalFields,
		input_schema: crate::expr::logical::Schema,
	},
	/// The rows of `input` for which `predicate` is truthy, evaluated once
	/// per row in the caller's frame.
	Filter {
		input: Box<Plan>,
		predicate: LogicalExpr,
	},
	/// Each row of `input` with the records its `fetches` paths reach
	/// fetched in place, as `FETCH` fetches them: the statement's last step
	/// before its tail.
	Fetch {
		input: Box<Plan>,
		fetches: Vec<LogicalExpr>,
	},
	/// Each row of `input` split on `targets` in turn, as `SPLIT` splits it
	/// (`operators::split_row`): one row per element of the collection each
	/// target reads, in `input`'s order and then the collection's.
	Split {
		input: Box<Plan>,
		targets: Vec<LogicalExpr>,
	},
	/// Each row of `input`, a binding row, turned by `step` into the binding
	/// rows it yields, in `input`'s order and then the step's: the nodes of a
	/// GQL `MATCH` plan that expand, join, bind and deduplicate binding rows.
	/// A join's build side is the plan the step holds.
	Step {
		input: Box<Plan>,
		step: Box<super::binding::PlanStep>,
	},
	/// The rows of `input` ordered by `keys`, each key evaluated once per
	/// row. Stable. With `keep`, only the first `keep` rows of the order are
	/// ever delivered, which a consumer that pages must guarantee.
	Sort {
		input: Box<Plan>,
		keys: Vec<SortKeySpec>,
		keep: Option<usize>,
		/// Whether the sort publishes the threshold of its first key to the
		/// scan beneath it ([`TopKKey`]).
		pushdown: bool,
		random: bool,
	},
	/// `GROUP BY group` over `input`, one row per group in group-key order,
	/// of what `fields` names. With no `group`, `GROUP ALL`: one row, whether
	/// or not `input` yields any.
	Aggregate {
		input: Box<Plan>,
		group: Vec<LogicalExpr>,
		fields: crate::expr::logical::LogicalFields,
		/// The positions in `fields` of the columns that are neither a key nor
		/// an aggregate and are answered by the first value the group's rows
		/// give, where an unlisted such column collects every row's value.
		first_value: Vec<usize>,
	},
	/// `START` then `LIMIT` over `input`. Both are evaluated once per
	/// statement, `limit` first, before any row is read.
	Limit {
		input: Box<Plan>,
		limit: Option<LogicalExpr>,
		start: Option<LogicalExpr>,
	},
	/// What each row of `input` is turned into.
	Project {
		input: Box<Plan>,
		projection: Projection,
	},
	/// A statement that is one expression: its value, evaluated once with
	/// the enclosing row as its document when it has one, and `fetches`
	/// fetched into it in order. A signal it raises leaves the statement.
	Eval {
		expr: LogicalExpr,
		fetches: Vec<LogicalExpr>,
	},
	/// A schema statement: one node carrying the statement, whose steps are
	/// its program's.
	Ddl {
		statement: crate::expr::logical::LogicalDdlStatement,
		/// What the statement's body checks it against in the catalog, read
		/// by the mapping.
		checks: std::sync::Arc<super::lower::DdlChecks>,
	},
	/// An `INFO` statement: one node carrying the statement, whose reads and
	/// rendering are its program's.
	Info {
		info: crate::expr::logical::LogicalInfo,
	},
	/// A block, `LET`, `FOR`, `RETURN` or `SLEEP`: control flow over the
	/// statements it runs, each planned with the program.
	Control {
		control: Control,
	},
	/// A statement the operator tree refuses as it plans it, before any of
	/// it runs: planning it raises `refusal`, and nothing of it is lowered.
	Refused {
		refusal: Refusal,
	},
	/// A mutation: each subject `subjects` produces is written by `write`'s
	/// per-record orders, one record at a time, in subject order. Its rows are
	/// what the statement's `RETURN` clause makes of each record written.
	Write {
		write: Box<crate::expr::logical::LogicalWrite>,
		subjects: WriteSubjects,
	},
}

/// The rows of a [`Plan::Join`] that are emitted for a row of its left input.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum JoinMatch {
	/// Each pairing with a matching row; a row nothing matches is dropped.
	Inner,
	/// Each pairing with a matching row; a row nothing matches is emitted once
	/// with `NULL` under the alias.
	Left,
	/// The row itself, once, where some row matches it; a row nothing matches
	/// is dropped. The row carries nothing of the row it matched.
	Semi,
}

/// The query vector of a brute-force KNN.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum KnnTarget {
	/// A vector known when the statement is planned.
	Literal(Vec<crate::val::Number>),
	/// An expression evaluated once when the statement runs, over no row, and
	/// coerced to `array<number>`.
	Deferred(LogicalExpr),
}

/// What a KNN scan gates each candidate of its search on, in the order the
/// search applies it: the table's permission, then a condition.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct KnnFilter {
	/// The table permissions the search judges each candidate against,
	/// innermost first, as the plan holds them above the scan.
	pub(crate) permissions: Vec<(
		LogicalExpr,
		Option<surrealdb_iam::AuthLimit>,
		crate::expr::logical::GuardKind,
		Option<ComputedReads>,
	)>,
	/// The statement's conjuncts with the `KNN` operators and every conjunct
	/// holding a `MATCHES` removed: the condition judged inside the search
	/// when no allow-list restricts it.
	pub(crate) residual: Option<LogicalExpr>,
	/// The statement's conjuncts with the `KNN` operators removed, judged
	/// where the allow-list cannot cover a `MATCHES`: for an index that
	/// carries no shared doc-IDs, and for a `MATCHES` the prefilter leaves
	/// out.
	pub(crate) matches_condition: Option<LogicalExpr>,
	/// The allow-list the search is restricted to, where the conjuncts can
	/// be answered by bitmaps.
	pub(crate) prefilter: Option<KnnPrefilter>,
}

/// A bitmap allow-list that restricts a KNN search to the records that satisfy
/// the conjuncts it covers.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct KnnPrefilter {
	/// The bitmap plan whose truth set is exactly the covered conjuncts.
	pub(crate) root: crate::exec::index::access_path::BitmapPlan,
	/// The conjuncts the bitmap does not cover, judged inside the search.
	pub(crate) residual: Option<LogicalExpr>,
	/// Whether a `MATCHES` conjunct the bitmap does not cover is judged inside
	/// the search beside the allow-list.
	pub(crate) uncovered_matches: bool,
}

/// What a table's `SELECT` permission does to a caller whose reads are
/// checked, as the count fast path needs it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CountSelect {
	/// Every record is visible.
	Allow,
	/// No record is visible, and none is counted.
	Deny,
	/// A predicate judges each record.
	Conditional,
}

/// How a [`Plan::Values`] reads the value it evaluates.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ValuesRead {
	/// A value or mock written in `FROM`, or an expression other than a
	/// parameter or a call: evaluated over `$this` as its document, when the
	/// statement is nested. An array's elements are the rows — an element that
	/// is an array contributing its elements, one level deep, when `written`
	/// says the array is written in `FROM` — and any other value is one row.
	/// `NONE` and `NULL` are no row; a record id is the record it names, read
	/// with the caller's permissions and computed fields, and no row where no
	/// record answers. A zero limit reads as any other.
	Expression {
		written: bool,
	},
	/// A parameter or a call: evaluated in the frame. A record id or range is
	/// read under the caller's table and field permissions, checking only its
	/// table and permission under a zero limit; a table is read whole under
	/// them, in key order, and not at all under a zero limit once its
	/// existence is checked; an array's elements are the rows, its records
	/// read with the caller's permissions and computed fields; `NONE` and
	/// `NULL` are no row, and any other value is one row.
	Dynamic,
	/// A parameter or a call under `FOR UPDATE`: evaluated in the frame, and
	/// read as a locked lookup of the record id it is. Any other value is
	/// refused when the source opens.
	Locked,
}

/// Where a [`Plan::Write`]'s subjects come from.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum WriteSubjects {
	/// The targets the statement names, each evaluated to the records it
	/// names, in written order.
	Targets(Vec<LogicalExpr>),
	/// The records a read yields, in its order.
	Rows(Box<Plan>),
	/// An `UPSERT` of tables: the records a read of the tables yields, in
	/// its order, then the record the last of `targets` the statement's gate
	/// admits names, when the statement's condition matched none of them.
	Guaranteed {
		rows: Box<Plan>,
		targets: Vec<LogicalExpr>,
	},
	/// `INSERT`'s rows, every one evaluated before the first is written.
	Inserts(crate::expr::logical::LogicalInserts),
	/// `RELATE`'s edges: an edge through `through`, evaluated per edge, from
	/// each record `from` names to each record `to` names, both evaluated
	/// once.
	Edges {
		from: LogicalExpr,
		through: LogicalExpr,
		to: LogicalExpr,
	},
}

/// One `ORDER BY` key.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct SortKeySpec {
	pub(crate) read: SortKeyRead,
	pub(crate) ascending: bool,
	pub(crate) collate: bool,
	pub(crate) numeric: bool,
}

/// How a sort key is read from the row the sort is handed.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum SortKeyRead {
	/// The value at a path of the row, read structurally: a missing step reads
	/// `NONE`, and a record id is not followed.
	Path(FieldPath),
	/// An expression evaluated against the row, once per row. A `RETURN` it
	/// signals is its value.
	Computed(LogicalExpr),
}

/// The output of a [`Plan::Project`].
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Projection {
	/// `SELECT *`: the row itself.
	All,
	/// `SELECT VALUE expr`.
	Value(LogicalExpr),
	/// `SELECT VALUE expr OMIT d`: `expr` evaluated over the row once each
	/// omitted path has been removed from it.
	OmittedValue {
		expr: LogicalExpr,
		omit: Vec<Omission>,
	},
	/// `SELECT VALUE expr AS name ORDER BY name`, with `expr` the value sort
	/// key `.0` computed once for the sort beneath.
	SortedValue(u16),
	/// `SELECT a, b AS c OMIT d`: a new object, each field evaluated in written
	/// order and set at its output path, then each omitted path removed.
	Fields {
		fields: Vec<ProjectedField>,
		omit: Vec<Omission>,
	},
	/// `SELECT *, a AS b OMIT c`: the row, with each field evaluated in
	/// written order and set at its output path, then each omitted
	/// path removed.
	Star {
		fields: Vec<ProjectedField>,
		omit: Vec<Omission>,
	},
}

/// A path an `OMIT` removes, in the order the clause writes it: each applies
/// to what the ones before it left.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Omission {
	/// A path of fields and literal indices, removed from an object.
	Path(FieldPath),
	/// Any other path, walked to the locations it names.
	Walk {
		/// The path as written.
		source: LogicalExpr,
		walk: Walk<LogicalExpr>,
	},
}

impl std::fmt::Display for Omission {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::Path(path) => write!(f, "{path}"),
			Self::Walk {
				source,
				..
			} => write!(f, "{}", surrealdb_types::ToSql::to_sql(source)),
		}
	}
}

/// The walk to the locations an `OMIT` path names, and what is removed where
/// it lands. `E` is how a step that is known only once evaluated holds its
/// expression.
///
/// The steps reach zero or more values inside the value they are applied to;
/// the action then removes something at each.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Walk<E> {
	pub(crate) steps: Vec<WalkStep<E>>,
	pub(crate) action: WalkAction<E>,
}

/// One step of a [`Walk`].
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum WalkStep<E> {
	/// `.name`: the field of an object, of each element of an array.
	Field(String),
	/// `[0]`: the element an array numbers, or the key an object spells.
	Index(usize),
	/// `[$i]`: the same, where the index is known only once evaluated.
	IndexAt(E),
	/// `[WHERE p]`: every element the predicate holds for.
	Matching(E),
	/// `.*`: every child of an object or array.
	Each,
	/// `[0]` at the front, and `[$]` at the back.
	First,
	Last,
	/// `.{ a, b }` before a further step: each field the destructure lists.
	Destructure(Vec<crate::expr::logical::LogicalDestructureField>),
	/// A step that names no location, such as a method call, which answers
	/// with a value: nothing under it is reached either.
	Nowhere,
}

/// What a [`Walk`] removes where its steps land.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum WalkAction<E> {
	/// `.name`: the field leaves the object, or each element of an array.
	RemoveField(String),
	/// `[0]`: the element leaves the array, closing the gap, or the key leaves
	/// the object.
	RemoveIndex(usize),
	/// `[$i]`: the same, evaluated.
	RemoveIndexAt(E),
	/// `[WHERE p]`: every element the predicate holds for leaves the array.
	RemoveMatching(E),
	RemoveFirst,
	RemoveLast,
	/// `.*`: the object or array is emptied, and stays.
	Clear,
	/// `.{ a, b }`: the listed fields leave the object.
	Destructure(Vec<crate::expr::logical::LogicalDestructureField>),
	/// A path that names no location, so removes nothing.
	Nothing,
}

impl<E> Walk<E> {
	/// The expressions the walk evaluates.
	pub(crate) fn exprs(&self) -> Vec<&E> {
		let steps = self.steps.iter().filter_map(|step| match step {
			WalkStep::IndexAt(expr) | WalkStep::Matching(expr) => Some(expr),
			_ => None,
		});
		let action = match &self.action {
			WalkAction::RemoveIndexAt(expr) | WalkAction::RemoveMatching(expr) => Some(expr),
			_ => None,
		};
		steps.chain(action).collect()
	}
}

/// One field of [`Projection::Fields`].
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ProjectedField {
	pub(crate) output: FieldPath,
	pub(crate) expr: LogicalExpr,
	/// Whether `output` is an explicit alias, which a projection function
	/// writes its answer at in place of the names it produces.
	pub(crate) aliased: bool,
	/// The sort key whose value this field is, computed once for the sort
	/// beneath the projection rather than evaluated again, when an
	/// `ORDER BY` names the field's alias.
	pub(crate) sort_key: Option<u16>,
}

#[cfg(test)]
impl Plan {
	/// The node's input, for a node that has one.
	pub(crate) fn input(&self) -> Option<&Plan> {
		match self {
			Plan::TableScan {
				..
			}
			| Plan::RecordScan {
				..
			}
			| Plan::IndexScan {
				..
			}
			| Plan::IndexUnion {
				..
			}
			| Plan::Bitmap {
				..
			}
			| Plan::FullTextScan {
				..
			}
			| Plan::KnnScan {
				..
			}
			| Plan::Count {
				..
			}
			| Plan::Union {
				..
			}
			| Plan::Join {
				..
			}
			| Plan::Values {
				..
			}
			| Plan::Subquery {
				..
			}
			| Plan::Write {
				..
			}
			| Plan::Eval {
				..
			}
			| Plan::Ddl {
				..
			}
			| Plan::Info {
				..
			}
			| Plan::Control {
				..
			}
			| Plan::Refused {
				..
			} => None,
			Plan::Filter {
				input,
				..
			}
			| Plan::PermissionFilter {
				input,
				..
			}
			| Plan::Layer {
				input,
				..
			}
			| Plan::Enforced {
				input,
				..
			}
			| Plan::Distinct {
				input,
			}
			| Plan::Split {
				input,
				..
			}
			| Plan::Step {
				input,
				..
			}
			| Plan::Fetch {
				input,
				..
			}
			| Plan::KnnTopK {
				input,
				..
			}
			| Plan::Sort {
				input,
				..
			}
			| Plan::Limit {
				input,
				..
			}
			| Plan::Aggregate {
				input,
				..
			}
			| Plan::Project {
				input,
				..
			}
			| Plan::Probe {
				build: input,
				..
			} => Some(input),
		}
	}
}

/// The `COMPUTED` fields of a table a permission predicate reads. A computed
/// field is not stored, so the rows reaching the predicate carry none; each is
/// judged instead against a copy with these fields, and those they read in
/// turn, derived in dependency order in the predicate's own frame.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ComputedReads {
	/// The table whose fields are derived.
	pub(crate) table: surrealdb_strand::TableName,
	/// The fields the predicate reads by name, or `None` when it reads the
	/// row other than by name, and every computed field is derived.
	pub(crate) fields: Option<Vec<String>>,
}
