//! The payload types of bytecode instructions and `phys` op attributes.
//!
//! A value of one of these types is inert: it holds no transaction, cursor, future or session
//! value, so a program that carries it can be compared, printed and shared across callers. The
//! physical dialect states each decision in one of them, and the lowering copies it into the
//! instruction that acts on it.

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
	/// Whether the scan may reject a packed row from its checked columns. A packed row is
	/// read whole by the ops above the scan, so a row rejected unread skips each of them: the
	/// flag holds only where every op the rows cross between the scan and the sort is
	/// infallible, deterministic and writes nothing, and a skipped row then hides no error and
	/// no effect. Rows stored as documents are rejected whatever the flag says.
	pub(crate) packed_rows: bool,
	/// Where the first key is a datetime component of the record id that a
	/// key-range scan delivers in the order of its whole second
	/// (`phys.key_range_scan`): the scan reads no stored bytes for the key,
	/// and stops once it reads past the second of the sort's threshold.
	pub(crate) coarse_cutoff: Option<CoarseCutoff>,
	/// Whether the scan rejects an entry whose key the sort's threshold shows worse than every
	/// row the sort keeps, so that the body of the entry never runs. The placement of the
	/// scan's pipeline states it: only a pipeline that raises for no row may leave a body unrun.
	/// A scan holding a [`Self::coarse_cutoff`] rejects no single entry whatever this says.
	pub(crate) rejects_entries: bool,
}

/// The cut a key-range scan makes beneath a sort that keeps `keep` rows,
/// ordered first by the datetime component at `position`, which the scan
/// delivers in the order of its whole second.
///
/// Once the sort holds `keep` rows, the first key of the last of them is its
/// threshold. Every record the scan reads after one whose component is in a
/// later second (an earlier one, reading backwards) orders strictly after the
/// threshold, so it can take no kept place: the scan stops there. Rows within
/// the threshold's own second are still read, since the key bytes do not
/// order them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CoarseCutoff {
	pub(crate) position: u32,
	pub(crate) keep: u64,
}

/// How many evaluations of a statement read through an index nested loop
/// before the rest of the run reads it through its probe, where it has one.
///
/// A run evaluating the statement for a few rows reads only the records
/// their keys reach. A run evaluating it for many reads the table once, as
/// the probe does, after at most this many lookups: the probe's cost, which
/// grows with the table, plus a bound. No count of the rows to come is known
/// when the statement is planned, so the run decides by the evaluations it
/// has made.
pub(crate) const LOOKUP_EVALUATIONS: u32 = 64;

/// An error raised while a statement is planned, before any of the statement
/// runs: the statement is answered with the error rather than run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Refusal {
	/// A `LET` of a name no statement may bind.
	ProtectedParam {
		name: String,
	},
	/// A statement the query's shape makes invalid, with the message raised
	/// for it.
	Query {
		message: String,
	},
	/// A KNN query vector known when the statement is planned that no
	/// distance ranks a record against, as written in `vector`.
	InvalidKnnVector {
		vector: String,
	},
	/// A KNN query vector known when the statement is planned that does not
	/// coerce to `array<number>`, with the coercion's error.
	UncoercibleKnnVector {
		message: String,
	},
	/// A statement written where no statement of its kind may stand, with the
	/// message raised for it.
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
	/// A `.@` marker that stands in no recursion's body, raised when it is
	/// reached.
	UnsupportedRepeatRecurse,
	/// An expression nested past the computation depth the planner allows.
	ComputationDepthExceeded,
	/// A `DEFINE ACCESS` naming the redaction placeholder as a key of the
	/// access `ac`.
	AccessRedactedKey {
		ac: String,
	},
	/// A schema clause, named as `clause`, written holding `construct`, an
	/// effect a schema clause must not have.
	SchemaClauseEffect {
		clause: String,
		construct: String,
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
			Self::InvalidKnnVector {
				vector,
			} => crate::err::Error::Exec(crate::exec::Error::InvalidKnnVector {
				vector: vector.clone(),
			}),
			Self::UncoercibleKnnVector {
				message,
			} => crate::err::Error::Exec(crate::exec::Error::UncoercibleKnnVector {
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
			Self::UnsupportedRepeatRecurse => {
				crate::err::Error::Exec(crate::exec::Error::UnsupportedRepeatRecurse)
			}
			Self::ComputationDepthExceeded => {
				crate::err::Error::Exec(crate::exec::Error::ComputationDepthExceeded)
			}
			Self::AccessRedactedKey {
				ac,
			} => crate::err::Error::Exec(crate::exec::Error::AccessRedactedKey {
				ac: ac.clone(),
			}),
			Self::SchemaClauseEffect {
				clause,
				construct,
			} => crate::err::Error::Exec(crate::exec::Error::SchemaClauseEffect {
				clause: clause.clone(),
				construct: construct.clone(),
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
			crate::err::Error::Exec(crate::exec::Error::InvalidKnnVector {
				vector,
			}) => Some(Self::InvalidKnnVector {
				vector: vector.clone(),
			}),
			crate::err::Error::Exec(crate::exec::Error::UncoercibleKnnVector {
				message,
			}) => Some(Self::UncoercibleKnnVector {
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
			crate::err::Error::Exec(crate::exec::Error::SchemaClauseEffect {
				clause,
				construct,
			}) => Some(Self::SchemaClauseEffect {
				clause: clause.clone(),
				construct: construct.clone(),
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
			crate::err::Error::Exec(crate::exec::Error::UnsupportedRepeatRecurse) => {
				Some(Self::UnsupportedRepeatRecurse)
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
			crate::err::Error::Engine(_)
			| crate::err::Error::Exec(_)
			| crate::err::Error::Catalog(_)
			| crate::err::Error::Idx(_)
			| crate::err::Error::Kvs(_)
			| crate::err::Error::IamError(_)
			| crate::err::Error::ApiError(_)
			| crate::err::Error::InvalidPath(_)
			| crate::err::Error::InvalidUrl(_)
			| crate::err::Error::Http(_) => None,
		}
	}
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

/// How a breaker — a node that holds state across its input before it
/// yields — holds that state once it outgrows the query's memory budget.
///
/// A plan names the variant and the run decides by the actual size. A sort
/// or aggregate labelled [`Self::Spillable`] is charged to the query's lease
/// and spills once the lease cannot cover it; one labelled
/// [`Self::Bounded`] holds no more than its bound and is charged nothing. A
/// sort under `TEMPFILES` also spills by row count ([`Tail::tempfiles`]). A
/// join's or distinct's build spills where the run finds the memory short.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Spill {
	/// The state is bounded by the plan: the rows a limit can reach, or one
	/// group. It never outgrows that bound, so there is nothing to spill.
	Bounded,
	/// Held in memory until the budget is reached, then spilled: an external
	/// sort's runs, a spilling aggregate's or distinct's partitions, a
	/// partitioned join's build.
	Spillable,
}

impl Spill {
	/// The variant for a sort that keeps `keep` rows of its order.
	pub(crate) fn of_sort(keep: Option<usize>) -> Self {
		match keep {
			Some(_) => Self::Bounded,
			None => Self::Spillable,
		}
	}
}

/// Which input of a `phys.join` the join holds while it streams the other.
///
/// Either way the join pairs the same rows in the same order: each `left` row
/// in order, with the `right` rows its keys match in the order `right` yields
/// them. A `right` row whose key holds a regex pairs after those whose key the
/// left row's key equals by ordering, as the build table pairs it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum JoinBuild {
	/// `right`, read whole into a build table keyed by its keys when the first
	/// `left` row reaches the join; `left` streams past it.
	Right,
	/// `left`'s one row: the plan proves `left` yields at most one, and
	/// `right` is read only once `left` has yielded it, streamed past it row by
	/// row. Chosen only where reading `right` raises no error and has no
	/// effect, its keys and residual raise none, and the statement writes
	/// nothing `right` could read, so pairing a row as `right` yields it, rather
	/// than once `right` is read whole, changes nothing a caller observes. A
	/// left row whose key holds a regex is paired once `right` is read whole,
	/// as [`Self::Right`] pairs it.
	Left,
}

/// The rows of a `phys.join` that are emitted for a row of its left input.
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

/// The paging `START start LIMIT limit` of a subquery whose rows a count
/// stands for: of the records counted, the count keeps those the paging keeps.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CountCap {
	pub(crate) limit: u64,
	pub(crate) start: u64,
}

impl CountCap {
	/// How many rows the paging reads at most: no count past it changes what
	/// the paging keeps.
	pub(crate) fn reach(self) -> usize {
		usize::try_from(self.start.saturating_add(self.limit)).unwrap_or(usize::MAX)
	}

	/// How many of `counted` rows the paging keeps.
	pub(crate) fn apply(self, counted: usize) -> usize {
		(counted as u64).saturating_sub(self.start).min(self.limit) as usize
	}
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

/// How a `phys.values` reads the value it evaluates. Beneath a write, the
/// value is evaluated as each variant says and the records it names are read as
/// storage holds them, for the write to judge.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ValuesRead {
	/// A value or mock written in `FROM`, or an expression other than a
	/// parameter or a call: evaluated over `$this` as its document, when the
	/// statement is nested. An array's elements are the rows — an element that
	/// is an array contributing its elements, one level deep, when `written`
	/// says the array is written in `FROM` — and any other value is one row.
	/// `NONE` and `NULL` are no row; a record id is the record it names, read
	/// with the caller's permissions and computed fields, and no row where no
	/// record answers, whatever the limit. A record range or a table the
	/// expression evaluates to is read as [`Self::Dynamic`] reads it.
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
