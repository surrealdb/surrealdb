//! Access path types for index-based record retrieval.
//!
//! An [`AccessPath`] represents a specific way to retrieve records from a table,
//! whether through a full table scan, point lookup, or index scan.

use std::borrow::Cow;
use std::ops::Bound;
use std::sync::Arc;

use anyhow::Result;
use surrealdb_datastore::index_shapes::{TableIndexShapes, ValueShape};

use super::IndexCandidate;
use super::key_range::{DeliveredKeyOrder, KeyRangeAccess, plan_key_range};
use crate::catalog::{
	DefineDefault, FieldDefinition, Index, IndexDefinition, KeyStructure,
	spells_normalised_decimal_keys,
};
use crate::err::Error;
use crate::exec::function::FunctionRegistry;
use crate::exec::operators::scan::pipeline::determine_scan_direction;
use crate::exec::planner::util::{
	expr_contains_matches, has_knn_operator, order_is_scan_compatible,
	resolve_projection_field_idioms, strip_knn_from_condition,
};
use crate::exec::planner::{
	RestrictedPrefixes, adjust_direction_for_order, detect_order_for_composite_union,
	drop_restricted_indexes,
};
use crate::expr::logical::{LogicalExpr, LogicalLiteral, LogicalOrdering};
use crate::expr::operator::MatchesOperator;
use crate::expr::with::With;
use crate::expr::{BinaryOperator, Idiom, Kind, KindLiteral, Part};
use crate::kvs::Direction;
use crate::val::{Datetime, Duration, Number, Range, TryAdd, TrySub, Value};

/// An index definition with the position it holds among its table's indexes.
///
/// The reference carries the one definition it names, not the table's whole
/// list, and compares by value: two references are equal when they name equal
/// definitions at the same position. The position orders the analyzer's
/// candidates and is otherwise not read. Cloning shares the definition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct IndexRef {
	/// The position of this index in its table's list when the reference was
	/// made.
	pub(crate) idx: usize,
	definition: Arc<IndexDefinition>,
	/// Per column, whether it is known to hold no array: the index has
	/// recorded storing none in it. A column past the end is not known to, and
	/// may hold one.
	scalar: ColumnFlags,
	/// Per column, whether it is a datetime component of the table's record
	/// id, whose entries order only by the whole second
	/// ([`super::derived_columns`]); a column past the end is not.
	coarse: ColumnFlags,
	/// Whether the record ids that follow the columns in the entries of a
	/// non-unique index order other than by value
	/// ([`super::derived_columns::id_orders_coarsely`]).
	coarse_id: bool,
}

impl IndexRef {
	/// Create a reference to the index at position `idx` of `indexes`.
	pub fn new(indexes: &[IndexDefinition], idx: usize) -> Self {
		Self {
			idx,
			definition: Arc::new(indexes[idx].clone()),
			scalar: ColumnFlags::default(),
			coarse: ColumnFlags::default(),
			coarse_id: false,
		}
	}

	/// The same reference, knowing the columns `scalar` marks to hold no
	/// array.
	pub(crate) fn with_scalar_columns(mut self, scalar: ColumnFlags) -> Self {
		self.scalar = scalar;
		self
	}

	/// The same reference, knowing the columns `coarse` marks to be datetime
	/// components of the record id.
	pub(crate) fn with_coarse_columns(mut self, coarse: ColumnFlags) -> Self {
		self.coarse = coarse;
		self
	}

	/// The same reference, knowing that the record ids its entries end with
	/// order other than by value.
	pub(crate) fn with_coarse_id(mut self) -> Self {
		self.coarse_id = true;
		self
	}

	/// Per column, whether it is known to hold no array; empty when no column is.
	pub(crate) fn scalar_columns(&self) -> &[bool] {
		&self.scalar
	}

	/// Whether the entries that agree on every column list their records in
	/// the order of `id`: the record id an entry ends with orders as its value
	/// does.
	pub(crate) fn orders_by_id(&self) -> bool {
		!self.coarse_id
	}

	/// Whether the entries of column `col` order only by the whole second: it
	/// is a datetime component of the table's record id. A scan lists the
	/// entries of such a column in no chronological order within a second, and
	/// a range over it reaches the records of its bounds' seconds.
	pub(crate) fn orders_coarsely(&self, col: usize) -> bool {
		self.coarse.get(col).copied().unwrap_or(false)
	}

	/// Whether column `col` may hold an array, which the index writer stores
	/// as one entry per element: a column spelled with no `*` or flatten that
	/// is not known to hold no array.
	fn may_hold_array(&self, col: usize) -> bool {
		self.definition.cols.get(col).is_some_and(|c| is_plain_column(c) && !self.is_scalar(col))
	}

	/// Whether column `col` of this index is known to hold every record under
	/// exactly one entry, at the place the record's whole value there sorts: a
	/// column spelled with no `*` or flatten that the index has recorded
	/// storing no array in ([`with_array_columns_as_elements`]). The index
	/// writer stores an array once per element, and an empty one under `NULL`,
	/// so a column that may hold one lists a record under keys its own value
	/// does not compare equal to.
	///
	/// The column's declared kind does not establish this. A declared kind is
	/// declared, not checked (see `FieldSchema::checked`): `DEFINE FIELD`
	/// coerces the values written after it and leaves the records already
	/// stored as they are, a later definition with another kind leaves the
	/// values written under the earlier one, and an import skips coercion.
	///
	/// Every planning decision that counts on a column's entries being its
	/// records asks this: whether a scan must list each record once (and so
	/// whether it can page at the scan), whether a scan delivers the column's
	/// order, whether a scan lists exactly the records its conjuncts accept
	/// (and so drops those conjuncts above it), whether counting keys counts
	/// rows, and whether a bitmap equals its predicate's truth set (an
	/// index-only count, a `NOT` subtraction, a KNN allow-list that replaces
	/// its conjunct).
	pub(crate) fn one_entry_per_record(&self, col: usize) -> bool {
		matches!(self.definition.index, crate::catalog::Index::Idx | crate::catalog::Index::Uniq)
			&& self
				.definition
				.cols
				.get(col)
				.is_some_and(|c| is_plain_column(c) && self.is_scalar(col))
	}

	fn is_scalar(&self, col: usize) -> bool {
		self.scalar.get(col).copied().unwrap_or(false)
	}

	/// Get the index definition.
	pub fn definition(&self) -> &IndexDefinition {
		&self.definition
	}

	/// Check if this is a unique index.
	pub fn is_unique(&self) -> bool {
		matches!(self.definition().index, crate::catalog::Index::Uniq)
	}
}

impl std::ops::Deref for IndexRef {
	type Target = IndexDefinition;

	fn deref(&self) -> &Self::Target {
		self.definition()
	}
}

impl std::hash::Hash for IndexRef {
	fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
		self.idx.hash(state);
	}
}

/// Represents a way to access records from a table.
///
/// The planner analyzes WHERE conditions and ORDER BY clauses to determine
/// the optimal access path for each table in the query.
#[derive(Debug, Clone)]
pub enum AccessPath {
	/// Full table scan - iterate all records in storage order.
	TableScan,

	/// Produces no rows.
	///
	/// Selected when the analyzer can statically prove the WHERE cannot
	/// match — for example a contradictory range (`a > 10 AND a < 5`),
	/// an empty `IN []`, or a fully false-folded predicate. Surfaces as
	/// the [`crate::exec::operators::EmptyScan`] operator and short-circuits
	/// the rest of the SELECT pipeline.
	EmptyScan,

	/// B-tree index scan (Idx or Uniq).
	///
	/// Supports equality lookups, range scans, and compound key access.
	BTreeScan {
		index_ref: IndexRef,
		access: BTreeAccess,
		direction: Direction,
	},

	/// Full-text search using MATCHES operator.
	FullTextSearch {
		index_ref: IndexRef,
		query: String,
		operator: MatchesOperator,
	},

	/// KNN vector search using an ANN index.
	KnnSearch {
		index_ref: IndexRef,
		/// The query vector to search for nearest neighbors of
		vector: Arc<[Number]>,
		/// Number of nearest neighbors to return
		k: u32,
		/// ANN search expansion factor
		ef: u32,
		/// Pre-filtered vector search: an exact bitmap plan over the
		/// index-covered WHERE conjuncts, plus the true residual. `None` when
		/// no conjunct is exactly index-coverable.
		prefilter: Option<KnnPrefilterPlan>,
	},

	/// Roaring-bitmap candidate fusion over the table's shared doc-ID space
	/// Candidate bitmaps from several indexes are combined with set algebra.
	///
	/// Chosen for an AND-composed WHERE clause with at least two index-backed
	/// members (or one plus a subtractable `NOT`), or a top-level OR whose
	/// every branch is bitmap-capable when every participating
	/// index carries doc-IDs, no index covers the ORDER BY, and no
	/// early-termination (LIMIT without ORDER BY) or VERSION constraints
	/// apply. Lowered to the VM's bitmap scan, which evaluates the plan
	/// as a [`BitmapPlan`]; the whole WHERE clause stays as the residual filter.
	///
	/// `fallback` carries the [`AccessPath::Union`] the OR fusion
	/// replaced. An OR may not drop an overflowing branch (the residual
	/// filter can only narrow the candidate set, never restore dropped
	/// rows), so instead of draining branches unbudgeted the executor
	/// abandons the bitmap on the first branch overflow and streams the
	/// fallback in its place. `None` for AND fusion, whose anchored root
	/// drops overflowing conjuncts and lets the residual filter compensate.
	BitmapFusion {
		root: BitmapPlan,
		fallback: Option<Box<AccessPath>>,
	},

	/// Union of multiple index scans (OR-union, scalar IN-expansion,
	/// or array-containment expansion).
	///
	/// `dedupe` records whether the analyser's construction can emit
	/// the same record from more than one branch:
	///
	/// - **`true`** — OR-union (independent predicates may both hold on the same row) and
	///   CONTAINSANY/ANYINSIDE on an array-element index (a row whose indexed array contains
	///   multiple branch values is in multiple branches' prefix ranges).
	/// - **`false`** — scalar `IN`-expansion. Each row's field value matches at most one literal,
	///   so branches are record-disjoint by construction.
	///
	/// `plan_union_index_source` reads this flag to choose between
	/// `MergeMode::ByIndexKey` (no dedupe; cheaper) and
	/// `MergeMode::ByIndexKeyDedup` (HashSet of record ids) when an
	/// ordered k-way merge is active.  Sequential and `ById` merge
	/// modes dedupe unconditionally; the flag is purely the explicit
	/// contract between the analyser and the union operator.
	Union {
		paths: Vec<AccessPath>,
		dedupe: bool,
	},

	/// Scan of the ranges of record keys a predicate over the components of
	/// the table's record id selects ([`super::key_range::plan_key_range`]).
	///
	/// Reads no index. A record has one key, so no record is reached twice
	/// and no dedupe is needed. The ranges are a superset of the records the
	/// predicate admits: the whole predicate is evaluated over the rows the
	/// scan yields.
	KeyRange(KeyRangeAccess),
}

impl AccessPath {
	/// Returns `true` if this is a B-tree index scan with no WHERE
	/// selectivity — i.e. a full-range scan that exists only because
	/// it satisfies ORDER BY.
	pub fn is_full_range_scan(&self) -> bool {
		matches!(
			self,
			AccessPath::BTreeScan {
				access: BTreeAccess::Range {
					range: OperandRange {
						start: Bound::Unbounded,
						end: Bound::Unbounded,
						..
					},
				},
				..
			}
		)
	}
}

/// Exact geometric pre-filter evaluated on the inline `(lon, lat)` point
/// coordinates stored in a `SPATIAL` index entry trailer.
#[derive(Debug, Clone, PartialEq)]
pub enum SpatialPointFilter {
	/// `geo::distance(field, center) < radius` or `<= radius`.
	Cap {
		column: crate::expr::Idiom,
		center: (f64, f64),
		radius: f64,
		inclusive: bool,
	},
	/// `field INSIDE geom` or `geom CONTAINS field`.
	Inside {
		column: crate::expr::Idiom,
		min_x: f64,
		max_x: f64,
		min_y: f64,
		max_y: f64,
		geom: crate::val::Geometry,
	},
	/// `field INTERSECTS geom` or `geom INTERSECTS field`.
	Intersects {
		column: crate::expr::Idiom,
		min_x: f64,
		max_x: f64,
		min_y: f64,
		max_y: f64,
		geom: crate::val::Geometry,
	},
}

impl SpatialPointFilter {
	/// The indexed geometry column this point filter evaluates.
	#[inline]
	pub fn column(&self) -> &crate::expr::Idiom {
		match self {
			Self::Cap {
				column,
				..
			}
			| Self::Inside {
				column,
				..
			}
			| Self::Intersects {
				column,
				..
			} => column,
		}
	}

	/// Evaluate the exact geometric predicate against inline `(x, y)` = `(lon, lat)` coordinates.
	#[inline]
	pub fn matches(&self, x: f64, y: f64) -> bool {
		match self {
			Self::Cap {
				center,
				radius,
				inclusive,
				..
			} => {
				let Ok(Value::Number(d)) = crate::fnc::geo::distance((
					crate::val::Geometry::Point((x, y).into()),
					crate::val::Geometry::Point((*center).into()),
				)) else {
					return false;
				};
				let d = d.as_float();
				if *inclusive {
					d <= *radius
				} else {
					d < *radius
				}
			}
			Self::Inside {
				min_x,
				max_x,
				min_y,
				max_y,
				geom,
				..
			} => {
				if x < *min_x || x > *max_x || y < *min_y || y > *max_y {
					return false;
				}
				#[cfg(feature = "geo")]
				{
					geom.contains(&crate::val::Geometry::Point((x, y).into()))
				}
				#[cfg(not(feature = "geo"))]
				{
					let _ = geom;
					false
				}
			}
			Self::Intersects {
				min_x,
				max_x,
				min_y,
				max_y,
				geom,
				..
			} => {
				if x < *min_x || x > *max_x || y < *min_y || y > *max_y {
					return false;
				}
				#[cfg(feature = "geo")]
				{
					crate::val::Geometry::Point((x, y).into()).intersects(geom)
				}
				#[cfg(not(feature = "geo"))]
				{
					let _ = geom;
					false
				}
			}
		}
	}

	/// Human-readable description of this point filter for `EXPLAIN` output.
	pub(crate) fn describe(&self) -> String {
		use surrealdb_types::ToSql;
		match self {
			Self::Cap {
				column,
				center,
				radius,
				inclusive,
			} => {
				let op = if *inclusive {
					"<="
				} else {
					"<"
				};
				format!(
					"geo::distance({}, ({}, {})) {op} {radius}",
					column.to_sql(),
					center.0,
					center.1
				)
			}
			Self::Inside {
				column,
				geom,
				..
			} => format!("{} INSIDE {}", column.to_sql(), geom.to_sql()),
			Self::Intersects {
				column,
				geom,
				..
			} => format!("{} INTERSECTS {}", column.to_sql(), geom.to_sql()),
		}
	}
}

/// If `branches` all scan the same index through [`BTreeAccess::Range`] with an identical
/// [`SpatialPointFilter`], identical prefix operands, and strictly disjoint ascending integer
/// cell-id bounds, returns that shared [`SpatialPointFilter`].
///
/// Such a union is produced by a point-only `SPATIAL` index covering: every branch scans
/// leaf-level Hilbert cell ranges (`include_ancestors == false`), each indexed point has at
/// most one entry across all branches, and entries that pass `point_filter` satisfy the exact
/// geometric predicate.
pub(crate) fn disjoint_spatial_point_union_filter<'a, I, K>(
	branches: I,
) -> Option<&'a SpatialPointFilter>
where
	I: IntoIterator<Item = (K, Option<&'a BTreeAccess>)>,
	K: PartialEq,
{
	let mut iter = branches.into_iter();
	let (first_key, first_access) = iter.next()?;
	let BTreeAccess::Range {
		range: first_range,
	} = first_access?
	else {
		return None;
	};
	let first_filter = first_range.point_filter.as_ref()?;
	let (
		Bound::Included(BoundOperand::Literal(Value::Number(Number::Int(first_min)))),
		Bound::Included(BoundOperand::Literal(Value::Number(Number::Int(first_max)))),
	) = (&first_range.start, &first_range.end)
	else {
		return None;
	};
	if first_min > first_max {
		return None;
	}
	let mut prev_max = *first_max;
	for (key, access) in iter {
		if key != first_key {
			return None;
		}
		let BTreeAccess::Range {
			range,
		} = access?
		else {
			return None;
		};
		if !range
			.point_filter
			.as_ref()
			.is_some_and(|pf| Arc::ptr_eq(pf, first_filter) || **pf == **first_filter)
		{
			return None;
		}
		let (
			Bound::Included(BoundOperand::Literal(Value::Number(Number::Int(min)))),
			Bound::Included(BoundOperand::Literal(Value::Number(Number::Int(max)))),
		) = (&range.start, &range.end)
		else {
			return None;
		};
		if *min > *max || prev_max >= *min {
			return None;
		}
		prev_max = *max;
	}
	Some(first_filter.as_ref())
}

/// Returns whether an `IndexScan` over `access` can construct rows directly from the index's
/// `RecordId`s without reading the record KVS (`id_only: yes` in `EXPLAIN`).
pub(crate) fn index_scan_is_id_only(
	repeats: bool,
	access: &BTreeAccess,
	versioned: bool,
	decode: Option<&crate::exec::operators::scan::common::DecodeFields>,
) -> bool {
	!repeats
		&& access.point_filter().is_some()
		&& !versioned
		&& decode.is_some_and(crate::exec::operators::scan::common::DecodeFields::is_id_only)
}

/// Returns whether a spatial `IndexUnion` with `disjoint_point_ranges` can construct rows directly
/// from the index's `RecordId`s without reading the record KVS (`id_only: yes` in `EXPLAIN`).
pub(crate) fn spatial_union_is_id_only(
	disjoint_point_ranges: bool,
	versioned: bool,
	decode: Option<&crate::exec::operators::scan::common::DecodeFields>,
) -> bool {
	disjoint_point_ranges
		&& !versioned
		&& decode.is_some_and(crate::exec::operators::scan::common::DecodeFields::is_id_only)
}

/// Plan-level pre-filter for a KNN search.
///
/// Built by [`crate::exec::index::analysis::IndexAnalyzer::try_knn_prefilter`]
/// from the KNN-stripped WHERE clause: each conjunct whose truth set is
/// *exactly* a candidate bitmap joins `root`; the others are re-ANDed into
/// `residual`. At execute time the bitmap becomes the ANN search's allow-list
/// and only the residual keeps the per-candidate record-fetch filter.
#[derive(Debug, Clone)]
pub struct KnnPrefilterPlan {
	/// Bitmap plan whose truth set exactly equals the AND of the covered
	/// conjuncts (see `IndexAnalyzer::bitmap_exact_plan` — exactness is
	/// mandatory because the covered conjuncts are dropped from the
	/// in-traversal filter, and an over-approximate bitmap would let
	/// non-matching candidates consume top-K slots).
	pub root: BitmapPlan,
	/// The indexes, in the `AND` chain of the condition the plan was made from, of the conjuncts
	/// `root` covers.
	pub covered: Vec<usize>,
	/// KNN-stripped conjuncts NOT covered by `root`, re-ANDed. `None` when
	/// the whole stripped WHERE is covered. MATCHES conjuncts never appear
	/// here: they are not evaluable through the plain in-traversal residual
	/// (no query executor).
	pub residual: Option<LogicalExpr>,
	/// `true` when a MATCHES conjunct could not be covered by `root` (its
	/// full-text index does not carry shared doc-IDs). Such a conjunct is
	/// absent from `residual` too, so the scan must keep its MATCHES-aware
	/// compiled condition active alongside the allow-list — otherwise the
	/// top-k is chosen without the text predicate and matching neighbours
	/// outside it are unrecoverable.
	pub uncovered_matches: bool,
}

/// Plan-level bitmap candidate expression tree.
///
/// Built by [`crate::exec::index::analysis::IndexAnalyzer::try_bitmap_fusion`]
/// and lowered to the VM's bitmap scan, which evaluates it. Leaves
/// produce a candidate bitmap over the table's shared doc-ID space; inner
/// nodes compose them with set algebra. `NOT` appears only as the subtract
/// side of [`BitmapPlan::AndNot`] — never standalone (a standalone NOT would
/// need a table-universe bitmap that is deliberately not maintained).
#[derive(Debug, Clone, PartialEq)]
pub enum BitmapPlan {
	/// Drain a b-tree index range, reading the doc-ID appended to each entry.
	BTree {
		index_ref: IndexRef,
		access: BTreeAccess,
	},
	/// A full-text query's merged posting bitmap (scoring deferred to the
	/// surviving documents).
	FullText {
		index_ref: IndexRef,
		query: String,
		operator: MatchesOperator,
	},
	/// A single-hop graph reachability semi-join: the doc-IDs of
	/// scanned-table rows adjacent to `source` through `edge_tables`,
	/// traversed in `direction` — the *inverse* of the direction the
	/// predicate was written in, so a `->edge->` containment anchors at its
	/// target literal and walks back to the candidate rows. Read entirely
	/// from adjacency pointer keys, which embed the far vertex; a
	/// key that does not embed a target overflows the branch, so the
	/// residual filter answers the predicate instead of an
	/// under-approximating set. Never exact: participates only as a droppable,
	/// budgeted AND conjunct beneath the original-WHERE residual.
	Graph {
		/// Plan-time literal record id to traverse from.
		source: crate::val::RecordId,
		/// Traversal direction from `source` (never `Both`).
		direction: crate::expr::Dir,
		/// Edge tables to walk; the predicate names them explicitly.
		edge_tables: Vec<surrealdb_strand::TableName>,
	},
	/// Intersection of all children.
	And(Vec<BitmapPlan>),
	/// Union of all children. An empty union is a provably-empty conjunct.
	Or(Vec<BitmapPlan>),
	/// `base AND NOT subtract`.
	AndNot {
		base: Box<BitmapPlan>,
		subtract: Box<BitmapPlan>,
	},
}

/// One bound, or one key component, of a b-tree access.
///
/// A literal is the value the condition names. The statement clock is read
/// when the scan opens, never when the statement is planned, so a plan holds
/// no instant: `at > time::now() - 1d` stays an index range and a program
/// that outlives its statement keeps reading the clock of the statement that
/// runs it.
#[derive(Debug, Clone, PartialEq)]
pub enum BoundOperand {
	/// The value the condition names.
	Literal(Value),
	/// The instant of the running top-level statement, moved by an offset.
	StatementClock(ClockBound),
}

/// The statement clock as a bound: the instant of the running statement, moved
/// by `offset` where there is one. It is what `time::now()` and
/// `time::now() ± <duration literal>` evaluate to in the statement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClockBound {
	pub offset: Option<(ClockSign, Duration)>,
}

/// The direction an offset moves the statement clock.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClockSign {
	Plus,
	Minus,
}

impl From<Value> for BoundOperand {
	fn from(value: Value) -> Self {
		BoundOperand::Literal(value)
	}
}

impl BoundOperand {
	/// The literal value, `None` for the clock.
	pub(crate) fn as_literal(&self) -> Option<&Value> {
		match self {
			BoundOperand::Literal(value) => Some(value),
			BoundOperand::StatementClock(_) => None,
		}
	}

	/// Whether this operand can resolve to a value that `ix` exempts from
	/// uniqueness ([`IndexDefinition::exempts_from_uniqueness`]). The statement
	/// clock always resolves to a [`Datetime`], which is never exempt.
	pub(crate) fn is_exempt_from_uniqueness(&self, ix: &IndexDefinition) -> bool {
		match self {
			BoundOperand::Literal(value) => ix.exempts_from_uniqueness(value),
			BoundOperand::StatementClock(_) => false,
		}
	}

	/// Whether this operand reads the statement clock.
	pub(crate) fn is_clock(&self) -> bool {
		matches!(self, BoundOperand::StatementClock(_))
	}

	/// A value that sorts where this operand does, which is all a plan-time
	/// check of a bound's place in the value order needs: the literal itself,
	/// or any datetime for the clock, since every datetime sorts in the
	/// datetime band whatever its instant.
	pub(crate) fn witness(&self) -> Cow<'_, Value> {
		match self {
			BoundOperand::Literal(value) => Cow::Borrowed(value),
			BoundOperand::StatementClock(_) => Cow::Owned(Value::Datetime(Datetime::MIN_UTC)),
		}
	}

	/// The value this operand denotes for a statement whose clock reads
	/// `clock`: a literal as it is, the clock moved by its offset with the
	/// arithmetic the expression `time::now() ± <duration>` evaluates with.
	pub(crate) fn resolve(&self, clock: &Datetime) -> Result<Value> {
		match self {
			BoundOperand::Literal(value) => Ok(value.clone()),
			BoundOperand::StatementClock(ClockBound {
				offset,
			}) => {
				let now = Value::Datetime(*clock);
				match offset {
					None => Ok(now),
					Some((ClockSign::Plus, by)) => now.try_add(Value::Duration(*by)),
					Some((ClockSign::Minus, by)) => now.try_sub(Value::Duration(*by)),
				}
			}
		}
	}

	/// How EXPLAIN renders the operand: a literal as its value, the clock as
	/// the call that reads it.
	pub(crate) fn describe(&self) -> String {
		use surrealdb_types::ToSql;
		match self {
			BoundOperand::Literal(value) => value.to_sql(),
			BoundOperand::StatementClock(ClockBound {
				offset: None,
			}) => "time::now()".to_owned(),
			BoundOperand::StatementClock(ClockBound {
				offset: Some((sign, by)),
			}) => {
				let sign = match sign {
					ClockSign::Plus => '+',
					ClockSign::Minus => '-',
				};
				format!("time::now() {sign} {by}")
			}
		}
	}
}

/// The bounds of a b-tree range scan, each a [`BoundOperand`].
///
/// Two ranges whose bounds are all literals combine as the values do; a range
/// with a clock bound combines only where no clock bound is compared with
/// another bound, because the instants a clock bound and a literal denote are
/// not known until the scan opens.
#[derive(Debug, Clone, PartialEq)]
pub struct OperandRange {
	pub start: Bound<BoundOperand>,
	pub end: Bound<BoundOperand>,
	pub point_filter: Option<Arc<SpatialPointFilter>>,
}

impl From<Range> for OperandRange {
	fn from(range: Range) -> Self {
		let bound = |bound: Bound<Value>| match bound {
			Bound::Included(value) => Bound::Included(BoundOperand::Literal(value)),
			Bound::Excluded(value) => Bound::Excluded(BoundOperand::Literal(value)),
			Bound::Unbounded => Bound::Unbounded,
		};
		OperandRange {
			start: bound(range.start),
			end: bound(range.end),
			point_filter: None,
		}
	}
}

impl OperandRange {
	/// The range holding every value: no bound on either side.
	pub(crate) fn unbounded() -> Self {
		OperandRange {
			start: Bound::Unbounded,
			end: Bound::Unbounded,
			point_filter: None,
		}
	}

	/// The range of literal bounds, `None` where a bound reads the clock.
	pub(crate) fn as_literal(&self) -> Option<Range> {
		let bound = |bound: &Bound<BoundOperand>| match bound {
			Bound::Included(operand) => operand.as_literal().cloned().map(Bound::Included),
			Bound::Excluded(operand) => operand.as_literal().cloned().map(Bound::Excluded),
			Bound::Unbounded => Some(Bound::Unbounded),
		};
		Some(Range {
			start: bound(&self.start)?,
			end: bound(&self.end)?,
		})
	}

	/// The bounds' operands, start then end.
	pub(crate) fn operands(&self) -> impl Iterator<Item = &BoundOperand> {
		[&self.start, &self.end].into_iter().filter_map(|bound| match bound {
			Bound::Included(operand) | Bound::Excluded(operand) => Some(operand),
			Bound::Unbounded => None,
		})
	}

	/// The values the range bounds, for a check of where its bounds sort: each
	/// operand's [`BoundOperand::witness`].
	pub(crate) fn witnesses(&self) -> (Bound<Cow<'_, Value>>, Bound<Cow<'_, Value>>) {
		fn bound(bound: &Bound<BoundOperand>) -> Bound<Cow<'_, Value>> {
			match bound {
				Bound::Included(operand) => Bound::Included(operand.witness()),
				Bound::Excluded(operand) => Bound::Excluded(operand.witness()),
				Bound::Unbounded => Bound::Unbounded,
			}
		}
		(bound(&self.start), bound(&self.end))
	}

	/// The intersection of two ranges, `None` where it cannot be formed
	/// before the scan opens: two bounds of the same side would have to be
	/// compared and one of them reads the clock. A side only one range bounds
	/// is taken as it is.
	pub(crate) fn intersect(&self, other: &Self) -> Option<Self> {
		if let (Some(a), Some(b)) = (self.as_literal(), other.as_literal()) {
			return Some(a.intersect(b).into());
		}
		let side = |a: &Bound<BoundOperand>, b: &Bound<BoundOperand>| match (a, b) {
			(Bound::Unbounded, other) | (other, Bound::Unbounded) => Some(other.clone()),
			_ => None,
		};
		Some(OperandRange {
			start: side(&self.start, &other.start)?,
			end: side(&self.end, &other.end)?,
			point_filter: None,
		})
	}

	/// Whether no value lies in the range, known only for literal bounds: a
	/// range with a clock bound is never proved empty.
	pub(crate) fn is_empty(&self) -> bool {
		self.as_literal().is_some_and(|range| range.is_empty())
	}
}

/// The query vector of a KNN operator's right-hand side, as the planner reads
/// it for an ANN index search or a brute-force ranking
/// ([`classify_knn_vector`](crate::exec::planner::util::classify_knn_vector)).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KnnQueryVector {
	/// A numeric array written in the statement.
	Literal(Arc<[Number]>),
	/// The right-hand side of the KNN operator, evaluated once with no row in
	/// scope before the search reads any input: any expression that does not
	/// read the row being scanned
	/// ([`classify_knn_vector`](crate::exec::planner::util::classify_knn_vector)),
	/// such as a parameter a `LET` or a `FOR` of an enclosing block or a
	/// closure argument binds, a function call, a cast, an `IF`, an
	/// uncorrelated subquery or an array with computed elements. The value is
	/// coerced to `array<number>`; one that does not coerce, or an evaluation
	/// that fails, fails the query.
	Deferred(LogicalExpr),
}

/// How to access an index.
///
/// Every value a variant pins or bounds is a [`BoundOperand`]: a literal, or
/// the statement clock, which resolves when the scan opens
/// ([`Self::resolve`]).
#[derive(Debug, Clone, PartialEq)]
pub enum BTreeAccess {
	/// Single value equality: `field = value`
	Equality(BoundOperand),

	/// Range scan with optional bounds: `field > a AND field < b`
	Range {
		range: OperandRange,
	},

	/// Compound index access with fixed prefix and optional range on next column.
	///
	/// Example: For index on (a, b, c), if query is `a = 1 AND b = 2 AND c > 3`,
	/// the prefix is [1, 2] and range is Some((MoreThan, 3)).
	Compound {
		/// Fixed values for leading columns
		prefix: Vec<BoundOperand>,
		/// Optional range condition on the next column after the prefix
		range: Option<(BinaryOperator, BoundOperand)>,
		/// The upper bound of a closed range on the next column. Only set
		/// when `range` is a lower bound (`>` or `>=`), and then an upper
		/// bound (`<` or `<=`) that is above it in the value order.
		upper: Option<(BinaryOperator, BoundOperand)>,
	},

	/// Full-text search access
	FullText {
		/// The search query string
		query: String,
		/// The MATCHES operator configuration
		operator: crate::expr::operator::MatchesOperator,
	},

	/// KNN vector search access via ANN index.
	Knn {
		/// The query vector
		vector: Arc<[Number]>,
		/// Number of nearest neighbors
		k: u32,
		/// ANN search expansion factor
		ef: u32,
	},
}

/// Whether no record can sit under both `a` and `b`, two equality prefixes of
/// one index: they differ at a position where both are literals. A prefix
/// that reads the clock is not known to differ from another before the scan
/// opens, so it never proves two prefixes disjoint.
pub(crate) fn prefixes_are_disjoint(a: &[BoundOperand], b: &[BoundOperand]) -> bool {
	a.iter().zip(b).any(|(x, y)| match (x.as_literal(), y.as_literal()) {
		(Some(x), Some(y)) => x != y,
		_ => false,
	})
}

/// A b-tree access with every operand resolved to a value: what the key
/// kernels encode into a key range.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum KeyAccess {
	Equality(Value),
	Range(Range),
	Compound {
		prefix: Vec<Value>,
		range: Option<(BinaryOperator, Value)>,
		upper: Option<(BinaryOperator, Value)>,
	},
}

impl BTreeAccess {
	/// An equality on the literal `value`.
	pub(crate) fn literal_equality(value: Value) -> Self {
		BTreeAccess::Equality(BoundOperand::Literal(value))
	}

	/// A range over the literal bounds of `range`.
	pub(crate) fn literal_range(range: Range) -> Self {
		BTreeAccess::Range {
			range: range.into(),
		}
	}

	/// Optional pre-fetch spatial point filter attached to a range access.
	pub(crate) fn point_filter(&self) -> Option<&Arc<SpatialPointFilter>> {
		match self {
			BTreeAccess::Range {
				range,
			} => range.point_filter.as_ref(),
			BTreeAccess::Equality(_)
			| BTreeAccess::Compound {
				..
			}
			| BTreeAccess::FullText {
				..
			}
			| BTreeAccess::Knn {
				..
			} => None,
		}
	}

	/// Attach an optional spatial point filter when this is a range access.
	pub(crate) fn with_point_filter(mut self, filter: Option<Arc<SpatialPointFilter>>) -> Self {
		if let BTreeAccess::Range {
			range,
		} = &mut self
		{
			range.point_filter = filter;
		}
		self
	}

	/// A compound access over literal values.
	pub(crate) fn literal_compound(
		prefix: Vec<Value>,
		range: Option<(BinaryOperator, Value)>,
	) -> Self {
		BTreeAccess::Compound {
			prefix: prefix.into_iter().map(BoundOperand::Literal).collect(),
			range: range.map(|(op, value)| (op, BoundOperand::Literal(value))),
			upper: None,
		}
	}

	/// The access with every operand resolved against `clock`, the instant of
	/// the statement the scan runs for, or against the wall clock at the call
	/// where no statement installed one. `None` for an access that is not a
	/// b-tree scan (full-text, KNN).
	///
	/// A literal resolves to itself, so the key range a program reads is the
	/// one the literal values encode whether or not the plan reads the clock.
	pub(crate) fn resolve(&self, clock: Option<&Datetime>) -> Result<Option<KeyAccess>> {
		let now;
		let clock = match clock {
			Some(clock) => clock,
			None => {
				now = Datetime::now();
				&now
			}
		};
		Ok(Some(match self {
			BTreeAccess::Equality(operand) => KeyAccess::Equality(operand.resolve(clock)?),
			BTreeAccess::Range {
				range,
			} => {
				let bound = |bound: &Bound<BoundOperand>| -> Result<Bound<Value>> {
					Ok(match bound {
						Bound::Included(operand) => Bound::Included(operand.resolve(clock)?),
						Bound::Excluded(operand) => Bound::Excluded(operand.resolve(clock)?),
						Bound::Unbounded => Bound::Unbounded,
					})
				};
				KeyAccess::Range(Range {
					start: bound(&range.start)?,
					end: bound(&range.end)?,
				})
			}
			BTreeAccess::Compound {
				prefix,
				range,
				upper,
			} => {
				let bound = |bound: &Option<(BinaryOperator, BoundOperand)>| match bound {
					Some((op, operand)) => Ok(Some((op.clone(), operand.resolve(clock)?))),
					None => Ok::<_, anyhow::Error>(None),
				};
				KeyAccess::Compound {
					prefix: prefix
						.iter()
						.map(|operand| operand.resolve(clock))
						.collect::<Result<Vec<_>>>()?,
					range: bound(range)?,
					upper: bound(upper)?,
				}
			}
			BTreeAccess::FullText {
				..
			}
			| BTreeAccess::Knn {
				..
			} => return Ok(None),
		}))
	}

	/// Human-readable description of the access shape for EXPLAIN output.
	///
	/// Shared by [`crate::exec::operators::IndexScan`] and the bitmap
	/// candidate operators so the `access:` attribute renders identically.
	/// `FullText`/`Knn` shapes are described by their dedicated operators. A
	/// bound that reads the clock is rendered as the call, never as an
	/// instant.
	pub(crate) fn describe(&self) -> String {
		match self {
			BTreeAccess::Equality(v) => format!("= {}", v.describe()),
			BTreeAccess::Range {
				range,
			} => {
				let from_str = match &range.start {
					Bound::Included(x) => format!(">={}", x.describe()),
					Bound::Excluded(x) => format!(">{}", x.describe()),
					Bound::Unbounded => String::new(),
				};
				let to_str = match &range.end {
					Bound::Included(x) => format!("<={}", x.describe()),
					Bound::Excluded(x) => format!("<{}", x.describe()),
					Bound::Unbounded => String::new(),
				};
				format!("{from_str} {to_str}").trim().to_string()
			}
			BTreeAccess::Compound {
				prefix,
				range,
				upper,
			} => {
				let prefix_str =
					prefix.iter().map(BoundOperand::describe).collect::<Vec<_>>().join(", ");
				let mut described = format!("[{prefix_str}]");
				for (op, val) in range.iter().chain(upper) {
					described.push_str(&format!(" {op:?} {}", val.describe()));
				}
				described
			}
			BTreeAccess::FullText {
				query,
				..
			} => format!("@@ {query}"),
			BTreeAccess::Knn {
				k,
				..
			} => format!("knn {k}"),
		}
	}
}

/// Whether `access` leaves one of `cols` that fans out unpinned, so the key
/// range reaches one record through several index entries.
///
/// A column whose idiom flattens (`*`) holds one entry per element. The
/// columns an equality prefix pins each contribute a single matching entry —
/// duplicate elements encode to one key — so only the columns from the prefix
/// onwards can multiply. The column a trailing range merely bounds is among
/// them: several of its elements can satisfy the bound.
///
/// Callers use this for the two things entries-are-not-rows breaks: yielding a
/// record once per entry, and counting entries as rows.
pub(crate) fn access_fans_out(cols: &[Idiom], access: &BTreeAccess) -> bool {
	let pinned = match access {
		BTreeAccess::Compound {
			prefix,
			..
		} => prefix.len(),
		BTreeAccess::Equality(_) => 1,
		BTreeAccess::Range {
			..
		}
		| BTreeAccess::FullText {
			..
		}
		| BTreeAccess::Knn {
			..
		} => 0,
	};
	cols.iter().skip(pinned).any(|c| c.0.iter().any(|p| matches!(p, Part::All)))
}

/// Whether a scan of `index` over `access` may reach one record through
/// several entries, and so must list each record once.
///
/// Beyond what [`access_fans_out`] states for a column spelled with `*`, a
/// column the access leaves unpinned that may hold an array — no `*` or
/// flatten in its spelling, and not known to hold one entry per record
/// ([`IndexRef::one_entry_per_record`]) — stores a record holding an array
/// once per element. A column known to hold one entry per record needs no
/// dedupe.
pub(crate) fn access_repeats_records(index: &IndexRef, access: &BTreeAccess) -> bool {
	if matches!(index.definition().index, Index::Spatial) {
		if access.point_filter().is_some() {
			return false;
		}
		if matches!(
			access,
			BTreeAccess::Range { .. }
				| BTreeAccess::Compound {
					range: Some(_),
					..
				}
		) {
			return true;
		}
	}
	if access_fans_out(&index.cols, access) {
		return true;
	}
	let pinned = match access {
		BTreeAccess::Compound {
			prefix,
			..
		} => prefix.len(),
		BTreeAccess::Equality(_) => 1,
		BTreeAccess::Range {
			..
		}
		| BTreeAccess::FullText {
			..
		}
		| BTreeAccess::Knn {
			..
		} => 0,
	};
	(pinned..index.cols.len()).any(|col| index.may_hold_array(col))
}

/// Whether a scan of `index` over `access` would miss a record its range
/// admits: the range bounds a column that may hold an array
/// ([`IndexRef::may_hold_array`]) and reaches the band of the value order
/// arrays sort in, after every `Uuid` and before every `Set`.
///
/// A record holding an array sits at its elements' keys, not at the place its
/// whole value sorts, so a range admitting the whole array (`['ant'] > 'p'`)
/// reaches none of its entries (`'ant'`) unless an element happens to fall
/// inside it. A range clear of the band admits no array, so the entries it
/// does reach are rechecked against the whole value and the scan misses
/// nothing. A range bounded on neither side states no predicate and reaches
/// every record.
pub(crate) fn access_misses_arrays(index: &IndexRef, access: &BTreeAccess) -> bool {
	let (col, start, end): (usize, Bound<Cow<'_, Value>>, Bound<Cow<'_, Value>>) = match access {
		BTreeAccess::Range {
			range,
		} => {
			let (start, end) = range.witnesses();
			(0, start, end)
		}
		BTreeAccess::Compound {
			prefix,
			range: Some((op, value)),
			upper,
		} => {
			let value = value.witness();
			let (start, mut end) = match op {
				BinaryOperator::MoreThan => (Bound::Excluded(value), Bound::Unbounded),
				BinaryOperator::MoreThanEqual => (Bound::Included(value), Bound::Unbounded),
				BinaryOperator::LessThan => (Bound::Unbounded, Bound::Excluded(value)),
				BinaryOperator::LessThanEqual => (Bound::Unbounded, Bound::Included(value)),
				BinaryOperator::Subtract
				| BinaryOperator::Add
				| BinaryOperator::Multiply
				| BinaryOperator::Divide
				| BinaryOperator::Remainder
				| BinaryOperator::Power
				| BinaryOperator::Equal
				| BinaryOperator::ExactEqual
				| BinaryOperator::NotEqual
				| BinaryOperator::AllEqual
				| BinaryOperator::AnyEqual
				| BinaryOperator::Or
				| BinaryOperator::And
				| BinaryOperator::NullCoalescing
				| BinaryOperator::TenaryCondition
				| BinaryOperator::Contain
				| BinaryOperator::NotContain
				| BinaryOperator::ContainAll
				| BinaryOperator::ContainAny
				| BinaryOperator::ContainNone
				| BinaryOperator::Inside
				| BinaryOperator::NotInside
				| BinaryOperator::AllInside
				| BinaryOperator::AnyInside
				| BinaryOperator::NoneInside
				| BinaryOperator::Outside
				| BinaryOperator::Intersects
				| BinaryOperator::Range
				| BinaryOperator::RangeInclusive
				| BinaryOperator::RangeSkip
				| BinaryOperator::RangeSkipInclusive
				| BinaryOperator::Matches(_)
				| BinaryOperator::NearestNeighbor(_) => return false,
			};
			if let Some((op, value)) = upper {
				let value = value.witness();
				if matches!(op, BinaryOperator::LessThan) {
					end = Bound::Excluded(value);
				} else if matches!(op, BinaryOperator::LessThanEqual) {
					end = Bound::Included(value);
				} else {
					return false;
				}
			}
			(prefix.len(), start, end)
		}
		BTreeAccess::Equality(_)
		| BTreeAccess::Compound {
			..
		}
		| BTreeAccess::FullText {
			..
		}
		| BTreeAccess::Knn {
			..
		} => return false,
	};
	if matches!((&start, &end), (Bound::Unbounded, Bound::Unbounded)) || !index.may_hold_array(col)
	{
		return false;
	}
	range_reaches_arrays(start.as_ref().map(|v| &**v), end.as_ref().map(|v| &**v))
}

/// Whether the keys `access` reads over `index` hold exactly the values its
/// predicate admits, for a column holding one entry per record: every number
/// the access names, as an equality, a compound prefix or a range bound, is
/// keyed where it compares ([`number_keyed_as_compared`]). Otherwise the
/// access reads keys that miss records its comparison accepts, or lists
/// records it rejects, and the comparison has to judge each one.
pub(crate) fn range_matches_comparison(index: &IndexDefinition, access: &BTreeAccess) -> bool {
	let normalised = spells_normalised_decimal_keys(index.format_version);
	// A bound that reads the clock is a datetime, which is no number.
	let inexact = |v: &BoundOperand| matches!(v.as_literal(), Some(Value::Number(n)) if !number_keyed_as_compared(n, normalised));
	match access {
		BTreeAccess::Equality(v) => !inexact(v),
		BTreeAccess::Range {
			range,
		} => !range.operands().any(inexact),
		BTreeAccess::Compound {
			prefix,
			range,
			upper,
		} => !prefix.iter().any(inexact) && range.iter().chain(upper).all(|(_, v)| !inexact(v)),
		BTreeAccess::FullText {
			..
		}
		| BTreeAccess::Knn {
			..
		} => true,
	}
}

/// Whether every number that compares equal to `n` is keyed at `n`'s own key,
/// in an index whose keys spell decimals without trailing zeros when
/// `normalised` ([`spells_normalised_decimal_keys`]). `n` must be the same
/// number as a float and as a decimal ([`exact_across_kinds`]), and the index
/// must spell decimals without trailing zeros: one that keeps them keys
/// `2.50dec` apart from `2.5dec`, and `2.0dec` apart from `2`, though each
/// pair compares equal.
pub(crate) fn number_keyed_as_compared(n: &Number, normalised: bool) -> bool {
	normalised && exact_across_kinds(n)
}

/// Whether `n` is the same number as a float and as a decimal: an integer, or
/// a finite float or a decimal whose value a float and a decimal both hold
/// exactly (`1.5`, not `0.1`). A comparison between a float and a decimal is
/// exact, and a number's key holds its exact value, so the key of such a
/// number is the key of every float and decimal equal to it.
pub(crate) fn exact_across_kinds(n: &Number) -> bool {
	use rust_decimal::Decimal;
	use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
	match n {
		Number::Int(_)
		| Number::I8(_)
		| Number::U8(_)
		| Number::I16(_)
		| Number::U16(_)
		| Number::I32(_)
		| Number::U32(_)
		| Number::U64(_) => true,
		Number::Float(f) => {
			f.is_finite()
				&& Decimal::from_f64_retain(*f).is_some_and(|d| Decimal::from_f64(*f) == Some(d))
		}
		Number::F32(f) => {
			let f = f64::from(*f);
			f.is_finite()
				&& Decimal::from_f64_retain(f).is_some_and(|d| Decimal::from_f64(f) == Some(d))
		}
		Number::F16(f) => {
			let f = (*f).to_f64();
			f.is_finite()
				&& Decimal::from_f64_retain(f).is_some_and(|d| Decimal::from_f64(f) == Some(d))
		}
		Number::Decimal(d) => d.to_f64().is_some_and(|f| Decimal::from_f64_retain(f) == Some(*d)),
		Number::BigDecimal(d) => {
			surrealdb_types::BigDecimal::try_from_f64(d.to_f64()).is_ok_and(|f| f == **d)
		}
	}
}

/// Whether some array lies between `start` and `end` in the value order a
/// comparison reads. Every array sorts at or after the empty array and before
/// the empty set.
fn range_reaches_arrays(start: Bound<&Value>, end: Bound<&Value>) -> bool {
	let starts_before_band_end = match start {
		Bound::Unbounded => true,
		Bound::Included(v) | Bound::Excluded(v) => *v < Value::Set(crate::val::Set::new()),
	};
	let empty = Value::Array(crate::val::Array::new());
	let ends_after_band_start = match end {
		Bound::Unbounded => true,
		Bound::Included(v) => *v >= empty,
		Bound::Excluded(v) => *v > empty,
	};
	starts_before_band_end && ends_after_band_start
}

/// `indexes` as access-path selection has to see them: every b-tree column
/// whose declared kind holds only arrays is spelled as the element column it
/// behaves as, and reported in [`ElementColumns::rewritten`].
///
/// The index writer expands an array value into one entry per element in any
/// column whose idiom does not end in a flatten, so `FIELDS tags` stores what
/// `FIELDS tags.*` stores whenever `tags` holds an array. Each entry then
/// carries one element, not the value a comparison reads, and the planner's
/// fan-out rules all key off `*` in the column: which predicates the column
/// answers (containment, not equality or ranges over the whole value),
/// whether a scan needs a record dedupe, which ordering it claims, and whether
/// counting its entries counts rows. Appending `*` to such a column puts it
/// under those rules.
///
/// Only a column whose declared kind admits nothing but arrays, `NONE` and
/// `NULL` is rewritten. The kind is the column's own field definition, or the
/// object literal type of a declared ancestor (`metadata TYPE { tags:
/// array<string> }` declares `metadata.tags`). A column already holding `*`
/// or a flatten is left alone: `items[*].tags` stores each item's array whole,
/// one entry per item, so its entries are not the arrays' elements. A kind
/// that also admits a scalar (`any`, or a union such as
/// `array<string> | string`) is taken as written, as is a column no
/// declaration reaches. A set is a distinct value the writer stores whole, so
/// `set<…>` is taken as written too.
///
/// A rewritten column is not one a containment leaf may seek. A declared kind
/// is not a guarantee about stored records: `DEFINE FIELD` does not rewrite the
/// records already there and an import skips coercion, so a string stored
/// before the kind was declared stays indexed whole, and `CONTAINS` on it is a
/// substring test an element seek cannot answer. The analyzer is given the
/// rewritten columns (see [`ElementColumns::analyzer`]) and answers
/// containment over them by scan; an index declared over elements (`tags.*`)
/// is what serves containment from the index.
///
/// A column that flattens a value declared a set (`tags.*` over `TYPE
/// set<string>`, or `meta.tags.*` where `meta` is `TYPE { tags: set<string>
/// }`) is not one a containment leaf may seek either, and is reported in
/// [`ElementColumns::sets`]. The writer stores a set whole under such a column,
/// one entry per record keyed by the entire set, so an element seek meets no
/// key. The column, and the index, still serve every other access: a compound
/// index's columns before it keep answering their own predicates. Only the
/// declared kind identifies the set, so one stored in a field declared `any`,
/// not declared at all, or redeclared as another kind after it was written is
/// not detected.
///
/// The rewrite is for planning only. Scans encode their keys from the index id
/// and the query's values; the column idioms reach execution only as the
/// ordering and constant-field claims, which this makes accurate. Returns
/// `indexes` itself when no column changes.
pub(crate) fn with_array_columns_as_elements(
	indexes: Arc<[IndexDefinition]>,
	fields: &[FieldDefinition],
	shapes: &TableIndexShapes,
) -> ElementColumns {
	let as_elements = |col: &Idiom| -> bool {
		!col.0.iter().any(|p| matches!(p, Part::All | Part::Flatten))
			&& declared_kind(&col.0, fields).is_some_and(kind_holds_only_arrays)
	};
	// Built only once a column changes, so an unchanged list is neither
	// cloned nor reallocated. The definitions and their rewritten-column flags
	// are pushed together, so the two stay aligned index for index.
	let mut changed: Option<(Vec<IndexDefinition>, Vec<ColumnFlags>)> = None;
	for (i, ix) in indexes.iter().enumerate() {
		let flags: Box<[bool]> = match ix.index {
			Index::Idx | Index::Uniq => ix.cols.iter().map(as_elements).collect(),
			Index::Spatial if ix.cols.len() > 1 => ix
				.cols
				.iter()
				.enumerate()
				.map(|(col_idx, col)| col_idx + 1 < ix.cols.len() && as_elements(col))
				.collect(),
			Index::Hnsw(_)
			| Index::FullText(_)
			| Index::Count(_)
			| Index::DiskAnn(_)
			| Index::Spatial => Box::default(),
		};
		if flags.contains(&true) {
			let (defs, masks) = changed.get_or_insert_with(|| {
				(indexes[..i].to_vec(), indexes[..i].iter().map(|_| Box::default()).collect())
			});
			let mut cols = ix.cols.clone();
			for (col, _) in cols.iter_mut().zip(flags.iter()).filter(|(_, rewrite)| **rewrite) {
				col.0.push(Part::All);
			}
			defs.push(IndexDefinition {
				cols,
				..ix.clone()
			});
			masks.push(flags);
		} else if let Some((defs, masks)) = changed.as_mut() {
			defs.push(ix.clone());
			masks.push(Box::default());
		}
	}
	// A plain column the index has recorded storing no array in keeps one
	// entry per record; its declared kind is not consulted (see
	// `IndexRef::one_entry_per_record`).
	let scalar: Vec<ColumnFlags> = indexes
		.iter()
		.map(|ix| {
			if !matches!(ix.index, Index::Idx | Index::Uniq) {
				return Box::default();
			}
			let stored = shapes.of(ix.index_id);
			ix.cols
				.iter()
				.enumerate()
				.map(|(i, col)| {
					is_plain_column(col)
						&& stored.is_some_and(|s| !s.may_hold(i, ValueShape::Array))
				})
				.collect()
		})
		.collect();
	let scalar = scalar.iter().any(|flags| flags.contains(&true)).then(|| scalar.into());
	let sets: Vec<ColumnFlags> = indexes
		.iter()
		.map(|ix| match ix.index {
			Index::Idx | Index::Uniq => {
				ix.cols.iter().map(|col| flattens_a_set(col, fields)).collect()
			}
			Index::Spatial if ix.cols.len() > 1 => ix
				.cols
				.iter()
				.enumerate()
				.map(|(col_idx, col)| col_idx + 1 < ix.cols.len() && flattens_a_set(col, fields))
				.collect(),
			Index::Hnsw(_)
			| Index::FullText(_)
			| Index::Count(_)
			| Index::DiskAnn(_)
			| Index::Spatial => Box::default(),
		})
		.collect();
	let sets = sets.iter().any(|flags| flags.contains(&true)).then(|| sets.into());
	// Derived from `shapes`, which `Planner::trusting_index_shapes(op.read_only())` supplies only
	// for read-only programs (`SELECT` / `RETURN`): mutating statements (`UPDATE`, `DELETE`,
	// `UPSERT`) and multi-statement write blocks intentionally receive empty `shapes` so they
	// retain ancestor cell probes and `DistinctRecords` deduplication across concurrent writes.
	let point_only_spatial: Vec<bool> = indexes
		.iter()
		.map(|ix| {
			if !matches!(ix.index, Index::Spatial) {
				return false;
			}
			let Some((last_idx, last_col)) = ix.cols.iter().enumerate().next_back() else {
				return false;
			};
			let stored = shapes.of(ix.index_id);
			is_plain_column(last_col)
				&& stored.is_some_and(|s| {
					!s.may_hold(last_idx, ValueShape::MultiCell)
						&& !s.may_hold(last_idx, ValueShape::Array)
				})
		})
		.collect();
	let point_only_spatial = point_only_spatial.contains(&true).then(|| point_only_spatial.into());
	let non_geometry_spatial: Vec<bool> = indexes
		.iter()
		.map(|ix| {
			if !matches!(ix.index, Index::Spatial) {
				return false;
			}
			let Some(last_idx) = ix.cols.len().checked_sub(1) else {
				return false;
			};
			let stored = shapes.of(ix.index_id);
			stored.is_some_and(|s| s.may_hold(last_idx, ValueShape::NonGeometry))
		})
		.collect();
	let non_geometry_spatial =
		non_geometry_spatial.contains(&true).then(|| non_geometry_spatial.into());
	// A vector index holds no entry for a record whose vector is `NONE` or `NULL`, and ranks an
	// array of arrays as the one vector it flattens to; only one tracked to have stored neither
	// ranks every record of its table as `vector::distance::*` measures it.
	let complete_vector: Vec<bool> = indexes
		.iter()
		.map(|ix| {
			matches!(ix.index, Index::Hnsw(_) | Index::DiskAnn(_))
				&& shapes.of(ix.index_id).is_some_and(|s| {
					!s.may_hold(0, ValueShape::Absent) && !s.may_hold(0, ValueShape::Nested)
				})
		})
		.collect();
	let complete_vector = complete_vector.contains(&true).then(|| complete_vector.into());
	// An element column the index has recorded storing nothing whole holds
	// every record under its elements or under `NONE` / `NULL`.
	let elements_or_nullish: Vec<ColumnFlags> = indexes
		.iter()
		.map(|ix| {
			if !matches!(ix.index, Index::Idx | Index::Uniq) {
				return Box::default();
			}
			let stored = shapes.of(ix.index_id);
			ix.cols
				.iter()
				.enumerate()
				.map(|(i, col)| {
					takes_each_of_fields(col)
						&& stored.is_some_and(|s| !s.may_hold(i, ValueShape::Whole))
				})
				.collect()
		})
		.collect();
	let elements_or_nullish = elements_or_nullish
		.iter()
		.any(|flags| flags.contains(&true))
		.then(|| elements_or_nullish.into());
	match changed {
		Some((defs, masks)) => ElementColumns {
			indexes: defs.into(),
			rewritten: Some(masks.into()),
			scalar,
			sets,
			point_only_spatial,
			non_geometry_spatial,
			complete_vector,
			elements_or_nullish,
		},
		None => ElementColumns {
			scalar,
			sets,
			point_only_spatial,
			non_geometry_spatial,
			complete_vector,
			elements_or_nullish,
			..ElementColumns::unchanged(indexes)
		},
	}
}

/// Whether `col` is spelled with no `*` or flatten: the writer lists an
/// array value in it once per element, and stores any other value whole.
fn is_plain_column(col: &Idiom) -> bool {
	!col.0.iter().any(|p| matches!(p, Part::All | Part::Flatten))
}

/// Whether `col` takes every element of a path of plain fields (`tags.*`,
/// `a.b.*`): the writer lists an array there once per element, and any other
/// value at the path makes the column a whole value or `NONE`.
fn takes_each_of_fields(col: &Idiom) -> bool {
	match col.0.split_last() {
		Some((Part::All, path)) => {
			!path.is_empty() && path.iter().all(|p| matches!(p, Part::Field(_)))
		}
		_ => false,
	}
}

/// Whether `col` flattens (`*`) a value that may be a set: one whose declared
/// kind admits a set, or one a computed step produces.
fn flattens_a_set(col: &Idiom, fields: &[FieldDefinition]) -> bool {
	col.0.iter().enumerate().any(|(i, part)| {
		matches!(part, Part::All)
			&& (computes(&col.0[..i]) || declared_admits_set(&col.0[..i], fields))
	})
}

/// Whether `path` holds a step whose result no declaration describes: a
/// method call (`.to_set()`), a graph lookup, a recursion, a destructure or
/// an expression start. Its value may be a set whatever the fields declare.
fn computes(path: &[Part]) -> bool {
	path.iter().any(|part| {
		matches!(
			part,
			Part::Method(..)
				| Part::Lookup(_)
				| Part::Recurse(..)
				| Part::RepeatRecurse
				| Part::Destructure(_)
				| Part::Start(_)
		)
	})
}

/// Whether the value at `path` may be a set by what `fields` declare: the
/// kind of the field named `path`, or any alternative of an ancestor's type
/// that reaches `path` (see [`kind_at_admits_set`]). Every declaration is
/// read: a field's own kind (`any`, or none) does not override an ancestor
/// declaring a set at the path, since the ancestor's kind is applied to the
/// value too. Unlike [`declared_kind`], a union of literal types is read
/// through every alternative.
fn declared_admits_set(path: &[Part], fields: &[FieldDefinition]) -> bool {
	(1..=path.len()).rev().any(|n| {
		fields
			.iter()
			.find(|fd| names_path(fd, &path[..n]))
			.and_then(|fd| fd.field_kind.as_ref())
			.is_some_and(|kind| kind_at_admits_set(kind, &path[n..]))
	})
}

/// Whether `fd` is declared at `path`. An optional part (`.?`) reads the value
/// it follows unchanged, so it is skipped: `meta.?.tags` is declared by a
/// field named `meta.tags`.
fn names_path(fd: &FieldDefinition, path: &[Part]) -> bool {
	fd.name.0.iter().eq(path.iter().filter(|part| !matches!(part, Part::Optional)))
}

/// Whether some value of `kind`, read at `path` through object literal,
/// tuple and collection types, is a set; every alternative of a union is
/// read. An optional part (`.?`) reads the value it follows unchanged. A
/// field read on an array is read on its elements, and any part read through a
/// set yields a set, since paths map over a set's elements and collect the
/// results back into a set. A part not read here may yield anything, so it is
/// taken to yield a set: a seek it would allow could miss rows.
fn kind_at_admits_set(kind: &Kind, path: &[Part]) -> bool {
	let Some((part, rest)) = path.split_first() else {
		return kind_admits_set(kind);
	};
	if matches!(part, Part::Optional) {
		return kind_at_admits_set(kind, rest);
	}
	match kind {
		Kind::Either(kinds) => kinds.iter().any(|kind| kind_at_admits_set(kind, path)),
		// A field read yields that field's value; `.*` leaves an object
		// unchanged.
		Kind::Literal(KindLiteral::Object(fields)) => {
			if let Part::Field(name) = part {
				fields.get(name.as_str()).is_some_and(|kind| kind_at_admits_set(kind, rest))
			} else if let Part::All = part {
				kind_at_admits_set(kind, rest)
			} else {
				true
			}
		}
		// A position (`[0]`, `[$]`, a number) selects one element. Any other
		// part read through a set is read on each element and the results are
		// collected back into a set, so the value is a set.
		Kind::Set(element, _) => {
			if matches!(part, Part::First | Part::Last)
				|| matches!(
					part,
					Part::Value(crate::expr::Expr::Literal(
						crate::expr::Literal::Integer(_)
							| crate::expr::Literal::Float(_)
							| crate::expr::Literal::Decimal(_)
							| crate::expr::Literal::I8(_)
							| crate::expr::Literal::U8(_)
							| crate::expr::Literal::I16(_)
							| crate::expr::Literal::U16(_)
							| crate::expr::Literal::I32(_)
							| crate::expr::Literal::U32(_)
							| crate::expr::Literal::U64(_)
							| crate::expr::Literal::F16(_)
							| crate::expr::Literal::F32(_)
					))
				) {
				kind_at_admits_set(element, rest)
			} else {
				true
			}
		}
		// `.*` and a position (any index expression, `[0]`, `[0.0]` or a
		// computed one) yield elements; a field read maps over them; a filter
		// keeps the array.
		Kind::Array(element, _) => match part {
			Part::All | Part::First | Part::Last | Part::Value(_) => {
				kind_at_admits_set(element, rest)
			}
			Part::Field(_) => kind_at_admits_set(element, path),
			Part::Where(_) => kind_at_admits_set(kind, rest),
			Part::Flatten
			| Part::Lookup(_)
			| Part::Start(_)
			| Part::Method(..)
			| Part::Destructure(_)
			| Part::Optional
			| Part::Recurse(..)
			| Part::Doc
			| Part::RepeatRecurse => true,
		},
		// A tuple reads its declared element at a known position, and may
		// yield any element at a position known only when the query runs.
		Kind::Literal(KindLiteral::Array(kinds)) => match tuple_position(part, kinds.len()) {
			Some(Some(position)) => {
				kinds.get(position).is_some_and(|kind| kind_at_admits_set(kind, rest))
			}
			Some(None) => kinds.iter().any(|kind| kind_at_admits_set(kind, rest)),
			None => {
				if let Part::Field(_) = part {
					kinds.iter().any(|kind| kind_at_admits_set(kind, path))
				} else if let Part::Where(_) = part {
					kind_at_admits_set(kind, rest)
				} else {
					true
				}
			}
		},
		Kind::Any
		| Kind::Named(_)
		| Kind::None
		| Kind::Null
		| Kind::Bool
		| Kind::Bytes
		| Kind::Datetime
		| Kind::Date
		| Kind::Time
		| Kind::Timestamp
		| Kind::Interval
		| Kind::Decimal
		| Kind::BigDecimal
		| Kind::Duration
		| Kind::Float
		| Kind::Int
		| Kind::I8
		| Kind::U8
		| Kind::I16
		| Kind::U16
		| Kind::I32
		| Kind::U32
		| Kind::U64
		| Kind::F16
		| Kind::F32
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
		| Kind::Literal(_)
		| Kind::File(_)
		| Kind::Enum(_) => false,
	}
}

/// The position `part` reads in a tuple of `len` elements: `Some(Some(n))`
/// for a literal position, `Some(None)` for one known only when the query
/// runs (`.*`, a computed or non-integer index), `None` when `part` reads no
/// position.
fn tuple_position(part: &Part, len: usize) -> Option<Option<usize>> {
	use crate::expr::{Expr, Literal};
	match part {
		Part::First => Some(Some(0)),
		Part::Last => Some(len.checked_sub(1)),
		Part::Value(Expr::Literal(Literal::Integer(i))) => Some(usize::try_from(*i).ok()),
		Part::Value(Expr::Literal(Literal::I8(i))) => Some(usize::try_from(*i).ok()),
		Part::Value(Expr::Literal(Literal::U8(u))) => Some(Some(usize::from(*u))),
		Part::Value(Expr::Literal(Literal::I16(i))) => Some(usize::try_from(*i).ok()),
		Part::Value(Expr::Literal(Literal::U16(u))) => Some(Some(usize::from(*u))),
		Part::Value(Expr::Literal(Literal::I32(i))) => Some(usize::try_from(*i).ok()),
		Part::Value(Expr::Literal(Literal::U32(u))) => Some(usize::try_from(*u).ok()),
		Part::Value(Expr::Literal(Literal::U64(u))) => Some(usize::try_from(*u).ok()),
		Part::All | Part::Value(_) => Some(None),
		Part::Flatten
		| Part::Field(_)
		| Part::Where(_)
		| Part::Lookup(_)
		| Part::Start(_)
		| Part::Method(..)
		| Part::Destructure(_)
		| Part::Optional
		| Part::Recurse(..)
		| Part::Doc
		| Part::RepeatRecurse => None,
	}
}

/// Whether some value of `kind` is a set. Unions are read through at any
/// depth.
fn kind_admits_set(kind: &Kind) -> bool {
	match kind {
		Kind::Set(..) => true,
		Kind::Either(kinds) => kinds.iter().any(kind_admits_set),
		Kind::Any
		| Kind::Named(_)
		| Kind::None
		| Kind::Null
		| Kind::Bool
		| Kind::Bytes
		| Kind::Datetime
		| Kind::Date
		| Kind::Time
		| Kind::Timestamp
		| Kind::Interval
		| Kind::Decimal
		| Kind::BigDecimal
		| Kind::Duration
		| Kind::Float
		| Kind::Int
		| Kind::I8
		| Kind::U8
		| Kind::I16
		| Kind::U16
		| Kind::I32
		| Kind::U32
		| Kind::U64
		| Kind::F16
		| Kind::F32
		| Kind::Number
		| Kind::Object
		| Kind::String
		| Kind::Uuid
		| Kind::Regex
		| Kind::Range
		| Kind::Table(_)
		| Kind::Record(_)
		| Kind::Geometry(_)
		| Kind::File(_)
		| Kind::Enum(_)
		| Kind::Function(..)
		| Kind::Array(..)
		| Kind::Literal(_) => false,
	}
}

/// The union of seeks answering a containment over an element column with
/// its nullish keys ([`IndexAnalyzer::try_nullish_containment`]), where it
/// is worth reading in place of the table.
///
/// The union fetches every record it yields to judge it, where a scan of
/// the table rejects most records from their stored bytes. Under a `LIMIT`
/// with no `ORDER BY` the table scan stops as early as the union and reads
/// sequentially, and under an `ORDER BY` the union does not deliver the
/// sort reads every match either way; the union is taken where it delivers
/// the `ORDER BY` (each branch pins the index's leading column, and the
/// ordering is its next one), and where neither is asked.
fn nullish_containment(
	analyzer: &crate::exec::index::analysis::IndexAnalyzer<'_>,
	cond: Option<&LogicalExpr>,
	order: Option<&LogicalOrdering>,
	has_limit: bool,
	direction: Direction,
) -> Option<AccessPath> {
	let path = analyzer.try_nullish_containment(cond, direction)?;
	let delivers_order = matches!(
		&path,
		AccessPath::Union {
			paths,
			..
		} if detect_order_for_composite_union(order, paths).is_some()
	);
	(delivers_order || (order.is_none() && !has_limit)).then_some(path)
}

/// One index's per-column flags: whether each column was rewritten.
pub(crate) type ColumnFlags = Box<[bool]>;

/// What [`with_array_columns_as_elements`] made of a table's indexes.
#[derive(Clone, Debug)]
pub(crate) struct ElementColumns {
	/// The indexes, with every rewritten column spelled with `*`.
	pub(crate) indexes: Arc<[IndexDefinition]>,
	/// Per index, in order, whether each column was rewritten; a column past
	/// the end of its index's flags was not. `None` when none was.
	pub(crate) rewritten: Option<Arc<[ColumnFlags]>>,
	/// Per index, in order, whether each column is one the index has recorded
	/// storing no array in; a column past the end of its index's flags is not.
	/// `None` when none is, or when the shapes were not read.
	pub(crate) scalar: Option<Arc<[ColumnFlags]>>,
	/// Per index, in order, whether each column flattens a value declared a
	/// set; a column past the end of its index's flags does not. `None` when
	/// none does, or when the fields were not read.
	pub(crate) sets: Option<Arc<[ColumnFlags]>>,
	/// Per index, in order, whether a `SPATIAL` index column is known from
	/// recorded [`super::super::datastore::index_shapes::TableIndexShapes`] to
	/// have stored only single-cell point geometries (neither multi-cell
	/// coverings recorded under
	/// [`super::super::datastore::index_shapes::ValueShape::MultiCell`] nor
	/// array/set geometries recorded under
	/// [`super::super::datastore::index_shapes::ValueShape::Array`]).
	pub(crate) point_only_spatial: Option<Arc<[bool]>>,
	/// Per index, in order, whether a `SPATIAL` index column may hold
	/// non-geometry values (including `NONE` and `NULL`, recorded under
	/// [`super::super::datastore::index_shapes::ValueShape::NonGeometry`]),
	/// which emit no spatial index cells and would cause `geo::distance` to
	/// raise a type error on a table scan.
	pub(crate) non_geometry_spatial: Option<Arc<[bool]>>,
	/// Per index, in order, whether an HNSW or DiskANN index is known from
	/// recorded [`super::super::datastore::index_shapes::TableIndexShapes`] to
	/// rank every record as `vector::distance::*` measures it: its vector column
	/// has stored no `NONE` or `NULL` (recorded under
	/// [`super::super::datastore::index_shapes::ValueShape::Absent`]) and no
	/// array of arrays (recorded under
	/// [`super::super::datastore::index_shapes::ValueShape::Nested`]). `None`
	/// when none is, or when the shapes were not read.
	pub(crate) complete_vector: Option<Arc<[bool]>>,
	/// Per index, in order, whether each column takes every element of a
	/// path of plain fields and is known from the recorded shapes to have
	/// stored nothing whole: every record whose value at the path contains a
	/// value is listed under that value, or under `NONE` or `NULL` when the
	/// value at the path is a scalar, `NONE`, `NULL` or an empty array. A
	/// column past the end of its index's flags is not. `None` when none is,
	/// or when the shapes were not read.
	pub(crate) elements_or_nullish: Option<Arc<[ColumnFlags]>>,
}

impl ElementColumns {
	/// The indexes unchanged, for a caller that rewrites nothing.
	pub(crate) fn unchanged(indexes: Arc<[IndexDefinition]>) -> Self {
		Self {
			indexes,
			rewritten: None,
			scalar: None,
			sets: None,
			point_only_spatial: None,
			non_geometry_spatial: None,
			complete_vector: None,
			elements_or_nullish: None,
		}
	}

	/// These columns with every b-tree column that flattens one of
	/// `prefixes` also reported in [`Self::sets`]: a column holding `*` right
	/// after a path in `prefixes`. The prefixes are those
	/// [`foreign_set_prefixes`] finds declared a set on another table.
	pub(crate) fn withholding_sets_at(mut self, prefixes: &[Vec<Part>]) -> Self {
		if prefixes.is_empty() {
			return self;
		}
		let at = |col: &Idiom| {
			col.0.iter().enumerate().any(|(i, part)| {
				matches!(part, Part::All) && prefixes.iter().any(|p| p.as_slice() == &col.0[..i])
			})
		};
		let sets: Vec<ColumnFlags> = self
			.indexes
			.iter()
			.enumerate()
			.map(|(i, ix)| {
				let known = self.sets.as_ref().and_then(|sets| sets.get(i));
				match ix.index {
					Index::Idx | Index::Uniq => ix
						.cols
						.iter()
						.enumerate()
						.map(|(c, col)| {
							known.and_then(|flags| flags.get(c)).copied().unwrap_or(false)
								|| at(col)
						})
						.collect(),
					Index::Spatial if ix.cols.len() > 1 => ix
						.cols
						.iter()
						.enumerate()
						.map(|(c, col)| {
							c + 1 < ix.cols.len()
								&& (known.and_then(|flags| flags.get(c)).copied().unwrap_or(false)
									|| at(col))
						})
						.collect(),
					Index::Hnsw(_)
					| Index::FullText(_)
					| Index::Count(_)
					| Index::DiskAnn(_)
					| Index::Spatial => Box::default(),
				}
			})
			.collect();
		self.sets = sets.iter().any(|flags| flags.contains(&true)).then(|| sets.into());
		self
	}

	/// An analyzer over these indexes that does not seek a rewritten column,
	/// or one that flattens a set, for containment.
	pub(crate) fn analyzer<'a>(
		&self,
		with: Option<&'a With>,
		registry: &'a crate::exec::function::FunctionRegistry,
	) -> crate::exec::index::analysis::IndexAnalyzer<'a> {
		let analyzer = crate::exec::index::analysis::IndexAnalyzer::new(
			Arc::clone(&self.indexes),
			with,
			registry,
		);
		let analyzer = match &self.scalar {
			Some(scalar) => analyzer.with_scalar_columns(Arc::clone(scalar)),
			None => analyzer,
		};
		let analyzer = match &self.point_only_spatial {
			Some(flags) => analyzer.with_point_only_spatial(Arc::clone(flags)),
			None => analyzer,
		};
		let analyzer = match &self.non_geometry_spatial {
			Some(flags) => analyzer.with_non_geometry_spatial(Arc::clone(flags)),
			None => analyzer,
		};
		let analyzer = match &self.complete_vector {
			Some(flags) => analyzer.with_complete_vector(Arc::clone(flags)),
			None => analyzer,
		};
		let analyzer = match &self.elements_or_nullish {
			Some(flags) => analyzer.with_elements_or_nullish(Arc::clone(flags)),
			None => analyzer,
		};
		match (&self.rewritten, &self.sets) {
			(Some(rewritten), Some(sets)) => {
				let either = |a: &ColumnFlags, b: &ColumnFlags| -> ColumnFlags {
					(0..a.len().max(b.len()))
						.map(|c| {
							a.get(c).copied().unwrap_or(false) || b.get(c).copied().unwrap_or(false)
						})
						.collect()
				};
				let merged: Vec<ColumnFlags> =
					rewritten.iter().zip(sets.iter()).map(|(a, b)| either(a, b)).collect();
				analyzer.without_containment_on(merged.into())
			}
			(Some(flags), None) | (None, Some(flags)) => {
				analyzer.without_containment_on(Arc::clone(flags))
			}
			(None, None) => analyzer,
		}
	}
}

/// Whether [`with_array_columns_as_elements`] could change, withhold or learn
/// anything of a column of `indexes`: some b-tree index has a column holding no
/// flatten, which may be rewritten, or one holding `*`, which may flatten a set,
/// or some vector index may be known to hold every record
/// ([`ElementColumns::complete_vector`]). Callers skip reading the field list
/// and the shapes when it could not.
pub(crate) fn may_have_array_columns(indexes: &[IndexDefinition]) -> bool {
	indexes.iter().any(|ix| match ix.index {
		Index::Idx | Index::Uniq | Index::Spatial => ix.cols.iter().any(|c| {
			c.0.iter().any(|p| matches!(p, Part::All))
				|| !c.0.iter().any(|p| matches!(p, Part::Flatten))
		}),
		Index::Hnsw(_) | Index::DiskAnn(_) => true,
		Index::FullText(_) | Index::Count(_) => false,
	})
}

/// How many record links, and views over a source table, the search for a set
/// declared on another table follows from one column. A column whose path
/// reaches past that many is taken to flatten a set.
const FOREIGN_SET_DEPTH: usize = 4;

/// The paths before a `*` in the b-tree columns of `indexes` whose value is
/// declared a set on another table: a path that reads through a field
/// declared a record link (`r.tags.*` with `r: record<o>` and `tags` on `o`
/// declared `set<…>`), or a field of a view that projects a source table's
/// field under that name (`s.*` on `v` for `DEFINE TABLE v AS SELECT s FROM
/// src`). A declaration of the path on `table` itself does not end the
/// search, since it does not change the value read through the link or view.
///
/// [`with_array_columns_as_elements`] reads only `table`'s own fields; this
/// reads `table`'s definition and the linked and source tables' definitions,
/// for each `*` column whose path `table`'s fields do not already declare a
/// set. The result goes to [`ElementColumns::withholding_sets_at`].
pub(crate) async fn foreign_set_prefixes(
	txn: &crate::kvs::Transaction,
	ns: crate::catalog::NamespaceId,
	db: crate::catalog::DatabaseId,
	table: &crate::val::TableName,
	indexes: &[IndexDefinition],
	fields: &[FieldDefinition],
	version: Option<u64>,
) -> anyhow::Result<Vec<Vec<Part>>> {
	let mut found: Vec<Vec<Part>> = Vec::new();
	let mut checked: Vec<&[Part]> = Vec::new();
	let mut reach = Reach {
		txn,
		ns,
		db,
		version,
		fields: std::collections::HashMap::new(),
		definitions: std::collections::HashMap::new(),
	};
	for ix in indexes {
		let cols: &[Idiom] = match ix.index {
			Index::Idx | Index::Uniq => &ix.cols,
			Index::Spatial if ix.cols.len() > 1 => &ix.cols[..ix.cols.len() - 1],
			Index::Hnsw(_)
			| Index::FullText(_)
			| Index::Count(_)
			| Index::DiskAnn(_)
			| Index::Spatial => continue,
		};
		for col in cols {
			for (i, part) in col.0.iter().enumerate() {
				let prefix = &col.0[..i];
				if !matches!(part, Part::All)
					|| prefix.is_empty()
					|| checked.contains(&prefix)
					|| declared_admits_set(prefix, fields)
				{
					continue;
				}
				checked.push(prefix);
				if reach.admits_set_elsewhere(table, prefix, fields, FOREIGN_SET_DEPTH).await? {
					found.push(prefix.to_vec());
				}
			}
		}
	}
	Ok(found)
}

/// The catalog [`foreign_set_prefixes`] reads other tables' definitions from,
/// each table's field list and definition read at most once per run.
struct Reach<'a> {
	txn: &'a crate::kvs::Transaction,
	ns: crate::catalog::NamespaceId,
	db: crate::catalog::DatabaseId,
	version: Option<u64>,
	fields: std::collections::HashMap<crate::val::TableName, Arc<[FieldDefinition]>>,
	definitions: std::collections::HashMap<
		crate::val::TableName,
		Option<Arc<crate::catalog::TableDefinition>>,
	>,
}

impl Reach<'_> {
	/// The field list of `table`.
	async fn fields(
		&mut self,
		table: &crate::val::TableName,
	) -> anyhow::Result<Arc<[FieldDefinition]>> {
		use crate::catalog::providers::TableProvider;
		if let Some(fields) = self.fields.get(table) {
			return Ok(Arc::clone(fields));
		}
		let fields = self.txn.all_tb_fields(self.ns, self.db, table, self.version).await?;
		self.fields.insert(table.clone(), Arc::clone(&fields));
		Ok(fields)
	}

	/// The definition of `table`, `None` when the catalog holds none.
	async fn definition(
		&mut self,
		table: &crate::val::TableName,
	) -> anyhow::Result<Option<Arc<crate::catalog::TableDefinition>>> {
		use crate::catalog::providers::TableProvider;
		if let Some(definition) = self.definitions.get(table) {
			return Ok(definition.clone());
		}
		let definition = self.txn.get_tb(self.ns, self.db, table, self.version).await?;
		self.definitions.insert(table.clone(), definition.clone());
		Ok(definition)
	}

	/// Whether the value at `path` on `table`, whose fields are `fields`, is
	/// declared a set: on `table` itself, through a record link, or through
	/// the source field of a view. `depth` bounds the links and views
	/// followed.
	async fn admits_set(
		&mut self,
		table: &crate::val::TableName,
		path: &[Part],
		fields: &[FieldDefinition],
		depth: usize,
	) -> anyhow::Result<bool> {
		if declared_admits_set(path, fields) {
			return Ok(true);
		}
		// A declaration on the path itself does not change a value read
		// through a record link or a view's projection, so those are always
		// followed.
		Box::pin(self.admits_set_elsewhere(table, path, fields, depth)).await
	}

	/// [`Self::admits_set`] for a `path` whose declarations on `table` do not
	/// say it is a set: through a record link or a view's source field.
	async fn admits_set_elsewhere(
		&mut self,
		table: &crate::val::TableName,
		path: &[Part],
		fields: &[FieldDefinition],
		depth: usize,
	) -> anyhow::Result<bool> {
		// Past the last link or view followed, the kind is unknown, and the
		// column is taken to flatten a set: the containment it declines is
		// answered by a scan, while a seek it allowed could miss rows.
		let Some(depth) = depth.checked_sub(1) else {
			return Ok(true);
		};
		// The record links every declared ancestor's type reaches along the
		// path, longest ancestor first, through every union alternative and
		// object literal, tuple and collection type: the rest of the path past
		// each link is read on every table it links to. Each ancestor's kind is
		// applied to the value, so each one's links are followed. A link that
		// names no table may reach any, so its value may be a set.
		for n in (1..path.len()).rev() {
			let Some(kind) = fields
				.iter()
				.find(|fd| names_path(fd, &path[..n]))
				.and_then(|fd| fd.field_kind.as_ref())
			else {
				continue;
			};
			let mut links = Vec::new();
			links_along(kind, &path[n..], &mut links);
			for (tables, rest) in links {
				if tables.is_empty() {
					return Ok(true);
				}
				for target in tables {
					let target_fields = self.fields(target).await?;
					if self.admits_set(target, rest, &target_fields, depth).await? {
						return Ok(true);
					}
				}
			}
		}
		// A view's field is the source field its projection reads.
		let definition = self.definition(table).await?;
		let Some(view) = definition.as_ref().and_then(|definition| definition.view.as_ref()) else {
			return Ok(false);
		};
		// A computed projection's value is not traced to a source field, so
		// it may be a set.
		let source_path = match view_source_path(view, path) {
			None => return Ok(false),
			Some(None) => return Ok(true),
			Some(Some(source_path)) => source_path,
		};
		for source in view.source_tables() {
			let source_fields = self.fields(source).await?;
			if self.admits_set(source, &source_path, &source_fields, depth).await? {
				return Ok(true);
			}
		}
		Ok(false)
	}
}

/// The record links a value of `kind` reaches when read at `path`, added to
/// `links` as the tables each may link to and the part of `path` read on
/// them. The type is read through every union alternative and through object
/// literal, tuple and collection types; an optional part (`.?`) reads the
/// value it follows unchanged, and a field read on a collection is read on its
/// elements. A `.*` read on a link dereferences it, so it is not part of the
/// path read on the linked table. A link whose table list is empty may link to
/// any table.
fn links_along<'a>(
	kind: &'a Kind,
	path: &'a [Part],
	links: &mut Vec<(&'a [crate::val::TableName], &'a [Part])>,
) {
	let Some((part, rest)) = path.split_first() else {
		return;
	};
	if matches!(part, Part::Optional) {
		return links_along(kind, rest, links);
	}
	match kind {
		// `.*` on a link fetches the record, so the path after it is read on
		// the linked table; a link read by any other part reads that part there.
		Kind::Record(tables) => {
			if let Part::All = part {
				if !rest.is_empty() {
					links.push((tables.as_slice(), rest));
				}
			} else {
				links.push((tables.as_slice(), path));
			}
		}
		Kind::Either(kinds) => kinds.iter().for_each(|kind| links_along(kind, path, links)),
		// A field read reads that field; `.*` leaves an object unchanged.
		Kind::Literal(KindLiteral::Object(fields)) => {
			if let Part::Field(name) = part {
				if let Some(kind) = fields.get(name.as_str()) {
					links_along(kind, rest, links);
				}
			} else if let Part::All = part {
				links_along(kind, rest, links);
			}
		}
		// `.*` and a position (any index expression) yield elements; a field
		// read maps over them; a filter keeps the collection.
		Kind::Array(element, _) | Kind::Set(element, _) => match part {
			Part::All | Part::First | Part::Last | Part::Value(_) => {
				links_along(element, rest, links)
			}
			Part::Field(_) => links_along(element, path, links),
			Part::Where(_) => links_along(kind, rest, links),
			Part::Flatten
			| Part::Lookup(_)
			| Part::Start(_)
			| Part::Method(..)
			| Part::Destructure(_)
			| Part::Optional
			| Part::Recurse(..)
			| Part::Doc
			| Part::RepeatRecurse => {}
		},
		Kind::Literal(KindLiteral::Array(kinds)) => match tuple_position(part, kinds.len()) {
			Some(Some(position)) => {
				if let Some(kind) = kinds.get(position) {
					links_along(kind, rest, links);
				}
			}
			Some(None) => kinds.iter().for_each(|kind| links_along(kind, rest, links)),
			None => {
				if let Part::Field(_) = part {
					kinds.iter().for_each(|kind| links_along(kind, path, links));
				} else if let Part::Where(_) = part {
					links_along(kind, rest, links);
				}
			}
		},
		Kind::Any
		| Kind::Named(_)
		| Kind::None
		| Kind::Null
		| Kind::Bool
		| Kind::Bytes
		| Kind::Datetime
		| Kind::Date
		| Kind::Time
		| Kind::Timestamp
		| Kind::Interval
		| Kind::Decimal
		| Kind::BigDecimal
		| Kind::Duration
		| Kind::Float
		| Kind::Int
		| Kind::I8
		| Kind::U8
		| Kind::I16
		| Kind::U16
		| Kind::I32
		| Kind::U32
		| Kind::U64
		| Kind::F16
		| Kind::F32
		| Kind::Number
		| Kind::Object
		| Kind::String
		| Kind::Uuid
		| Kind::Regex
		| Kind::Table(_)
		| Kind::Geometry(_)
		| Kind::Function(..)
		| Kind::Range
		| Kind::Literal(_)
		| Kind::File(_)
		| Kind::Enum(_) => {}
	}
}

/// `path` past a prefix naming `name`, with optional parts (`.?`) skipped in
/// both; `None` when `path` does not start with `name`.
fn strip_name<'a>(name: &[Part], path: &'a [Part]) -> Option<&'a [Part]> {
	let mut name = name.iter().filter(|part| !matches!(part, Part::Optional));
	let mut at = 0;
	let mut next = name.next();
	while let Some(part) = next {
		let p = path.get(at)?;
		if matches!(p, Part::Optional) {
		} else if p == part {
			next = name.next();
		} else {
			return None;
		}
		at += 1;
	}
	Some(&path[at..])
}

/// The path on a view's source table that the view's field at `path` is
/// read from: under a projection of a plain path (`SELECT s`, `SELECT a.b AS
/// s`, or `SELECT s.*`, whose output field is `s`), the projected path
/// followed by the rest of `path`; otherwise, under `SELECT *`, `path`
/// itself. Of several projections naming the field, the last one written
/// decides. `Some(None)` when that projection is computed rather than a plain
/// path, so its value is not traceable to a source field; `None` when nothing
/// names the field. An optional part (`.?`) reads the value it follows
/// unchanged, so names are matched with it skipped on both sides:
/// `meta.?.tags` is the field a `SELECT meta.tags` projection names.
fn view_source_path(
	view: &crate::catalog::ViewDefinition,
	path: &[Part],
) -> Option<Option<Vec<Part>>> {
	use crate::catalog::ViewDefinition;
	use crate::expr::{Expr, Field, Fields};
	let fields = match view {
		ViewDefinition::Materialized {
			fields,
			..
		}
		| ViewDefinition::Aggregated {
			fields,
			..
		}
		| ViewDefinition::Select {
			fields,
			..
		} => fields,
	};
	let Fields::Select(list) = fields else {
		return None;
	};
	// A projection naming the field wins over `*`, wherever each is written.
	// When several name it, the last one written is the value the view holds,
	// so they are read in reverse; a computed one names no source path. A
	// projection is named as its output field is: its alias, or else its
	// expression's simplified idiom (`tags.*` is stored under `tags`).
	let named = list.iter().rev().find_map(|field| {
		let Field::Single(selector) = field else {
			return None;
		};
		let name = match &selector.alias {
			Some(alias) => alias.clone(),
			None => selector.expr.to_idiom(),
		};
		let rest = strip_name(&name.0, path)?;
		let target = if let Expr::Idiom(source) = &selector.expr {
			Some(source.0.iter().chain(rest).cloned().collect())
		} else {
			None
		};
		Some(target)
	});
	match named {
		Some(traced) => Some(traced),
		None => list.iter().any(|field| matches!(field, Field::All)).then(|| Some(path.to_vec())),
	}
}

/// The kind declared for the value at `col`, found by walking from the
/// column up through its ancestors' definitions.
///
/// The nearest definition that declares a kind settles it — directly for the
/// column itself, or for the rest of the path through its object literal,
/// tuple or collection (`.*`) type — since a field's own type is applied after
/// its parent's. An untyped definition passed on the way leaves the
/// ancestor's type in force, unless it produces a value of its own (`VALUE`,
/// `DEFAULT` or `COMPUTED`): that value replaces what the ancestor's type
/// coerced and is checked against nothing, so the kind is unknown. `None` when
/// no declaration settles it.
fn declared_kind<'a>(col: &[Part], fields: &'a [FieldDefinition]) -> Option<&'a Kind> {
	let produces_value = |fd: &FieldDefinition| {
		fd.value.is_some() || fd.computed.is_some() || !matches!(fd.default, DefineDefault::None)
	};
	for len in (1..=col.len()).rev() {
		let Some(fd) = fields.iter().find(|fd| fd.name.0.as_slice() == &col[..len]) else {
			continue;
		};
		if let Some(kind) = fd.field_kind.as_ref() {
			return col[len..].iter().try_fold(kind, |kind, part| match part {
				Part::Field(name) => object_literal_field(kind, name.as_str()),
				Part::First => tuple_element(kind, |_| Some(0)),
				Part::Last => tuple_element(kind, |len| len.checked_sub(1)),
				Part::Value(crate::expr::Expr::Literal(crate::expr::Literal::Integer(i))) => {
					tuple_element(kind, |_| usize::try_from(*i).ok())
				}
				Part::Value(crate::expr::Expr::Literal(crate::expr::Literal::I8(i))) => {
					tuple_element(kind, |_| usize::try_from(*i).ok())
				}
				Part::Value(crate::expr::Expr::Literal(crate::expr::Literal::U8(u))) => {
					tuple_element(kind, |_| Some(usize::from(*u)))
				}
				Part::Value(crate::expr::Expr::Literal(crate::expr::Literal::I16(i))) => {
					tuple_element(kind, |_| usize::try_from(*i).ok())
				}
				Part::Value(crate::expr::Expr::Literal(crate::expr::Literal::U16(u))) => {
					tuple_element(kind, |_| Some(usize::from(*u)))
				}
				Part::Value(crate::expr::Expr::Literal(crate::expr::Literal::I32(i))) => {
					tuple_element(kind, |_| usize::try_from(*i).ok())
				}
				Part::Value(crate::expr::Expr::Literal(crate::expr::Literal::U32(u))) => {
					tuple_element(kind, |_| usize::try_from(*u).ok())
				}
				Part::Value(crate::expr::Expr::Literal(crate::expr::Literal::U64(u))) => {
					tuple_element(kind, |_| usize::try_from(*u).ok())
				}
				Part::All => collection_element(kind),
				Part::Flatten
				| Part::Where(_)
				| Part::Lookup(_)
				| Part::Value(_)
				| Part::Start(_)
				| Part::Method(..)
				| Part::Destructure(_)
				| Part::Optional
				| Part::Recurse(..)
				| Part::Doc
				| Part::RepeatRecurse => None,
			});
		}
		if produces_value(fd) {
			return None;
		}
	}
	None
}

/// The element kind of an array or set type (or one made optional), which
/// `.*` reads. As for [`object_literal_field`], only one collection
/// alternative is read.
fn collection_element(kind: &Kind) -> Option<&Kind> {
	if let Kind::Array(element, _) | Kind::Set(element, _) = kind {
		Some(element)
	} else if let Kind::Either(kinds) = kind {
		let mut collections = kinds.iter().filter(|k| !matches!(k, Kind::None | Kind::Null));
		match (collections.next(), collections.next()) {
			(Some(collection), None) => collection_element(collection),
			_ => None,
		}
	} else {
		None
	}
}

/// The kind a tuple type (or one made optional) declares for the element
/// `position` picks out of its length. As for [`object_literal_field`], only
/// the tuple's declaration is returned.
fn tuple_element(kind: &Kind, position: impl Fn(usize) -> Option<usize>) -> Option<&Kind> {
	if let Kind::Literal(KindLiteral::Array(kinds)) = kind {
		kinds.get(position(kinds.len())?)
	} else if let Kind::Either(kinds) = kind {
		let mut tuples = kinds.iter().filter(|k| !matches!(k, Kind::None | Kind::Null));
		match (tuples.next(), tuples.next()) {
			(Some(tuple), None) => tuple_element(tuple, position),
			_ => None,
		}
	} else {
		None
	}
}

/// The kind an object literal type (or one made optional) declares for
/// `name`. A `NONE` or `NULL` parent reads its fields as `NONE`, which the
/// field's own kind need not admit, so only the literal's declaration is
/// returned; any other alternative leaves the field's kind unknown.
fn object_literal_field<'a>(kind: &'a Kind, name: &str) -> Option<&'a Kind> {
	if let Kind::Literal(KindLiteral::Object(fields)) = kind {
		fields.get(name)
	} else if let Kind::Either(kinds) = kind {
		let mut objects = kinds.iter().filter(|k| !matches!(k, Kind::None | Kind::Null));
		match (objects.next(), objects.next()) {
			(Some(object), None) => object_literal_field(object, name),
			_ => None,
		}
	} else {
		None
	}
}

/// Whether every value of `kind` is a `Value::Array`, `NONE` or `NULL`, and
/// some value is an array. Unions are read through at any depth.
fn kind_holds_only_arrays(kind: &Kind) -> bool {
	/// `None` when some value of `kind` is neither an array, `NONE` nor
	/// `NULL`; otherwise whether some value is an array.
	fn arrays(kind: &Kind) -> Option<bool> {
		if matches!(kind, Kind::Array(..) | Kind::Literal(KindLiteral::Array(_))) {
			Some(true)
		} else if matches!(kind, Kind::None | Kind::Null) {
			Some(false)
		} else if let Kind::Either(kinds) = kind {
			kinds.iter().try_fold(false, |seen, k| Some(arrays(k)? || seen))
		} else {
			None
		}
	}
	arrays(kind) == Some(true)
}

/// The order a b-tree scan over `access` yields its rows in, stated as the
/// sort properties an ORDER BY is matched against.
///
/// A key orders its entries by the index columns and then, in a non-unique
/// index, by the record id. The columns the access pins hold one value across
/// the scan and order nothing, so the claim starts after them. It then claims
/// each free column in turn and stops at the first whose record may sit at one
/// key position per element rather than at one place in the order: a column it
/// cannot state as a field path, which includes every column holding `*`, and a
/// plain column that may hold an array ([`IndexRef::may_hold_array`]), whose
/// entries follow element order while the column's whole value, an array, sorts
/// after every scalar. It stops as well at a column whose entries order only by
/// the whole second ([`IndexRef::orders_coarsely`]). The id is claimed only when
/// every free column was: it orders the entries that agree on all of them, and
/// says nothing across a column the claim left out. Nor is it claimed where the
/// record ids do not order by value ([`IndexRef::orders_by_id`]).
pub(crate) fn scan_ordering(
	index_ref: &IndexRef,
	access: &BTreeAccess,
	direction: Direction,
) -> Vec<crate::exec::ordering::SortProperty> {
	use crate::exec::field_path::FieldPath;
	use crate::exec::field_path_convert::field_path_from_idiom;
	use crate::exec::operators::SortDirection;
	use crate::exec::ordering::SortProperty;

	let direction = match direction {
		Direction::Forward => SortDirection::Asc,
		Direction::Backward => SortDirection::Desc,
	};
	let property = |path| SortProperty {
		path,
		direction,
		collate: false,
		numeric: false,
	};
	let ix_def = index_ref.definition();
	let pinned = match access {
		BTreeAccess::Compound {
			prefix,
			..
		} => prefix.len(),
		BTreeAccess::Equality(_) => ix_def.cols.len(),
		BTreeAccess::Range {
			..
		}
		| BTreeAccess::FullText {
			..
		}
		| BTreeAccess::Knn {
			..
		} => 0,
	};
	let free = ix_def.cols.iter().enumerate().skip(pinned);
	let mut ordering: Vec<SortProperty> = free
		.clone()
		.map_while(|(at, col)| {
			if matches!(ix_def.index, Index::Spatial) && at + 1 == ix_def.cols.len() {
				return None;
			}
			if index_ref.may_hold_array(at) {
				return None;
			}
			let path = field_path_from_idiom(col).ok()?;
			(!index_ref.orders_coarsely(at)).then(|| property(path))
		})
		.collect();
	if !index_ref.is_unique()
		&& !ix_def.cols.is_empty()
		&& ordering.len() == free.len()
		&& index_ref.orders_by_id()
	{
		ordering.push(property(FieldPath::field("id")));
	}
	ordering
}

/// Select the best access path from candidates based on hints and heuristics.
///
/// Selection priority:
/// 1. WITH NOINDEX - always use table scan
/// 2. WITH INDEX names - use specified index(es)
/// 3. Best effort heuristics:
///    - Prefer unique index for equality (returns 1 row)
///    - Prefer compound index that matches more columns
///    - Prefer index that covers ORDER BY, over any range under a LIMIT
///      ([`select_access_path_paged`])
///    - Otherwise, pick the first of the tied indexes in catalog order
///
/// The final rule is load-bearing rather than arbitrary: two indexes that a
/// query scores identically must resolve to the same plan on every run, or an
/// `EXPLAIN` naming one of them is fragile against the order candidates happen
/// to be pushed in. Catalog order is stable for a given schema and is what
/// `INFO FOR TABLE` reports — but it is the catalog's own ordering, which is
/// keyed by index name, so it coincides with definition order only where the
/// two agree.
pub fn select_access_path(
	candidates: Vec<IndexCandidate>,
	with_hints: Option<&With>,
	direction: Direction,
) -> AccessPath {
	select_access_path_paged(candidates, with_hints, direction, false)
}

/// Whether a KNN scan overrides a `WITH INDEX` hint naming `names`: the scan's
/// own index is not hinted, and no hinted index is read by its prefilter's
/// allow-list. A hinted index the prefilter does not read (prefiltering is
/// off, the index stores no doc IDs, or it only delivers an `ORDER BY`) leaves
/// its predicate to be judged per record, so the hint is overridden.
pub(crate) fn knn_overrides_hint(path: &AccessPath, names: &[String]) -> bool {
	let hinted = |index_ref: &IndexRef| names.iter().any(|n| n.as_str() == index_ref.name.as_str());
	let AccessPath::KnnSearch {
		index_ref,
		prefilter,
		..
	} = path
	else {
		return false;
	};
	!hinted(index_ref) && !prefilter.as_ref().is_some_and(|p| bitmap_reads(&p.root, &hinted))
}

/// Whether `plan` reads an index `hinted` accepts.
fn bitmap_reads(plan: &BitmapPlan, hinted: &impl Fn(&IndexRef) -> bool) -> bool {
	match plan {
		BitmapPlan::BTree {
			index_ref,
			..
		}
		| BitmapPlan::FullText {
			index_ref,
			..
		} => hinted(index_ref),
		BitmapPlan::Graph {
			..
		} => false,
		BitmapPlan::And(children) | BitmapPlan::Or(children) => {
			children.iter().any(|c| bitmap_reads(c, hinted))
		}
		BitmapPlan::AndNot {
			base,
			subtract,
		} => bitmap_reads(base, hinted) || bitmap_reads(subtract, hinted),
	}
}

/// Logs that a KNN scan overrides a `WITH INDEX` hint ([`knn_overrides_hint`]),
/// as an unmatched hint is logged, so a plan that ignores WITH INDEX can be
/// traced in the logs.
fn warn_if_knn_overrides_hint(path: &AccessPath, with: Option<&With>) {
	if let Some(With::Index(names)) = with
		&& knn_overrides_hint(path, names)
	{
		tracing::warn!(
			target: "surreal::index",
			hinted = ?names,
			"WITH INDEX hint overridden: a KNN operator can only be computed by its KnnScan",
		);
	}
}

/// [`select_access_path`] for a statement that does or does not page its rows
/// with a `LIMIT`: under one, candidates are ranked by
/// [`IndexCandidate::rank`], which prefers an index that delivers the
/// `ORDER BY` to a range over another index.
pub fn select_access_path_paged(
	mut candidates: Vec<IndexCandidate>,
	with_hints: Option<&With>,
	direction: Direction,
	limited: bool,
) -> AccessPath {
	// WITH NOINDEX forces table scan
	if matches!(with_hints, Some(With::NoIndex)) {
		return AccessPath::TableScan;
	}

	// A KNN operator can only be computed by its KnnScan: every other driver
	// leaves `<|k,ef|>` behind in a per-row residual where it is never
	// truthy, silently returning zero rows. So when a KNN candidate exists it
	// drives unconditionally — overriding score selection (a unique equality
	// outscores KNN, 1000 > 800) and WITH INDEX hints, both of which would
	// otherwise pick a silently-wrong plan. The sibling predicates feed the
	// KNN prefilter or the pushed-down residual instead.
	let knn_pos = if let Some(With::Index(names)) = with_hints {
		names
			.iter()
			.find_map(|name| {
				candidates.iter().position(|c| {
					matches!(c.access, BTreeAccess::Knn { .. })
						&& c.index_ref.name.as_str() == name.as_str()
				})
			})
			.or_else(|| candidates.iter().position(|c| matches!(c.access, BTreeAccess::Knn { .. })))
	} else {
		candidates.iter().position(|c| matches!(c.access, BTreeAccess::Knn { .. }))
	};
	if let Some(pos) = knn_pos {
		// Whether this overrides a WITH INDEX hint is known only once the KNN
		// prefilter is built: `warn_if_knn_overrides_hint`.
		return candidates.swap_remove(pos).to_access_path(direction);
	}

	// WITH INDEX names - find the hinted index
	if let Some(With::Index(names)) = with_hints {
		if let Some(candidate) = find_hinted_index(&candidates, names) {
			return candidate.to_access_path(direction);
		}
		// Hint did not match any candidate. The most common cause is that
		// the user named an index but no WHERE conjunct refers to its
		// leading column. A warning is logged and selection then falls back to
		// best-effort selection or a table scan.
		// The warning names the unmatched hint.
		tracing::warn!(
			target: "surreal::index",
			hinted = ?names,
			candidates = ?candidates.iter().map(|c| c.index_ref.name.as_str()).collect::<Vec<_>>(),
			"WITH INDEX hint did not match any analyzed candidate; falling back to best-effort plan",
		);
	}

	// No candidates - table scan
	if candidates.is_empty() {
		return AccessPath::TableScan;
	}

	// Best effort: highest score wins, and catalog position breaks a tie.
	// Sorting rather than `max_by_key` makes the choice independent of the
	// order the analysis passes pushed the candidates in.
	candidates.sort_by_key(|c| (std::cmp::Reverse(c.rank(limited)), c.index_ref.idx));
	candidates
		.into_iter()
		.next()
		.map(|c| c.to_access_path(direction))
		.unwrap_or(AccessPath::TableScan)
}

/// Find a candidate matching one of the hinted index names.
fn find_hinted_index<'a>(
	candidates: &'a [IndexCandidate],
	names: &[String],
) -> Option<&'a IndexCandidate> {
	for name in names {
		if let Some(candidate) = candidates.iter().find(|c| &c.index_ref.name == name) {
			return Some(candidate);
		}
	}
	None
}

// ---------------------------------------------------------------------------------------------
// The access-path decision
// ---------------------------------------------------------------------------------------------

/// What [`decide_access`] reads about a table, already read from the catalog.
///
/// The planner's reads and the IR's table answers both fill one of these, so the decision that
/// follows the reads is one function. Every field that can fail to be read is an `Option`, so an
/// unreadable answer stays distinct from an empty one.
pub(crate) struct AccessInputs<'a> {
	/// The table's online indexes, at the stamp a `VERSION` read names. The decision narrows them
	/// to the indexes a read may be served from ([`admissible_indexes`]).
	pub(crate) indexes: Arc<[IndexDefinition]>,
	/// Which declared fields a non-`Full` `SELECT` permission governs for the caller.
	pub(crate) restricted: &'a RestrictedPrefixes,
	/// The structure of the table's record ids where they name their components, read only when
	/// the statement has a condition or an ordering.
	pub(crate) key: Option<Arc<KeyStructure>>,
	/// The element columns over `admissible_indexes(indexes, restricted, reads_versioned)`, or
	/// `None` when the field list or the indexes' recorded shapes could not be read. Read only
	/// when that list is not empty.
	pub(crate) columns: Option<ElementColumns>,
	/// The query vectors of the `KNN` operators whose right operand is a parameter holding an
	/// array of numbers, read only when an approximate index survives the narrowing.
	pub(crate) knn_vectors: &'a super::analysis::KnnQueryVectors,
	/// Whether the caller may call `time::now()`, so a comparison against it is an index bound.
	pub(crate) allows_clock: bool,
	/// Whether the read observes a `VERSION`: the statement names one, or an enclosing statement
	/// does.
	pub(crate) reads_versioned: bool,
	/// Whether a table permission predicate is part of the condition, so that a key range over the
	/// components of the record id takes only literal bounds.
	pub(crate) permissioned: bool,
	/// The literal count `k` of a `LIMIT k` without `START`, when the statement states one.
	pub(crate) limit: Option<u32>,
}

/// The indexes a read may be served from: `indexes` with, for a versioned read, only the
/// full-text ones, since an index entry carries no version and a `MATCHES` has no evaluation but
/// its index), and without any index whose columns a restricted field governs.
///
/// The one narrowing both the decision and the reads that feed it use, so they never disagree on
/// which indexes are candidates.
pub(crate) fn admissible_indexes(
	indexes: Arc<[IndexDefinition]>,
	restricted: &RestrictedPrefixes,
	reads_versioned: bool,
) -> Arc<[IndexDefinition]> {
	let indexes: Arc<[IndexDefinition]> =
		if reads_versioned && indexes.iter().any(|ix| !matches!(ix.index, Index::FullText(_))) {
			indexes.iter().filter(|ix| matches!(ix.index, Index::FullText(_))).cloned().collect()
		} else {
			indexes
		};
	if indexes.is_empty() {
		return indexes;
	}
	drop_restricted_indexes(indexes, restricted)
}

/// `cond` as the index analyzer reads it: projection function calls (`type::field("name")`)
/// rewritten to the paths they name.
pub(crate) fn analysis_condition(
	cond: Option<&LogicalExpr>,
	registry: &FunctionRegistry,
) -> Option<LogicalExpr> {
	cond.map(|c| {
		let mut c = c.clone();
		resolve_projection_field_idioms(&mut c, registry);
		c
	})
}

/// The answers that need no catalog read, decided before any read is made: a condition folded to
/// `false` admits no row, `WITH NOINDEX` scans the table, and an unresolved permission context
/// declines the plan-time path.
///
/// `None` when the decision needs the reads. `Some(None)` is a decline, as [`decide_access`]
/// answers it.
pub(crate) fn decide_without_reads(
	cond: Option<&LogicalExpr>,
	order: Option<&LogicalOrdering>,
	with: Option<&With>,
	restricted: &RestrictedPrefixes,
) -> Option<Option<(AccessPath, Direction)>> {
	let direction = determine_scan_direction(order);
	if matches!(cond, Some(LogicalExpr::Literal(LogicalLiteral::Bool(false)))) {
		return Some(Some((AccessPath::EmptyScan, direction)));
	}
	if matches!(with, Some(With::NoIndex)) {
		return Some(Some((AccessPath::TableScan, direction)));
	}
	// SECURITY: `AssumeRestricted` records that plan-time permission context was unavailable, not
	// that a restricted field exists. Resolving here would withhold every index on the table and
	// pin a table scan, which also turns a `MATCHES` on a fully readable column into
	// `NoIndexFoundForMatch` because no full-text index can be named. The plan-time path is
	// declined instead: the run-time fallback applies the same guard against the actor's resolved
	// field permissions, so it is fail-closed without over-applying.
	if matches!(restricted, RestrictedPrefixes::AssumeRestricted) {
		return Some(None);
	}
	None
}

/// The access path for a table source at plan time, with the predicate `cond` and ordering
/// `order` offered to it: the decision that follows the catalog reads, which builds nothing.
///
/// A read that observes a `VERSION` ([`AccessInputs::reads_versioned`]) plans no bitmap and no KNN
/// prefilter. `None` when the plan-time path is declined: the permission context is unresolved, or
/// the element columns could not be read.
///
/// Over a table whose record ids name their components, or an aggregated view, whose rows are
/// keyed by their group values, a key range over the record keys ([`plan_key_range`]) competes
/// with the index candidates (`IndexCandidate::outranks_key_range`); it is not planned under a
/// `VERSION`, a `WITH` hint, a `MATCHES` or a KNN operator.
pub(crate) fn decide_access(
	inputs: &AccessInputs<'_>,
	cond: Option<&LogicalExpr>,
	order: Option<&LogicalOrdering>,
	with: Option<&With>,
	has_limit: bool,
	registry: &FunctionRegistry,
) -> Result<Option<(AccessPath, Direction)>, Error> {
	if let Some(done) = decide_without_reads(cond, order, with, inputs.restricted) {
		return Ok(done);
	}
	let direction = determine_scan_direction(order);
	let restricted_select = inputs.restricted;
	let key = &inputs.key;
	// A read at a stamp plans no key range: the structure was checked against the keys present
	// when it was defined, not against the keys a past stamp holds. A `WITH` hint chooses the
	// read, and a `MATCHES` or a KNN operator has no answer but its index.
	let key_range = match (key, cond) {
		(Some(key), Some(cond))
			if with.is_none()
				&& !inputs.reads_versioned
				&& !expr_contains_matches(cond)
				&& !has_knn_operator(cond) =>
		{
			// A conjunct that reads a field the caller may not read is left to the reduced row, as
			// it is for an index.
			let conjuncts: Vec<&LogicalExpr> = conjuncts(cond)
				.into_iter()
				.filter(|c| !restricted_select.cond_touches(c))
				.collect();
			plan_key_range(key, &conjuncts, order, inputs.permissioned, registry)
		}
		_ => None,
	};
	let key_range_path = |access: KeyRangeAccess| {
		let direction = access.direction;
		(AccessPath::KeyRange(access), direction)
	};

	let indexes =
		admissible_indexes(Arc::clone(&inputs.indexes), restricted_select, inputs.reads_versioned);
	if indexes.is_empty() {
		return Ok(Some(key_range.map_or((AccessPath::TableScan, direction), key_range_path)));
	}

	let rewritten_cond = analysis_condition(cond, registry);
	let analysis_cond = rewritten_cond.as_ref();

	let Some(columns) = inputs.columns.as_ref() else {
		return Ok(None);
	};
	let analyzer = columns
		.analyzer(with, registry)
		.with_key(key.clone())
		.with_clock(inputs.allows_clock)
		.with_knn_query_vectors(inputs.knn_vectors)
		.with_limit(inputs.limit);
	let candidates = analyzer.analyze(analysis_cond, order);

	// A key range is chosen unless an index candidate outranks it, and only a candidate that
	// outranks it is chosen in its place.
	let candidates = match key_range {
		Some(access) => {
			let pinned = access.eq.len();
			let delivers = access.delivers == DeliveredKeyOrder::Exact;
			let outranking: Vec<_> = candidates
				.into_iter()
				.filter(|c| c.outranks_key_range(pinned, delivers, has_limit))
				.collect();
			if outranking.is_empty() {
				if !(has_limit && delivers)
					&& let Some((spatial_path, _, spatial_pinned)) =
						analyzer.try_spatial_expansion_scored(analysis_cond, direction)
					&& spatial_pinned > pinned
				{
					return Ok(Some((spatial_path, direction)));
				}
				return Ok(Some(key_range_path(access)));
			}
			outranking
		}
		None => candidates,
	};

	// Bitmap candidate fusion: when the WHERE clause is an AND of several index-backed predicates,
	// intersect per-branch candidate bitmaps over the table's shared doc-ID space instead of
	// driving from one index and filtering the rest per row. Chosen conservatively — the streaming
	// plans keep every case where they have a structural advantage:
	// - an index covers ORDER BY → keep sort elimination / sorted-merge early termination;
	// - LIMIT without ORDER BY → any streaming plan terminates early, a bitmap plan materializes
	//   every branch first;
	// - VERSION queries → doc-ID mappings are not time-travel-aware.
	let order_covered = candidates.iter().any(|c| c.covers_order);
	if with.is_none()
		&& !inputs.reads_versioned
		&& !order_covered
		&& (order.is_some() || !has_limit)
		&& let Some(cond) = analysis_cond
		&& matches!(
			cond,
			LogicalExpr::Binary {
				op: BinaryOperator::And,
				..
			}
		) && let Some(root) = analyzer
		.try_bitmap_fusion(Some(cond), &candidates, &|index, col| index.one_entry_per_record(col))
	{
		return Ok(Some((
			AccessPath::BitmapFusion {
				root,
				fallback: None,
			},
			direction,
		)));
	}

	// Top-level OR fusion: union the per-branch candidate bitmaps over the shared doc-ID space
	// instead of k-way-merging RecordId streams, whose dedupe set grows with the result size.
	// Gated like the AND fusion above: LIMIT without ORDER BY keeps streaming early termination (a
	// bitmap plan materializes every branch first), an index-covered ORDER BY keeps the union
	// scan's sorted-merge and sort-elimination plans, and ORDER BY id keeps the union's
	// merge-by-id. An ordering nothing covers installs a blocking Sort above either plan, so
	// fusion is allowed there even under LIMIT.
	if with.is_none()
		&& !inputs.reads_versioned
		&& ((order.is_none() && !has_limit)
			|| (order.is_some() && !order_covered && !order_is_scan_compatible(order)))
		&& let Some((root, fallback)) = analyzer.try_bitmap_union_fusion(analysis_cond, direction)
	{
		// `order_covered` only sees whole-condition candidates, which for an OR root never include
		// the branches' index accesses — so it misses the union's composite k-way merge: when every
		// branch pins the same composite-index prefix and the ordering is that index's next
		// column, the union satisfies the ORDER BY with no Sort. Keep the streaming union for that
		// shape.
		let union_covers_order = matches!(
			&fallback,
			AccessPath::Union {
				paths,
				..
			} if detect_order_for_composite_union(order, paths).is_some()
		);
		if !union_covers_order {
			return Ok(Some((
				AccessPath::BitmapFusion {
					root,
					fallback: Some(Box::new(fallback)),
				},
				direction,
			)));
		}
	}

	if candidates.is_empty() {
		if let Some(path) = analyzer.try_or_union(analysis_cond, direction) {
			return Ok(Some((path, direction)));
		}
		// A nested OR conjunct can be the only indexable part of an AND when every sibling conjunct
		// is unindexed (e.g. an unindexed `category = $c AND (title @1@ $q OR body @2@ $q)`).
		// `analyze` ignores OR subtrees, so there is no single-index candidate and we reach here
		// before the score-gated check further below. With no single-index driver the union always
		// beats the table-scan fallback, so take it unconditionally.
		if let Some((path, _)) = analyzer.try_and_nested_or_union(analysis_cond, direction) {
			return Ok(Some((path, direction)));
		}
		// Try expanding IN operators into union of equality lookups
		if let Some(path) = analyzer.try_in_expansion(analysis_cond, direction) {
			return Ok(Some((path, direction)));
		}
		// Try expanding CONTAINSALL/CONTAINSANY into union of equality lookups
		if let Some(path) = analyzer.try_containment_expansion(analysis_cond, direction) {
			return Ok(Some((path, direction)));
		}
		if let Some(path) =
			nullish_containment(&analyzer, analysis_cond, order, has_limit, direction)
		{
			return Ok(Some((path, direction)));
		}
		// Try expanding spatial geometry predicates (INSIDE / CONTAINS / INTERSECTS /
		// geo::distance) into union of Hilbert cell range scans on a SPATIAL index
		if let Some(path) = analyzer.try_spatial_expansion(analysis_cond, direction) {
			return Ok(Some((path, direction)));
		}
		return Ok(Some((AccessPath::TableScan, direction)));
	}

	// If a `WITH INDEX` hint names a `SPATIAL` index before any matched candidate in
	// `candidates` (and no KNN operator requires driving from its ANN scan), honor the
	// spatial index hint if `try_spatial_expansion` can satisfy a WHERE conjunct on it.
	if let Some(With::Index(names)) = with
		&& !candidates.iter().any(|c| matches!(c.access, BTreeAccess::Knn { .. }))
	{
		let best_candidate_hint_pos = candidates
			.iter()
			.filter_map(|c| names.iter().position(|n| n.as_str() == c.index_ref.name.as_str()))
			.min()
			.unwrap_or(usize::MAX);
		let best_spatial_hint_pos = analyzer
			.indexes
			.iter()
			.filter(|ix| {
				!ix.prepare_remove
					&& matches!(ix.index, Index::Spatial)
					&& crate::catalog::uses_current_spatial_covering(ix.format_version)
			})
			.filter_map(|ix| names.iter().position(|n| n.as_str() == ix.name.as_str()))
			.min()
			.unwrap_or(usize::MAX);
		if best_spatial_hint_pos < best_candidate_hint_pos
			&& let Some(spatial_path) = analyzer.try_spatial_expansion(analysis_cond, direction)
		{
			return Ok(Some((spatial_path, direction)));
		}
	}

	// Rank of the best single-index driver, captured before `select_access_path_paged` consumes
	// `candidates`. Used below to decide whether a nested-OR multi-index union or a 2D spatial
	// index expansion would be a better driver.
	let best_single_score = candidates.iter().map(|c| c.rank(has_limit)).max().unwrap_or(0);

	// A 2D spatial covering bounds both longitude and latitude simultaneously (effective score
	// 210–350 for single-column depending on spherical surface-area fraction, or 500 + 50 *
	// prefix_len for a compound spatial index with pinned equality prefix columns), which for
	// selective regions outranks an un-ordered 1D B-tree range scan (200 half-bounded, 300 bounded)
	// or a single-column equality on a prefix column alone (500).
	if with.is_none()
		&& !order_covered
		&& !candidates.iter().any(|c| matches!(c.access, BTreeAccess::Knn { .. }))
		&& let Some((spatial_path, spatial_score, _)) =
			analyzer.try_spatial_expansion_scored(analysis_cond, direction)
		&& spatial_score > best_single_score
	{
		return Ok(Some((spatial_path, direction)));
	}

	let path = select_access_path_paged(candidates, with, direction, has_limit);

	// When the chosen index covers ORDER BY, derive the correct scan direction from the ORDER BY
	// clause rather than the default `determine_scan_direction` (which only handles ORDER BY id).
	// This enables LIMIT pushdown and sort elimination for queries like
	// `ORDER BY metadata.payload_metadata.modified DESC LIMIT 25`.
	let (mut path, direction) = adjust_direction_for_order(path, order, direction);

	// Pre-filtered vector search: evaluate the KNN query's exactly index-coverable conjuncts into
	// an allow-list bitmap before the ANN search, instead of fetching every visited candidate's
	// record. Gated on no WITH NOINDEX (explicit WITH INDEX hints restrict which secondary indexes
	// may contribute to the allow-list) and no VERSION (doc-ID mappings are not time-travel-aware).
	if let AccessPath::KnnSearch {
		prefilter,
		..
	} = &mut path
		&& !matches!(with, Some(With::NoIndex))
		&& !inputs.reads_versioned
		&& *surrealdb_cnf::KNN_PREFILTER_ENABLED
	{
		*prefilter = knn_prefilter_plan(&analyzer, analysis_cond);
	}
	warn_if_knn_overrides_hint(&path, with);

	// When the best single-index path is a full-range scan (ORDER BY only, no WHERE selectivity),
	// also try a multi-index union for OR conditions. The union reads only matching rows from each
	// branch, which is typically far better than scanning every row in the index. The outer
	// pipeline adds a Sort when the union does not satisfy ORDER BY.
	if path.is_full_range_scan()
		&& let Some(union_path) = analyzer.try_or_union(analysis_cond, direction)
	{
		return Ok(Some((union_path, direction)));
	}
	// Same logic for containment expansion (CONTAINSANY / ANYINSIDE against an array-element
	// index): the full-range scan would walk every indexed entry — typically one per array element
	// per row — and filter post-hoc. A Union of per-value Compound prefix scans reads only the
	// matching prefix ranges. The k-way merge with `MergeMode::ByIndexKeyDedup` preserves the
	// trailing ORDER BY column's sort order and dedupes rows that match multiple branches.
	if path.is_full_range_scan()
		&& let Some(union_path) = analyzer.try_containment_expansion(analysis_cond, direction)
	{
		return Ok(Some((union_path, direction)));
	}
	if path.is_full_range_scan()
		&& let Some(union_path) =
			nullish_containment(&analyzer, analysis_cond, order, has_limit, direction)
	{
		return Ok(Some((union_path, direction)));
	}
	if path.is_full_range_scan()
		&& let Some(union_path) = analyzer.try_spatial_expansion(analysis_cond, direction)
	{
		return Ok(Some((union_path, direction)));
	}
	// `try_in_expansion` is deliberately not tried here. The full-range scan covers ORDER BY,
	// enabling sort elimination and early termination with the batch ceiling. Replacing it with a
	// Union of prefix scans would require an expensive Sort of ALL matching records, which is far
	// worse for ORDER BY + LIMIT queries. IN expansion is only helpful in the
	// `candidates.is_empty()` fallback above when no index covers ORDER BY at all.

	// When the WHERE clause is a conjunction and one conjunct is a pure OR whose every branch is
	// independently indexable, consider driving from that OR's multi-index union and applying the
	// remaining conjuncts as a residual filter. A nested OR of full-text matches like
	// `type = $t AND (title @1@ $q OR body @2@ $q)` should not drive from the low-selectivity
	// `type` equality, which would evaluate the OR as a per-row filter — re-scoring the FT index
	// for every `type` row. Switch only when the union's weakest branch is more selective than the
	// chosen single-index driver (higher score); this keeps a selective unique/compound equality
	// (or an ORDER BY-covering scan, which carries the +100 order bonus) as the driver.
	if with.is_none()
		&& let Some((union_path, union_score)) =
			analyzer.try_and_nested_or_union(analysis_cond, direction)
		&& union_score > best_single_score
	{
		return Ok(Some((union_path, direction)));
	}

	Ok(Some((path, direction)))
}

/// The pre-filter plan of a KNN access path: the KNN-stripped condition split per conjunct, those
/// whose truth set is exactly a candidate bitmap joining the allow-list and the rest staying the
/// true residual.
fn knn_prefilter_plan(
	analyzer: &super::analysis::IndexAnalyzer<'_>,
	cond: Option<&LogicalExpr>,
) -> Option<KnnPrefilterPlan> {
	let cond = cond?;
	let stripped = strip_knn_from_condition(cond)?;
	let mut plan =
		analyzer.try_knn_prefilter(&stripped, &|index, col| index.one_entry_per_record(col))?;
	// The plan indexes the chain of `stripped`, which has no KNN conjunct; its indexes are
	// restated in the chain of `cond`.
	let kept: Vec<usize> = conjuncts(cond)
		.into_iter()
		.enumerate()
		.filter(|(_, conjunct)| strip_knn_from_condition(conjunct).is_some())
		.map(|(at, _)| at)
		.collect();
	if kept.len() != conjuncts(&stripped).len() {
		return None;
	}
	plan.covered = plan.covered.iter().map(|at| kept[*at]).collect();
	Some(plan)
}

/// The terms of `expr`'s top-level `AND` chain, in evaluation order.
fn conjuncts(expr: &LogicalExpr) -> Vec<&LogicalExpr> {
	fn collect<'a>(expr: &'a LogicalExpr, out: &mut Vec<&'a LogicalExpr>) {
		if let LogicalExpr::Binary {
			left,
			op: BinaryOperator::And,
			right,
		} = expr
		{
			collect(left, out);
			collect(right, out);
		} else {
			out.push(expr);
		}
	}
	let mut out = Vec::new();
	collect(expr, &mut out);
	out
}

#[cfg(test)]
mod range_comparison_tests {
	use std::ops::Bound;
	use std::str::FromStr;

	use super::{BTreeAccess, BoundOperand, range_matches_comparison};
	use crate::catalog::{
		BTREE_NORMALISED_DECIMAL_KEYS_FORMAT_VERSION, INDEX_FORMAT_VERSION, Index, IndexDefinition,
		IndexId,
	};
	use crate::expr::Idiom;
	use crate::val::{Number, Range, Value};

	fn below(v: Value) -> BTreeAccess {
		BTreeAccess::literal_range(Range {
			start: Bound::Unbounded,
			end: Bound::Excluded(v),
		})
	}

	fn index(format_version: u16) -> IndexDefinition {
		IndexDefinition {
			index_id: IndexId(1),
			name: "ix".into(),
			table_name: "t".into(),
			cols: vec![Idiom::from_str("c").expect("valid idiom")],
			index: Index::Idx,
			count_cond: None,
			comment: None,
			prepare_remove: false,
			format_version,
			nulls_not_distinct: false,
		}
	}

	fn decimal(s: &str) -> Value {
		Value::Number(Number::Decimal(s.parse().expect("decimal")))
	}

	#[test]
	fn a_number_bounds_a_range_exactly_when_both_kinds_hold_it() {
		let index = index(INDEX_FORMAT_VERSION);
		for (bound, exact) in [
			(Value::from(5), true),
			(Value::from(i64::MAX), true),
			(Value::Number(Number::Float(1.5)), true),
			(Value::Number(Number::Float(0.1)), false),
			(Value::Number(Number::Float(f64::NAN)), false),
			(decimal("1.5"), true),
			(decimal("0.1"), false),
			(Value::from("p"), true),
		] {
			assert_eq!(range_matches_comparison(&index, &below(bound.clone())), exact, "{bound:?}");
		}
		for (value, exact) in [
			(Value::Number(Number::Float(0.1)), false),
			(Value::Number(Number::Float(1.5)), true),
			(decimal("0.1"), false),
			(Value::from(3), true),
			(Value::from("a"), true),
		] {
			assert_eq!(
				range_matches_comparison(&index, &BTreeAccess::literal_equality(value.clone())),
				exact,
				"= {value:?}"
			);
			assert_eq!(
				range_matches_comparison(
					&index,
					&BTreeAccess::Compound {
						prefix: vec![
							BoundOperand::Literal(Value::from("a")),
							BoundOperand::Literal(value.clone()),
						],
						range: None,
						upper: None,
					}
				),
				exact,
				"compound prefix {value:?}"
			);
		}
	}

	/// An index that keeps a decimal's trailing zeros keys `2.50dec` apart from
	/// `2.5dec` and `2.0dec` apart from `2`, so no number bounds a range over it
	/// exactly, nor is sought by an equality exactly.
	#[test]
	fn no_number_is_exact_over_an_index_that_keeps_trailing_zeros() {
		let index = index(BTREE_NORMALISED_DECIMAL_KEYS_FORMAT_VERSION - 1);
		for value in
			[Value::from(5), Value::Number(Number::Float(1.5)), decimal("2.5"), decimal("2.50")]
		{
			assert!(!range_matches_comparison(&index, &below(value.clone())), "{value:?}");
			assert!(
				!range_matches_comparison(&index, &BTreeAccess::literal_equality(value.clone())),
				"= {value:?}"
			);
		}
		assert!(range_matches_comparison(&index, &below(Value::from("p"))));
		assert!(range_matches_comparison(&index, &BTreeAccess::literal_equality(Value::from("a"))));
	}

	#[test]
	fn spatial_element_columns_preserve_sets_and_track_point_only_shapes() {
		use std::sync::Arc;

		use surrealdb_datastore::index_shapes::IndexShapes;

		use super::{TableIndexShapes, ValueShape, with_array_columns_as_elements};
		use crate::catalog::FieldDefinition;
		use crate::expr::kind::GeometryKind;
		use crate::expr::{Kind, Part};

		let make_field = |name: &str, kind: Option<Kind>| FieldDefinition {
			name: Idiom::from_str(name).expect("valid idiom"),
			field_kind: kind,
			..Default::default()
		};

		let spatial_ix = IndexDefinition {
			index_id: IndexId(1),
			name: "idx_spatial".into(),
			table_name: "t".into(),
			cols: vec![
				Idiom::from_str("tags.*").expect("valid idiom"),
				Idiom::from_str("foreign.*").expect("valid idiom"),
				Idiom::from_str("pts").expect("valid idiom"),
			],
			index: Index::Spatial,
			count_cond: None,
			comment: None,
			prepare_remove: false,
			nulls_not_distinct: false,
			format_version: INDEX_FORMAT_VERSION,
		};
		let indexes: Arc<[IndexDefinition]> = Arc::from([spatial_ix]);
		let fields_point = vec![
			make_field("tags", Some(Kind::Set(Box::new(Kind::String), None))),
			make_field("pts", Some(Kind::Geometry(vec![GeometryKind::Point]))),
		];

		// 1. Without tracked `TableIndexShapes`, `DEFINE FIELD pts TYPE geometry<point>` alone must
		//    NOT enable `point_only_spatial` (pre-existing records before `DEFINE FIELD` could hold
		//    polygons or multipoints).
		let untracked_shapes = TableIndexShapes::default();
		let elem_untracked =
			with_array_columns_as_elements(Arc::clone(&indexes), &fields_point, &untracked_shapes);
		assert!(elem_untracked.point_only_spatial.is_none());

		// 2. When `TableIndexShapes` is tracked and column 2 (`pts`) has only recorded
		//    `ValueShape::Whole` (single in-bounds points), `point_only_spatial` is `Some([true])`,
		//    and `withholding_sets_at` preserves `tags.*` (set) while adding `foreign.*`.
		let point_only_shapes = TableIndexShapes::new([(
			IndexId(1),
			Arc::new(IndexShapes::new(true, vec![0, 0, ValueShape::Whole.bit()], Vec::new())),
		)]);
		let elem_cols =
			with_array_columns_as_elements(Arc::clone(&indexes), &fields_point, &point_only_shapes)
				.withholding_sets_at(&[vec![Part::Field("foreign".into())]]);
		assert_eq!(elem_cols.point_only_spatial.as_deref(), Some(&[true][..]));
		let sets = elem_cols.sets.as_ref().expect("sets should be populated");
		assert_eq!(sets[0].as_ref(), &[true, true, false]);

		// 3. Once column 2 records `ValueShape::MultiCell` (e.g. a Polygon, MultiPoint, or
		//    out-of-bounds Point) or `ValueShape::Array` (an array/set of geometries),
		//    `point_only_spatial` is disabled.
		let multicell_shapes = TableIndexShapes::new([(
			IndexId(1),
			Arc::new(IndexShapes::new(
				true,
				vec![0, 0, ValueShape::Whole.bit() | ValueShape::MultiCell.bit()],
				Vec::new(),
			)),
		)]);
		let elem_multicell =
			with_array_columns_as_elements(Arc::clone(&indexes), &fields_point, &multicell_shapes);
		assert!(elem_multicell.point_only_spatial.is_none());

		let mixed_shapes = TableIndexShapes::new([(
			IndexId(1),
			Arc::new(IndexShapes::new(
				true,
				vec![0, 0, ValueShape::Whole.bit() | ValueShape::Array.bit()],
				Vec::new(),
			)),
		)]);
		let elem_mixed =
			with_array_columns_as_elements(Arc::clone(&indexes), &fields_point, &mixed_shapes);
		assert!(elem_mixed.point_only_spatial.is_none());

		// 4. When column 2 records `ValueShape::NonGeometry` alongside `ValueShape::Whole`,
		//    `point_only_spatial` remains `Some([true])` (for `INSIDE` / `INTERSECTS`), while
		//    `non_geometry_spatial` is `Some([true])` so `geo::distance` (`Cap`) declines index
		//    pushdown and preserves table-scan type errors on non-geometry rows.
		let non_geom_shapes = TableIndexShapes::new([(
			IndexId(1),
			Arc::new(IndexShapes::new(
				true,
				vec![0, 0, ValueShape::Whole.bit() | ValueShape::NonGeometry.bit()],
				Vec::new(),
			)),
		)]);
		let elem_non_geom =
			with_array_columns_as_elements(Arc::clone(&indexes), &fields_point, &non_geom_shapes);
		assert_eq!(elem_non_geom.point_only_spatial.as_deref(), Some(&[true][..]));
		assert_eq!(elem_non_geom.non_geometry_spatial.as_deref(), Some(&[true][..]));
	}

	#[test]
	fn an_element_column_storing_nothing_whole_lists_every_record_by_element_or_nullish() {
		use std::sync::Arc;

		use surrealdb_datastore::index_shapes::IndexShapes;

		use super::{TableIndexShapes, ValueShape, with_array_columns_as_elements};

		let ix = |cols: &[&str]| IndexDefinition {
			index_id: IndexId(1),
			name: "ix".into(),
			table_name: "t".into(),
			cols: cols.iter().map(|c| Idiom::from_str(c).expect("valid idiom")).collect(),
			index: Index::Idx,
			count_cond: None,
			comment: None,
			prepare_remove: false,
			nulls_not_distinct: false,
			format_version: INDEX_FORMAT_VERSION,
		};
		let shapes = |tracked: bool, col0: u8| {
			TableIndexShapes::new([(
				IndexId(1),
				Arc::new(IndexShapes::new(tracked, vec![col0], Vec::new())),
			)])
		};
		let flags = |cols: &[&str], shapes: &TableIndexShapes| {
			with_array_columns_as_elements(Arc::from([ix(cols)]), &[], shapes)
				.elements_or_nullish
				.map(|flags| flags[0].to_vec())
		};
		let arrays = ValueShape::Array.bit();
		// Tracked, and only arrays stored: flagged.
		assert_eq!(
			flags(&["tags.*", "created_at"], &shapes(true, arrays)),
			Some(vec![true, false])
		);
		assert_eq!(flags(&["a.b.*"], &shapes(true, 0)), Some(vec![true]));
		// A whole value stored, an untracked index, or shapes not read: not flagged.
		assert_eq!(flags(&["tags.*"], &shapes(true, arrays | ValueShape::Whole.bit())), None);
		assert_eq!(flags(&["tags.*"], &shapes(false, arrays)), None);
		assert_eq!(flags(&["tags.*"], &TableIndexShapes::default()), None);
		// Only a trailing `*` over plain fields qualifies.
		for col in ["tags", "a.*.b", "tags.*.*", "tags.*.first()"] {
			let idiom = Idiom::from_str(col).expect("valid idiom");
			assert!(!super::takes_each_of_fields(&idiom), "{col}");
		}
		assert!(super::takes_each_of_fields(&Idiom::from_str("tags[*]").expect("valid idiom")));
	}
}
