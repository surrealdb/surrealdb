//! Access path types for index-based record retrieval.
//!
//! An [`AccessPath`] represents a specific way to retrieve records from a table,
//! whether through a full table scan, point lookup, or index scan.

use std::ops::Bound;
use std::sync::Arc;

use super::IndexCandidate;
use crate::catalog::{FieldDefinition, Index, IndexDefinition};
use crate::expr::operator::MatchesOperator;
use crate::expr::with::With;
use crate::expr::{BinaryOperator, Cond, Idiom, Kind, KindLiteral, Part};
use crate::kvs::Direction;
use crate::val::{Number, Range, Value};

/// A reference to an index definition with its position in the schema.
///
/// This is a lightweight reference that can be cloned efficiently.
#[derive(Debug, Clone)]
pub(crate) struct IndexRef {
	/// The full list of indexes for the table
	pub(crate) indexes: Arc<[IndexDefinition]>,
	/// The position of this index in the list
	pub(crate) idx: usize,
}

impl IndexRef {
	/// Create a new index reference.
	pub fn new(indexes: Arc<[IndexDefinition]>, idx: usize) -> Self {
		Self {
			indexes,
			idx,
		}
	}

	/// Get the index definition.
	pub fn definition(&self) -> &IndexDefinition {
		&self.indexes[self.idx]
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

impl PartialEq for IndexRef {
	fn eq(&self, other: &Self) -> bool {
		self.idx == other.idx
	}
}

impl Eq for IndexRef {}

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
		vector: Vec<Number>,
		/// Number of nearest neighbors to return
		k: u32,
		/// ANN search expansion factor
		ef: u32,
		/// Pre-filtered vector search (#548): an exact bitmap plan over the
		/// index-covered WHERE conjuncts, plus the true residual. `None` when
		/// no conjunct is exactly index-coverable.
		prefilter: Option<KnnPrefilterPlan>,
	},

	/// Roaring-bitmap candidate fusion over the table's shared doc-ID space
	/// (issue #547).
	///
	/// Chosen for an AND-composed WHERE clause with at least two index-backed
	/// members (or one plus a subtractable `NOT`), or a top-level OR whose
	/// every branch is bitmap-capable (issue #550), when every participating
	/// index carries doc-IDs, no index covers the ORDER BY, and no
	/// early-termination (LIMIT without ORDER BY) or VERSION constraints
	/// apply. Surfaces as a `BitmapResolve` operator over a `BitmapNode`
	/// tree; the whole WHERE clause stays as the residual filter.
	///
	/// `fallback` carries the streaming [`AccessPath::Union`] the OR fusion
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
					range: Range {
						start: Bound::Unbounded,
						end: Bound::Unbounded,
					},
				},
				..
			}
		)
	}
}

/// Plan-level pre-filter for a KNN search (issue #548).
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
	/// KNN-stripped conjuncts NOT covered by `root`, re-ANDed. `None` when
	/// the whole stripped WHERE is covered. MATCHES conjuncts never appear
	/// here: they are not evaluable through the plain in-traversal residual
	/// (no query executor).
	pub residual: Option<Cond>,
	/// `true` when a MATCHES conjunct could not be covered by `root` (its
	/// full-text index does not carry shared doc-IDs). Such a conjunct is
	/// absent from `residual` too, so the scan must keep its MATCHES-aware
	/// compiled condition active alongside the allow-list — otherwise the
	/// top-k is chosen without the text predicate and matching neighbours
	/// outside it are unrecoverable.
	pub uncovered_matches: bool,
}

/// Plan-level bitmap candidate expression tree (issue #547).
///
/// Built by [`crate::exec::index::analysis::IndexAnalyzer::try_bitmap_fusion`]
/// and converted into `BitmapNode` operators by the SELECT planner. Leaves
/// produce a candidate bitmap over the table's shared doc-ID space; inner
/// nodes compose them with set algebra. `NOT` appears only as the subtract
/// side of [`BitmapPlan::AndNot`] — never standalone (a standalone NOT would
/// need a table-universe bitmap that is deliberately not maintained).
#[derive(Debug, Clone)]
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
	/// A single-hop graph reachability semi-join (issue #549): the doc-IDs of
	/// scanned-table rows adjacent to `source` through `edge_tables`,
	/// traversed in `direction` — the *inverse* of the direction the
	/// predicate was written in, so a `->edge->` containment anchors at its
	/// target literal and walks back to the candidate rows. Read entirely
	/// from adjacency pointer keys, which embed the far vertex; a
	/// legacy-format key (no embedded target) overflows the branch, so an
	/// unmigrated scope degrades to the residual filter instead of
	/// under-approximating. Never exact: participates only as a droppable,
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

/// How to access an index.
#[derive(Debug, Clone)]
pub enum BTreeAccess {
	/// Single value equality: `field = value`
	Equality(Value),

	/// Range scan with optional bounds: `field > a AND field < b`
	Range {
		range: Range,
	},

	/// Compound index access with fixed prefix and optional range on next column.
	///
	/// Example: For index on (a, b, c), if query is `a = 1 AND b = 2 AND c > 3`,
	/// the prefix is [1, 2] and range is Some((MoreThan, 3)).
	Compound {
		/// Fixed values for leading columns
		prefix: Vec<Value>,
		/// Optional range condition on the next column after the prefix
		range: Option<(BinaryOperator, Value)>,
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
		vector: Vec<Number>,
		/// Number of nearest neighbors
		k: u32,
		/// ANN search expansion factor
		ef: u32,
	},
}

impl BTreeAccess {
	/// Human-readable description of the access shape for EXPLAIN output.
	///
	/// Shared by [`crate::exec::operators::IndexScan`] and the bitmap
	/// candidate operators so the `access:` attribute renders identically.
	/// `FullText`/`Knn` shapes are described by their dedicated operators.
	pub(crate) fn describe(&self) -> String {
		use surrealdb_types::ToSql;
		match self {
			BTreeAccess::Equality(v) => format!("= {}", v.to_sql()),
			BTreeAccess::Range {
				range,
			} => {
				let from_str = match range.start.as_ref() {
					Bound::Included(x) => format!(">={}", x.to_sql()),
					Bound::Excluded(x) => format!(">{}", x.to_sql()),
					Bound::Unbounded => String::new(),
				};
				let to_str = match range.end.as_ref() {
					Bound::Included(x) => format!("<={}", x.to_sql()),
					Bound::Excluded(x) => format!("<{}", x.to_sql()),
					Bound::Unbounded => String::new(),
				};
				format!("{from_str} {to_str}").trim().to_string()
			}
			BTreeAccess::Compound {
				prefix,
				range,
			} => {
				let prefix_str = prefix.iter().map(|v| v.to_sql()).collect::<Vec<_>>().join(", ");
				if let Some((op, val)) = range {
					let val_sql = val.to_sql();
					format!("[{prefix_str}] {op:?} {val_sql}")
				} else {
					format!("[{prefix_str}]")
				}
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
		_ => 0,
	};
	cols.iter().skip(pinned).any(|c| c.0.iter().any(|p| matches!(p, Part::All)))
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
/// The rewrite is for planning only. Scans encode their keys from the index id
/// and the query's values; the column idioms reach execution only as the
/// ordering and constant-field claims, which this makes accurate. Returns
/// `indexes` itself when no column changes.
pub(crate) fn with_array_columns_as_elements(
	indexes: Arc<[IndexDefinition]>,
	fields: &[FieldDefinition],
) -> ElementColumns {
	let as_elements = |col: &Idiom| -> bool {
		!col.0.iter().any(|p| matches!(p, Part::All | Part::Flatten))
			&& declared_kind(col, fields).is_some_and(kind_holds_only_arrays)
	};
	// Built only once a column changes, so an unchanged list is neither
	// cloned nor reallocated. The definitions and their rewritten-column flags
	// are pushed together, so the two stay aligned index for index.
	let mut changed: Option<(Vec<IndexDefinition>, Vec<ColumnFlags>)> = None;
	for (i, ix) in indexes.iter().enumerate() {
		let flags: Box<[bool]> = if matches!(ix.index, Index::Idx | Index::Uniq) {
			ix.cols.iter().map(as_elements).collect()
		} else {
			Box::default()
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
	match changed {
		Some((defs, masks)) => ElementColumns {
			indexes: defs.into(),
			rewritten: Some(masks.into()),
		},
		None => ElementColumns::unchanged(indexes),
	}
}

/// One index's per-column flags: whether each column was rewritten.
pub(crate) type ColumnFlags = Box<[bool]>;

/// What [`with_array_columns_as_elements`] made of a table's indexes.
pub(crate) struct ElementColumns {
	/// The indexes, with every rewritten column spelled with `*`.
	pub(crate) indexes: Arc<[IndexDefinition]>,
	/// Per index, in order, whether each column was rewritten; a column past
	/// the end of its index's flags was not. `None` when none was.
	pub(crate) rewritten: Option<Arc<[ColumnFlags]>>,
}

impl ElementColumns {
	/// The indexes unchanged, for a caller that rewrites nothing.
	pub(crate) fn unchanged(indexes: Arc<[IndexDefinition]>) -> Self {
		Self {
			indexes,
			rewritten: None,
		}
	}

	/// An analyzer over these indexes that does not seek a rewritten column
	/// for containment.
	pub(crate) fn analyzer<'a>(
		&self,
		with: Option<&'a With>,
	) -> crate::exec::index::analysis::IndexAnalyzer<'a> {
		let analyzer =
			crate::exec::index::analysis::IndexAnalyzer::new(Arc::clone(&self.indexes), with);
		match &self.rewritten {
			Some(rewritten) => analyzer.without_containment_on(Arc::clone(rewritten)),
			None => analyzer,
		}
	}
}

/// Whether [`with_array_columns_as_elements`] could change any of `indexes`:
/// some b-tree index has a column holding neither `*` nor a flatten. Callers
/// skip reading the field list when it could not.
pub(crate) fn may_have_array_columns(indexes: &[IndexDefinition]) -> bool {
	indexes.iter().any(|ix| {
		matches!(ix.index, Index::Idx | Index::Uniq)
			&& ix.cols.iter().any(|c| !c.0.iter().any(|p| matches!(p, Part::All | Part::Flatten)))
	})
}

/// `indexes` without its b-tree indexes, for a caller that cannot tell which
/// of their columns store array elements: a b-tree access path planned
/// without that knowledge can count, order or seek element entries as whole
/// values.
pub(crate) fn without_btree_indexes(indexes: Arc<[IndexDefinition]>) -> Arc<[IndexDefinition]> {
	if !indexes.iter().any(|ix| matches!(ix.index, Index::Idx | Index::Uniq)) {
		return indexes;
	}
	indexes.iter().filter(|ix| !matches!(ix.index, Index::Idx | Index::Uniq)).cloned().collect()
}

/// The kind declared for the value at `col`: the kind of the field named
/// `col`, or, when that field declares no kind or only an ancestor is
/// declared, the kind the ancestor's object literal type gives the remaining
/// path. `None` when no declaration reaches it.
fn declared_kind<'a>(col: &Idiom, fields: &'a [FieldDefinition]) -> Option<&'a Kind> {
	// A definition of the column itself that declares no kind leaves the
	// ancestor's type in force: the parent still coerces the value.
	if let Some(kind) =
		fields.iter().find(|fd| &fd.name == col).and_then(|fd| fd.field_kind.as_ref())
	{
		return Some(kind);
	}
	(1..col.0.len()).rev().find_map(|n| {
		let fd = fields.iter().find(|fd| fd.name.0.as_slice() == &col.0[..n])?;
		col.0[n..].iter().try_fold(fd.field_kind.as_ref()?, |kind, part| match part {
			Part::Field(name) => object_literal_field(kind, name.as_str()),
			_ => None,
		})
	})
}

/// The kind an object literal type (or one made optional) declares for
/// `name`. A `NONE` or `NULL` parent reads its fields as `NONE`, which the
/// field's own kind need not admit, so only the literal's declaration is
/// returned; any other alternative leaves the field's kind unknown.
fn object_literal_field<'a>(kind: &'a Kind, name: &str) -> Option<&'a Kind> {
	match kind {
		Kind::Literal(KindLiteral::Object(fields)) => fields.get(name),
		Kind::Either(kinds) => {
			let mut objects = kinds.iter().filter(|k| !matches!(k, Kind::None | Kind::Null));
			match (objects.next(), objects.next()) {
				(Some(object), None) => object_literal_field(object, name),
				_ => None,
			}
		}
		_ => None,
	}
}

/// Whether every value of `kind` is a `Value::Array`, `NONE` or `NULL`, and
/// some value is an array. Unions are read through at any depth.
fn kind_holds_only_arrays(kind: &Kind) -> bool {
	/// `None` when some value of `kind` is neither an array, `NONE` nor
	/// `NULL`; otherwise whether some value is an array.
	fn arrays(kind: &Kind) -> Option<bool> {
		match kind {
			Kind::Array(..) | Kind::Literal(KindLiteral::Array(_)) => Some(true),
			Kind::None | Kind::Null => Some(false),
			Kind::Either(kinds) => kinds.iter().try_fold(false, |seen, k| Some(arrays(k)? || seen)),
			_ => None,
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
/// each free column in turn and stops at the first it cannot state as a field
/// path — which includes every column holding `*`, whose record sits at one key
/// position per element rather than at one place in the order. The id is
/// claimed only when every free column was: it orders the entries that agree
/// on all of them, and says nothing across a column the claim left out.
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
		_ => 0,
	};
	let free = ix_def.cols.iter().skip(pinned);
	let mut ordering: Vec<SortProperty> =
		free.clone().map_while(|col| field_path_from_idiom(col).ok().map(property)).collect();
	if !index_ref.is_unique() && !ix_def.cols.is_empty() && ordering.len() == free.len() {
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
///    - Prefer index that covers ORDER BY
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
	mut candidates: Vec<IndexCandidate>,
	with_hints: Option<&With>,
	direction: Direction,
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
	// KNN prefilter (#548) or the pushed-down residual instead.
	if let Some(pos) = candidates.iter().position(|c| matches!(c.access, BTreeAccess::Knn { .. })) {
		// Overriding an explicit hint deserves a signal — mirroring the
		// unmatched-hint warning below — so "why isn't my index being used"
		// investigations have something to find.
		if let Some(With::Index(names)) = with_hints {
			tracing::warn!(
				target: "surreal::index",
				hinted = ?names,
				"WITH INDEX hint overridden: a KNN operator can only be computed by its KnnScan",
			);
		}
		return candidates.swap_remove(pos).to_access_path(direction);
	}

	// WITH INDEX names - find the hinted index
	if let Some(With::Index(names)) = with_hints {
		if let Some(candidate) = find_hinted_index(&candidates, names) {
			return candidate.to_access_path(direction);
		}
		// Hint did not match any candidate. The most common cause is that
		// the user named an index but no WHERE conjunct refers to its
		// leading column. We log a warning so debugging "why isn't my
		// index being used" tickets has a signal, then fall through to
		// best-effort selection / table scan.
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
	candidates.sort_by_key(|c| (std::cmp::Reverse(c.score()), c.index_ref.idx));
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

#[cfg(test)]
mod tests {
	//! Unit tests for the plan-time access-path types.
	//!
	//! Four concerns, one nested module each:
	//!
	//! - [`index_ref`] — how an [`IndexRef`] resolves and compares, which the analyser's candidate
	//!   dedupe and the union planner both rely on.
	//! - [`full_range`] — the `is_full_range_scan` flag the SELECT planner reads before swapping a
	//!   selectivity-free index scan for a multi-index union.
	//! - [`describe`] — the `access:` attribute rendered into EXPLAIN output.
	//! - [`selection`] — `select_access_path` hint precedence and scoring.
	//! - [`residual`] — which WHERE conjuncts the chosen shape lets the planner drop from the
	//!   residual filter. Both directions matter: keeping a covered leaf only costs time, dropping
	//!   an uncovered one returns wrong rows.
	//! - [`array_columns`] — which b-tree columns `with_array_columns_as_elements` plans as element
	//!   columns, from the field kinds declared for them.

	use std::str::FromStr;

	use surrealdb_strand::Strand;
	use surrealdb_types::ToSql;

	use super::*;
	use crate::catalog::{FullTextParams, Index, IndexId, Scoring};
	use crate::exec::index::IndexCandidate;
	use crate::exec::planner::util::strip_index_conditions;
	use crate::expr::operator::{BooleanOperator, MatchesOperator};
	use crate::expr::{Cond, Expr, Idiom};

	// ------------------------------------------------------------------
	// Fixtures
	// ------------------------------------------------------------------

	/// A minimal `IndexDefinition`. `index_id` is synthetic — nothing here
	/// touches the catalog.
	fn idx_def(id: u32, name: &str, cols: &[&str], kind: Index) -> IndexDefinition {
		IndexDefinition {
			index_id: IndexId(id),
			name: Strand::from(name),
			table_name: "t".into(),
			cols: cols.iter().map(|c| Idiom::from_str(c).expect("valid idiom")).collect(),
			index: kind,
			count_cond: None,
			comment: None,
			prepare_remove: false,
			format_version: 1,
		}
	}

	fn idx_basic(id: u32, name: &str, cols: &[&str]) -> IndexDefinition {
		idx_def(id, name, cols, Index::Idx)
	}

	fn idx_uniq(id: u32, name: &str, cols: &[&str]) -> IndexDefinition {
		idx_def(id, name, cols, Index::Uniq)
	}

	fn idx_ft(id: u32, name: &str, cols: &[&str]) -> IndexDefinition {
		idx_def(
			id,
			name,
			cols,
			Index::FullText(FullTextParams {
				analyzer: "simple".into(),
				highlight: false,
				scoring: Scoring::Bm {
					k1: 1.2,
					b: 0.75,
				},
			}),
		)
	}

	/// Wrap definitions in the shared list an [`IndexRef`] indexes into.
	fn refs(defs: Vec<IndexDefinition>) -> Arc<[IndexDefinition]> {
		Arc::<[_]>::from(defs.into_boxed_slice())
	}

	fn index_ref(defs: Vec<IndexDefinition>, idx: usize) -> IndexRef {
		IndexRef::new(refs(defs), idx)
	}

	fn candidate(defs: Vec<IndexDefinition>, idx: usize, access: BTreeAccess) -> IndexCandidate {
		IndexCandidate::new(index_ref(defs, idx), access)
	}

	fn matches_op() -> MatchesOperator {
		MatchesOperator {
			rf: None,
			operator: BooleanOperator::And,
		}
	}

	fn num(n: i64) -> Value {
		Value::from(n)
	}

	fn range(start: Bound<Value>, end: Bound<Value>) -> BTreeAccess {
		BTreeAccess::Range {
			range: Range {
				start,
				end,
			},
		}
	}

	/// Parse `snippet` as the WHERE clause of a SELECT so the tests state
	/// predicates as source text rather than hand-built AST nodes.
	fn parse_cond(snippet: &str) -> Cond {
		let src = format!("SELECT * FROM t WHERE {snippet}");
		let ast = crate::syn::parse(&src).expect("parse");
		let mut exprs = ast.expressions;
		assert_eq!(exprs.len(), 1, "expected one statement from {src:?}");
		let top: crate::expr::TopLevelExpr = exprs.remove(0).into();
		match top {
			crate::expr::TopLevelExpr::Expr(Expr::Select(s)) => s.cond.expect("WHERE"),
			other => panic!("expected SELECT, got {other:?}"),
		}
	}

	/// The residual WHERE left after the given access shape consumed what it
	/// covers, rendered as SurrealQL. `None` means the index consumed all of it
	/// and the planner installs no Filter.
	fn residual(cond: &str, access: &BTreeAccess, cols: &[&str]) -> Option<String> {
		let cols: Vec<Idiom> =
			cols.iter().map(|c| Idiom::from_str(c).expect("valid idiom")).collect();
		strip_index_conditions(&parse_cond(cond), access, &cols).map(|c| c.0.to_sql())
	}

	// ------------------------------------------------------------------
	// 1. IndexRef
	// ------------------------------------------------------------------
	mod index_ref {
		use super::*;

		#[test]
		fn resolves_the_definition_at_its_own_position() {
			let r =
				index_ref(vec![idx_basic(1, "ix_a", &["a"]), idx_basic(2, "ix_b", &["b", "c"])], 1);
			assert_eq!(r.definition().name.as_str(), "ix_b");
			// Deref reaches the same definition's fields directly.
			assert_eq!(r.cols.len(), 2);
		}

		#[test]
		fn is_unique_holds_only_for_the_uniq_kind() {
			assert!(!index_ref(vec![idx_basic(1, "ix", &["a"])], 0).is_unique());
			assert!(index_ref(vec![idx_uniq(1, "ix", &["a"])], 0).is_unique());
			assert!(
				!index_ref(vec![idx_ft(1, "ix", &["a"])], 0).is_unique(),
				"a full-text index is not a unique b-tree"
			);
		}

		#[test]
		fn identity_is_the_position_alone() {
			use std::collections::hash_map::DefaultHasher;
			use std::hash::{Hash, Hasher};

			fn hash(r: &IndexRef) -> u64 {
				let mut h = DefaultHasher::new();
				r.hash(&mut h);
				h.finish()
			}

			let list = vec![idx_basic(1, "ix_a", &["a"]), idx_basic(2, "ix_b", &["b"])];
			let first = index_ref(list.clone(), 0);
			let second = index_ref(list, 1);
			assert_ne!(first, second);
			assert_ne!(hash(&first), hash(&second));

			// Equality and hashing ignore the list itself, so refs are only
			// comparable when they were drawn from the same table's index
			// list — which is the only way the analyser builds them.
			let other_list = index_ref(vec![idx_uniq(9, "unrelated", &["z"])], 0);
			assert_eq!(first, other_list);
			assert_eq!(hash(&first), hash(&other_list));
		}
	}

	// ------------------------------------------------------------------
	// 2. is_full_range_scan — read by the planner before union substitution
	// ------------------------------------------------------------------
	mod full_range {
		use super::*;

		fn btree(access: BTreeAccess) -> AccessPath {
			AccessPath::BTreeScan {
				index_ref: index_ref(vec![idx_basic(1, "ix_a", &["a"])], 0),
				access,
				direction: Direction::Forward,
			}
		}

		#[test]
		fn doubly_unbounded_btree_range_is_a_full_range_scan() {
			assert!(btree(range(Bound::Unbounded, Bound::Unbounded)).is_full_range_scan());
		}

		#[test]
		fn any_bound_makes_the_scan_selective() {
			for access in [
				range(Bound::Included(num(1)), Bound::Unbounded),
				range(Bound::Excluded(num(1)), Bound::Unbounded),
				range(Bound::Unbounded, Bound::Included(num(9))),
				range(Bound::Unbounded, Bound::Excluded(num(9))),
				range(Bound::Included(num(1)), Bound::Included(num(9))),
			] {
				assert!(
					!btree(access.clone()).is_full_range_scan(),
					"{} carries WHERE selectivity",
					access.describe()
				);
			}
		}

		#[test]
		fn other_shapes_are_never_full_range_scans() {
			let ft = index_ref(vec![idx_ft(1, "ix_ft", &["body"])], 0);
			let paths = vec![
				AccessPath::TableScan,
				AccessPath::EmptyScan,
				btree(BTreeAccess::Equality(num(1))),
				// A prefix-less compound covers the whole index but is not
				// recognised as a full-range scan.
				btree(BTreeAccess::Compound {
					prefix: vec![],
					range: None,
				}),
				AccessPath::FullTextSearch {
					index_ref: ft.clone(),
					query: "hello".to_owned(),
					operator: matches_op(),
				},
				AccessPath::KnnSearch {
					index_ref: ft,
					vector: vec![Number::Int(1)],
					k: 3,
					ef: 10,
					prefilter: None,
				},
				AccessPath::Union {
					paths: vec![btree(range(Bound::Unbounded, Bound::Unbounded))],
					dedupe: true,
				},
			];
			for path in paths {
				assert!(!path.is_full_range_scan(), "{path:?} is not a full-range b-tree scan");
			}
		}
	}

	// ------------------------------------------------------------------
	// 3. describe — the EXPLAIN `access:` attribute
	// ------------------------------------------------------------------
	mod describe {
		use super::*;

		#[test]
		fn equality_renders_the_sql_literal() {
			assert_eq!(BTreeAccess::Equality(num(5)).describe(), "= 5");
			assert_eq!(BTreeAccess::Equality(Value::from("x")).describe(), "= 'x'");
			assert_eq!(BTreeAccess::Equality(Value::None).describe(), "= NONE");
		}

		#[test]
		fn every_range_bound_combination_renders() {
			let cases = [
				(Bound::Included(num(1)), Bound::Included(num(9)), ">=1 <=9"),
				(Bound::Included(num(1)), Bound::Excluded(num(9)), ">=1 <9"),
				(Bound::Excluded(num(1)), Bound::Included(num(9)), ">1 <=9"),
				(Bound::Excluded(num(1)), Bound::Excluded(num(9)), ">1 <9"),
				(Bound::Included(num(1)), Bound::Unbounded, ">=1"),
				(Bound::Excluded(num(1)), Bound::Unbounded, ">1"),
				(Bound::Unbounded, Bound::Included(num(9)), "<=9"),
				(Bound::Unbounded, Bound::Excluded(num(9)), "<9"),
				// A doubly-unbounded range describes as nothing at all.
				(Bound::Unbounded, Bound::Unbounded, ""),
			];
			for (start, end, expected) in cases {
				assert_eq!(range(start, end).describe(), expected);
			}
		}

		#[test]
		fn compound_renders_the_prefix_and_any_range() {
			let prefix = vec![num(1), Value::from("b")];
			assert_eq!(
				BTreeAccess::Compound {
					prefix: prefix.clone(),
					range: None,
				}
				.describe(),
				"[1, 'b']"
			);
			assert_eq!(
				BTreeAccess::Compound {
					prefix,
					range: Some((BinaryOperator::MoreThan, num(3))),
				}
				.describe(),
				"[1, 'b'] MoreThan 3"
			);
		}

		#[test]
		fn fulltext_and_knn_render_their_own_shorthand() {
			assert_eq!(
				BTreeAccess::FullText {
					query: "hello world".to_owned(),
					operator: matches_op(),
				}
				.describe(),
				"@@ hello world"
			);
			assert_eq!(
				BTreeAccess::Knn {
					vector: vec![Number::Int(1), Number::Int(2)],
					k: 4,
					ef: 40,
				}
				.describe(),
				"knn 4"
			);
		}
	}

	// ------------------------------------------------------------------
	// 4. select_access_path
	// ------------------------------------------------------------------
	mod selection {
		use super::*;

		fn defs() -> Vec<IndexDefinition> {
			vec![idx_basic(1, "ix_a", &["a"]), idx_uniq(2, "ix_b", &["b"])]
		}

		fn scan_index(path: &AccessPath) -> &str {
			match path {
				AccessPath::BTreeScan {
					index_ref,
					..
				} => index_ref.name.as_str(),
				other => panic!("expected BTreeScan, got {other:?}"),
			}
		}

		#[test]
		fn noindex_hint_forces_a_table_scan() {
			// Outranks everything, including a candidate the analyser proved
			// empty (which would otherwise short-circuit the whole pipeline).
			let mut empty = candidate(defs(), 1, BTreeAccess::Equality(num(1)));
			empty.empty = true;
			let path = select_access_path(vec![empty], Some(&With::NoIndex), Direction::Forward);
			assert!(matches!(path, AccessPath::TableScan));
		}

		#[test]
		fn named_hint_wins_over_a_better_scoring_candidate() {
			// `ix_b` is a unique equality (score 1000) and `ix_a` a
			// half-bounded range (200), yet the hint decides.
			let candidates = vec![
				candidate(defs(), 0, range(Bound::Included(num(1)), Bound::Unbounded)),
				candidate(defs(), 1, BTreeAccess::Equality(num(1))),
			];
			let with = With::Index(vec!["ix_a".to_owned()]);
			let path = select_access_path(candidates, Some(&with), Direction::Forward);
			assert_eq!(scan_index(&path), "ix_a");
		}

		#[test]
		fn hint_name_order_decides_between_two_hinted_candidates() {
			let candidates = vec![
				candidate(defs(), 0, BTreeAccess::Equality(num(1))),
				candidate(defs(), 1, BTreeAccess::Equality(num(1))),
			];
			// The names are searched in the order the user wrote them, not in
			// candidate order.
			let with = With::Index(vec!["ix_b".to_owned(), "ix_a".to_owned()]);
			let path = select_access_path(candidates, Some(&with), Direction::Forward);
			assert_eq!(scan_index(&path), "ix_b");
		}

		#[test]
		fn unmatched_hint_falls_back_to_best_effort_selection() {
			let candidates = vec![candidate(defs(), 1, BTreeAccess::Equality(num(1)))];
			let with = With::Index(vec!["nonexistent".to_owned()]);
			let path = select_access_path(candidates, Some(&with), Direction::Forward);
			assert_eq!(scan_index(&path), "ix_b", "an unmatched hint does not veto the plan");
		}

		#[test]
		fn no_candidates_is_a_table_scan() {
			assert!(matches!(
				select_access_path(vec![], None, Direction::Forward),
				AccessPath::TableScan
			));
		}

		#[test]
		fn an_empty_candidate_short_circuits_to_empty_scan() {
			// `empty` scores u32::MAX, so it wins over any real access shape
			// and `to_access_path` discards the shape entirely.
			let mut empty = candidate(defs(), 0, range(Bound::Included(num(1)), Bound::Unbounded));
			empty.empty = true;
			let candidates = vec![candidate(defs(), 1, BTreeAccess::Equality(num(1))), empty];
			let path = select_access_path(candidates, None, Direction::Forward);
			assert!(matches!(path, AccessPath::EmptyScan));
		}

		#[test]
		fn a_score_tie_resolves_to_the_first_index_in_catalog_order() {
			// Both candidates are non-unique equalities, so they tie at 500
			// and catalog position decides. The fixture lists them in catalog
			// order, which for a real table is name order.
			let defs = vec![idx_basic(1, "ix_a1", &["a"]), idx_basic(2, "ix_a2", &["a"])];
			let candidates = vec![
				candidate(defs.clone(), 0, BTreeAccess::Equality(num(1))),
				candidate(defs, 1, BTreeAccess::Equality(num(1))),
			];
			let path = select_access_path(candidates, None, Direction::Forward);
			assert_eq!(scan_index(&path), "ix_a1");
		}

		#[test]
		fn a_score_tie_resolves_the_same_way_whatever_order_candidates_arrive_in() {
			let defs = vec![idx_basic(1, "ix_a1", &["a"]), idx_basic(2, "ix_a2", &["a"])];
			let reversed = vec![
				candidate(defs.clone(), 1, BTreeAccess::Equality(num(1))),
				candidate(defs, 0, BTreeAccess::Equality(num(1))),
			];
			let path = select_access_path(reversed, None, Direction::Forward);
			assert_eq!(scan_index(&path), "ix_a1");
		}

		#[test]
		fn the_requested_direction_reaches_the_btree_scan() {
			let candidates = vec![candidate(defs(), 1, BTreeAccess::Equality(num(1)))];
			let path = select_access_path(candidates, None, Direction::Backward);
			match path {
				AccessPath::BTreeScan {
					direction,
					..
				} => assert_eq!(direction, Direction::Backward),
				other => panic!("expected BTreeScan, got {other:?}"),
			}
		}

		#[test]
		fn specialised_access_shapes_get_their_own_path_kind() {
			// A full-text or KNN candidate must not become a b-tree scan —
			// those shapes are executed by dedicated operators.
			let ft_defs = vec![idx_ft(1, "ix_ft", &["body"])];
			let ft = candidate(
				ft_defs.clone(),
				0,
				BTreeAccess::FullText {
					query: "hello".to_owned(),
					operator: matches_op(),
				},
			);
			assert!(matches!(
				select_access_path(vec![ft], None, Direction::Forward),
				AccessPath::FullTextSearch { .. }
			));

			let knn = candidate(
				ft_defs,
				0,
				BTreeAccess::Knn {
					vector: vec![Number::Int(1)],
					k: 3,
					ef: 10,
				},
			);
			assert!(matches!(
				select_access_path(vec![knn], None, Direction::Forward),
				AccessPath::KnnSearch {
					k: 3,
					ef: 10,
					..
				}
			));
		}
	}

	// ------------------------------------------------------------------
	// 5. Residual WHERE after the access path consumed what it covers
	// ------------------------------------------------------------------
	mod residual {
		use super::*;

		#[test]
		fn equality_consumes_its_own_leaf_and_leaves_the_rest() {
			let access = BTreeAccess::Equality(num(5));
			assert_eq!(residual("a = 5", &access, &["a"]), None);
			assert_eq!(residual("a = 5 AND b = 1", &access, &["a"]), Some("b = 1".to_owned()));
		}

		#[test]
		fn equality_on_another_value_or_column_is_retained() {
			let access = BTreeAccess::Equality(num(5));
			// The seek is on 5; a leaf comparing against 6 still has to run.
			assert_eq!(residual("a = 6", &access, &["a"]), Some("a = 6".to_owned()));
			assert_eq!(residual("b = 5", &access, &["a"]), Some("b = 5".to_owned()));
		}

		#[test]
		fn range_consumes_only_the_leaf_its_bound_came_from() {
			let access = range(Bound::Excluded(num(5)), Bound::Unbounded);
			assert_eq!(residual("a > 5", &access, &["a"]), None);
			// An inclusive leaf admits `a = 5`, which the exclusive bound skips.
			assert_eq!(residual("a >= 5", &access, &["a"]), Some("a >= 5".to_owned()));
			assert_eq!(residual("a > 6", &access, &["a"]), Some("a > 6".to_owned()));
		}

		#[test]
		fn a_bounded_range_consumes_both_of_its_leaves() {
			let access = range(Bound::Included(num(1)), Bound::Excluded(num(9)));
			assert_eq!(residual("a >= 1 AND a < 9", &access, &["a"]), None);
		}

		#[test]
		fn flipped_operand_order_is_still_consumed() {
			// `5 < a` is the same constraint as `a > 5`.
			let access = range(Bound::Excluded(num(5)), Bound::Unbounded);
			assert_eq!(residual("5 < a", &access, &["a"]), None);
		}

		#[test]
		fn compound_prefix_consumes_positional_equalities() {
			let access = BTreeAccess::Compound {
				prefix: vec![num(1), num(2)],
				range: None,
			};
			assert_eq!(residual("a = 1 AND b = 2", &access, &["a", "b", "c"]), None);
			assert_eq!(
				residual("a = 1 AND b = 2 AND c = 3", &access, &["a", "b", "c"]),
				Some("c = 3".to_owned()),
				"no prefix value pins c"
			);
		}

		#[test]
		fn a_prefix_value_at_the_wrong_column_is_retained() {
			// prefix [1, 2] pins a = 1 and b = 2. `a = 2` matches a prefix
			// *value* but not at a's position, so dropping it would return
			// rows where a = 1.
			let access = BTreeAccess::Compound {
				prefix: vec![num(1), num(2)],
				range: None,
			};
			assert_eq!(residual("a = 2", &access, &["a", "b"]), Some("a = 2".to_owned()));
			assert_eq!(residual("b = 1", &access, &["a", "b"]), Some("b = 1".to_owned()));
		}

		#[test]
		fn compound_range_is_consumed_only_on_the_column_after_the_prefix() {
			let access = BTreeAccess::Compound {
				prefix: vec![num(1)],
				range: Some((BinaryOperator::MoreThan, num(2))),
			};
			assert_eq!(residual("a = 1 AND b > 2", &access, &["a", "b", "c"]), None);
			// Same operator and value, wrong column.
			assert_eq!(
				residual("a = 1 AND c > 2", &access, &["a", "b", "c"]),
				Some("c > 2".to_owned())
			);
			// Right column, different operator.
			assert_eq!(
				residual("a = 1 AND b >= 2", &access, &["a", "b", "c"]),
				Some("b >= 2".to_owned())
			);
		}

		#[test]
		fn not_none_is_consumed_through_its_exclusive_none_encoding() {
			// `a != NONE` is analysed into a range excluding NONE, and into a
			// compound `(MoreThan, NONE)` after an equality prefix.
			let as_range = range(Bound::Excluded(Value::None), Bound::Unbounded);
			assert_eq!(residual("a != NONE", &as_range, &["a"]), None);

			let as_compound = BTreeAccess::Compound {
				prefix: vec![num(1)],
				range: Some((BinaryOperator::MoreThan, Value::None)),
			};
			assert_eq!(residual("a = 1 AND b != NONE", &as_compound, &["a", "b"]), None);
		}

		#[test]
		fn a_leaf_under_or_is_never_consumed() {
			// Stripping stops at AND boundaries: an OR branch is only a
			// candidate for the whole predicate, never for a partial strip.
			let access = BTreeAccess::Equality(num(5));
			assert_eq!(
				residual("a = 5 OR b = 1", &access, &["a"]),
				Some("a = 5 OR b = 1".to_owned())
			);
		}

		#[test]
		fn single_element_in_is_consumed_only_with_the_idiom_on_the_left() {
			let access = BTreeAccess::Equality(num(5));
			// `a IN [5]` is the analyser's canonical form for `a = 5`.
			assert_eq!(residual("a IN [5]", &access, &["a"]), None);
			// `[5] INSIDE a` means `[5].contains(a)` — different semantics, and
			// the analyser produces no candidate for it, so it must stay.
			assert_eq!(residual("[5] INSIDE a", &access, &["a"]), Some("[5] INSIDE a".to_owned()));
		}

		#[test]
		fn containment_is_consumed_only_on_an_array_element_column() {
			// The analyser turns `tags CONTAINS 'x'` on a `tags.*` index into
			// an equality seek, so the leaf is already enforced by the range.
			let access = BTreeAccess::Equality(Value::from("x"));
			assert_eq!(residual("tags CONTAINS 'x'", &access, &["tags.*"]), None);
			// The same leaf against a scalar column is not covered.
			assert_eq!(
				residual("tags CONTAINS 'x'", &access, &["tags"]),
				Some("tags CONTAINS 'x'".to_owned())
			);
		}

		#[test]
		fn a_non_literal_operand_is_retained() {
			// Only plan-time literals can be compared against the seek value.
			let access = BTreeAccess::Equality(num(5));
			assert_eq!(residual("a = $p", &access, &["a"]), Some("a = $p".to_owned()));
		}

		#[test]
		fn fulltext_and_knn_shapes_consume_nothing() {
			// Those paths have their own strippers; this one must not touch
			// their predicates.
			let ft = BTreeAccess::FullText {
				query: "hello".to_owned(),
				operator: matches_op(),
			};
			assert_eq!(residual("a = 5", &ft, &["a"]), Some("a = 5".to_owned()));

			let knn = BTreeAccess::Knn {
				vector: vec![Number::Int(1)],
				k: 3,
				ef: 10,
			};
			assert_eq!(residual("a = 5", &knn, &["a"]), Some("a = 5".to_owned()));
		}
	}

	mod array_columns {
		use super::*;

		fn field(name: &str, kind: Option<Kind>) -> FieldDefinition {
			FieldDefinition {
				name: Idiom::from_str(name).expect("valid idiom"),
				field_kind: kind,
				..Default::default()
			}
		}

		/// The columns of the first index after the rewrite, as SurrealQL.
		fn planned(cols: &[&str], kind: Index, fields: &[FieldDefinition]) -> Vec<String> {
			let indexes: Arc<[IndexDefinition]> = vec![idx_def(1, "ix", cols, kind)].into();
			with_array_columns_as_elements(indexes, fields).indexes[0]
				.cols
				.iter()
				.map(|c| c.to_sql())
				.collect()
		}

		fn array_of_strings() -> Kind {
			Kind::Array(Box::new(Kind::String), None)
		}

		#[test]
		fn a_column_whose_kind_holds_only_arrays_is_planned_as_its_elements() {
			let kinds = [
				array_of_strings(),
				Kind::Either(vec![Kind::None, array_of_strings()]),
				Kind::Either(vec![Kind::None, Kind::Null, array_of_strings()]),
				Kind::Literal(KindLiteral::Array(vec![Kind::String])),
			];
			for kind in kinds {
				let fields = [field("acl", Some(kind.clone()))];
				assert_eq!(planned(&["acl"], Index::Idx, &fields), ["acl.*"], "{kind:?}");
				assert_eq!(planned(&["acl"], Index::Uniq, &fields), ["acl.*"], "{kind:?}");
			}
		}

		#[test]
		fn only_the_columns_that_admit_an_array_change() {
			let fields =
				[field("acl", Some(array_of_strings())), field("name", Some(Kind::String))];
			assert_eq!(planned(&["name", "acl"], Index::Idx, &fields), ["name", "acl.*"]);
		}

		#[test]
		fn a_column_that_can_store_a_whole_value_is_taken_as_written() {
			let kinds = [
				Kind::String,
				Kind::Either(vec![Kind::None, Kind::Int]),
				// A set is stored whole, not one entry per element.
				Kind::Set(Box::new(Kind::String), None),
				Kind::Object,
				// A kind that also admits a scalar: a string is stored whole, so an
				// element seek misses the substring `CONTAINS` finds in it.
				Kind::Any,
				Kind::Either(vec![Kind::String, array_of_strings()]),
				Kind::Either(vec![Kind::None, Kind::Null]),
			];
			for kind in kinds {
				let fields = [field("acl", Some(kind.clone()))];
				assert_eq!(planned(&["acl"], Index::Idx, &fields), ["acl"], "{kind:?}");
			}
		}

		#[test]
		fn an_undeclared_kind_is_taken_as_written() {
			assert_eq!(planned(&["acl"], Index::Idx, &[field("acl", None)]), ["acl"]);
			assert_eq!(planned(&["acl"], Index::Idx, &[]), ["acl"]);
		}

		#[test]
		fn a_column_already_over_elements_or_flattened_is_taken_as_written() {
			let fields = [field("acl", Some(array_of_strings()))];
			let fields_flat = [field("acl…", Some(array_of_strings()))];
			assert_eq!(planned(&["acl.*"], Index::Idx, &fields), ["acl.*"]);
			assert_eq!(planned(&["acl…"], Index::Idx, &fields_flat), ["acl…"]);
		}

		#[test]
		fn only_btree_indexes_are_rewritten() {
			let fields = [field("acl", Some(array_of_strings()))];
			assert_eq!(planned(&["acl"], Index::Count(None), &fields), ["acl"]);
		}

		#[test]
		fn a_union_is_read_through_at_any_depth() {
			let nested = Kind::Either(vec![
				Kind::None,
				Kind::Either(vec![array_of_strings(), Kind::Array(Box::new(Kind::Int), None)]),
			]);
			assert_eq!(planned(&["acl"], Index::Idx, &[field("acl", Some(nested))]), ["acl.*"]);
			let nested_scalar = Kind::Either(vec![
				array_of_strings(),
				Kind::Either(vec![Kind::None, Kind::String]),
			]);
			assert_eq!(
				planned(&["acl"], Index::Idx, &[field("acl", Some(nested_scalar))]),
				["acl"]
			);
		}

		#[test]
		fn an_ancestor_object_literal_declares_the_column_kind() {
			let object = |kind: Kind| {
				Kind::Literal(KindLiteral::Object(
					[(Strand::from("tags"), kind)].into_iter().collect(),
				))
			};
			let declared = [field("metadata", Some(object(array_of_strings())))];
			assert_eq!(planned(&["metadata.tags"], Index::Idx, &declared), ["metadata.tags.*"]);
			// An optional parent reads its fields as NONE, which an element
			// column stores as the column does.
			let optional = [field(
				"metadata",
				Some(Kind::Either(vec![Kind::None, object(array_of_strings())])),
			)];
			assert_eq!(planned(&["metadata.tags"], Index::Idx, &optional), ["metadata.tags.*"]);
			let scalar = [field("metadata", Some(object(Kind::String)))];
			assert_eq!(planned(&["metadata.tags"], Index::Idx, &scalar), ["metadata.tags"]);
			// A plain `object` declares nothing about its fields.
			let opaque = [field("metadata", Some(Kind::Object))];
			assert_eq!(planned(&["metadata.tags"], Index::Idx, &opaque), ["metadata.tags"]);
			// The column's own definition wins over its ancestor's literal.
			let both = [
				field("metadata", Some(object(array_of_strings()))),
				field("metadata.tags", Some(Kind::String)),
			];
			assert_eq!(planned(&["metadata.tags"], Index::Idx, &both), ["metadata.tags"]);
			// A definition of the column that declares no kind leaves the
			// ancestor's literal in force.
			let untyped_child =
				[field("metadata", Some(object(array_of_strings()))), field("metadata.tags", None)];
			assert_eq!(
				planned(&["metadata.tags"], Index::Idx, &untyped_child),
				["metadata.tags.*"]
			);
		}

		#[test]
		fn a_column_through_an_element_path_is_taken_as_written() {
			// `items[*].tags` stores each item's array whole, one entry per
			// item, so its entries are arrays rather than their elements.
			let fields = [field("items[*].tags", Some(array_of_strings()))];
			assert_eq!(planned(&["items[*].tags"], Index::Idx, &fields), ["items.*.tags"]);
		}

		#[test]
		fn a_partly_rewritten_list_keeps_every_index_in_place() {
			let indexes: Arc<[IndexDefinition]> = vec![
				idx_def(1, "a", &["name"], Index::Idx),
				idx_def(2, "b", &["acl"], Index::Idx),
				idx_def(3, "c", &["name"], Index::Uniq),
			]
			.into();
			let fields =
				[field("acl", Some(array_of_strings())), field("name", Some(Kind::String))];
			let out = with_array_columns_as_elements(indexes, &fields);
			let flags: Vec<Vec<bool>> =
				out.rewritten.as_deref().expect("rewritten").iter().map(|f| f.to_vec()).collect();
			assert_eq!(flags, [vec![], vec![true], vec![]]);
			let cols: Vec<_> =
				out.indexes.iter().map(|ix| (ix.name.as_str(), ix.cols[0].to_sql())).collect();
			assert_eq!(
				cols,
				[("a", "name".to_owned()), ("b", "acl.*".to_owned()), ("c", "name".to_owned())]
			);
		}

		#[test]
		fn the_field_list_is_needed_only_for_a_btree_column_without_elements() {
			let with = |cols: &[&str], kind: Index| {
				may_have_array_columns(&[idx_def(1, "ix", cols, kind)])
			};
			assert!(with(&["acl"], Index::Idx));
			assert!(with(&["acl.*", "name"], Index::Uniq));
			assert!(!with(&["acl.*"], Index::Idx));
			assert!(!with(&["acl"], Index::Count(None)));
		}

		#[test]
		fn withholding_btree_indexes_keeps_the_others() {
			let indexes: Arc<[IndexDefinition]> = vec![
				idx_def(1, "a", &["acl"], Index::Idx),
				idx_ft(2, "b", &["body"]),
				idx_def(3, "c", &["name"], Index::Uniq),
			]
			.into();
			let names: Vec<_> = without_btree_indexes(indexes)
				.iter()
				.map(|ix| ix.name.as_str().to_owned())
				.collect();
			assert_eq!(names, ["b"]);
		}

		#[test]
		fn an_unchanged_list_is_returned_as_is() {
			let indexes: Arc<[IndexDefinition]> =
				vec![idx_def(1, "ix", &["name"], Index::Idx)].into();
			let fields = [field("name", Some(Kind::String))];
			let out = with_array_columns_as_elements(Arc::clone(&indexes), &fields);
			assert!(Arc::ptr_eq(&indexes, &out.indexes));
			assert!(out.rewritten.is_none());
		}

		#[test]
		fn every_column_of_a_wide_index_is_rewritten() {
			let names: Vec<String> = (0..70).map(|c| format!("c{c}")).collect();
			let cols: Vec<&str> = names.iter().map(String::as_str).collect();
			let fields: Vec<_> = names.iter().map(|n| field(n, Some(array_of_strings()))).collect();
			let indexes: Arc<[IndexDefinition]> = vec![idx_def(1, "ix", &cols, Index::Idx)].into();
			let out = with_array_columns_as_elements(indexes, &fields);
			assert_eq!(out.indexes[0].cols[69].to_sql(), "c69.*");
			let flags = &out.rewritten.as_ref().expect("rewritten")[0];
			assert_eq!(flags.len(), 70);
			assert!(flags.iter().all(|f| *f));
		}
	}
}
