//! Index analysis for matching WHERE conditions to available indexes.
//!
//! The [`IndexAnalyzer`] examines query conditions and ORDER BY clauses to find
//! indexes that can accelerate the query.

use std::collections::HashMap;
use std::ops::Bound;
use std::sync::Arc;

use super::access_path::{
	AccessPath, BTreeAccess, BitmapPlan, BoundOperand, IndexRef, KnnPrefilterPlan, OperandRange,
	SpatialPointFilter, number_keyed_as_compared, select_access_path,
};
use crate::catalog::{Index, IndexDefinition, KeyStructure, spells_normalised_decimal_keys};
use crate::exec::planner::util::{
	KnnQueryVector, KnnVectorClass, classify_knn_vector, expr_contains_matches,
	idiom_matches_column_in, idiom_matches_containment, try_bound_operand, try_literal_to_value,
};
use crate::expr::logical::{LogicalCall, LogicalExpr, LogicalOrdering};
use crate::expr::operator::{MatchesOperator, NearestNeighbor, PrefixOperator};
use crate::expr::with::With;
use crate::expr::{BinaryOperator, Function, Idiom, Part};
use crate::kvs::Direction;
use crate::val::{Number, Range, RecordIdKey, Value};

/// Analyzes query conditions to find matching indexes.
pub struct IndexAnalyzer<'a> {
	/// Available indexes for the table
	pub indexes: Arc<[IndexDefinition]>,
	/// Optional WITH INDEX/NOINDEX hints
	pub with_hints: Option<&'a With>,
	/// The values of the parameters a KNN operator names as its query vector
	/// (see [`KnnQueryVectors`]).
	knn_query_vectors: Option<&'a KnnQueryVectors>,
	/// Per index of `indexes`, whether a containment leaf may not seek each
	/// column; a column past the end of its index's flags may be sought.
	/// `None` withholds nothing.
	no_containment: Option<Arc<[super::access_path::ColumnFlags]>>,
	/// Per index of `indexes`, whether each column is known to hold no array
	/// (see [`IndexRef::one_entry_per_record`]); a column past the end of its
	/// index's flags is not known to. `None` knows of none. Carried on each
	/// [`IndexRef`] made.
	scalar: Option<Arc<[super::access_path::ColumnFlags]>>,
	/// The function registry, which says which calls read the row (see
	/// [`classify_knn_vector`]).
	registry: &'a crate::exec::function::FunctionRegistry,
	/// The structure of the table's record ids, when they name their
	/// components: a b-tree column and a path that address the same
	/// component match whichever way each spells it
	/// ([`idiom_matches_column_in`]). `None` matches as written.
	key: Option<Arc<KeyStructure>>,
	/// Whether the caller may call `time::now()`, so that a comparison
	/// against it is a candidate bound that reads the statement clock. The
	/// scan reads the clock without checking the caller's capabilities, so a
	/// caller denied the function is never offered the bound: its comparison
	/// stays a filter, which raises the denial when it is evaluated.
	clock_allowed: bool,
	/// The literal count `k` of a `LIMIT k` without `START`, used to satisfy
	/// `ORDER BY vector::distance::<metric>(field, vec) ASC LIMIT k` via an ANN index.
	limit_k: Option<u32>,
	/// Per index of `indexes`, whether an HNSW or DiskANN index is known to
	/// hold an entry for every record of its table
	/// ([`super::access_path::ElementColumns::complete_vector`]): only such an
	/// index answers `ORDER BY vector::distance::<metric>(...) LIMIT k`, which
	/// orders every record.
	complete_vector: Option<Arc<[bool]>>,
	/// Per index of `indexes`, whether a `SPATIAL` index column is known from
	/// recorded [`super::super::datastore::index_shapes::TableIndexShapes`] to
	/// have stored only single-cell point geometries (no multi-cell covering
	/// recorded under [`super::super::datastore::index_shapes::ValueShape::MultiCell`]
	/// and no array recorded under [`super::super::datastore::index_shapes::ValueShape::Array`]),
	/// allowing query coverings to omit coarse ancestor probes.
	point_only_spatial: Option<Arc<[bool]>>,
	/// Per index of `indexes`, whether a `SPATIAL` index column may hold
	/// non-geometry values (including `NONE` and `NULL`, recorded under
	/// [`super::super::datastore::index_shapes::ValueShape::NonGeometry`]),
	/// which emit no spatial index cells and would cause `geo::distance` to
	/// raise a type error on a table scan.
	non_geometry_spatial: Option<Arc<[bool]>>,
	/// Per index of `indexes`, whether each column lists every record whose
	/// value at the column's path contains a value under that value, or under
	/// `NONE` or `NULL` (see
	/// [`super::access_path::ElementColumns::elements_or_nullish`]); a column
	/// past the end of its index's flags does not. `None` knows of none.
	elements_or_nullish: Option<Arc<[super::access_path::ColumnFlags]>>,
	/// Memoized Hilbert cell coverings and spherical area fractions computed
	/// during this analyzer's lifetime, keyed by a 64-bit hash of
	/// `(predicate, include_ancestors, max_ops)` and verified by equality on
	/// lookup so repeated planner entry points (`try_bitmap_count_fusion`,
	/// `try_bitmap_fusion`, `try_spatial_expansion`) do not recompute coverings
	/// or linearly compare large geometries.
	spatial_plan_cache: std::cell::RefCell<HashMap<u64, SpatialPlanCacheBucket>>,
}

type SpatialPlanCacheBucket =
	Vec<(SpatialPredicate, bool, usize, Arc<[surrealdb_idx::spatial::SpatialScanOp]>, f64)>;

/// The query vectors of the KNN operators whose right operand is a parameter,
/// by parameter name. Binding leaves such a parameter in place instead of
/// substituting a literal node per component; the planner resolves each one
/// once, and a KNN operator naming one reads its vector here.
pub type KnnQueryVectors = HashMap<String, Arc<[Number]>>;

impl<'a> IndexAnalyzer<'a> {
	/// Create a new analyzer for the given table and indexes.
	pub fn new(
		indexes: Arc<[IndexDefinition]>,
		with_hints: Option<&'a With>,
		registry: &'a crate::exec::function::FunctionRegistry,
	) -> Self {
		Self {
			indexes,
			with_hints,
			knn_query_vectors: None,
			no_containment: None,
			scalar: None,
			registry,
			key: None,
			clock_allowed: false,
			limit_k: None,
			complete_vector: None,
			point_only_spatial: None,
			non_geometry_spatial: None,
			elements_or_nullish: None,
			spatial_plan_cache: std::cell::RefCell::new(HashMap::new()),
		}
	}

	/// Mark the `SPATIAL` indexes that `flags` indicates hold only single-cell
	/// point geometries (no coarse cell coverings recorded) so query coverings
	/// skip coarse ancestor probes.
	pub fn with_point_only_spatial(mut self, flags: Arc<[bool]>) -> Self {
		self.point_only_spatial = Some(flags);
		self
	}

	/// Mark the element columns that `flags` indicates list every record
	/// under its elements or under `NONE` or `NULL`, so a containment leaf over
	/// the column's path that does not take each element is answered by seeking
	/// the value and both nullish keys.
	pub(crate) fn with_elements_or_nullish(
		mut self,
		flags: Arc<[super::access_path::ColumnFlags]>,
	) -> Self {
		self.elements_or_nullish = Some(flags);
		self
	}

	fn is_point_only_spatial(&self, idx: usize) -> bool {
		self.point_only_spatial.as_ref().and_then(|flags| flags.get(idx)).copied().unwrap_or(false)
	}

	/// Mark the `SPATIAL` indexes that `flags` indicates may hold non-geometry
	/// values (including `NONE` and `NULL`, recorded under
	/// `ValueShape::NonGeometry`) so `geo::distance` (`Cap`) declines index
	/// pushdown and preserves table-scan type errors.
	pub fn with_non_geometry_spatial(mut self, flags: Arc<[bool]>) -> Self {
		self.non_geometry_spatial = Some(flags);
		self
	}

	fn has_non_geometry_spatial(&self, idx: usize) -> bool {
		self.non_geometry_spatial
			.as_ref()
			.and_then(|flags| flags.get(idx))
			.copied()
			.unwrap_or(false)
	}

	/// Mark the HNSW and DiskANN indexes that `flags` indicates hold an entry
	/// for every record of their table.
	pub fn with_complete_vector(mut self, flags: Arc<[bool]>) -> Self {
		self.complete_vector = Some(flags);
		self
	}

	fn is_complete_vector(&self, idx: usize) -> bool {
		self.complete_vector.as_ref().and_then(|flags| flags.get(idx)).copied().unwrap_or(false)
	}

	/// Record the literal `LIMIT k` count of the statement so `analyze_order`
	/// can match `ORDER BY vector::distance::<metric>(field, vec) ASC LIMIT k`
	/// against an HNSW or DiskANN index.
	pub fn with_limit(mut self, limit_k: Option<u32>) -> Self {
		self.limit_k = limit_k;
		self
	}

	/// Accept a comparison against `time::now()`, or `time::now() ± <duration
	/// literal>`, as an index bound that reads the statement clock, where
	/// `allowed` says the caller may call `time::now()`.
	pub(crate) fn with_clock(mut self, allowed: bool) -> Self {
		self.clock_allowed = allowed;
		self
	}

	/// Match the b-tree columns that address a component of the table's
	/// record id by the component, as `key` describes them.
	pub(crate) fn with_key(mut self, key: Option<Arc<KeyStructure>>) -> Self {
		self.key = key;
		self
	}

	/// Know the columns `flags` marks, per index of `indexes`, to hold no
	/// array.
	pub(crate) fn with_scalar_columns(
		mut self,
		flags: Arc<[super::access_path::ColumnFlags]>,
	) -> Self {
		self.scalar = Some(flags);
		self
	}

	/// A reference to index `idx` of `indexes`, carrying which of its columns
	/// are known to hold no array and which are datetime components of the
	/// record id, whose entries order only by the whole second, and whether
	/// the record ids its entries end with order by value.
	fn index_ref(&self, idx: usize) -> IndexRef {
		let index_ref = IndexRef::new(&self.indexes, idx);
		let index_ref = match self.scalar.as_ref().and_then(|flags| flags.get(idx)) {
			Some(flags) if flags.contains(&true) => index_ref.with_scalar_columns(flags.clone()),
			_ => index_ref,
		};
		let Some(key) = self.key.as_deref() else {
			return index_ref;
		};
		let definition = &self.indexes[idx];
		let index_ref = match super::derived_columns::coarse_columns(
			key,
			&definition.cols,
			definition.format_version,
		) {
			Some(coarse) => index_ref.with_coarse_columns(coarse),
			None => index_ref,
		};
		if super::derived_columns::id_orders_coarsely(key) {
			index_ref.with_coarse_id()
		} else {
			index_ref
		}
	}

	/// A reference to every index of the analyzer, in order, as the candidates it builds carry
	/// them.
	pub(crate) fn index_refs(&self) -> Vec<IndexRef> {
		(0..self.indexes.len()).map(|idx| self.index_ref(idx)).collect()
	}

	/// Withhold the containment seek from the columns `flags` marks, per
	/// index of `indexes`.
	pub(crate) fn without_containment_on(
		mut self,
		flags: Arc<[super::access_path::ColumnFlags]>,
	) -> Self {
		self.no_containment = Some(flags);
		self
	}

	/// Whether a containment leaf over `path` may seek column `col` of index
	/// `idx`. Every containment match goes through here.
	///
	/// An element column (`tags.*`) is keyed by the elements of a stored
	/// array, while `tags CONTAINS v` is also satisfied by values that hold no
	/// element entry for `v`: a string or uuid holding `v` as a substring, an
	/// object with key `v`, a range spanning `v`, a geometry containing `v`.
	/// The seek is taken only when the column holds elements for the path, is
	/// not withheld, and either:
	/// - the query path itself takes every element (`tags.* CONTAINS v`, `tags[*]`), under which a
	///   stored scalar or set evaluates to `NONE` and cannot match. The flatten step (`tags...`)
	///   does not qualify: it returns a scalar unchanged. Nor is the rule exact: a stored object is
	///   kept whole and a stored record link is replaced by the record it names, and either matches
	///   a key `v` with no element entry for it, so the seek misses that record. Only evidence from
	///   the writer can rule such values out; or
	/// - the seek on the column is known to be complete ([`Self::element_seek_is_complete`]).
	fn seeks_containment(&self, idx: usize, col: usize, path: &LogicalExpr) -> bool {
		let withheld = self
			.no_containment
			.as_ref()
			.is_some_and(|flags| flags.get(idx).and_then(|f| f.get(col)).copied().unwrap_or(false));
		!withheld
			&& self.indexes[idx].cols.get(col).is_some_and(|c| path_matches_containment(path, c))
			&& (path_takes_each(path) || self.element_seek_is_complete(idx, col))
	}

	/// Whether a seek for an element on column `col` of index `idx` finds
	/// every record whose stored value contains that element.
	///
	/// The writer keys an array by its elements, while other stored values can
	/// contain a value they hold no element entry for: a string or uuid a
	/// substring, an object or a linked record a key, a range a value it
	/// spans, a geometry a geometry. The seek is complete only for a column no
	/// record stored such a value in. A declared kind does not establish that
	/// (`DEFINE FIELD` does not rewrite records stored before it, and an import
	/// skips coercion), and nothing records it yet, so this is `false`.
	fn element_seek_is_complete(&self, _idx: usize, _col: usize) -> bool {
		false
	}

	/// Whether a containment leaf over `path` may be answered on column `col`
	/// of index `idx` by seeking its value together with `NONE` and `NULL`.
	///
	/// The column takes every element of a path of plain fields, and the
	/// index has recorded storing nothing whole in it. A value at the path
	/// that contains `v` is then an array or a set holding `v` (listed under
	/// `v`), or a value whose elements the path does not take — a scalar,
	/// whose column value is `NONE` — while an object, the one other value
	/// that contains a key, would have been stored whole. The seek is a
	/// superset: the leaf is judged again over every record it yields.
	fn seeks_containment_with_nullish(&self, idx: usize, col: usize, path: &LogicalExpr) -> bool {
		let flagged = |flags: &Option<Arc<[super::access_path::ColumnFlags]>>| {
			flags.as_ref().is_some_and(|flags| {
				flags.get(idx).and_then(|f| f.get(col)).copied().unwrap_or(false)
			})
		};
		!flagged(&self.no_containment)
			&& flagged(&self.elements_or_nullish)
			&& !path_takes_each(path)
			&& self.indexes[idx].cols.get(col).is_some_and(|c| path_matches_containment(path, c))
	}

	/// The analyzer reading the query vector of a KNN operator whose right
	/// operand is a parameter from `vectors`.
	pub fn with_knn_query_vectors(mut self, vectors: &'a KnnQueryVectors) -> Self {
		self.knn_query_vectors = Some(vectors);
		self
	}

	/// Analyze conditions and ORDER BY to find candidate access paths.
	///
	/// Returns a list of index candidates that could be used for this query.
	pub fn analyze(
		&self,
		cond: Option<&LogicalExpr>,
		order: Option<&LogicalOrdering>,
	) -> Vec<IndexCandidate> {
		let mut candidates = Vec::new();

		// Skip analysis if indexes are empty
		if self.indexes.is_empty() {
			return candidates;
		}

		// Analyze WHERE conditions
		if let Some(cond) = cond {
			// First, collect all simple conditions (path op value)
			let mut conditions = Vec::new();
			self.collect_conditions(cond, &mut conditions);

			// Try to build compound index access for multi-column indexes
			self.analyze_compound_conditions(&conditions, &mut candidates);

			// Also analyze for single-column matches and special operators
			self.analyze_condition(cond, &mut candidates);
		}

		// Analyze ORDER BY for index-ordered scans
		if let Some(ordering) = order {
			self.analyze_order(ordering, &mut candidates);
		}

		// Filter out indexes not allowed by WITH hints. A KNN candidate is
		// exempt: the KNN operator can only be computed by its KnnScan, so it
		// must survive to `select_access_path`, whose must-drive rule
		// overrides the hint (with a warning) — otherwise the operator lands
		// in a per-row residual where it is never truthy and the query
		// silently returns zero rows.
		if let Some(With::Index(names)) = self.with_hints {
			let hint_matches_knn = candidates.iter().any(|c| {
				matches!(c.access, BTreeAccess::Knn { .. })
					&& names.iter().any(|n| n.as_str() == c.index_ref.name.as_str())
			});
			candidates.retain(|c| {
				(!hint_matches_knn && matches!(c.access, BTreeAccess::Knn { .. }))
					|| names.iter().any(|n| n.as_str() == c.index_ref.name.as_str())
			});
		}

		// Merge half-bounded ranges on the same index into bounded ranges
		// (e.g. field > 5 AND field < 10 → Range(>5, <10))
		self.merge_range_candidates(&mut candidates);

		// A range that admits an array whose entries all sit outside it is not
		// answered by the index; the merge runs first, since bounding a
		// half-open range can take it clear of the array band.
		candidates.retain(|c| !super::access_path::access_misses_arrays(&c.index_ref, &c.access));

		// A range over a datetime component of the record id reaches the
		// whole seconds of its bounds. It is widened after the bounds merge,
		// which judges the bounds as written: a range the merge proves empty is
		// empty whatever the widening makes of its bounds. A component's kind
		// is a declared scalar, so the array check above never judges a column
		// the widening changes. A candidate whose datetime bound has no
		// widening is discarded: without the bound it would read as exact.
		candidates.retain_mut(|candidate| {
			super::derived_columns::widen_access(&candidate.index_ref, &mut candidate.access)
		});

		// Deduplicate candidates - prefer compound over simple
		self.deduplicate_candidates(&mut candidates);

		candidates
	}

	/// Try to build a multi-index union access path for OR conditions.
	///
	/// For `A OR B OR C`, each branch is analyzed independently. If EVERY branch
	/// has at least one index candidate, the best candidate from each is combined
	/// into an `AccessPath::Union`. If any branch lacks an index candidate, the
	/// union cannot be used and `None` is returned (the caller should fall back
	/// to a table scan).
	pub fn try_or_union(
		&self,
		cond: Option<&LogicalExpr>,
		direction: Direction,
	) -> Option<AccessPath> {
		let cond = cond?;

		// Check for WITH NOINDEX
		if matches!(self.with_hints, Some(With::NoIndex)) {
			return None;
		}

		self.or_union_from_expr(cond, direction).map(|(path, _)| path)
	}

	/// Build a multi-index union `AccessPath` from a single OR expression.
	///
	/// Flattens the OR into branches, analyzes each independently, and — if
	/// EVERY branch has at least one index candidate — combines the best
	/// candidate from each into an `AccessPath::Union`. Returns the union
	/// together with its *effective score*: the minimum branch score. An OR
	/// reads the union of all branches' rows, so it is only as selective as
	/// its least-selective branch, and the min lets the caller compare the
	/// union against a single-index driver on the same scale as
	/// [`IndexCandidate::score`]. Returns `None` if any branch lacks an index
	/// candidate (the caller must fall back to a table scan).
	fn or_union_from_expr(
		&self,
		or_expr: &LogicalExpr,
		direction: Direction,
	) -> Option<(AccessPath, u32)> {
		// Flatten OR branches from the expression tree
		let mut branches = Vec::new();
		Self::flatten_or(or_expr, &mut branches);

		// Need at least 2 branches for a union to make sense
		if branches.len() < 2 {
			return None;
		}

		// Share the scan-op budget across OR branches so multi-branch spatial ORs coarsen
		// per branch instead of hitting a cliff when the sum of uncoarsened branches exceeds
		// `MAX_SPATIAL_SCAN_OPS`.
		let per_branch_spatial_max_ops =
			(surrealdb_idx::spatial::MAX_SPATIAL_SCAN_OPS / branches.len()).max(1);

		// Analyze each branch independently
		let mut branch_paths = Vec::with_capacity(branches.len());
		let mut min_score = u32::MAX;
		let mut total_spatial_area = 0.0_f64;
		for branch_expr in branches {
			let candidates = self.analyze(Some(branch_expr), None);
			let branch_score = candidates.iter().map(|c| c.score()).max().unwrap_or(0);
			if let Some((
				AccessPath::Union {
					paths,
					..
				},
				spatial_score,
				branch_area,
				pinned_cols,
			)) = self.try_spatial_expansion_for_or_branch(
				Some(branch_expr),
				direction,
				per_branch_spatial_max_ops,
			) && spatial_score > branch_score
			{
				if branch_paths.len() + paths.len() > surrealdb_idx::spatial::MAX_SPATIAL_SCAN_OPS {
					return None;
				}
				branch_paths.extend(paths);
				if pinned_cols == 0 {
					total_spatial_area = (total_spatial_area + branch_area).clamp(0.0, 1.0);
					min_score = min_score.min(Self::single_spatial_score(total_spatial_area));
				} else {
					min_score = min_score.min(spatial_score);
				}
				continue;
			}
			if candidates.is_empty() {
				// This branch has no index — cannot use union
				return None;
			}
			let path = select_access_path(candidates, self.with_hints, direction);
			match path {
				AccessPath::TableScan => {
					// WITH hints rejected all candidates for this branch
					return None;
				}
				AccessPath::EmptyScan => {
					// Branch is provably empty (e.g. contradictory range);
					// it contributes no rows to the OR, so drop it. Note:
					// `build_union_sub_operator` has no arm for EmptyScan
					// and would error otherwise.
					continue;
				}
				AccessPath::BTreeScan {
					..
				}
				| AccessPath::FullTextSearch {
					..
				}
				| AccessPath::KnnSearch {
					..
				}
				| AccessPath::BitmapFusion {
					..
				}
				| AccessPath::Union {
					..
				}
				| AccessPath::KeyRange(_) => {
					branch_paths.push(path);
					min_score = min_score.min(branch_score);
				}
			}
		}

		// If every branch turned out to be empty the whole OR is empty.
		if branch_paths.is_empty() {
			return Some((AccessPath::EmptyScan, u32::MAX));
		}

		// A single surviving branch is a degenerate union — return it
		// directly rather than wrapping in `AccessPath::Union(...)`
		// (the planner's union dispatch expects ≥ 2 sub-paths).
		if branch_paths.len() == 1 {
			return branch_paths.into_iter().next().map(|p| (p, min_score));
		}

		// OR branches are independent predicates that may both hold
		// on the same row, so the union can emit the same record
		// from multiple branches — dedupe required.
		Some((
			AccessPath::Union {
				paths: branch_paths,
				dedupe: true,
			},
			min_score,
		))
	}

	/// Try to build a multi-index union from an OR nested inside an AND.
	///
	/// [`Self::try_or_union`] only fires when the *whole* WHERE clause is an
	/// OR. When the OR is one conjunct of an AND — e.g.
	/// `type = $t AND (title @1@ $q OR body @2@ $q)` — the single-index
	/// analyzer drives from the other conjunct (`type`) and evaluates the OR
	/// as a per-row residual filter, which is catastrophic for full-text
	/// matches (every `type` row is re-scored against the FT index).
	///
	/// This walks the top-level AND conjuncts and, for each conjunct that is
	/// itself an OR of independently-indexable predicates, builds the union.
	/// The union covers only that OR; the remaining conjuncts are enforced by
	/// the residual `Filter` the SELECT planner installs above the
	/// `UnionIndexScan`.  Those conjuncts must survive
	/// `strip_union_index_conditions`, which is why it only drops a
	/// containment leaf whose literals contain *every* branch value — a
	/// sibling conjunct narrower than the OR's branch values is not implied
	/// by the union and stays in the filter.
	///
	/// Returns the best such union (the one whose weakest branch is most
	/// selective) with its effective score, so the caller can compare it
	/// against the chosen single-index driver and only switch when the union
	/// is heuristically better. Skipped under explicit `WITH` hints — the
	/// user has already pinned the plan.
	pub fn try_and_nested_or_union(
		&self,
		cond: Option<&LogicalExpr>,
		direction: Direction,
	) -> Option<(AccessPath, u32)> {
		let cond = cond?;

		// Respect explicit index hints; leave the pinned plan alone.
		if self.with_hints.is_some() {
			return None;
		}

		// A KNN (nearest-neighbour) operator must be consumed by a KnnScan:
		// it is stripped from the WHERE before residual filtering and cannot
		// be evaluated as a per-row residual (unlike a MATCHES `@@`, which is
		// correct — if slower — as a filter). Substituting a higher-scoring OR
		// union here would demote the KNN to a residual that does not
		// restricts rows to the neighbour set, returning rows outside it. Leave
		// any KNN query to drive from its KnnScan.
		if Self::expr_contains_knn(cond) {
			return None;
		}

		// Only meaningful for a conjunction. A top-level OR is already
		// handled by `try_or_union`.
		let mut conjuncts = Vec::new();
		Self::flatten_and(cond, &mut conjuncts);
		if conjuncts.len() < 2 {
			return None;
		}

		// Among the OR conjuncts that form a valid union, keep the one whose
		// weakest branch is the most selective (highest effective score).
		let mut best: Option<(AccessPath, u32)> = None;
		for conjunct in conjuncts {
			if !matches!(
				conjunct,
				LogicalExpr::Binary {
					op: BinaryOperator::Or,
					..
				}
			) {
				continue;
			}
			if let Some((path, score)) = self.or_union_from_expr(conjunct, direction)
				&& best.as_ref().is_none_or(|(_, best_score)| score > *best_score)
			{
				best = Some((path, score));
			}
		}

		best
	}

	/// Try to build a bitmap fusion plan for an AND-composed WHERE clause.
	///
	/// The plan intersects per-branch candidate bitmaps over the table's
	/// shared doc-ID space instead of driving from a single index and
	/// filtering per row. Members of the intersection are:
	///
	/// - the standard per-index candidates from [`Self::analyze`] (equality, merged ranges,
	///   compound prefixes, MATCHES), greedily deduplicated so a branch is only kept when it pins
	///   at least one column no earlier (higher-scoring) branch already pins;
	/// - AND-conjuncts that are themselves ORs / `IN [...]` / containment expansions whose every
	///   branch is independently bitmap-capable ([`BitmapPlan::Or`]);
	/// - `NOT <predicate>` conjuncts composed via [`BitmapPlan::AndNot`] — only when the subtracted
	///   bitmap provably equals the predicate's truth set (see [`Self::bitmap_exact_plan`]); an
	///   inexact subtraction would drop rows the residual filter can never restore.
	///
	/// Every b-tree branch requires an index whose entries carry doc-IDs
	/// ([`StoredIndexDefinition::has_entry_doc_ids`]). Conjuncts that don't qualify simply stay
	/// out of the plan — the caller keeps the whole WHERE clause as the
	/// residual filter, so the bitmap only needs to over-approximate the
	/// result per omitted conjunct.
	///
	/// Returns `None` (caller uses the union plans) unless the
	/// plan has at least two positive members, or one positive member plus a
	/// subtraction. Order/limit/version gating is the caller's
	/// responsibility; `exact_col` reports whether a column of an index holds
	/// one entry per record ([`IndexRef::one_entry_per_record`]: array values
	/// fan out to one index entry per element, making b-tree bitmaps
	/// over-approximate and therefore unusable for subtraction).
	pub(crate) fn try_bitmap_fusion(
		&self,
		cond: Option<&LogicalExpr>,
		candidates: &[IndexCandidate],
		exact_col: &dyn Fn(&IndexRef, usize) -> bool,
	) -> Option<BitmapPlan> {
		let cond = cond?;
		// Explicit hints pin the plan (NOINDEX is handled before analysis).
		if self.with_hints.is_some() {
			return None;
		}
		// A KNN operator must be consumed by its KnnScan (see
		// `try_and_nested_or_union`); a KNN query's sibling conjuncts are
		// fused by [`Self::try_knn_prefilter`] into the scan's allow-list
		// bitmap instead, so the query never becomes a plain
		// `BitmapFusion` source.
		if Self::expr_contains_knn(cond) {
			return None;
		}
		let mut conjuncts = Vec::new();
		Self::flatten_and(cond, &mut conjuncts);
		if conjuncts.len() < 2 {
			return None;
		}
		// A provably-empty candidate empties the whole conjunction; leave
		// that to the EmptyScan path.
		if candidates.iter().any(|c| c.empty) {
			return None;
		}

		// Positive single-index branches, most selective (by score) first —
		// the leading AND child anchors the intersection and is drained
		// without a drained-entry budget.
		let mut leaves: Vec<&IndexCandidate> =
			candidates.iter().filter(|c| Self::bitmap_capable_candidate(c)).collect();
		leaves.sort_by_key(|c| std::cmp::Reverse(c.score()));

		let mut and_children: Vec<BitmapPlan> = Vec::new();
		let mut and_children_cols: Vec<Vec<Idiom>> = Vec::new();
		let mut covered: Vec<Idiom> = Vec::new();
		let mut anchor_at_most_one_row = false;
		for c in leaves {
			let cols = Self::candidate_pinned_columns(c);
			if cols.is_empty() || cols.iter().all(|col| covered.contains(col)) {
				// Redundant with an already-selected branch (e.g. a
				// single-column index shadowed by a selected compound
				// prefix): another intersection cannot narrow the result.
				continue;
			}
			covered.extend(cols.clone());
			if let Some(leaf) = Self::bitmap_leaf_from_candidate(c) {
				if and_children.is_empty() {
					anchor_at_most_one_row = Self::candidate_returns_at_most_one_row(c);
				}
				and_children.push(leaf);
				and_children_cols.push(cols);
			}
		}

		// OR / IN / containment conjuncts become union members when every
		// branch is independently bitmap-capable.
		for conjunct in &conjuncts {
			let path = if let LogicalExpr::Binary {
				op: BinaryOperator::Or,
				..
			} = conjunct
			{
				self.or_union_from_expr(conjunct, Direction::Forward).map(|(p, _)| p)
			} else if let LogicalExpr::Binary {
				op:
					BinaryOperator::Inside
					| BinaryOperator::ContainAll
					| BinaryOperator::ContainAny
					| BinaryOperator::AllInside
					| BinaryOperator::AnyInside,
				..
			} = conjunct
			{
				self.try_in_expansion(Some(conjunct), Direction::Forward)
					.or_else(|| self.try_containment_expansion(Some(conjunct), Direction::Forward))
			} else {
				None
			};
			if let Some(path) = path
				&& let Some(node) = Self::bitmap_plan_from_access_path(&path)
			{
				and_children.push(node);
				and_children_cols.push(Vec::new());
			}
		}

		// Spatial conjuncts become union members when every branch is independently
		// bitmap-capable. Skip when a unique index already pins at most one row.
		if !anchor_at_most_one_row {
			for (path, _, _, mut cols) in
				self.spatial_expansions_in_context(Some(cond), Some(cond), Direction::Forward)
			{
				cols.pop();
				if let Some(node) = Self::bitmap_plan_from_access_path(&path) {
					if !cols.is_empty() {
						// Drop any non-unique single/compound B-tree leaf whose pinned columns are
						// already pinned by this compound spatial index's equality prefix.
						let mut i = 0;
						while i < and_children_cols.len() {
							let leaf_cols = &and_children_cols[i];
							if !leaf_cols.is_empty()
								&& leaf_cols.iter().all(|col| cols.contains(col))
							{
								and_children.remove(i);
								and_children_cols.remove(i);
							} else {
								i += 1;
							}
						}
					}
					and_children.push(node);
					and_children_cols.push(Vec::new());
				}
			}
		}

		// Graph reachability semi-joins: a single-hop
		// `->edge->vertex CONTAINS <literal id>` conjunct anchors at the
		// literal and walks the inverse direction back to the candidate
		// rows, entirely from adjacency pointer keys. Appended after the
		// index leaves so a graph leaf never anchors the intersection — an
		// adjacency band has no selectivity guarantee — and only alongside
		// at least one index-backed branch, so it is always droppable on
		// overflow. Skipped entirely when the anchoring branch guarantees
		// at most one row: draining an adjacency band to intersect against
		// a ≤1-row anchor can never beat the residual per-row traversal,
		// which is a single adjacency-band lookup for that row.
		if !and_children.is_empty() && !anchor_at_most_one_row {
			for conjunct in &conjuncts {
				if let Some(leaf) = Self::graph_semijoin_leaf(conjunct) {
					and_children.push(leaf);
				}
			}
		}

		// `NOT <predicate>` conjuncts, folded into one subtraction:
		// `A AND NOT B AND NOT C` ⇒ `A \ (B ∪ C)`.
		let mut nots: Vec<BitmapPlan> = Vec::new();
		for conjunct in &conjuncts {
			if let Some(inner) = Self::as_negated_expr(conjunct)
				&& let Some(node) = self.bitmap_exact_plan(inner, exact_col)
			{
				nots.push(node);
			}
		}

		// Worth fusing only with ≥2 positive members, or one positive plus a
		// subtraction; a single positive member is better served by the
		// streaming single-index plan, and a subtraction with no positive
		// anchor would need a table-universe bitmap we don't maintain.
		if and_children.is_empty() || (and_children.len() < 2 && nots.is_empty()) {
			return None;
		}

		let base = if and_children.len() == 1 {
			and_children.pop().expect("checked non-empty")
		} else {
			BitmapPlan::And(and_children)
		};
		Some(if nots.is_empty() {
			base
		} else {
			let subtract = if nots.len() == 1 {
				nots.pop().expect("checked non-empty")
			} else {
				BitmapPlan::Or(nots)
			};
			BitmapPlan::AndNot {
				base: Box::new(base),
				subtract: Box::new(subtract),
			}
		})
	}

	/// Build a bitmap union plan for a top-level OR: every
	/// branch's best candidate drains to a doc-ID bitmap and the union
	/// resolves once and is inherently deduplicated. Unlike the
	/// `UnionIndexScan` RecordId merge, it needs no dedupe set that grows with the
	/// result size. The [`AccessPath::Union`] the plan was derived from is
	/// returned alongside it, as the executor's overflow fallback: a union
	/// branch exceeding its drained-entry budget abandons the bitmap plan
	/// and streams the union instead.
	///
	/// Returns `None` unless every branch is independently bitmap-capable
	/// (a union missing a branch would silently drop rows) and at least two
	/// branches survive: a degenerate single-branch OR is better served by
	/// the streaming single-index plan, and a provably-empty OR by the
	/// EmptyScan path. The caller gates on the query shape: LIMIT without
	/// ORDER BY keeps streaming early termination, and an ORDER BY declines
	/// fusion only when an index covers the ordering or the ordering is
	/// record-id order — both of which the union scan can satisfy without a
	/// Sort; an ordering nothing covers installs a Sort above either plan.
	pub(crate) fn try_bitmap_union_fusion(
		&self,
		cond: Option<&LogicalExpr>,
		direction: Direction,
	) -> Option<(BitmapPlan, AccessPath)> {
		let cond = cond?;
		// Explicit hints pin the plan (NOINDEX is handled before analysis).
		if self.with_hints.is_some() {
			return None;
		}
		// A KNN operator must be consumed by its KnnScan; see
		// [`Self::try_bitmap_fusion`].
		if Self::expr_contains_knn(cond) {
			return None;
		}
		if !matches!(
			cond,
			LogicalExpr::Binary {
				op: BinaryOperator::Or,
				..
			}
		) {
			return None;
		}
		let (path, _) = self.or_union_from_expr(cond, direction)?;
		if let BitmapPlan::Or(children) = Self::bitmap_plan_from_access_path(&path)?
			&& children.len() >= 2
			&& !Self::equality_branches_disjoint(&children)
		{
			Some((BitmapPlan::Or(children), path))
		} else {
			None
		}
	}

	/// Whether every union member is an equality lookup on the same index
	/// with pairwise-distinct values — record-disjoint branches (barring
	/// array-valued fan-out, where declining is merely conservative). The
	/// fused plan's structural win over the streaming union is dedupe, so
	/// with no duplicates to save the union's direct row streaming beats the
	/// bitmap's extra doc-ID resolve step: the caller declines fusion and
	/// the streaming `UnionIndexScan` keeps the query.
	fn equality_branches_disjoint(children: &[BitmapPlan]) -> bool {
		let mut shared_index: Option<&IndexRef> = None;
		let mut values: Vec<&Value> = Vec::with_capacity(children.len());
		for child in children {
			// A clock bound and a literal are not provably distinct before
			// the scan opens, so a branch that reads the clock proves nothing.
			let BitmapPlan::BTree {
				index_ref,
				access: BTreeAccess::Equality(BoundOperand::Literal(value)),
			} = child
			else {
				return false;
			};
			match shared_index {
				None => shared_index = Some(index_ref),
				Some(seen) if seen == index_ref => {}
				Some(_) => return false,
			}
			if values.contains(&value) {
				return false;
			}
			values.push(value);
		}
		true
	}

	/// Match a negated expression: the `!` prefix operator, or the builtin
	/// `not()` function — the parsed form of `NOT (...)`. Both negate
	/// truthiness identically.
	pub(crate) fn as_negated_expr(expr: &LogicalExpr) -> Option<&LogicalExpr> {
		if let LogicalExpr::Prefix {
			op: PrefixOperator::Not,
			expr: inner,
		} = expr
		{
			Some(inner)
		} else if let LogicalExpr::Call(call) = expr
			&& call.args.len() == 1
			&& matches!(
				&call.receiver,
				crate::expr::function::Function::Normal(name) if name == "not"
			) {
			Some(&call.args[0])
		} else {
			None
		}
	}

	/// Whether a candidate can produce a bitmap branch: b-tree accesses need
	/// entry doc-IDs, full-text needs the current format, KNN never
	/// participates (its operator is consumed by the KnnScan the bitmap
	/// feeds, see [`Self::try_knn_prefilter`]), and a full-range scan (no
	/// WHERE selectivity) contributes nothing to an intersection.
	fn bitmap_capable_candidate(c: &IndexCandidate) -> bool {
		if c.empty {
			return false;
		}
		match &c.access {
			BTreeAccess::Equality(_)
			| BTreeAccess::Compound {
				..
			} => c.index_ref.definition().has_entry_doc_ids(),
			BTreeAccess::Range {
				range,
			} => {
				!matches!((&range.start, &range.end), (Bound::Unbounded, Bound::Unbounded))
					&& c.index_ref.definition().has_entry_doc_ids()
			}
			BTreeAccess::FullText {
				..
			} => c.index_ref.definition().uses_shared_doc_ids(),
			BTreeAccess::Knn {
				..
			} => false,
		}
	}

	/// The index columns a candidate pins, used to skip branches made
	/// redundant by an already-selected candidate.
	fn candidate_pinned_columns(c: &IndexCandidate) -> Vec<Idiom> {
		let cols = &c.index_ref.definition().cols;
		match &c.access {
			BTreeAccess::Equality(_)
			| BTreeAccess::Range {
				..
			}
			| BTreeAccess::FullText {
				..
			} => cols.first().cloned().into_iter().collect(),
			BTreeAccess::Compound {
				prefix,
				range,
				..
			} => {
				let n = (prefix.len() + usize::from(range.is_some())).min(cols.len());
				cols[..n].to_vec()
			}
			BTreeAccess::Knn {
				..
			} => Vec::new(),
		}
	}

	/// Whether a candidate's access is guaranteed to match at most one row:
	/// it pins the entire key of a unique index, either as a single-column
	/// equality (multi-column indexes always surface as `Compound`, see
	/// [`Self::try_match_comparison`]) or as a rangeless compound prefix
	/// covering every column, and none of the pinned values is exempt from
	/// uniqueness ([`IndexDefinition::exempts_from_uniqueness`]).
	fn candidate_returns_at_most_one_row(c: &IndexCandidate) -> bool {
		if !c.index_ref.is_unique() {
			return false;
		}
		let def = c.index_ref.definition();
		match &c.access {
			BTreeAccess::Equality(v) => def.cols.len() == 1 && !v.is_exempt_from_uniqueness(def),
			BTreeAccess::Compound {
				prefix,
				range,
				..
			} => {
				range.is_none()
					&& prefix.len() >= def.cols.len()
					&& !prefix.iter().any(|v| v.is_exempt_from_uniqueness(def))
			}
			BTreeAccess::Range {
				..
			}
			| BTreeAccess::FullText {
				..
			}
			| BTreeAccess::Knn {
				..
			} => false,
		}
	}

	/// Convert a positive candidate into a bitmap leaf.
	fn bitmap_leaf_from_candidate(c: &IndexCandidate) -> Option<BitmapPlan> {
		match &c.access {
			BTreeAccess::Equality(_)
			| BTreeAccess::Range {
				..
			}
			| BTreeAccess::Compound {
				..
			} => Some(BitmapPlan::BTree {
				index_ref: c.index_ref.clone(),
				access: c.access.clone(),
			}),
			BTreeAccess::FullText {
				query,
				operator,
			} => Some(BitmapPlan::FullText {
				index_ref: c.index_ref.clone(),
				query: query.clone(),
				operator: operator.clone(),
			}),
			BTreeAccess::Knn {
				..
			} => None,
		}
	}

	/// Convert an OR-union access path into a bitmap union.
	///
	/// Returns `None` when any branch is not bitmap-capable — a union
	/// missing a branch would silently drop rows, so partial conversion is
	/// never allowed. `EmptyScan` converts to an empty union (a provably
	/// empty conjunct).
	fn bitmap_plan_from_access_path(path: &AccessPath) -> Option<BitmapPlan> {
		match path {
			AccessPath::BTreeScan {
				index_ref,
				access,
				..
			} => {
				if !index_ref.definition().has_entry_doc_ids() {
					return None;
				}
				Some(BitmapPlan::BTree {
					index_ref: index_ref.clone(),
					access: access.clone(),
				})
			}
			AccessPath::FullTextSearch {
				index_ref,
				query,
				operator,
			} if index_ref.definition().uses_shared_doc_ids() => Some(BitmapPlan::FullText {
				index_ref: index_ref.clone(),
				query: query.clone(),
				operator: operator.clone(),
			}),
			AccessPath::FullTextSearch {
				..
			} => None,
			AccessPath::Union {
				paths,
				..
			} => {
				let mut children = Vec::with_capacity(paths.len());
				for p in paths {
					children.push(Self::bitmap_plan_from_access_path(p)?);
				}
				Some(BitmapPlan::Or(children))
			}
			AccessPath::EmptyScan => Some(BitmapPlan::Or(Vec::new())),
			// A key range reads no index, so it has no doc-IDs to fuse.
			AccessPath::TableScan
			| AccessPath::KeyRange(_)
			| AccessPath::KnnSearch {
				..
			}
			| AccessPath::BitmapFusion {
				..
			} => None,
		}
	}

	/// Recognize a single-hop graph containment conjunct as a reachability
	/// semi-join leaf: `->edge->vertex CONTAINS vertex:lit` or the flipped
	/// `vertex:lit INSIDE ->edge->vertex`.
	///
	/// Admitted only when the truth set is exactly "rows adjacent to the
	/// literal": two bare lookup hops over the row in one fixed direction,
	/// plain table subjects, no lookup clauses, and a plan-time record-id
	/// literal whose table the vertex hop names. The produced leaf traverses
	/// the INVERSE direction from the literal. Anything else — multi-hop,
	/// `<->`, ranges, edge conditions, unfolded parameters — keeps the
	/// per-row evaluation.
	fn graph_semijoin_leaf(conjunct: &LogicalExpr) -> Option<BitmapPlan> {
		let (path, lit) = if let LogicalExpr::Binary {
			left,
			op: BinaryOperator::Contain,
			right,
		} = conjunct
		{
			if left.is_path() {
				(&**left, &**right)
			} else {
				return None;
			}
		} else if let LogicalExpr::Binary {
			left,
			op: BinaryOperator::Inside,
			right,
		} = conjunct
		{
			if right.is_path() {
				(&**right, &**left)
			} else {
				return None;
			}
		} else {
			return None;
		};
		let Some(Value::RecordId(anchor)) = try_literal_to_value(lit) else {
			return None;
		};
		// Two hops over the row: lowering places a run of lookups that
		// continues nowhere inside a `Broadcast` over the current value, or
		// bare when nothing follows; both spell the same path.
		let hops = if let LogicalExpr::Broadcast {
			base,
			body,
			..
		} = path && matches!(**base, LogicalExpr::Current)
		{
			&**body
		} else {
			path
		};
		let LogicalExpr::Lookup {
			base,
			lookup: vertex_hop,
		} = hops
		else {
			return None;
		};
		let LogicalExpr::Lookup {
			base,
			lookup: edge_hop,
		} = &**base
		else {
			return None;
		};
		if !matches!(**base, LogicalExpr::Current) {
			return None;
		}
		let (edge_tables, dir) = Self::plain_graph_hop(edge_hop)?;
		let (vertex_tables, vertex_dir) = Self::plain_graph_hop(vertex_hop)?;
		if dir != vertex_dir || edge_tables.is_empty() {
			return None;
		}
		// The vertex hop names the tables the far end may belong to; the
		// anchor must be one of them or the predicate cannot hold — leave
		// that shape to the ordinary filter.
		if !vertex_tables.contains(&anchor.table) {
			return None;
		}
		let direction = match dir {
			crate::expr::Dir::Out => crate::expr::Dir::In,
			crate::expr::Dir::In => crate::expr::Dir::Out,
			crate::expr::Dir::Both => return None,
		};
		Some(BitmapPlan::Graph {
			source: anchor,
			direction,
			edge_tables,
		})
	}

	/// A bare graph hop in one fixed direction over plain table subjects:
	/// the tables it may reach and its direction. `None` for `<->`, for a
	/// hop with any clause, and for a key-range or referencing subject.
	fn plain_graph_hop(
		lookup: &crate::expr::logical::LogicalLookup,
	) -> Option<(Vec<surrealdb_strand::TableName>, crate::expr::Dir)> {
		use crate::expr::logical::LogicalLookupSubject;
		use crate::expr::lookup::LookupKind;
		let crate::expr::logical::LogicalLookup {
			kind,
			expr,
			only,
			what,
			cond,
			split,
			group,
			order,
			limit,
			start,
			alias,
			..
		} = lookup;
		let LookupKind::Graph(dir) = kind else {
			return None;
		};
		if matches!(dir, crate::expr::Dir::Both) {
			return None;
		}
		if expr.is_some()
			|| *only || cond.is_some()
			|| split.is_some()
			|| group.is_some()
			|| order.is_some()
			|| limit.is_some()
			|| start.is_some()
			|| alias.is_some()
		{
			return None;
		}
		let mut tables = Vec::with_capacity(what.len());
		for s in what {
			if let LogicalLookupSubject::Table {
				table,
				referencing_field: None,
			} = s
			{
				tables.push(table.clone());
			} else {
				return None;
			}
		}
		Some((tables, *dir))
	}

	/// Build an *exact* bitmap fusion plan for an index-only COUNT
	/// `SELECT count() FROM t WHERE <fully index-backed AND>
	/// GROUP ALL` becomes the cardinality of the fused bitmap, with zero
	/// record fetches.
	///
	/// Unlike [`Self::try_bitmap_fusion`] there is no residual filter to
	/// correct any approximation, so *every* conjunct must be exactly
	/// represented by its branch bitmap (see [`Self::bitmap_exact_plan`]);
	/// a single non-qualifying conjunct disqualifies the plan. A single-conjunct
	/// WHERE clause is admitted only when it is an exact point-only spatial
	/// predicate ([`Self::try_spatial_exact_bitmap_plan`]); all other
	/// single-conjunct WHERE clauses are left to the single-index key-only count.
	pub(crate) fn try_bitmap_count_fusion(
		&self,
		cond: &LogicalExpr,
		exact_col: &dyn Fn(&IndexRef, usize) -> bool,
	) -> Option<BitmapPlan> {
		if self.with_hints.is_some() {
			return None;
		}
		if Self::expr_contains_knn(cond) {
			return None;
		}
		let mut conjuncts = Vec::new();
		Self::flatten_and(cond, &mut conjuncts);
		if conjuncts.len() < 2 {
			if conjuncts.len() == 1 && Self::as_negated_expr(conjuncts[0]).is_none() {
				return self.try_spatial_exact_bitmap_plan(conjuncts[0], None);
			}
			return None;
		}
		let mut positives: Vec<BitmapPlan> = Vec::new();
		let mut nots: Vec<BitmapPlan> = Vec::new();
		for conjunct in conjuncts {
			if let Some(inner) = Self::as_negated_expr(conjunct) {
				nots.push(
					self.bitmap_exact_plan(inner, exact_col)
						.or_else(|| self.try_spatial_exact_bitmap_plan(inner, None))?,
				);
			} else {
				positives.push(
					self.bitmap_exact_plan(conjunct, exact_col)
						.or_else(|| self.try_spatial_exact_bitmap_plan(conjunct, None))?,
				);
			}
		}
		// A subtraction needs a positive base; `NOT`-only clauses would need
		// a table-universe bitmap that is deliberately not maintained.
		if positives.is_empty() {
			return None;
		}
		let base = if positives.len() == 1 {
			positives.pop().expect("checked non-empty")
		} else {
			BitmapPlan::And(positives)
		};
		Some(if nots.is_empty() {
			base
		} else {
			let subtract = if nots.len() == 1 {
				nots.pop().expect("checked non-empty")
			} else {
				BitmapPlan::Or(nots)
			};
			BitmapPlan::AndNot {
				base: Box::new(base),
				subtract: Box::new(subtract),
			}
		})
	}

	/// Build a bitmap plan that *exactly* equals a predicate's truth set —
	/// `bitmap(B) == truth(B)` — as required wherever no residual filter can
	/// correct an approximation: the subtract side of an AND-NOT (an inexact
	/// subtraction would drop rows where `NOT B` holds) and every branch of
	/// an index-only COUNT. That admits:
	///
	/// - full-text MATCHES leaves — the posting bitmap *is* the operator's truth set (the MATCHES
	///   filter path evaluates membership on the same bitmaps);
	/// - b-tree equality/range/compound leaves whose pinned columns each hold one entry per record
	///   (`exact_col`) — array values fan out to one index entry per element, which would make the
	///   bitmap a superset of the predicate's truth set — whose range no number bounds that the
	///   index keys apart from values it compares equal to
	///   ([`super::access_path::range_matches_comparison`]), and whose equalities name only values
	///   [`Self::key_equality_covers`] admits for that index ([`Self::equality_keys_cover`]);
	/// - an OR of such leaves (`B OR C` is exactly `B ∪ C`).
	///
	/// Anything else returns `None`.
	fn bitmap_exact_plan(
		&self,
		inner: &LogicalExpr,
		exact_col: &dyn Fn(&IndexRef, usize) -> bool,
	) -> Option<BitmapPlan> {
		self.bitmap_plan_of(inner, &|c| match &c.access {
			BTreeAccess::FullText {
				..
			} => true,
			// A range over a datetime component of the record id lists records
			// whose datetime lies in the second of a bound, not beyond it, so
			// its bitmap is a superset of the truth set.
			access if super::derived_columns::range_is_widened(&c.index_ref, access) => false,
			BTreeAccess::Equality(_)
			| BTreeAccess::Range {
				..
			}
			| BTreeAccess::Compound {
				..
			}
			| BTreeAccess::Knn {
				..
			} => {
				super::access_path::range_matches_comparison(&c.index_ref, &c.access)
					&& Self::equality_keys_cover(&c.index_ref, &c.access)
					&& (0..Self::candidate_pinned_columns(c).len())
						.all(|col| exact_col(&c.index_ref, col))
			}
		})
	}

	/// Build an exact bitmap plan for a spatial predicate when the target `SPATIAL`
	/// index is point-only and every cell range carries an inline [`SpatialPointFilter`].
	fn try_spatial_exact_bitmap_plan(
		&self,
		inner: &LogicalExpr,
		context_cond: Option<&LogicalExpr>,
	) -> Option<BitmapPlan> {
		if !matches!(
			inner,
			LogicalExpr::Binary {
				op,
				..
			} if !matches!(op, BinaryOperator::And | BinaryOperator::Or)
		) {
			return None;
		}
		let (path, _, _) =
			self.try_spatial_expansion_in_context(Some(inner), context_cond, Direction::Forward)?;
		let AccessPath::Union {
			paths,
			dedupe: false,
		} = &path
		else {
			return None;
		};
		if paths.is_empty()
			|| !paths.iter().all(|p| {
				matches!(
					p,
					AccessPath::BTreeScan {
						access: BTreeAccess::Range {
							range: OperandRange {
								point_filter: Some(_),
								..
							},
						},
						..
					}
				)
			}) {
			return None;
		}
		Self::bitmap_plan_from_access_path(&path)
	}

	/// Whether every value `access` seeks by equality over `index` is one
	/// [`Self::key_equality_covers`] admits for it: the keys it reads then hold
	/// every record the equality accepts. Another value (an array, an object, a
	/// regex, a record id with a compound key, a number the index keys apart
	/// from numbers equal to it) may equal values stored under keys of their
	/// own, which a bitmap that stands in for the conjunct would omit. A range
	/// bound is judged by [`super::access_path::range_matches_comparison`].
	fn equality_keys_cover(index: &IndexDefinition, access: &BTreeAccess) -> bool {
		let normalised = spells_normalised_decimal_keys(index.format_version);
		let covers = |operand: &BoundOperand| {
			operand.as_literal().is_none_or(|value| Self::key_equality_covers(value, normalised))
		};
		match access {
			BTreeAccess::Equality(value) => covers(value),
			BTreeAccess::Compound {
				prefix,
				..
			} => prefix.iter().all(covers),
			BTreeAccess::Range {
				..
			}
			| BTreeAccess::FullText {
				..
			}
			| BTreeAccess::Knn {
				..
			} => true,
		}
	}

	/// The bitmap plan of `inner` built from the candidates `admits` accepts:
	/// an OR is the union of its branches' plans, any other single predicate
	/// the leaf of its best admitted candidate. An AND has none.
	fn bitmap_plan_of(
		&self,
		inner: &LogicalExpr,
		admits: &dyn Fn(&IndexCandidate) -> bool,
	) -> Option<BitmapPlan> {
		// `B OR C` — the union, when every branch has a plan.
		if let LogicalExpr::Binary {
			op: BinaryOperator::Or,
			..
		} = inner
		{
			let mut branches = Vec::new();
			Self::flatten_or(inner, &mut branches);
			let mut children = Vec::with_capacity(branches.len());
			for branch in branches {
				children.push(self.bitmap_plan_of(branch, admits)?);
			}
			return Some(BitmapPlan::Or(children));
		}

		// Only a single simple predicate can be subtracted exactly from the
		// analyzer's output. `NOT (A AND B)` is out: `analyze` may cover the
		// conjunction only partially (one candidate per index), and a
		// partially-covering bitmap is a superset of `A AND B` — subtracting
		// it would drop rows where only one conjunct holds.
		if matches!(
			inner,
			LogicalExpr::Binary {
				op: BinaryOperator::And,
				..
			}
		) {
			return None;
		}

		// A single predicate: analyze it in isolation and keep the best
		// admitted candidate.
		let candidates = self.analyze(Some(inner), None);
		candidates
			.into_iter()
			.filter(|c| Self::bitmap_capable_candidate(c) && admits(c))
			.max_by_key(|c| c.score())
			.as_ref()
			.and_then(Self::bitmap_leaf_from_candidate)
	}

	/// A bitmap plan holding every record that satisfies `inner`, and possibly
	/// others, whatever the indexed columns hold: an equality between a plain
	/// column and a value [`Self::key_equality_covers`] admits for the index
	/// answering it, answered by a b-tree leaf that pins every column it reads,
	/// or an OR of such equalities.
	///
	/// A value equal to the sought one is stored under its own key, and a
	/// record holding an array is listed under each element besides, so the
	/// leaf lists every record the equality accepts. No other comparison is
	/// answered this way. A range is satisfied by values stored outside it —
	/// a value of a kind that sorts after the bound, such as `[] > 'blue'` or
	/// `[1, 2] > 'blue'`, while an empty array is stored under one key below
	/// every string and `[1, 2]` under its numbers — and a containment by a
	/// string holding the sought one as a substring, stored whole.
	fn bitmap_superset_plan(
		&self,
		inner: &LogicalExpr,
		context_cond: Option<&LogicalExpr>,
	) -> Option<BitmapPlan> {
		if let LogicalExpr::Binary {
			op: BinaryOperator::Or,
			..
		} = inner
		{
			let mut branches = Vec::new();
			Self::flatten_or(inner, &mut branches);
			let children = branches
				.into_iter()
				.map(|branch| self.bitmap_superset_plan(branch, context_cond))
				.collect::<Option<Vec<_>>>()?;
			Some(BitmapPlan::Or(children))
		} else if let LogicalExpr::Binary {
			op: BinaryOperator::Equal | BinaryOperator::ExactEqual,
			..
		} = inner
		{
			self.bitmap_plan_of(inner, &|c| {
				let pinned_plain = || {
					Self::candidate_pinned_columns(c)
						.iter()
						.all(|col| !col.0.iter().any(|p| matches!(p, Part::All | Part::Flatten)))
				};
				let sought: &[BoundOperand] = match &c.access {
					BTreeAccess::Equality(value) => std::slice::from_ref(value),
					BTreeAccess::Compound {
						prefix,
						range: None,
						..
					} => prefix,
					BTreeAccess::Range {
						..
					}
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
				let normalised = spells_normalised_decimal_keys(c.index_ref.format_version);
				// An operand that reads the clock is no value fixed before the scan opens.
				sought.iter().all(|operand| {
					operand
						.as_literal()
						.is_some_and(|value| Self::key_equality_covers(value, normalised))
				}) && pinned_plain()
			})
		} else if matches!(
			inner,
			LogicalExpr::Binary {
				op,
				..
			} if !matches!(op, BinaryOperator::And | BinaryOperator::Or)
		) {
			self.try_spatial_expansion_in_context(Some(inner), context_cond, Direction::Forward)
				.and_then(|(path, _, _)| Self::bitmap_plan_from_access_path(&path))
		} else {
			None
		}
	}

	/// Whether every value `=` holds equal to `value` is stored under
	/// `value`'s own key, in an index whose keys spell decimals without
	/// trailing zeros when `normalised`. True for the kinds whose equality is
	/// equality of their keys: `NONE`, `NULL`, booleans, strings, durations,
	/// datetimes, dates, times, timestamps, intervals (equal intervals share the key of their
	/// normalised total), UUIDs, record ids whose key is a whole number, string or
	/// UUID, and a number keyed where it compares
	/// ([`number_keyed_as_compared`]). An equality naming any other number, or
	/// a record id whose key is an array, object or range and so may hold one,
	/// is judged on the rows, which costs reads and never rows. Not for a
	/// regex, which `=` holds equal to every string it matches, each stored
	/// under its own key; nor for any other kind.
	fn key_equality_covers(value: &Value, normalised: bool) -> bool {
		matches!(
			value,
			Value::None
				| Value::Null
				| Value::Bool(_)
				| Value::String(_)
				| Value::Duration(_)
				| Value::Datetime(_)
				| Value::Date(_)
				| Value::Time(_)
				| Value::Timestamp(_)
				| Value::Interval(_)
				| Value::Uuid(_)
		) || matches!(value, Value::Number(n) if number_keyed_as_compared(n, normalised))
			|| matches!(
				value,
				Value::RecordId(id) if matches!(
					id.key,
					RecordIdKey::Number(_) | RecordIdKey::String(_) | RecordIdKey::Uuid(_)
				)
			)
	}

	/// Split a KNN query's stripped WHERE into an allow-list bitmap plan and
	/// a true residual (#548, pre-filtered vector search). Every record that
	/// satisfies the stripped WHERE is in the allow-list, and every record of
	/// the allow-list that satisfies the residual satisfies it.
	///
	/// Per conjunct of the AND chain:
	/// - a conjunct containing a MATCHES operator joins the prefilter when it is exactly
	///   bitmap-representable (its full-text index is online in the current format); otherwise it
	///   is dropped entirely — MATCHES is not evaluable inside the ANN traversal (no query executor
	///   there, it computes to `false` and would reject every candidate), so an uncovered MATCHES
	///   is enforced only by the outer `Filter` re-applying the KNN-stripped WHERE;
	/// - a negated conjunct always stays residual (no table-universe bitmap is maintained, and
	///   index-NOT is inexact for records missing the field);
	/// - any other conjunct whose truth set is *exactly* a candidate bitmap
	///   ([`Self::bitmap_exact_plan`]) joins the prefilter and is dropped from the in-traversal
	///   filter;
	/// - any other conjunct an index answers only as a superset ([`Self::bitmap_superset_plan`])
	///   joins the prefilter and also stays in the residual: the bitmap narrows the candidates, and
	///   the residual rejects the ones it holds that the conjunct does not, so none takes a top-K
	///   slot;
	/// - the rest stay in the residual.
	///
	/// One covered conjunct suffices — unlike [`Self::try_bitmap_fusion`]'s
	/// ≥ 2 rule — because it already narrows the search to its members.
	///
	/// The covered conjuncts are reported by their index in the `AND` chain of `stripped`
	/// ([`KnnPrefilterPlan::covered`]), never by their text, so a caller maps them back to its own
	/// conjuncts whatever rewriting `stripped` went through.
	pub(crate) fn try_knn_prefilter(
		&self,
		stripped: &LogicalExpr,
		exact_col: &dyn Fn(&IndexRef, usize) -> bool,
	) -> Option<KnnPrefilterPlan> {
		if matches!(self.with_hints, Some(With::NoIndex)) {
			return None;
		}
		let mut conjuncts = Vec::new();
		Self::flatten_and(stripped, &mut conjuncts);
		let mut covered: Vec<BitmapPlan> = Vec::new();
		let mut covered_at: Vec<usize> = Vec::new();
		let mut residual: Vec<&LogicalExpr> = Vec::new();
		let mut uncovered_matches = false;
		for (at, conjunct) in conjuncts.into_iter().enumerate() {
			if expr_contains_matches(conjunct) {
				if let Some(leaf) = self.bitmap_exact_plan(conjunct, exact_col) {
					covered.push(leaf);
					covered_at.push(at);
				} else {
					// Not exactly coverable and excluded from the residual
					// (see the method doc): flag it so the scan keeps its
					// MATCHES-aware condition active alongside the allow-list.
					uncovered_matches = true;
				}
			} else if Self::as_negated_expr(conjunct).is_some() {
				residual.push(conjunct);
			} else if let Some(leaf) = self
				.bitmap_exact_plan(conjunct, exact_col)
				.or_else(|| self.try_spatial_exact_bitmap_plan(conjunct, Some(stripped)))
			{
				covered.push(leaf);
				covered_at.push(at);
			} else if let Some(leaf) = self.bitmap_superset_plan(conjunct, Some(stripped)) {
				covered.push(leaf);
				residual.push(conjunct);
			} else {
				residual.push(conjunct);
			}
		}
		if covered.is_empty() {
			return None;
		}
		let root = if covered.len() == 1 {
			covered.pop().expect("checked non-empty")
		} else {
			BitmapPlan::And(covered)
		};
		let residual = residual.into_iter().cloned().reduce(|left, right| LogicalExpr::Binary {
			left: Box::new(left),
			op: BinaryOperator::And,
			right: Box::new(right),
		});
		Some(KnnPrefilterPlan {
			root,
			covered: covered_at,
			residual,
			uncovered_matches,
		})
	}

	/// Maximum number of array elements to expand for `field IN [...]`.
	///
	/// Beyond this threshold, the per-operator overhead of creating individual
	/// `IndexScan` operators inside a `UnionIndexScan` outweighs the benefit
	/// of targeted lookups. Arrays larger than this fall back to a table scan
	/// with a predicate filter, which performs a single sequential pass.
	///
	/// The value 32 is a heuristic, not measured against a crossover point. Raising it requires
	/// benchmarking the per-element `UnionIndexScan` overhead vs the table-scan cost for typical
	/// row counts. The `index_analyzer` criterion bench at
	/// `surrealdb/core/benches/index_analyzer.rs` is the right place to
	/// gather that signal.
	const MAX_IN_EXPANSION_SIZE: usize = 32;

	/// Try to expand `field IN [v1, v2, ...]` into a union of equality lookups.
	///
	/// Walks the condition (through AND nodes) looking for `INSIDE` expressions
	/// where the right side is a multi-element array literal. For each, if a
	/// single-column index exists on the field, creates `AccessPath::Union`
	/// with one `BTreeScan::Equality` per array element.
	///
	/// Arrays larger than [`Self::MAX_IN_EXPANSION_SIZE`] are not expanded to
	/// avoid excessive per-operator overhead.
	///
	/// This is a fallback for when `analyze()` and `try_or_union()` both fail
	/// to find index candidates (e.g. standalone `field IN [1, 2]`).
	pub fn try_in_expansion(
		&self,
		cond: Option<&LogicalExpr>,
		direction: Direction,
	) -> Option<AccessPath> {
		let cond = cond?;

		if matches!(self.with_hints, Some(With::NoIndex)) {
			return None;
		}

		// Collect IN expressions from the condition
		let mut in_exprs = Vec::new();
		Self::collect_in_expressions(cond, &mut in_exprs);

		for (path, values) in &in_exprs {
			if values.len() < 2 || values.len() > Self::MAX_IN_EXPANSION_SIZE {
				continue; // Single-element handled by match_operator_to_access; too-large skipped
			}
			// No key is a whole array (see `match_operator_to_access`), so a
			// list holding one cannot be answered branch by branch.
			if values.iter().any(|v| matches!(v, Value::Array(_))) {
				continue;
			}

			// Track best candidate: prefer single-column indexes (fewer
			// columns) because they produce BTreeAccess::Equality sub-paths
			// which enable merge-by-id on UnionIndexScan for ORDER BY id
			// sort elimination.  Multi-column indexes create Compound
			// sub-paths that are sorted by remaining columns, not by id,
			// so they cannot participate in the merge optimisation.  When
			// no single-column index exists, we fall back to the narrowest
			// compound index available.
			let mut best: Option<(usize, usize)> = None; // (index idx, num cols)

			for (idx, ix_def) in self.indexes.iter().enumerate() {
				if ix_def.prepare_remove {
					continue;
				}
				if !matches!(ix_def.index, crate::catalog::Index::Idx | crate::catalog::Index::Uniq)
				{
					continue;
				}

				if let Some(With::Index(names)) = self.with_hints
					&& !names.iter().any(|n| n.as_str() == ix_def.name.as_str())
				{
					continue;
				}

				// The IN column must be the FIRST column of the index.
				if let Some(first_col) = ix_def.cols.first()
					&& path_matches_column(path, first_col, self.key.as_deref())
				{
					let ncols = ix_def.cols.len();
					if best.is_none_or(|(_, best_ncols)| ncols < best_ncols) {
						best = Some((idx, ncols));
					}
				}
			}

			if let Some((idx, ncols)) = best {
				let index_ref = self.index_ref(idx);
				let paths: Vec<AccessPath> = if ncols == 1 {
					// Single-column index: equality scans
					values
						.iter()
						.map(|v| AccessPath::BTreeScan {
							index_ref: index_ref.clone(),
							access: BTreeAccess::literal_equality(v.clone()),
							direction,
						})
						.collect()
				} else {
					// Compound index: prefix scans with IN value as first
					// column.  The remaining columns provide ordering and
					// selectivity for other WHERE conditions.
					values
						.iter()
						.map(|v| AccessPath::BTreeScan {
							index_ref: index_ref.clone(),
							access: BTreeAccess::literal_compound(vec![v.clone()], None),
							direction,
						})
						.collect()
				};
				// Scalar `IN`-expansion: each row's field value equals
				// at most one literal, so branches are record-disjoint
				// — no dedupe needed.
				return Some(AccessPath::Union {
					paths,
					dedupe: false,
				});
			}
		}

		None
	}

	/// Try to expand CONTAINSALL/CONTAINSANY/ALLINSIDE/ANYINSIDE expressions
	/// into `AccessPath::Union` of equality scans on array indexes.
	///
	/// For `field CONTAINSALL [a, b]` with an index on `field[*]`, creates a
	/// union of equality scans: one for `a` and one for `b`. This parallels
	/// `try_in_expansion` but matches against array indexes (columns with
	/// `*`) using [`path_matches_containment`].
	pub fn try_containment_expansion(
		&self,
		cond: Option<&LogicalExpr>,
		direction: Direction,
	) -> Option<AccessPath> {
		let cond = cond?;

		if matches!(self.with_hints, Some(With::NoIndex)) {
			return None;
		}

		let mut exprs = Vec::new();
		Self::collect_containment_expressions(cond, &mut exprs);

		for (path, values) in &exprs {
			if values.is_empty() || values.len() > Self::MAX_IN_EXPANSION_SIZE {
				continue;
			}

			for (idx, ix_def) in self.indexes.iter().enumerate() {
				if ix_def.prepare_remove {
					continue;
				}
				if !matches!(ix_def.index, crate::catalog::Index::Idx | crate::catalog::Index::Uniq)
				{
					continue;
				}

				if let Some(With::Index(names)) = self.with_hints
					&& !names.iter().any(|n| n.as_str() == ix_def.name.as_str())
				{
					continue;
				}

				if self.seeks_containment(idx, 0, path) {
					let index_ref = self.index_ref(idx);
					let is_composite = ix_def.cols.len() > 1;
					let paths: Vec<AccessPath> = values
						.iter()
						.map(|v| {
							let access = if is_composite {
								BTreeAccess::literal_compound(vec![v.clone()], None)
							} else {
								BTreeAccess::literal_equality(v.clone())
							};
							AccessPath::BTreeScan {
								index_ref: index_ref.clone(),
								access,
								direction,
							}
						})
						.collect();
					// CONTAINS-on-array: a row whose indexed array
					// contains multiple branch values sits in
					// multiple branches' prefix ranges.  Dedupe
					// required to avoid emitting the row twice
					// through the merge.
					return Some(AccessPath::Union {
						paths,
						dedupe: true,
					});
				}
			}
		}

		None
	}

	/// A union answering a containment conjunct of `cond` over the path of an
	/// element column whose seek of the contained values is complete only
	/// with the nullish keys ([`Self::seeks_containment_with_nullish`]): one
	/// seek per value, one for `NONE` and one for `NULL`. Every record a branch
	/// yields is judged by the whole condition again, so a record listed
	/// under a nullish key that holds none of the values is dropped there.
	///
	/// Each record it yields is fetched to be judged, where a scan of the
	/// table rejects most records before decoding them: the caller weighs
	/// that ([`super::access_path::decide_access`]).
	pub fn try_nullish_containment(
		&self,
		cond: Option<&LogicalExpr>,
		direction: Direction,
	) -> Option<AccessPath> {
		let cond = cond?;
		if matches!(self.with_hints, Some(With::NoIndex)) {
			return None;
		}
		let mut singles = Vec::new();
		Self::collect_single_containments(cond, &mut singles);
		if let Some(path) = singles.iter().find_map(|(path, value)| {
			self.containment_with_nullish(path, std::slice::from_ref(value), direction)
		}) {
			return Some(path);
		}
		let mut exprs = Vec::new();
		Self::collect_containment_expressions(cond, &mut exprs);
		exprs.iter().find_map(|(path, values)| {
			(!values.is_empty() && values.len() <= Self::MAX_IN_EXPANSION_SIZE)
				.then(|| self.containment_with_nullish(path, values, direction))
				.flatten()
		})
	}

	/// Collect `path CONTAINS v` and `v INSIDE path` of a literal `v` from an
	/// AND tree.
	fn collect_single_containments(expr: &LogicalExpr, results: &mut Vec<(LogicalExpr, Value)>) {
		if let LogicalExpr::Binary {
			left,
			op: BinaryOperator::And,
			right,
		} = expr
		{
			Self::collect_single_containments(left, results);
			Self::collect_single_containments(right, results);
		} else if let LogicalExpr::Binary {
			left,
			op: BinaryOperator::Contain,
			right,
		} = expr
		{
			if left.is_path()
				&& let Some(v) = index_literal(right)
			{
				results.push((left.as_ref().clone(), v));
			}
		} else if let LogicalExpr::Binary {
			left,
			op: BinaryOperator::Inside,
			right,
		} = expr
		{
			if right.is_path()
				&& let Some(v) = index_literal(left)
			{
				results.push((right.as_ref().clone(), v));
			}
		} else if let LogicalExpr::Prefix {
			op,
			expr: inner,
		} = expr && Self::is_truth_preserving_prefix(op, inner)
		{
			Self::collect_single_containments(inner, results);
		}
	}

	/// A union answering a containment of `values` over `path` from a column
	/// that lists every record under its elements or under `NONE` or `NULL`
	/// ([`Self::seeks_containment_with_nullish`]): one seek per value and one
	/// per nullish key. `None` when no such column is the leading column of an
	/// index.
	fn containment_with_nullish(
		&self,
		path: &LogicalExpr,
		values: &[Value],
		direction: Direction,
	) -> Option<AccessPath> {
		let (idx, ix_def) = self.indexes.iter().enumerate().find(|(idx, ix_def)| {
			!ix_def.prepare_remove
				&& matches!(ix_def.index, Index::Idx | Index::Uniq)
				&& !matches!(self.with_hints, Some(With::Index(names))
					if !names.iter().any(|n| n.as_str() == ix_def.name.as_str()))
				&& self.seeks_containment_with_nullish(*idx, 0, path)
		})?;
		let index_ref = self.index_ref(idx);
		let is_composite = ix_def.cols.len() > 1;
		let mut keys: Vec<Value> = Vec::with_capacity(values.len() + 2);
		for v in values.iter().cloned().chain([Value::None, Value::Null]) {
			if !keys.contains(&v) {
				keys.push(v);
			}
		}
		let paths = keys
			.into_iter()
			.map(|v| AccessPath::BTreeScan {
				index_ref: index_ref.clone(),
				access: if is_composite {
					BTreeAccess::literal_compound(vec![v], None)
				} else {
					BTreeAccess::literal_equality(v)
				},
				direction,
			})
			.collect();
		// A record holding several of the values sits in several branches.
		Some(AccessPath::Union {
			paths,
			dedupe: true,
		})
	}

	/// Collect CONTAINSALL/CONTAINSANY (field path on left, array literal on
	/// right) and ALLINSIDE/ANYINSIDE (array literal on left, field path on
	/// right) from an AND tree.
	fn collect_containment_expressions(
		expr: &LogicalExpr,
		results: &mut Vec<(LogicalExpr, Vec<Value>)>,
	) {
		if let LogicalExpr::Binary {
			left,
			op: BinaryOperator::And,
			right,
		} = expr
		{
			Self::collect_containment_expressions(left, results);
			Self::collect_containment_expressions(right, results);
		} else if let LogicalExpr::Binary {
			left,
			op: BinaryOperator::ContainAll | BinaryOperator::ContainAny,
			right,
		} = expr
		{
			if left.is_path()
				&& let Some(Value::Array(arr)) = index_literal(right)
			{
				results.push((left.as_ref().clone(), arr.0));
			}
		} else if let LogicalExpr::Binary {
			left,
			op: BinaryOperator::AllInside | BinaryOperator::AnyInside,
			right,
		} = expr
		{
			if right.is_path()
				&& let Some(Value::Array(arr)) = index_literal(left)
			{
				results.push((right.as_ref().clone(), arr.0));
			}
		} else if let LogicalExpr::Prefix {
			op,
			expr: inner,
		} = expr && Self::is_truth_preserving_prefix(op, inner)
		{
			Self::collect_containment_expressions(inner, results);
		}
	}

	/// Collect `field INSIDE [values]` expressions from an AND tree.
	fn collect_in_expressions(expr: &LogicalExpr, results: &mut Vec<(LogicalExpr, Vec<Value>)>) {
		if let LogicalExpr::Binary {
			left,
			op: BinaryOperator::And,
			right,
		} = expr
		{
			Self::collect_in_expressions(left, results);
			Self::collect_in_expressions(right, results);
		} else if let LogicalExpr::Binary {
			left,
			op: BinaryOperator::Inside,
			right,
		} = expr
		{
			if left.is_path()
				&& let Some(Value::Array(arr)) = index_literal(right)
			{
				results.push((left.as_ref().clone(), arr.0));
			}
		} else if let LogicalExpr::Prefix {
			op,
			expr: inner,
		} = expr && !matches!(op, PrefixOperator::Not)
		{
			Self::collect_in_expressions(inner, results);
		}
	}

	/// Base selectivity score for a single-column `SPATIAL` index covering.
	///
	/// Without table statistics, [`Self::SCORE_SPATIAL_SINGLE`] (`350`) is scaled down by up to
	/// [`Self::MAX_SPATIAL_SCORE_PENALTY`] (`140`, reaching `210` at
	/// [`Self::SPATIAL_SCORE_PENALTY_MAX_AREA_FRACTION`]) and then by up to an additional
	/// [`Self::MAX_WIDE_SPATIAL_SCORE_PENALTY`] (`100`, reaching `110` at `100%` of Earth's
	/// surface) according to the query predicate's spherical surface-area fraction so that:
	/// - a selective spatial predicate (`< 0.35%` of Earth) outranks a 1-D bounded B-tree range
	///   (`300`);
	/// - a regional spatial predicate (`~0.35%..=1%` of Earth) ranks between a bounded B-tree range
	///   (`300`) and a half-bounded B-tree range (`200`);
	/// - a wide continental or global spatial covering (`>= 2%` of Earth) drops below a
	///   half-bounded B-tree range (`200`), so a selective `n > ...` B-tree range is preferred over
	///   scanning a continental/global spatial covering.
	const SCORE_SPATIAL_SINGLE: u32 = 350;
	/// Linear score reduction applied up to [`Self::SPATIAL_SCORE_PENALTY_MAX_AREA_FRACTION`]
	/// (`350 - 140 = 210` at `1%` of Earth's surface).
	const MAX_SPATIAL_SCORE_PENALTY: u32 = 140;
	/// Additional logarithmic score reduction applied between `1%` and `100%` of Earth's surface
	/// (`210 - 100 = 110` at `100%`).
	const MAX_WIDE_SPATIAL_SCORE_PENALTY: u32 = 100;
	/// Spherical surface-area fraction (`0.0..=1.0`, `1%` of Earth's surface) at which the fine
	/// linear penalty saturates and the wide logarithmic penalty begins.
	const SPATIAL_SCORE_PENALTY_MAX_AREA_FRACTION: f64 = 0.01;
	/// Base selectivity score for a compound `SPATIAL` index whose equality prefix is matched.
	const SCORE_SPATIAL_COMPOUND_BASE: u32 = 500;
	/// Per-column score bonus for each matched equality prefix column on a compound `SPATIAL`
	/// index.
	const SCORE_COMPOUND_PREFIX_COL_BONUS: u32 = 50;
	/// Maximum number of equality prefix columns that contribute to the compound score bonus.
	const MAX_COMPOUND_PREFIX_BONUS_COLS: u32 = 6;

	fn single_spatial_score(area_fraction: f64) -> u32 {
		let f = area_fraction.clamp(0.0, 1.0);
		let fine_penalty = (f / Self::SPATIAL_SCORE_PENALTY_MAX_AREA_FRACTION).min(1.0)
			* (Self::MAX_SPATIAL_SCORE_PENALTY as f64);
		let wide_penalty = if f > Self::SPATIAL_SCORE_PENALTY_MAX_AREA_FRACTION {
			((f / Self::SPATIAL_SCORE_PENALTY_MAX_AREA_FRACTION).log10() / 2.0).clamp(0.0, 1.0)
				* (Self::MAX_WIDE_SPATIAL_SCORE_PENALTY as f64)
		} else {
			0.0
		};
		Self::SCORE_SPATIAL_SINGLE.saturating_sub((fine_penalty + wide_penalty).round() as u32)
	}

	fn plan_spatial_scans(
		&self,
		predicate: &SpatialPredicate,
		include_ancestors: bool,
		max_ops: usize,
	) -> (Arc<[surrealdb_idx::spatial::SpatialScanOp]>, f64) {
		let key = predicate.cache_key(include_ancestors, max_ops);
		if let Some(hit) = self.spatial_plan_cache.borrow().get(&key).and_then(|entries| {
			entries.iter().find(|(p, inc, cap, _, _)| {
				*inc == include_ancestors && *cap == max_ops && p == predicate
			})
		}) {
			return (Arc::clone(&hit.3), hit.4);
		}
		let (ops_vec, area_frac) = predicate.plan_scans_and_area(include_ancestors, max_ops);
		let ops: Arc<[surrealdb_idx::spatial::SpatialScanOp]> = ops_vec.into();
		self.spatial_plan_cache.borrow_mut().entry(key).or_default().push((
			predicate.clone(),
			include_ancestors,
			max_ops,
			Arc::clone(&ops),
			area_frac,
		));
		(ops, area_frac)
	}

	/// Try to expand spatial geometry predicates (`INSIDE`, `CONTAINS`, `INTERSECTS`,
	/// `CONTAINSANY`, `CONTAINSALL`, `ANYINSIDE`, `ALLINSIDE`, and `geo::distance(field, pt) < r`)
	/// into an `AccessPath::Union` of Hilbert cell B-Tree range/equality scans on a `SPATIAL`
	/// index.
	///
	/// Because cell coverings are a superset of the true geometric intersection, the original
	/// `WHERE` predicate remains active as the exact post-filter unless `AnsweredByUnion` proves
	/// the union is a point-only disjoint covering where every branch carries an exact inline
	/// [`SpatialPointFilter`].
	pub fn try_spatial_expansion(
		&self,
		cond: Option<&LogicalExpr>,
		direction: Direction,
	) -> Option<AccessPath> {
		self.try_spatial_expansion_scored(cond, direction).map(|(path, _, _)| path)
	}

	/// Expand a spatial geometry predicate into an `AccessPath::Union` of Hilbert cell B-Tree
	/// scans alongside its selectivity score and number of equality-pinned prefix columns:
	/// - `110..=350` ([`Self::SCORE_SPATIAL_SINGLE`] scaled down by spherical surface-area
	///   fraction) for a single-column `SPATIAL` index (`prefix_len == 0`)
	/// - `500 + 50 * min(prefix_len, 6)` for a compound `SPATIAL` index whose leading equality
	///   prefix columns are all pinned by `cond`.
	///
	/// When multiple `SPATIAL` indexes or multiple spatial predicates in an `AND` tree match,
	/// selects the highest-scoring candidate (tie-breaking by fewest `scan_ops`, then catalog
	/// index order).
	pub fn try_spatial_expansion_scored(
		&self,
		cond: Option<&LogicalExpr>,
		direction: Direction,
	) -> Option<(AccessPath, u32, usize)> {
		self.try_spatial_expansion_in_context(cond, cond, direction)
			.map(|(path, score, cols)| (path, score, cols.len().saturating_sub(1)))
	}

	fn try_spatial_expansion_for_or_branch(
		&self,
		cond: Option<&LogicalExpr>,
		direction: Direction,
		max_ops: usize,
	) -> Option<(AccessPath, u32, f64, usize)> {
		self.spatial_expansions_in_context_with_max_ops(cond, cond, direction, max_ops, true)
			.into_iter()
			.next()
			.map(|(path, score, area, cols)| (path, score, area, cols.len().saturating_sub(1)))
	}

	fn try_spatial_expansion_in_context(
		&self,
		expr: Option<&LogicalExpr>,
		context_cond: Option<&LogicalExpr>,
		direction: Direction,
	) -> Option<(AccessPath, u32, Vec<Idiom>)> {
		self.spatial_expansions_in_context(expr, context_cond, direction)
			.into_iter()
			.next()
			.map(|(path, score, _, cols)| (path, score, cols))
	}

	/// Collect the best spatial expansion per distinct indexed geometry column in `expr`,
	/// ordered from highest-scoring / tightest covering to lowest.
	fn spatial_expansions_in_context(
		&self,
		expr: Option<&LogicalExpr>,
		context_cond: Option<&LogicalExpr>,
		direction: Direction,
	) -> Vec<(AccessPath, u32, f64, Vec<Idiom>)> {
		self.spatial_expansions_in_context_with_max_ops(
			expr,
			context_cond,
			direction,
			surrealdb_idx::spatial::MAX_SPATIAL_SCAN_OPS,
			false,
		)
	}

	fn spatial_expansions_in_context_with_max_ops(
		&self,
		expr: Option<&LogicalExpr>,
		context_cond: Option<&LogicalExpr>,
		direction: Direction,
		max_ops: usize,
		for_or_branch: bool,
	) -> Vec<(AccessPath, u32, f64, Vec<Idiom>)> {
		let Some(expr) = expr else {
			return Vec::new();
		};

		if matches!(self.with_hints, Some(With::NoIndex)) {
			return Vec::new();
		}

		if !self.indexes.iter().any(|ix| {
			!ix.prepare_remove
				&& matches!(ix.index, crate::catalog::Index::Spatial)
				&& crate::catalog::uses_current_spatial_covering(ix.format_version)
		}) {
			return Vec::new();
		}

		let mut exprs = Vec::new();
		Self::collect_spatial_expressions(expr, &mut exprs);
		if exprs.is_empty() {
			return Vec::new();
		}

		let mut simple_conds = Vec::new();
		self.collect_conditions(expr, &mut simple_conds);
		if let Some(ctx) = context_cond
			&& !std::ptr::eq(ctx, expr)
		{
			self.collect_conditions(ctx, &mut simple_conds);
		}

		struct BestSpatialMatch {
			idx: usize,
			prefix_values: Vec<BoundOperand>,
			scan_ops: Arc<[surrealdb_idx::spatial::SpatialScanOp]>,
			include_ancestors: bool,
			point_filter: Option<Arc<SpatialPointFilter>>,
			score: u32,
			area_fraction: f64,
		}

		let mut best_per_geom_col: Vec<(Idiom, BestSpatialMatch)> = Vec::new();

		for (path, predicate) in exprs {
			for (idx, ix_def) in self.indexes.iter().enumerate() {
				if ix_def.prepare_remove {
					continue;
				}
				if !matches!(ix_def.index, crate::catalog::Index::Spatial) {
					continue;
				}
				if !crate::catalog::uses_current_spatial_covering(ix_def.format_version) {
					continue;
				}
				if let Some(With::Index(names)) = self.with_hints
					&& !names.iter().any(|n| n.as_str() == ix_def.name.as_str())
				{
					continue;
				}

				let Some((geom_col, prefix_cols)) = ix_def.cols.split_last() else {
					continue;
				};
				if !spatial_path_matches_column(&path, geom_col, self.key.as_deref()) {
					continue;
				}

				let normalised = spells_normalised_decimal_keys(ix_def.format_version);
				let mut prefix_values = Vec::with_capacity(prefix_cols.len());
				let mut all_prefix_matched = true;
				for (col_idx, col) in prefix_cols.iter().enumerate() {
					let matching_eq = simple_conds.iter().find(|c| {
						path_matches_column(&c.path, col, self.key.as_deref())
							&& matches!(c.op, BinaryOperator::Equal | BinaryOperator::ExactEqual)
							&& c.value
								.as_literal()
								.is_none_or(|v| Self::key_equality_covers(v, normalised))
					});
					let matching_containment = if matching_eq.is_none() {
						simple_conds.iter().find(|c| {
							matches!(
								(&c.op, c.position),
								(BinaryOperator::Contain, IdiomPosition::Left)
									| (BinaryOperator::Inside, IdiomPosition::Right)
							) && self.seeks_containment(idx, col_idx, &c.path)
								&& c.value
									.as_literal()
									.is_none_or(|v| Self::key_equality_covers(v, normalised))
						})
					} else {
						None
					};
					if let Some(c) = matching_eq.or(matching_containment) {
						prefix_values.push(c.value.clone());
					} else {
						all_prefix_matched = false;
						break;
					}
				}
				if !all_prefix_matched {
					continue;
				}

				let include_ancestors = !self.is_point_only_spatial(idx);
				// `geo::distance(field, pt) < r` returns `NONE` (`NONE < r` is `true` in SurrealQL)
				// for non-point geometries (`Polygon`, `Line`, etc.), which may lie outside the
				// spherical cap around `pt`, and raises a runtime type error on non-geometry
				// values (`NONE`, `NULL`, strings, numbers, non-finite points), which emit no
				// spatial index entries. Only expand `SpatialPredicate::Cap` when the index is
				// known to store only single-cell point geometries (`!include_ancestors`) and no
				// non-geometry values (`!self.has_non_geometry_spatial(idx)`).
				if (include_ancestors || self.has_non_geometry_spatial(idx))
					&& matches!(predicate, SpatialPredicate::Cap { .. })
				{
					continue;
				}
				let (scan_ops, area_fraction) =
					self.plan_spatial_scans(&predicate, include_ancestors, max_ops);
				if scan_ops.is_empty() {
					continue;
				}
				if for_or_branch
					&& include_ancestors
					&& scan_ops
						.iter()
						.copied()
						.any(surrealdb_idx::spatial::SpatialScanOp::covers_root)
				{
					continue;
				}
				let point_filter = if !include_ancestors
					&& prefix_values.is_empty()
					&& path_matches_column(&path, geom_col, self.key.as_deref())
				{
					predicate.to_point_filter(geom_col.clone()).map(Arc::new)
				} else {
					None
				};

				let score = if prefix_values.is_empty() {
					Self::single_spatial_score(area_fraction)
				} else {
					let len =
						(prefix_values.len() as u32).min(Self::MAX_COMPOUND_PREFIX_BONUS_COLS);
					Self::SCORE_SPATIAL_COMPOUND_BASE + len * Self::SCORE_COMPOUND_PREFIX_COL_BONUS
				};

				let candidate = BestSpatialMatch {
					idx,
					prefix_values,
					scan_ops,
					include_ancestors,
					point_filter,
					score,
					area_fraction,
				};

				if let Some((_, prev)) =
					best_per_geom_col.iter_mut().find(|(col, _)| col == geom_col)
				{
					let is_better = (
						candidate.score,
						std::cmp::Reverse(candidate.scan_ops.len()),
						std::cmp::Reverse(candidate.idx),
					) > (
						prev.score,
						std::cmp::Reverse(prev.scan_ops.len()),
						std::cmp::Reverse(prev.idx),
					);
					if is_better {
						*prev = candidate;
					}
				} else {
					best_per_geom_col.push((geom_col.clone(), candidate));
				}
			}
		}

		best_per_geom_col.sort_by(|(_, a), (_, b)| {
			(b.score, a.scan_ops.len(), a.idx).cmp(&(a.score, b.scan_ops.len(), b.idx))
		});

		best_per_geom_col
			.into_iter()
			.map(|(_, winner)| {
				let index_ref = self.index_ref(winner.idx);
				let branch_direction = if winner
					.scan_ops
					.iter()
					.all(|op| matches!(op, surrealdb_idx::spatial::SpatialScanOp::Exact(_)))
				{
					direction
				} else {
					Direction::Forward
				};
				let paths: Vec<AccessPath> = winner
					.scan_ops
					.iter()
					.map(|op| {
						let access = if winner.prefix_values.is_empty() {
							match *op {
								surrealdb_idx::spatial::SpatialScanOp::Exact(id) => {
									BTreeAccess::literal_equality(Value::Number(
										crate::val::Number::Int(id),
									))
								}
								surrealdb_idx::spatial::SpatialScanOp::Range {
									min,
									max,
								} => BTreeAccess::literal_range(Range {
									start: Bound::Included(Value::Number(Number::Int(min))),
									end: Bound::Included(Value::Number(Number::Int(max))),
								})
								.with_point_filter(winner.point_filter.clone()),
							}
						} else {
							match *op {
								surrealdb_idx::spatial::SpatialScanOp::Exact(id) => {
									let mut prefix = winner.prefix_values.clone();
									prefix.push(BoundOperand::Literal(Value::Number(Number::Int(
										id,
									))));
									BTreeAccess::Compound {
										prefix,
										range: None,
										upper: None,
									}
								}
								surrealdb_idx::spatial::SpatialScanOp::Range {
									min,
									max,
								} => BTreeAccess::Compound {
									prefix: winner.prefix_values.clone(),
									range: Some((
										BinaryOperator::MoreThanEqual,
										BoundOperand::Literal(Value::Number(Number::Int(min))),
									)),
									upper: Some((
										BinaryOperator::LessThanEqual,
										BoundOperand::Literal(Value::Number(Number::Int(max))),
									)),
								},
							}
						};
						AccessPath::BTreeScan {
							index_ref: index_ref.clone(),
							access,
							direction: branch_direction,
						}
					})
					.collect();
				(
					AccessPath::Union {
						paths,
						dedupe: winner.include_ancestors,
					},
					winner.score,
					winner.area_fraction,
					self.indexes[winner.idx].cols.clone(),
				)
			})
			.collect()
	}

	/// Collect spatial predicates on a geometry field path from an `AND` tree.
	fn collect_spatial_expressions(
		expr: &LogicalExpr,
		results: &mut Vec<(LogicalExpr, SpatialPredicate)>,
	) {
		if let LogicalExpr::Binary {
			left,
			op,
			right,
		} = expr
		{
			if matches!(op, BinaryOperator::And) {
				Self::collect_spatial_expressions(left, results);
				Self::collect_spatial_expressions(right, results);
			} else if matches!(
				op,
				BinaryOperator::Inside
					| BinaryOperator::Contain
					| BinaryOperator::Intersects
					| BinaryOperator::ContainAny
					| BinaryOperator::ContainAll
					| BinaryOperator::AnyInside
					| BinaryOperator::AllInside
			) {
				if left.is_path()
					&& !matches!(op, BinaryOperator::AllInside)
					&& let Some((geom, is_single_geom)) = Self::try_eval_literal_geometry(right)
				{
					if matches!(op, BinaryOperator::Intersects) && !is_single_geom {
						return;
					}
					let pred = match (op, geom) {
						(BinaryOperator::ContainAll, crate::val::Geometry::Collection(items))
							if !items.is_empty() =>
						{
							SpatialPredicate::CollectionAll(items)
						}
						(BinaryOperator::Inside, geom) => SpatialPredicate::Geometry {
							geom,
							exact_kind: is_single_geom.then_some(SpatialExactKind::Inside),
						},
						(BinaryOperator::Intersects, geom) => SpatialPredicate::Geometry {
							geom,
							exact_kind: is_single_geom.then_some(SpatialExactKind::Intersects),
						},
						(_, geom) => SpatialPredicate::Geometry {
							geom,
							exact_kind: None,
						},
					};
					results.push((left.as_ref().clone(), pred));
				} else if right.is_path()
					&& !matches!(op, BinaryOperator::ContainAll)
					&& let Some((geom, is_single_geom)) = Self::try_eval_literal_geometry(left)
				{
					if matches!(op, BinaryOperator::Intersects) && !is_single_geom {
						return;
					}
					let pred = match (op, geom) {
						(BinaryOperator::AllInside, crate::val::Geometry::Collection(items))
							if !items.is_empty() =>
						{
							SpatialPredicate::CollectionAll(items)
						}
						(BinaryOperator::Contain, geom) => SpatialPredicate::Geometry {
							geom,
							exact_kind: is_single_geom.then_some(SpatialExactKind::Inside),
						},
						(BinaryOperator::Intersects, geom) => SpatialPredicate::Geometry {
							geom,
							exact_kind: is_single_geom.then_some(SpatialExactKind::Intersects),
						},
						(_, geom) => SpatialPredicate::Geometry {
							geom,
							exact_kind: None,
						},
					};
					results.push((right.as_ref().clone(), pred));
				}
			} else if matches!(op, BinaryOperator::LessThan | BinaryOperator::LessThanEqual) {
				if let Some((path, center @ crate::val::Geometry::Point(_), radius, inclusive)) =
					Self::extract_geo_distance_radius(
						left,
						right,
						matches!(op, BinaryOperator::LessThanEqual),
					) {
					results.push((
						path,
						SpatialPredicate::Cap {
							center,
							radius,
							inclusive,
						},
					));
				}
			} else if matches!(op, BinaryOperator::MoreThan | BinaryOperator::MoreThanEqual)
				&& let Some((path, center @ crate::val::Geometry::Point(_), radius, inclusive)) =
					Self::extract_geo_distance_radius(
						right,
						left,
						matches!(op, BinaryOperator::MoreThanEqual),
					) {
				results.push((
					path,
					SpatialPredicate::Cap {
						center,
						radius,
						inclusive,
					},
				));
			}
		} else if let LogicalExpr::Prefix {
			op,
			expr: inner,
		} = expr && Self::is_truth_preserving_prefix(op, inner)
		{
			Self::collect_spatial_expressions(inner, results);
		}
	}

	/// Whether applying `op` to `inner` preserves the boolean truth set and error
	/// behavior of `inner` when evaluated as a `WHERE` filter condition:
	/// - `<any> expr` (`PrefixOperator::Cast(Kind::Any)`) is always an identity cast;
	/// - `<bool> expr` (`PrefixOperator::Cast(Kind::Bool)`) is an identity cast when `inner` is
	///   statically guaranteed to produce a `Value::Bool` (`inner.checked_kind(&Schema::open()) ==
	///   Some(Kind::Bool)`);
	/// - all other prefix operators (`!`, `+`, `-`, `..`, `..=`, and casts to other kinds such as
	///   `<string>`) can invert truthiness, convert `false` to a truthy non-empty `Value::String`
	///   (`'false'`) or a falsy `Value::Range`, or raise a cast/type error.
	fn is_truth_preserving_prefix(op: &PrefixOperator, inner: &LogicalExpr) -> bool {
		match op {
			PrefixOperator::Cast(crate::expr::Kind::Any) => true,
			PrefixOperator::Cast(crate::expr::Kind::Bool) => {
				inner.checked_kind(&crate::expr::logical::Schema::open())
					== Some(crate::expr::Kind::Bool)
			}
			PrefixOperator::Not
			| PrefixOperator::Positive
			| PrefixOperator::Negate
			| PrefixOperator::Range
			| PrefixOperator::RangeInclusive
			| PrefixOperator::Cast(_) => false,
		}
	}

	/// Extract `(field_path, center_point, radius_meters, inclusive)` from
	/// `geo::distance(field, point) < radius` or `<= radius`.
	///
	/// When `radius` is a [`crate::val::Number`] (`Decimal` or `Int`) that does not
	/// round-trip losslessly to `f64`, normalizes `(radius, inclusive)` against the
	/// exact [`crate::val::Number`] ordering (`(f_hi, false)` where
	/// `Number::Float(f_hi.next_down()) < n < Number::Float(f_hi)`) so that
	/// comparing the `f64` returned by `geo::distance` against `radius` in
	/// [`SpatialPointFilter::Cap`] is 100% equivalent to `Number::cmp` and the
	/// covering radius is conservatively rounded up (`f_hi >= n`).
	fn extract_geo_distance_radius(
		dist_expr: &LogicalExpr,
		radius_expr: &LogicalExpr,
		inclusive: bool,
	) -> Option<(LogicalExpr, crate::val::Geometry, f64, bool)> {
		let Value::Number(n) = index_literal(radius_expr)? else {
			return None;
		};
		if n.is_nan() || n < crate::val::Number::Float(0.0) {
			return None;
		}
		let mut radius = n.as_float();
		if !radius.is_finite() || radius < 0.0 {
			return None;
		}
		for _ in 0..8 {
			if crate::val::Number::Float(radius) >= n {
				break;
			}
			let next = radius.next_up();
			if !next.is_finite() {
				return None;
			}
			radius = next;
		}
		for _ in 0..8 {
			if radius == 0.0 || crate::val::Number::Float(radius) <= n {
				break;
			}
			let prev = radius.next_down();
			if crate::val::Number::Float(prev) < n {
				break;
			}
			radius = prev;
		}
		let inclusive = match crate::val::Number::Float(radius).cmp(&n) {
			std::cmp::Ordering::Equal => inclusive,
			std::cmp::Ordering::Greater
				if radius > 0.0 && crate::val::Number::Float(radius.next_down()) < n =>
			{
				false
			}
			std::cmp::Ordering::Less | std::cmp::Ordering::Greater => return None,
		};
		if let LogicalExpr::Call(call) = dist_expr
			&& let Function::Normal(name) = &call.receiver
			&& name.eq_ignore_ascii_case("geo::distance")
			&& call.args.len() == 2
		{
			if call.args[0].is_path()
				&& let Some((geom @ crate::val::Geometry::Point(_), true)) =
					Self::try_eval_literal_geometry(&call.args[1])
			{
				return Some((call.args[0].clone(), geom, radius, inclusive));
			}
			if call.args[1].is_path()
				&& let Some((geom @ crate::val::Geometry::Point(_), true)) =
					Self::try_eval_literal_geometry(&call.args[0])
			{
				return Some((call.args[1].clone(), geom, radius, inclusive));
			}
		}
		None
	}

	/// Evaluate a constant `LogicalExpr` into `(Geometry, is_single_geom)` if it is a geometry
	/// literal or a GeoJSON object literal (`is_single_geom == true`), or an array/set of
	/// geometries/GeoJSON objects (`is_single_geom == false`).
	fn try_eval_literal_geometry(expr: &LogicalExpr) -> Option<(crate::val::Geometry, bool)> {
		if let LogicalExpr::Prefix {
			op: PrefixOperator::Cast(kind @ crate::expr::Kind::Geometry(_)),
			expr: inner,
		} = expr
		{
			if let Some(val) = try_literal_to_value(inner) {
				return Self::value_to_geometry(val.cast_to_kind(kind).ok()?);
			}
			let (geom, _) = Self::try_eval_literal_geometry(inner)?;
			return Self::value_to_geometry(Value::Geometry(geom).cast_to_kind(kind).ok()?);
		}
		let val = try_literal_to_value(expr)?;
		Self::value_to_geometry(val)
	}

	fn value_to_geometry(val: Value) -> Option<(crate::val::Geometry, bool)> {
		let collect_items = |items: Vec<Value>| -> Option<crate::val::Geometry> {
			let mut geoms = Vec::with_capacity(items.len());
			for item in items {
				let g = match item {
					Value::Geometry(g) => g,
					Value::Object(obj) => crate::val::Geometry::try_from_object(&obj)?,
					Value::None
					| Value::Null
					| Value::Bool(_)
					| Value::Number(_)
					| Value::String(_)
					| Value::Duration(_)
					| Value::Datetime(_)
					| Value::Date(_)
					| Value::Time(_)
					| Value::Timestamp(_)
					| Value::Interval(_)
					| Value::Uuid(_)
					| Value::Array(_)
					| Value::Set(_)
					| Value::Bytes(_)
					| Value::Table(_)
					| Value::RecordId(_)
					| Value::File(_)
					| Value::Range(_)
					| Value::Regex(_)
					| Value::Closure(_) => return None,
				};
				geoms.push(g);
			}
			if geoms.is_empty() {
				None
			} else {
				Some(crate::val::Geometry::Collection(geoms))
			}
		};
		let (geom, is_single_geom) = match val {
			Value::Geometry(g) => (Some(g), true),
			Value::Object(obj) => (crate::val::Geometry::try_from_object(&obj), true),
			Value::Array(arr) => (collect_items(arr.0), false),
			Value::Set(set) => (collect_items(set.into_iter().collect()), false),
			Value::None
			| Value::Null
			| Value::Bool(_)
			| Value::Number(_)
			| Value::String(_)
			| Value::Duration(_)
			| Value::Datetime(_)
			| Value::Date(_)
			| Value::Time(_)
			| Value::Timestamp(_)
			| Value::Interval(_)
			| Value::Uuid(_)
			| Value::Bytes(_)
			| Value::Table(_)
			| Value::RecordId(_)
			| Value::File(_)
			| Value::Range(_)
			| Value::Regex(_)
			| Value::Closure(_) => (None, false),
		};
		geom.filter(|g| {
			!Self::geometry_has_empty_component(g)
				&& surrealdb_idx::spatial::geometry_all_coords_finite(g)
		})
		.map(|g| (g, is_single_geom))
	}

	fn geometry_has_empty_component(geom: &crate::val::Geometry) -> bool {
		match geom {
			crate::val::Geometry::Point(_) => false,
			crate::val::Geometry::Line(l) => l.0.is_empty(),
			crate::val::Geometry::Polygon(p) => p.exterior().0.is_empty(),
			crate::val::Geometry::MultiPoint(mp) => mp.0.is_empty(),
			crate::val::Geometry::MultiLine(ml) => {
				ml.0.is_empty() || ml.0.iter().any(|l| l.0.is_empty())
			}
			crate::val::Geometry::MultiPolygon(mp) => {
				mp.0.is_empty() || mp.0.iter().any(|p| p.exterior().0.is_empty())
			}
			crate::val::Geometry::Collection(items) => {
				items.is_empty() || items.iter().any(Self::geometry_has_empty_component)
			}
		}
	}

	/// Flatten nested OR expressions into a list of branches.
	///
	/// `A OR B OR C` (parsed as `(A OR B) OR C`) becomes `[A, B, C]`.
	fn flatten_or<'b>(expr: &'b LogicalExpr, branches: &mut Vec<&'b LogicalExpr>) {
		if let LogicalExpr::Binary {
			left,
			op: BinaryOperator::Or,
			right,
		} = expr
		{
			Self::flatten_or(left, branches);
			Self::flatten_or(right, branches);
		} else {
			branches.push(expr);
		}
	}

	/// Flatten nested AND expressions into a list of conjuncts.
	///
	/// `A AND B AND C` (parsed as `(A AND B) AND C`) becomes `[A, B, C]`.
	/// Non-AND nodes (including OR sub-trees) are returned as single
	/// conjuncts so the caller can inspect them.
	fn flatten_and<'b>(expr: &'b LogicalExpr, conjuncts: &mut Vec<&'b LogicalExpr>) {
		if let LogicalExpr::Binary {
			left,
			op: BinaryOperator::And,
			right,
		} = expr
		{
			Self::flatten_and(left, conjuncts);
			Self::flatten_and(right, conjuncts);
		} else {
			conjuncts.push(expr);
		}
	}

	/// Returns `true` if the expression tree contains a KNN (nearest-neighbour)
	/// operator. Such an operator is consumed by a `KnnScan` and stripped from
	/// the WHERE before residual filtering, so it must not be left behind as a
	/// residual by an index substitution (see `try_and_nested_or_union`).
	fn expr_contains_knn(expr: &LogicalExpr) -> bool {
		if let LogicalExpr::Binary {
			left,
			op,
			right,
		} = expr
		{
			matches!(op, BinaryOperator::NearestNeighbor(_))
				|| Self::expr_contains_knn(left)
				|| Self::expr_contains_knn(right)
		} else if let LogicalExpr::Prefix {
			expr: inner,
			..
		} = expr
		{
			Self::expr_contains_knn(inner)
		} else {
			false
		}
	}

	/// Collect all simple conditions from an AND tree.
	fn collect_conditions(&self, expr: &LogicalExpr, conditions: &mut Vec<SimpleCondition>) {
		if let LogicalExpr::Binary {
			left,
			op,
			right,
		} = expr
		{
			if matches!(op, BinaryOperator::And) {
				// Recurse into AND branches
				self.collect_conditions(left, conditions);
				self.collect_conditions(right, conditions);
			} else if matches!(op, BinaryOperator::Or) {
				// Don't collect from OR branches
			} else {
				// Try to extract a simple condition
				if let Some(cond) = self.extract_simple_condition(left, op, right) {
					conditions.push(cond);
				}
			}
		} else if let LogicalExpr::Prefix {
			op,
			expr: inner,
		} = expr && Self::is_truth_preserving_prefix(op, inner)
		{
			self.collect_conditions(inner, conditions);
		}
	}

	/// The bound operand `expr` denotes as the value side of a comparison
	/// with `op`: a literal, or the statement clock where the caller may read
	/// it and `op` is an equality or an ordering comparison
	/// ([`try_bound_operand`]).
	fn operand_of(&self, expr: &LogicalExpr, op: &BinaryOperator) -> Option<BoundOperand> {
		let operand = try_bound_operand(expr, self.clock_allowed && is_ordering_comparison(op))?;
		match operand.as_literal() {
			Some(value) if !keyed_as_compared(value, is_range_comparison(op)) => None,
			_ => Some(operand),
		}
	}

	/// Extract a simple condition (path op value) from a binary expression:
	/// one operand is a path over the row, the other a literal value or the
	/// statement clock.
	fn extract_simple_condition(
		&self,
		left: &LogicalExpr,
		op: &BinaryOperator,
		right: &LogicalExpr,
	) -> Option<SimpleCondition> {
		let (path, value, position) = match (left, right) {
			(lhs, rhs) if lhs.is_path() && !rhs.is_path() => {
				(lhs.clone(), self.operand_of(rhs, op)?, IdiomPosition::Left)
			}
			(lhs, rhs) if rhs.is_path() => {
				(rhs.clone(), self.operand_of(lhs, op)?, IdiomPosition::Right)
			}
			_ => return None,
		};

		// Normalise single-element `field IN [v]` to `field = v` so it can
		// participate in compound-prefix building. Without this, a query
		// like `a IN [1] AND b = 2` would lose the leading equality on the
		// compound index `(a, b)`.
		let (op, value) = if matches!(op, BinaryOperator::Inside) && position == IdiomPosition::Left
		{
			if let Some(Value::Array(arr)) = value.as_literal()
				&& arr.len() == 1
			{
				(BinaryOperator::Equal, BoundOperand::Literal(arr[0].clone()))
			} else {
				(op.clone(), value)
			}
		} else {
			(op.clone(), value)
		};

		Some(SimpleCondition {
			path,
			op,
			value,
			position,
		})
	}

	/// Analyze conditions to find compound index opportunities.
	///
	/// Collects leading equality conditions into a prefix, and optionally
	/// captures a single range condition on the next column after the
	/// equality prefix. This allows the index scan to narrow the key range
	/// (e.g. `city = 'london' AND age > 50` on index `(city, age)` scans
	/// only keys matching both conditions rather than all `city = 'london'`
	/// entries).
	fn analyze_compound_conditions(
		&self,
		conditions: &[SimpleCondition],
		candidates: &mut Vec<IndexCandidate>,
	) {
		// For each index, check if multiple columns are covered by conditions
		for (idx, ix_def) in self.indexes.iter().enumerate() {
			if ix_def.prepare_remove {
				continue;
			}

			// Only Idx and Uniq support compound access
			if !matches!(ix_def.index, Index::Idx | Index::Uniq) {
				continue;
			}

			// Need at least 2 columns for compound access
			if ix_def.cols.len() < 2 {
				continue;
			}

			// Try to match conditions to index columns in order.
			// The prefix collects leading equality conditions, and a
			// containment condition on a fanning column pins that column to
			// one element. After the prefix, a single range condition on the
			// next column is captured and encoded into the compound key
			// range so the index scan is narrowed at the storage level.
			let mut prefix_values: Vec<BoundOperand> = Vec::new();
			let mut range_condition: Option<(BinaryOperator, BoundOperand)> = None;
			let mut upper_condition: Option<(BinaryOperator, BoundOperand)> = None;

			for (col_idx, col) in ix_def.cols.iter().enumerate() {
				// Prefer an equality condition that pins this column so that an
				// earlier non-equality conjunct on the same column (such as
				// `b != 99` or a range bound) does not prematurely terminate
				// prefix building or shadow usable range bounds. No key is a
				// whole array (see `match_operator_to_access`), so a comparison
				// to one cannot pin a column.
				let equality_cond = conditions.iter().find(|c| {
					path_matches_column(&c.path, col, self.key.as_deref())
						&& matches!(c.op, BinaryOperator::Equal | BinaryOperator::ExactEqual)
						&& !matches!(c.value.as_literal(), Some(Value::Array(_)))
				});

				if let Some(cond) = equality_cond {
					prefix_values.push(cond.value.clone());
					continue;
				}

				// `path_matches_column` vetoes any column holding `*`, so a
				// fanning column is reached only through the containment
				// matcher. `field CONTAINS v` / `v INSIDE field` hold
				// exactly when the column has an element equal to `v`,
				// which is exactly what a key carrying `v` at this position
				// means — so the value extends the prefix the way an
				// equality does.
				//
				// Pinning a fanning column also removes its fan-out: the
				// record's keys under this prefix all carry `v` here, so
				// only the columns the prefix does not reach can still
				// multiply them. Whether any do is what `access_fans_out`
				// asks when it decides to collapse the scan, so there is
				// nothing for this loop to withhold — an index whose
				// columns all fan out is served exactly when a containment
				// leaf pins each of them.
				let containment_cond = conditions.iter().find(|c| {
					matches!(
						(&c.op, c.position),
						(BinaryOperator::Contain, IdiomPosition::Left)
							| (BinaryOperator::Inside, IdiomPosition::Right)
					) && !matches!(c.value.as_literal(), Some(Value::Array(_)))
						&& self.seeks_containment(idx, col_idx, &c.path)
				});

				if let Some(cond) = containment_cond {
					prefix_values.push(cond.value.clone());
					continue;
				}

				// Non-equality (range) -- capture the range condition on this
				// column and stop. The range narrows the scan beyond the
				// equality prefix.
				if let Some(op) = conditions
					.iter()
					.filter(|c| {
						path_matches_column(&c.path, col, self.key.as_deref())
							&& !matches!(c.value.as_literal(), Some(Value::Array(_)))
					})
					.find_map(|c| normalize_range_op(&c.op, c.position))
				{
					let cond_is_lower =
						matches!(op, BinaryOperator::MoreThan | BinaryOperator::MoreThanEqual);
					// A conjunct bounding the same column from the other side
					// closes the range, so that the scan stops at the far end
					// rather than reading every key past it. Of several literal
					// bounds on one side the tightest is kept, so the scanned
					// interval does not depend on the order of the conjuncts. A
					// bound comparing with an array bounds nothing, since no key
					// is a whole array. Nor does a bound naming a number the index
					// keys apart from numbers equal to it
					// ([`number_keyed_as_compared`]), the first conjunct's
					// included: its key could end the scan before records the
					// comparison accepts, which no filter recovers. Such a bound
					// is judged by the residual filter alone, and a column left
					// with no bound is scanned whole under the prefix.
					let normalised = spells_normalised_decimal_keys(ix_def.format_version);
					let mut lower = None;
					let mut upper = None;
					for c in conditions
						.iter()
						.filter(|c| path_matches_column(&c.path, col, self.key.as_deref()))
					{
						let Some(op) = normalize_range_op(&c.op, c.position) else {
							continue;
						};
						if matches!(c.value.as_literal(), Some(Value::Array(_))) {
							continue;
						}
						if matches!(
							c.value.as_literal(),
							Some(Value::Number(n)) if !number_keyed_as_compared(n, normalised)
						) {
							continue;
						}
						let is_lower =
							matches!(op, BinaryOperator::MoreThan | BinaryOperator::MoreThanEqual);
						let side = if is_lower {
							&mut lower
						} else {
							&mut upper
						};
						if side
							.as_ref()
							.is_none_or(|kept| bound_is_tighter(is_lower, (&op, &c.value), kept))
						{
							*side = Some((op, &c.value));
						}
					}
					// Bounds that admit nothing leave the scan to the tightest bound
					// on the side of the first conjunct, rather than a reversed key
					// range; the other side is judged by the residual filter. A
					// column bounded on one side alone is scanned from that bound.
					let closes = match (&lower, &upper) {
						(Some((lower_op, lo)), Some((upper_op, up))) => {
							match (lo.as_literal(), up.as_literal()) {
								(Some(lo), Some(up)) => {
									lo < up
										|| (lo == up
											&& matches!(lower_op, BinaryOperator::MoreThanEqual)
											&& matches!(upper_op, BinaryOperator::LessThanEqual))
								}
								_ => false,
							}
						}
						_ => false,
					};
					let (preferred, other) = if closes || cond_is_lower {
						(&lower, &upper)
					} else {
						(&upper, &lower)
					};
					range_condition = preferred
						.as_ref()
						.or(other.as_ref())
						.map(|(op, value)| (op.clone(), (*value).clone()));
					if closes && let Some((op, value)) = upper {
						upper_condition = Some((op, value.clone()));
					}
				} else if let Some(cond) = conditions.iter().find(|c| {
					path_matches_column(&c.path, col, self.key.as_deref())
						&& matches!(c.op, BinaryOperator::NotEqual)
						&& matches!(c.value.as_literal(), Some(Value::None))
				}) {
					// `field != NONE`. NONE sorts first in BTree key
					// ordering, so this is equivalent to `field > NONE`
					// and yields every NULL and concrete value
					// (matching the filter semantics).
					//
					// `field != NULL` is intentionally not handled here.
					// NONE sorts before NULL, so an exclusive `> NULL`
					// scan would silently drop NONE rows — and
					// `NONE != NULL` is true under SurrealQL semantics.
					// Leaving this branch unmatched lets the filter
					// pipeline apply the predicate correctly.
					range_condition = Some((BinaryOperator::MoreThan, cond.value.clone()));
				}
				break;
			}

			// Create compound candidate if we have at least 2 equality columns,
			// or at least 1 equality column with a range on the next column.
			if prefix_values.len() >= 2 || (!prefix_values.is_empty() && range_condition.is_some())
			{
				let access = BTreeAccess::Compound {
					prefix: prefix_values,
					range: range_condition,
					upper: upper_condition,
				};

				let index_ref = self.index_ref(idx);
				candidates.push(IndexCandidate::new(index_ref, access));
			}
		}
	}

	/// Merge half-bounded range candidates on the same index into bounded ranges.
	///
	/// When the WHERE clause contains `field > A AND field < B`, the analyzer
	/// produces two separate half-bounded Range candidates for the same index.
	/// This pass merges them into a single `Range { from: >A, to: <B }` which
	/// narrows the index scan and avoids scanning rows only to filter them out.
	fn merge_range_candidates(&self, candidates: &mut Vec<IndexCandidate>) {
		// Sort by index so candidates on the same index are adjacent
		candidates.sort_by_key(|c| c.index_ref.idx);

		let mut i = 0;
		while i < candidates.len() {
			let mut j = i + 1;
			while j < candidates.len() && candidates[j].index_ref.idx == candidates[i].index_ref.idx
			{
				if let BTreeAccess::Range {
					range: a,
				} = &candidates[i].access
					&& let BTreeAccess::Range {
						range: b,
					} = &candidates[j].access
				{
					// Two bounds of one side are compared to keep the tighter;
					// where one reads the clock they cannot be, so the two
					// candidates stay apart and the better one is chosen.
					match a.intersect(b) {
						Some(range) if range.is_empty() => {
							candidates[i].empty = true;
							candidates.remove(j);
						}
						Some(range) => {
							let covers_order =
								candidates[i].covers_order || candidates[j].covers_order;
							candidates[i].access = BTreeAccess::Range {
								range,
							};
							candidates[i].covers_order = covers_order;
							candidates.remove(j);
						}
						None => j += 1,
					}
				} else {
					j += 1;
				}
			}
			i += 1;
		}
	}

	/// Remove duplicate candidates, preferring compound over simple.
	fn deduplicate_candidates(&self, candidates: &mut Vec<IndexCandidate>) {
		// Sort by index and score (higher score first)
		candidates.sort_by(|a, b| match a.index_ref.idx.cmp(&b.index_ref.idx) {
			std::cmp::Ordering::Equal => b.score().cmp(&a.score()),
			other @ (std::cmp::Ordering::Less | std::cmp::Ordering::Greater) => other,
		});

		// Keep only the best candidate per index
		candidates.dedup_by(|a, b| a.index_ref.idx == b.index_ref.idx);
	}

	/// Analyze a single expression for index opportunities.
	fn analyze_condition(&self, expr: &LogicalExpr, candidates: &mut Vec<IndexCandidate>) {
		if let LogicalExpr::Binary {
			left,
			op,
			right,
		} = expr
		{
			// Handle AND/OR by recursing into children
			match op {
				BinaryOperator::And => {
					// For AND, both sides contribute candidates independently
					self.analyze_condition(left, candidates);
					self.analyze_condition(right, candidates);
				}
				BinaryOperator::Or => {
					// For OR, we need all branches to use the same index
					// This is more complex. OR conditions are not indexed here; the OR union
					// analysis covers them.
				}
				// MATCHES operator for full-text search
				BinaryOperator::Matches(mo) => {
					self.try_match_fulltext(left, mo, right, candidates);
				}
				// KNN operator for vector search
				BinaryOperator::NearestNeighbor(nn) => {
					self.try_match_knn(left, right, nn, candidates);
				}
				BinaryOperator::Contain | BinaryOperator::Inside => {
					self.try_match_containment(left, op, right, candidates);
					self.try_match_comparison(left, op, right, candidates);
				}
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
				| BinaryOperator::NullCoalescing
				| BinaryOperator::TenaryCondition
				| BinaryOperator::LessThan
				| BinaryOperator::LessThanEqual
				| BinaryOperator::MoreThan
				| BinaryOperator::MoreThanEqual
				| BinaryOperator::NotContain
				| BinaryOperator::ContainAll
				| BinaryOperator::ContainAny
				| BinaryOperator::ContainNone
				| BinaryOperator::NotInside
				| BinaryOperator::AllInside
				| BinaryOperator::AnyInside
				| BinaryOperator::NoneInside
				| BinaryOperator::Outside
				| BinaryOperator::Intersects
				| BinaryOperator::Range
				| BinaryOperator::RangeInclusive
				| BinaryOperator::RangeSkip
				| BinaryOperator::RangeSkipInclusive => {
					self.try_match_comparison(left, op, right, candidates);
				}
			}
		} else if let LogicalExpr::Call(call) = expr {
			// A prefix test names a contiguous stretch of the key space.
			self.try_match_prefix_call(call, candidates);
		} else if let LogicalExpr::Prefix {
			op,
			expr: inner,
		} = expr && !matches!(op, PrefixOperator::Not)
		{
			// Nested expression in parentheses (but NOT negation).
			// Do NOT recurse into NOT — negated predicates invert
			// the result set and index candidates would be wrong.
			self.analyze_condition(inner, candidates);
		}
	}

	/// Try to match a comparison expression to an index.
	fn try_match_comparison(
		&self,
		left: &LogicalExpr,
		op: &BinaryOperator,
		right: &LogicalExpr,
		candidates: &mut Vec<IndexCandidate>,
	) {
		// Extract the field path and value from the comparison. Parameters
		// are folded into literals before the analyzer runs (binding's
		// `logical::params::resolve_params`, for a read's predicate and a
		// mutation's `WHERE` alike). A bare
		// `LogicalExpr::Param` reaching the analyzer means the param could not
		// be resolved at plan time, so it cannot be pushed down to the index
		// and the comparison falls through to the table-scan path.
		let (path, value, position) = match (left, right) {
			(lhs, rhs) if lhs.is_path() && !rhs.is_path() => match self.operand_of(rhs, op) {
				Some(value) => (lhs, value, IdiomPosition::Left),
				None => return,
			},
			(lhs, rhs) if rhs.is_path() => match self.operand_of(lhs, op) {
				Some(value) => (rhs, value, IdiomPosition::Right),
				None => return,
			},
			_ => return,
		};

		// Find indexes that match this path
		for (idx, ix_def) in self.indexes.iter().enumerate() {
			// Skip indexes being removed
			if ix_def.prepare_remove {
				continue;
			}

			// Check if the path matches the first column of the index
			if let Some(first_col) = ix_def.cols.first()
				&& path_matches_column(path, first_col, self.key.as_deref())
				&& let Some(access) =
					self.match_operator_to_access(op, &value, position, &ix_def.index)
			{
				// For compound indexes (>1 column), a single-column equality
				// match on the first column must use a prefix scan rather
				// than a point lookup, because the index key includes all
				// columns.  E.g. WHERE a = 1 on INDEX (a, b) must scan the
				// prefix [1] to find all (1, *) entries.
				let access = if ix_def.cols.len() > 1 {
					if let BTreeAccess::Equality(v) = access {
						BTreeAccess::Compound {
							prefix: vec![v],
							range: None,
							upper: None,
						}
					} else {
						access
					}
				} else {
					access
				};

				let index_ref = self.index_ref(idx);
				candidates.push(IndexCandidate::new(index_ref, access));
			}
		}
	}

	/// Convert an operator and value to a BTreeAccess pattern.
	fn match_operator_to_access(
		&self,
		op: &BinaryOperator,
		value: &BoundOperand,
		position: IdiomPosition,
		index_type: &Index,
	) -> Option<BTreeAccess> {
		// Only Idx and Uniq support these access patterns
		if !matches!(index_type, Index::Idx | Index::Uniq) {
			return None;
		}
		// An index stores an array one entry per element, so no key is a
		// whole array: a comparison to one is not answered by a key or a
		// range of keys. `IN` reads the array's elements as its keys, so an
		// element that is itself an array is no key either.
		if let Some(Value::Array(arr)) = value.as_literal()
			&& (!matches!(op, BinaryOperator::Inside)
				|| arr.iter().any(|v| matches!(v, Value::Array(_))))
		{
			return None;
		}

		let range = |start: Bound<BoundOperand>, end: Bound<BoundOperand>| {
			Some(BTreeAccess::Range {
				range: OperandRange {
					start,
					end,
					point_filter: None,
				},
			})
		};
		match (op, position) {
			// Equality
			(BinaryOperator::Equal | BinaryOperator::ExactEqual, _) => {
				Some(BTreeAccess::Equality(value.clone()))
			}

			// Less than (field < value)
			// More then (value > value)
			(BinaryOperator::LessThan, IdiomPosition::Left)
			| (BinaryOperator::MoreThan, IdiomPosition::Right) => {
				range(Bound::Unbounded, Bound::Excluded(value.clone()))
			}

			// Less than or equal (field <= value)
			// More than or equal (value >= field)
			(BinaryOperator::LessThanEqual, IdiomPosition::Left)
			| (BinaryOperator::MoreThanEqual, IdiomPosition::Right) => {
				range(Bound::Unbounded, Bound::Included(value.clone()))
			}

			// Greater than (field > value)
			// Less than (value < field)
			(BinaryOperator::MoreThan, IdiomPosition::Left)
			| (BinaryOperator::LessThan, IdiomPosition::Right) => {
				range(Bound::Excluded(value.clone()), Bound::Unbounded)
			}

			// Greater than or equal (field >= value)
			// Less than or equal (value <= field)
			(BinaryOperator::MoreThanEqual, IdiomPosition::Left)
			| (BinaryOperator::LessThanEqual, IdiomPosition::Right) => {
				range(Bound::Included(value.clone()), Bound::Unbounded)
			}
			// IN clause (field IN [values])
			(BinaryOperator::Inside, IdiomPosition::Left) => {
				let lit = value.as_literal()?;
				if let Value::Array(arr) = lit
					&& arr.len() == 1
				{
					// Single-element array: treat as equality (field IN [v] → field = v)
					Some(BTreeAccess::literal_equality(arr[0].clone()))
				} else if let Value::Range(r) = lit {
					// A range literal states the bounds directly, so it is
					// the same access as the two comparisons that spell it
					// out: `field IN 1..9` reads the keys `>= 1` and `< 9`.
					// The literal's own bounds carry which end is open, so
					// every spelling (`..`, `..=`, `>..`, `>..=`) maps
					// without a case of its own.
					Some(BTreeAccess::literal_range((**r).clone()))
				} else {
					None
				}
			}

			// `field != NONE` — NONE sorts first in BTree key ordering,
			// so this is exactly `field > NONE` (yielding every NULL and
			// every concrete value, matching the filter semantics).
			//
			// `field != NULL` is intentionally NOT handled. NONE sorts
			// before NULL, so an exclusive `> NULL` range scan silently
			// drops rows where the field is NONE — which contradicts
			// SurrealQL filter semantics, where `NONE != NULL` is `true`.
			// The access-path model cannot express the union
			// `< NULL OR > NULL`, so this case falls through to the table-scan
			// + filter path, which handles `!= NULL` correctly.
			(BinaryOperator::NotEqual, _) if matches!(value.as_literal(), Some(Value::None)) => {
				range(Bound::Excluded(value.clone()), Bound::Unbounded)
			}

			_ => None,
		}
	}

	/// Try to match `string::starts_with(<indexed path>, '<literal>')` to a
	/// b-tree range.
	///
	/// The strings carrying a prefix are exactly the strings at or after it
	/// and before its successor, and the key encoding orders strings the same
	/// way the predicate compares them, so the prefix is one contiguous range.
	///
	/// The upper bound is dropped when no successor can be formed — an empty
	/// prefix, or one ending at the last scalar value — which widens the range
	/// rather than narrowing it wrongly.
	///
	/// The bounded and the open form both hold exactly the strings carrying
	/// the prefix, so the range decides the answer on its own; the call is
	/// left in the residual because nothing here strips it, not because the
	/// scan needs checking. A sibling conjunct on a later column is the one
	/// thing the residual still carries.
	fn try_match_prefix_call(&self, call: &LogicalCall, candidates: &mut Vec<IndexCandidate>) {
		let Function::Normal(name) = &call.receiver else {
			return;
		};
		if name != "string::starts_with" {
			return;
		}
		let [path, literal] = call.args.as_slice() else {
			return;
		};
		if !path.is_path() {
			return;
		}
		let Some(Value::String(prefix)) = try_literal_to_value(literal) else {
			return;
		};
		if prefix.as_str().is_empty() {
			return;
		}
		let end = match next_string_after_prefix(prefix.as_str()) {
			Some(next) => Bound::Excluded(Value::String(next.into())),
			None => Bound::Unbounded,
		};
		let access = BTreeAccess::literal_range(Range {
			start: Bound::Included(Value::String(prefix)),
			end,
		});
		for (idx, ix_def) in self.indexes.iter().enumerate() {
			if ix_def.prepare_remove {
				continue;
			}
			if !matches!(ix_def.index, Index::Idx | Index::Uniq) {
				continue;
			}
			if let Some(With::Index(names)) = self.with_hints
				&& !names.iter().any(|n| n.as_str() == ix_def.name.as_str())
			{
				continue;
			}
			if let Some(col) = ix_def.cols.first()
				&& path_matches_column(path, col, self.key.as_deref())
			{
				let index_ref = self.index_ref(idx);
				candidates.push(IndexCandidate::new(index_ref, access.clone()));
			}
		}
	}

	/// Try to match a containment expression to an array-element index.
	///
	/// Handles single-value containment:
	/// - `field CONTAINS scalar` -> lookup on a `field[*]` (or `field.*`) index
	/// - `scalar INSIDE field`   -> lookup on a `field[*]` (or `field.*`) index
	///
	/// For single-column indexes the access is `Equality(scalar)`; for compound
	/// indexes whose leading column is the matched array-element flatten, the
	/// access is `Compound { prefix: [scalar], range: None }` so the iterator
	/// walks the leading-prefix range and trailing columns remain available
	/// for ORDER BY pushdown via `index_covers_ordering`.
	fn try_match_containment(
		&self,
		left: &LogicalExpr,
		op: &BinaryOperator,
		right: &LogicalExpr,
		candidates: &mut Vec<IndexCandidate>,
	) {
		let (path, value) = if let BinaryOperator::Contain = op {
			if left.is_path()
				&& let Some(v) = index_literal(right)
			{
				(left, v)
			} else {
				return;
			}
		} else if let BinaryOperator::Inside = op {
			if right.is_path()
				&& let Some(v) = index_literal(left)
			{
				(right, v)
			} else {
				return;
			}
		} else {
			return;
		};

		for (idx, ix_def) in self.indexes.iter().enumerate() {
			if ix_def.prepare_remove {
				continue;
			}
			if !matches!(ix_def.index, Index::Idx | Index::Uniq) {
				continue;
			}
			if self.seeks_containment(idx, 0, path) {
				// Compound indexes need a prefix scan rather than a point
				// lookup, because the on-disk key includes all columns. The
				// trailing columns can then be used by `index_covers_ordering`
				// to satisfy ORDER BY without a post-iteration sort. Mirrors
				// the equality rewrite in `try_match_comparison`.
				let access = if ix_def.cols.len() > 1 {
					BTreeAccess::literal_compound(vec![value.clone()], None)
				} else {
					BTreeAccess::literal_equality(value.clone())
				};
				let index_ref = self.index_ref(idx);
				candidates.push(IndexCandidate::new(index_ref, access));
			}
		}
	}

	/// Try to match a MATCHES expression to a full-text index.
	fn try_match_fulltext(
		&self,
		left: &LogicalExpr,
		operator: &MatchesOperator,
		right: &LogicalExpr,
		candidates: &mut Vec<IndexCandidate>,
	) {
		// Extract the field path from the left side and the query string
		// from the right side
		let (path, query) = match try_literal_to_value(right) {
			Some(Value::String(s)) if left.is_path() => (left, s),
			_ => return,
		};

		// Find full-text indexes that match this path
		for (idx, ix_def) in self.indexes.iter().enumerate() {
			if ix_def.prepare_remove {
				continue;
			}

			// Only FullText indexes support MATCHES
			if !matches!(ix_def.index, Index::FullText(_)) {
				continue;
			}

			// The probe a full-text scan answers is stripped from the predicate
			// by matching its column as written, so it is matched as written here.
			if let Some(first_col) = ix_def.cols.first()
				&& path_matches_column(path, first_col, None)
			{
				let index_ref = self.index_ref(idx);
				candidates.push(IndexCandidate::new(
					index_ref,
					BTreeAccess::FullText {
						query: query.as_str().to_owned(),
						operator: operator.clone(),
					},
				));
			}
		}
	}

	/// Try to match a KNN expression to an ANN index.
	fn try_match_knn(
		&self,
		left: &LogicalExpr,
		right: &LogicalExpr,
		nn: &NearestNeighbor,
		candidates: &mut Vec<IndexCandidate>,
	) {
		// Approximate uses the ANN operator directly; K(k,d) uses an ANN index
		// when the distance matches the index definition.
		let (k, user_ef, required_distance) = match nn {
			NearestNeighbor::Approximate(k, ef) => (*k, Some(*ef), None),
			NearestNeighbor::K(k, d) => (*k, None, Some(crate::catalog::Distance::from(d.clone()))),
			_ => return,
		};
		self.analyze_knn_condition(left, right, k, user_ef, required_distance.as_ref(), candidates);
	}

	fn analyze_knn_condition(
		&self,
		left: &LogicalExpr,
		right: &LogicalExpr,
		k: u32,
		user_ef: Option<u32>,
		required_distance: Option<&crate::catalog::Distance>,
		candidates: &mut Vec<IndexCandidate>,
	) {
		// Extract the field path from the left side
		if !left.is_path() {
			return;
		}
		let path = left;

		// A parameter the planner resolved is that vector ([`KnnQueryVectors`]),
		// empty where it is known only when the statement runs. Any other
		// right-hand side is read exactly as brute force reads it, so a value
		// either refuses when it runs (one that is not `array<number>`) raises
		// the same error in both: a vector computed when the statement runs is
		// the empty vector here, and the scan takes the operator's operand as
		// its query. One that reads the row being scanned matches no index:
		// the planner refuses it before an access path is chosen.
		let resolved = if let LogicalExpr::Param(param) = right {
			self.knn_query_vectors.and_then(|v| v.get(param.as_str()))
		} else {
			None
		};
		let vector = match resolved {
			Some(vector) => {
				if required_distance.is_some()
					&& !vector.is_empty()
					&& !crate::exec::planner::util::is_rankable_query_vector(vector)
				{
					return;
				}
				Arc::clone(vector)
			}
			None => match classify_knn_vector(right, self.registry) {
				KnnVectorClass::Vector(KnnQueryVector::Literal(vector)) => {
					if required_distance.is_some()
						&& !crate::exec::planner::util::is_rankable_query_vector(&vector)
					{
						return;
					}
					vector
				}
				KnnVectorClass::Vector(KnnQueryVector::Deferred(_)) => {
					if required_distance.is_some() && try_literal_to_value(right).is_some() {
						return;
					}
					Arc::from(Vec::new())
				}
				KnnVectorClass::ReadsRow => return,
			},
		};

		// Find ANN indexes that match this path
		for (idx, ix_def) in self.indexes.iter().enumerate() {
			if ix_def.prepare_remove {
				continue;
			}

			let ef = match &ix_def.index {
				Index::Hnsw(hnsw) => {
					if let Some(d) = required_distance
						&& (*d != hnsw.distance
							|| (!vector.is_empty() && vector.len() != hnsw.dimension as usize))
					{
						continue;
					}
					user_ef.unwrap_or_else(|| k.max(hnsw.ef_construction as u32))
				}
				Index::DiskAnn(diskann) => {
					if let Some(d) = required_distance
						&& (*d != diskann.distance
							|| (!vector.is_empty() && vector.len() != diskann.dimension as usize))
					{
						continue;
					}
					user_ef.unwrap_or_else(|| k.max(diskann.l_build as u32))
				}
				Index::Idx
				| Index::Uniq
				| Index::FullText(_)
				| Index::Count(_)
				| Index::Spatial => {
					continue;
				}
			};

			// An ANN index is matched by its column as written, as the KNN
			// operator's other consumers match it.
			if let Some(first_col) = ix_def.cols.first()
				&& path_matches_column(path, first_col, None)
			{
				let index_ref = self.index_ref(idx);
				candidates.push(IndexCandidate::new(
					index_ref,
					BTreeAccess::Knn {
						vector: Arc::clone(&vector),
						k,
						ef,
					},
				));
			}
		}
	}

	/// Analyze ORDER BY for index-ordered scan opportunities.
	///
	/// Delegates to the planner's authoritative
	/// [`crate::exec::planner::util::optimization::index_covers_ordering`]
	/// helper, which compares the index's effective `SortProperty` vector
	/// against the requested ORDER BY (accounting for equality-pinned
	/// prefix columns and the implicit trailing `id` of non-unique
	/// indexes). The analyzer tries both forward and backward scans —
	/// `adjust_direction_for_order` will pick the correct direction later
	/// at plan time.
	///
	/// Two passes:
	/// - Existing candidates whose access pattern can satisfy ORDER BY in some direction are tagged
	///   with `covers_order = true`.
	/// - For each index where no candidate exists yet, synthesize a full-range scan candidate when
	///   that scan would satisfy ORDER BY.
	fn analyze_order(&self, ordering: &LogicalOrdering, candidates: &mut Vec<IndexCandidate>) {
		use crate::exec::planner::util::index_covers_ordering;

		// Pass 0 — `ORDER BY vector::distance::<metric>(field, query_vec) [ASC] LIMIT k`:
		// When an HNSW or DiskANN index known to hold every record exists on `field` with a
		// matching distance metric, satisfy the top-k vector ordering via KnnScan (and compose with
		// any WHERE pre-filters). The scan chooses the k records by the index's own ranking, which
		// is approximate: the graph search may miss a nearer record, and the index measures vectors
		// converted to its `TYPE`, so records near-tied under the record values may swap. The sort
		// kept above the scan orders the k it returns by the record values. A query that needs the
		// exact top k writes `WITH NOINDEX`.
		if let Some(k) = self.limit_k.filter(|&k| k > 0)
			&& !candidates.iter().any(|c| matches!(c.access, BTreeAccess::Knn { .. }))
			&& let LogicalOrdering::Order(keys) = ordering
			&& let [key] = keys.as_slice()
			&& key.direction
			&& !key.collate
			&& !key.numeric
			&& let LogicalExpr::Call(call) = &key.value
			&& let crate::expr::Function::Normal(name) = &call.receiver
		{
			let parsed = match (name.as_str(), call.args.as_slice()) {
				("vector::distance::chebyshev", [a, b]) => {
					Some((a, b, crate::catalog::Distance::Chebyshev))
				}
				("vector::distance::cosine", [a, b]) => {
					Some((a, b, crate::catalog::Distance::Cosine))
				}
				("vector::distance::euclidean", [a, b]) => {
					Some((a, b, crate::catalog::Distance::Euclidean))
				}
				("vector::distance::hamming", [a, b]) => {
					Some((a, b, crate::catalog::Distance::Hamming))
				}
				("vector::distance::manhattan", [a, b]) => {
					Some((a, b, crate::catalog::Distance::Manhattan))
				}
				("vector::distance::minkowski", [a, b, p]) => {
					if let Some(Value::Number(n)) = try_literal_to_value(p) {
						Some((a, b, crate::catalog::Distance::Minkowski(n)))
					} else {
						None
					}
				}
				_ => None,
			};
			if let Some((arg0, arg1, distance)) = parsed {
				let hint_allows_ann = match self.with_hints {
					None => true,
					Some(With::NoIndex) => false,
					Some(With::Index(names)) => self.indexes.iter().any(|ix| {
						!ix.prepare_remove
							&& matches!(ix.index, Index::Hnsw(_) | Index::DiskAnn(_))
							&& names.iter().any(|n| n.as_str() == ix.name.as_str())
					}),
				};
				if hint_allows_ann {
					for (field_expr, vec_expr) in [(arg0, arg1), (arg1, arg0)] {
						let before = candidates.len();
						self.analyze_knn_condition(
							field_expr,
							vec_expr,
							k,
							None,
							Some(&distance),
							candidates,
						);
						// The sort orders every record, and the vector of a record an index holds
						// no entry for (`NONE` or `NULL`) raises in it: only an index known to hold
						// every record answers it.
						let hinted = |name: &str| match self.with_hints {
							Some(With::Index(names)) => names.iter().any(|n| n.as_str() == name),
							_ => true,
						};
						let mut i = before;
						while i < candidates.len() {
							let index = &candidates[i].index_ref;
							if hinted(index.name.as_str()) && self.is_complete_vector(index.idx) {
								i += 1;
							} else {
								candidates.remove(i);
							}
						}
						if candidates.len() > before {
							for candidate in &mut candidates[before..] {
								candidate.covers_order = true;
							}
							break;
						}
					}
				}
			}
		}

		// Pass 1 — mark existing candidates that the authoritative check
		// proves can satisfy ORDER BY in either direction.
		for candidate in candidates.iter_mut() {
			if covers_ordering_either_direction(
				&candidate.index_ref,
				&candidate.access,
				ordering,
				index_covers_ordering,
			) {
				candidate.covers_order = true;
			}
		}

		// Pass 2 — for indexes that have no candidate yet, synthesize a
		// full-range scan if that scan covers ORDER BY. This is how
		// ORDER BY can use an index even without a WHERE clause.
		for (idx, ix_def) in self.indexes.iter().enumerate() {
			if ix_def.prepare_remove || !ix_def.index.supports_order() {
				continue;
			}
			if candidates.iter().any(|c| c.index_ref.idx == idx) {
				continue;
			}
			let index_ref = self.index_ref(idx);
			let full_range = BTreeAccess::Range {
				range: OperandRange::unbounded(),
			};
			if covers_ordering_either_direction(
				&index_ref,
				&full_range,
				ordering,
				index_covers_ordering,
			) {
				let mut candidate = IndexCandidate::new(index_ref, full_range);
				candidate.covers_order = true;
				candidates.push(candidate);
			}
		}
	}
}

/// Returns `true` if either a forward or backward scan of the index can
/// satisfy the requested ORDER BY. The analyzer cannot know the final
/// scan direction (`adjust_direction_for_order` decides later) so we
/// must accept both.
fn covers_ordering_either_direction<F>(
	index_ref: &IndexRef,
	access: &BTreeAccess,
	ordering: &LogicalOrdering,
	covers: F,
) -> bool
where
	F: Fn(&IndexRef, &BTreeAccess, Direction, &LogicalOrdering) -> bool,
{
	covers(index_ref, access, Direction::Forward, ordering)
		|| covers(index_ref, access, Direction::Backward, ordering)
}

/// A candidate index access path.
#[derive(Debug, Clone)]
pub struct IndexCandidate {
	/// Reference to the index definition
	pub index_ref: IndexRef,
	/// How to access the index
	pub access: BTreeAccess,
	/// Whether this index can satisfy ORDER BY
	pub covers_order: bool,
	/// Set when the analyzer can prove no row can satisfy the predicate
	/// (e.g. contradictory range merge). Causes `to_access_path` to emit
	/// [`AccessPath::EmptyScan`] regardless of `access`.
	pub empty: bool,
}

impl IndexCandidate {
	/// Construct a new candidate that is not (yet) marked empty and does
	/// not yet cover ORDER BY. Used by the analyzer to keep call sites
	/// short — `covers_order` and `empty` are set later as the analysis
	/// progresses.
	pub fn new(index_ref: IndexRef, access: BTreeAccess) -> Self {
		Self {
			index_ref,
			access,
			covers_order: false,
			empty: false,
		}
	}

	/// Score this candidate for comparison (higher is better).
	///
	/// The weights below are heuristic — without table statistics there is
	/// no true cost. They are ordered by expected row count from most
	/// selective (point lookups on a unique key) to least selective (full
	/// index scan covering only ORDER BY). The intent is that *kind*
	/// dominates *rank* within a kind, and that two candidates of the same
	/// shape on different indexes tie-break deterministically downstream
	/// (via `index_ref.idx`).
	///
	/// Roughly:
	///
	/// | Access pattern                          | Score | Why                                  |
	/// |-----------------------------------------|------:|--------------------------------------|
	/// | Provably empty                          |  MAX  | No rows to read — zero cost          |
	/// | Unique equality, non-exempt key         | 1_000 | Returns at most one row              |
	/// | Non-unique, or exempt key               |   500 | Returns a small bucket               |
	/// | Compound prefix len N (capped at 6)     | 400+50N | Each pinned column shrinks scan    |
	/// | Compound prefix + range on next col     |   +25 | Slight further narrowing             |
	/// | FullText / KNN                          |   800 | Specialised; only applies for MATCHES/<\|N\|> |
	/// | Bounded range                           |   300 | Both sides bounded                   |
	/// | Half-bounded range                      |   200 | One side bounded                     |
	/// | Full range (covers ORDER BY only)       |    50 | Sort-elim worth more than table scan |
	/// | Plus: `covers_order` bonus              |  +100 | Avoids in-memory Sort                |
	///
	/// An equality is scored as unique when its key takes part in uniqueness
	/// (see [`IndexAnalyzer::candidate_returns_at_most_one_row`]).
	///
	/// The compound prefix bonus is capped so a 12-column prefix doesn't
	/// silently outscore a unique equality. Selectivity is not actually
	/// linear in column count — once the first column pins the key, later
	/// columns add diminishing returns.
	pub fn score(&self) -> u32 {
		// Compound-prefix bonus is capped to keep wide indexes from
		// dominating a unique equality (which returns at most one row).
		const MAX_COMPOUND_PREFIX_BONUS_COLS: u32 = 6;

		if self.empty {
			return u32::MAX;
		}

		let mut score = 0u32;
		match &self.access {
			BTreeAccess::Equality(_) => {
				score += if IndexAnalyzer::candidate_returns_at_most_one_row(self) {
					1000
				} else {
					500
				};
			}
			BTreeAccess::Compound {
				prefix,
				range,
				..
			} => {
				let len = (prefix.len() as u32).min(MAX_COMPOUND_PREFIX_BONUS_COLS);
				score += 400 + len * 50;
				if range.is_some() {
					score += 25;
				}
			}
			BTreeAccess::Range {
				range: OperandRange {
					start,
					end,
					..
				},
			} => {
				score += match (start, end) {
					(Bound::Unbounded, Bound::Unbounded) => 50,
					(_, Bound::Unbounded) | (Bound::Unbounded, _) => 200,
					(_, _) => 300,
				};
			}
			BTreeAccess::FullText {
				..
			} => {
				score += 800;
			}
			BTreeAccess::Knn {
				..
			} => {
				score += 800;
			}
		}

		if self.covers_order {
			score += 100;
		}

		score
	}

	/// [`Self::score`] for a statement that does or does not page its rows
	/// with a `LIMIT`.
	///
	/// Under a `LIMIT`, a candidate that delivers the `ORDER BY` lets the
	/// pipeline stop once the limit is met, where any other driver reads every
	/// row it lists and sorts them before the first is returned. Such a
	/// candidate then outranks a range over another index and a table scan,
	/// but not an equality on a key or a key prefix, whose bucket the rules
	/// take to be small enough to sort. A scan that reaches a record through
	/// several entries delivers no order whatever its columns, and earns
	/// nothing.
	pub fn rank(&self, limited: bool) -> u32 {
		/// Lifts an order-covering full-range scan (50 + 100) above a bounded
		/// range (300) and keeps it below a one-column compound prefix (450).
		const ORDER_UNDER_LIMIT_BONUS: u32 = 200;
		let score = self.score();
		if limited
			&& self.covers_order
			&& !self.empty
			&& !super::access_path::access_fans_out(&self.index_ref.cols, &self.access)
		{
			score.saturating_add(ORDER_UNDER_LIMIT_BONUS)
		} else {
			score
		}
	}

	/// Whether this candidate is chosen over a key range over the components
	/// of the table's record id ([`AccessPath::KeyRange`]) that pins `pinned`
	/// leading components to values, of which `delivers_order` says whether
	/// it delivers the `ORDER BY`. Under `limited` the statement pages its rows.
	///
	/// The scores above weigh index shapes against each other; a key range is
	/// weighed by the same intent, with no statistics:
	///
	/// - a provably empty candidate reads nothing, and wins;
	/// - a full-text or ANN candidate answers an operator nothing else answers, and wins;
	/// - a unique equality whose key takes part in uniqueness (see
	///   [`IndexAnalyzer::candidate_returns_at_most_one_row`]) returns at most one row, and wins;
	/// - an index pinning strictly more columns than the key range wins: each pinned column narrows
	///   the scan more than the key range's next one can;
	/// - at an equal count, under a `LIMIT`, an index that covers the `ORDER BY` wins over a key
	///   range that does not deliver it, since it can stop once the page is full;
	/// - otherwise the key range wins: it reads each record once, from the record keys themselves,
	///   with no index entry to follow.
	pub(crate) fn outranks_key_range(
		&self,
		pinned: usize,
		delivers_order: bool,
		limited: bool,
	) -> bool {
		if self.empty {
			return true;
		}
		let equalities = match &self.access {
			BTreeAccess::FullText {
				..
			}
			| BTreeAccess::Knn {
				..
			} => return true,
			BTreeAccess::Equality(_) => 1,
			BTreeAccess::Compound {
				prefix,
				..
			} => prefix.len(),
			BTreeAccess::Range {
				..
			} => 0,
		};
		if IndexAnalyzer::candidate_returns_at_most_one_row(self) {
			return true;
		}
		if equalities > pinned {
			return true;
		}
		equalities == pinned
			&& limited
			&& self.covers_order
			&& !delivers_order
			&& !super::access_path::access_fans_out(&self.index_ref.cols, &self.access)
	}

	/// Convert this candidate to an AccessPath.
	pub fn to_access_path(&self, direction: Direction) -> AccessPath {
		if self.empty {
			return AccessPath::EmptyScan;
		}
		match &self.access {
			BTreeAccess::FullText {
				query,
				operator,
			} => AccessPath::FullTextSearch {
				index_ref: self.index_ref.clone(),
				query: query.clone(),
				operator: operator.clone(),
			},
			BTreeAccess::Knn {
				vector,
				k,
				ef,
			} => AccessPath::KnnSearch {
				index_ref: self.index_ref.clone(),
				vector: Arc::clone(vector),
				k: *k,
				ef: *ef,
				// The prefilter is a plan-level concern; the SELECT planner
				// attaches it after path selection.
				prefilter: None,
			},
			BTreeAccess::Equality(_)
			| BTreeAccess::Range {
				..
			}
			| BTreeAccess::Compound {
				..
			} => AccessPath::BTreeScan {
				index_ref: self.index_ref.clone(),
				access: self.access.clone(),
				direction,
			},
		}
	}
}

/// Position of the field path in a comparison expression.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IdiomPosition {
	/// The path is on the left: `field = value`
	Left,
	/// The path is on the right: `value = field`
	Right,
}

/// A simple condition extracted from the WHERE clause.
#[derive(Debug, Clone)]
struct SimpleCondition {
	/// The path operand, compared against index columns by
	/// [`path_matches_column`].
	path: LogicalExpr,
	op: BinaryOperator,
	value: BoundOperand,
	position: IdiomPosition,
}

/// Whether `op` compares by order rather than by equality.
fn is_range_comparison(op: &BinaryOperator) -> bool {
	matches!(
		op,
		BinaryOperator::LessThan
			| BinaryOperator::LessThanEqual
			| BinaryOperator::MoreThan
			| BinaryOperator::MoreThanEqual
	)
}

/// Whether `value` has an index key whose byte order agrees with value
/// comparison, for a range comparison when `range`.
///
/// - A regex has no key encoding at all (`storekey::Encode` rejects it) and compares with strings
///   by pattern matching, so it is never an index operand.
/// - A geometry encodes raw IEEE 754 `f64` coordinate bytes (`-0.0` and `+0.0` encode to distinct
///   bytes, and polygon ring closing/terminators differ from `Geometry`'s `PartialEq` and `Ord`),
///   so its index key does not agree with value equality or ordering.
/// - A record id with an array or object key encodes its elements in the record-id key format,
///   where a datetime is RFC 3339 text whose byte order is not chronological, so a key holding a
///   datetime is only an index operand for an equality. A key holding a `date`, `time`, `timestamp`
///   or `interval` is not an index operand at all: the record-id encoding of an interval carries
///   its exact triple after the normalised total, so equal intervals do not share a key there, and
///   the other three are left to the scan that reads them.
/// - A `date`, `time`, `timestamp` or `interval` is keyed where it compares: chronologically for
///   the first three, by the normalised total for an interval, which is also where `Ord` and `Eq`
///   compare it. It is an operand for an equality and for a range. A literal of another kind than
///   the indexed column compares by kind rank, and the key opens with the kind, so the index holds
///   it on the same side of the bound as the comparison does.
///
/// A value or record-id key is judged through everything it holds, the bounds
/// of a range in it included. A value nested in an array, set or object is
/// judged as the comparison judges the container, and a range (`IN a..b`) by
/// its bounds as a range comparison.
fn keyed_as_compared(value: &Value, range: bool) -> bool {
	fn element_keyed(value: &Value, range: bool) -> bool {
		match value {
			Value::Regex(_) | Value::Geometry(_) => false,
			Value::Date(_) | Value::Time(_) | Value::Timestamp(_) | Value::Interval(_) => false,
			Value::Datetime(_) => !range,
			Value::Array(items) => items.iter().all(|v| element_keyed(v, range)),
			Value::Set(items) => items.iter().all(|v| element_keyed(v, range)),
			Value::Object(entries) => entries.values().all(|v| element_keyed(v, range)),
			Value::RecordId(id) => key_keyed(&id.key, range),
			Value::Range(r) => [&r.start, &r.end].into_iter().all(|bound| match bound {
				Bound::Included(v) | Bound::Excluded(v) => element_keyed(v, range),
				Bound::Unbounded => true,
			}),
			Value::None
			| Value::Null
			| Value::Bool(_)
			| Value::Number(_)
			| Value::String(_)
			| Value::Duration(_)
			| Value::Uuid(_)
			| Value::Bytes(_)
			| Value::Table(_)
			| Value::File(_)
			| Value::Closure(_) => true,
		}
	}
	fn key_keyed(key: &RecordIdKey, range: bool) -> bool {
		match key {
			RecordIdKey::Array(items) => items.iter().all(|v| element_keyed(v, range)),
			RecordIdKey::Object(entries) => entries.values().all(|v| element_keyed(v, range)),
			RecordIdKey::Range(r) => [&r.start, &r.end].into_iter().all(|bound| match bound {
				Bound::Included(k) | Bound::Excluded(k) => key_keyed(k, range),
				Bound::Unbounded => true,
			}),
			RecordIdKey::Number(_) | RecordIdKey::String(_) | RecordIdKey::Uuid(_) => true,
		}
	}
	match value {
		Value::Regex(_) | Value::Geometry(_) => false,
		Value::RecordId(id) => key_keyed(&id.key, range),
		Value::Array(items) => items.iter().all(|v| keyed_as_compared(v, range)),
		Value::Set(items) => items.iter().all(|v| keyed_as_compared(v, range)),
		Value::Object(entries) => entries.values().all(|v| keyed_as_compared(v, range)),
		Value::Range(r) => [&r.start, &r.end].into_iter().all(|bound| match bound {
			Bound::Included(v) | Bound::Excluded(v) => keyed_as_compared(v, true),
			Bound::Unbounded => true,
		}),
		Value::None
		| Value::Null
		| Value::Bool(_)
		| Value::Number(_)
		| Value::String(_)
		| Value::Duration(_)
		| Value::Datetime(_)
		| Value::Date(_)
		| Value::Time(_)
		| Value::Timestamp(_)
		| Value::Interval(_)
		| Value::Uuid(_)
		| Value::Bytes(_)
		| Value::Table(_)
		| Value::File(_)
		| Value::Closure(_) => true,
	}
}

/// `expr` as a literal an equality-style index access (`CONTAINS`, `INSIDE`,
/// an `IN` list, `CONTAINSANY` / `CONTAINSALL`) may seek, when it is one
/// ([`keyed_as_compared`]).
fn index_literal(expr: &LogicalExpr) -> Option<Value> {
	try_literal_to_value(expr).filter(|value| keyed_as_compared(value, false))
}

/// Whether `op` compares for equality or order, the comparisons whose value
/// side may read the statement clock as an index bound.
fn is_ordering_comparison(op: &BinaryOperator) -> bool {
	matches!(
		op,
		BinaryOperator::Equal
			| BinaryOperator::ExactEqual
			| BinaryOperator::LessThan
			| BinaryOperator::LessThanEqual
			| BinaryOperator::MoreThan
			| BinaryOperator::MoreThanEqual
	)
}

/// Whether a condition's path is exactly an index column, step for step
/// ([`idiom_matches_column`](crate::exec::planner::util::idiom_matches_column)): a column may
/// hold any part a path can — a
/// field, a computed index such as `id[1]`, a method call such as
/// `id.id().r` — and matches the expression that renders the same way.
///
/// Columns containing `*` (flattened field paths like `marks.*.mark`) are
/// excluded because the Scan predicate filter cannot correctly evaluate
/// comparison operators on flattened paths — `[40] = 40` evaluates to false.
/// Users should use CONTAINS/INSIDE operators for array-aware queries.
///
/// With `key`, the structure of the table's record ids where they name their
/// components, a path and a column that address the same component match
/// whichever way each spells it ([`idiom_matches_column_in`]).
fn path_matches_column(path: &LogicalExpr, index_col: &Idiom, key: Option<&KeyStructure>) -> bool {
	!index_col.0.iter().any(|p| matches!(p, Part::All))
		&& idiom_matches_column_in(path, index_col, key)
}

/// Whether a field reference in a containment leaf (`field CONTAINS v`,
/// `v INSIDE field`) is served by an array-element index column
/// ([`idiom_matches_containment`]): the column must flatten (`.*` / `[*]`),
/// and the two paths must agree once every flatten is removed from both.
fn path_matches_containment(path: &LogicalExpr, index_col: &Idiom) -> bool {
	idiom_matches_containment(path, index_col)
}

/// Whether `path` in a spatial predicate matches the `SPATIAL` index column `index_col`.
/// Matches direct columns, explicit `.*` / `[*]` element columns, and plain `SPATIAL`
/// columns (which also index `Array` and `Set` elements individually unless flattened).
fn spatial_path_matches_column(
	path: &LogicalExpr,
	index_col: &Idiom,
	key: Option<&KeyStructure>,
) -> bool {
	if path_matches_column(path, index_col, key) || path_matches_containment(path, index_col) {
		return true;
	}
	if !index_col.0.iter().any(|p| matches!(p, Part::All | Part::Flatten)) {
		let mut elem_col = index_col.clone();
		elem_col.0.push(Part::All);
		return idiom_matches_containment(path, &elem_col);
	}
	false
}

fn hash_geometry<H: std::hash::Hasher>(geom: &crate::val::Geometry, state: &mut H) {
	use std::hash::Hash;
	std::mem::discriminant(geom).hash(state);
	match geom {
		crate::val::Geometry::Point(p) => {
			p.x().to_bits().hash(state);
			p.y().to_bits().hash(state);
		}
		crate::val::Geometry::Line(l) => {
			l.0.len().hash(state);
			for c in &l.0 {
				c.x.to_bits().hash(state);
				c.y.to_bits().hash(state);
			}
		}
		crate::val::Geometry::Polygon(poly) => {
			poly.exterior().0.len().hash(state);
			for c in &poly.exterior().0 {
				c.x.to_bits().hash(state);
				c.y.to_bits().hash(state);
			}
			poly.interiors().len().hash(state);
			for ring in poly.interiors() {
				ring.0.len().hash(state);
				for c in &ring.0 {
					c.x.to_bits().hash(state);
					c.y.to_bits().hash(state);
				}
			}
		}
		crate::val::Geometry::MultiPoint(mp) => {
			mp.0.len().hash(state);
			for p in &mp.0 {
				p.x().to_bits().hash(state);
				p.y().to_bits().hash(state);
			}
		}
		crate::val::Geometry::MultiLine(ml) => {
			ml.0.len().hash(state);
			for l in &ml.0 {
				l.0.len().hash(state);
				for c in &l.0 {
					c.x.to_bits().hash(state);
					c.y.to_bits().hash(state);
				}
			}
		}
		crate::val::Geometry::MultiPolygon(mp) => {
			mp.0.len().hash(state);
			for poly in &mp.0 {
				poly.exterior().0.len().hash(state);
				for c in &poly.exterior().0 {
					c.x.to_bits().hash(state);
					c.y.to_bits().hash(state);
				}
				poly.interiors().len().hash(state);
				for ring in poly.interiors() {
					ring.0.len().hash(state);
					for c in &ring.0 {
						c.x.to_bits().hash(state);
						c.y.to_bits().hash(state);
					}
				}
			}
		}
		crate::val::Geometry::Collection(items) => {
			items.len().hash(state);
			for item in items {
				hash_geometry(item, state);
			}
		}
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum SpatialExactKind {
	Inside,
	Intersects,
}

/// Extracted spatial predicate before quadtree cell covering is computed.
#[derive(Debug, Clone, PartialEq)]
enum SpatialPredicate {
	/// Any record intersecting `Geometry` may match (`INSIDE`, `CONTAINS`, `INTERSECTS`,
	/// `CONTAINSANY`, `ANYINSIDE`).
	Geometry {
		geom: crate::val::Geometry,
		exact_kind: Option<SpatialExactKind>,
	},
	/// Every item in the collection must be contained (`CONTAINSALL` / `ALLINSIDE`), so the
	/// single item with the tightest non-empty covering can drive the index scan.
	CollectionAll(Vec<crate::val::Geometry>),
	/// Spherical cap `geo::distance(field, center) < radius` or `<= radius`.
	Cap {
		center: crate::val::Geometry,
		radius: f64,
		inclusive: bool,
	},
}

impl SpatialPredicate {
	fn cache_key(&self, include_ancestors: bool, max_ops: usize) -> u64 {
		use std::hash::{Hash, Hasher};
		let mut hasher = std::collections::hash_map::DefaultHasher::new();
		include_ancestors.hash(&mut hasher);
		max_ops.hash(&mut hasher);
		std::mem::discriminant(self).hash(&mut hasher);
		match self {
			SpatialPredicate::Geometry {
				geom,
				exact_kind,
			} => {
				exact_kind.hash(&mut hasher);
				hash_geometry(geom, &mut hasher);
			}
			SpatialPredicate::CollectionAll(items) => {
				items.len().hash(&mut hasher);
				for item in items {
					hash_geometry(item, &mut hasher);
				}
			}
			SpatialPredicate::Cap {
				center,
				radius,
				inclusive,
			} => {
				hash_geometry(center, &mut hasher);
				radius.to_bits().hash(&mut hasher);
				inclusive.hash(&mut hasher);
			}
		}
		hasher.finish()
	}

	fn plan_scans_and_area(
		&self,
		include_ancestors: bool,
		max_ops: usize,
	) -> (Vec<surrealdb_idx::spatial::SpatialScanOp>, f64) {
		use surrealdb_idx::spatial::{
			CoverConfig, plan_cap_scans_with_area, plan_geometry_scans_with_area,
		};
		let config = CoverConfig::default_query();
		match self {
			SpatialPredicate::Geometry {
				geom,
				..
			} => plan_geometry_scans_with_area(geom, &config, include_ancestors, max_ops),
			SpatialPredicate::CollectionAll(items) => items
				.iter()
				.map(|item| {
					plan_geometry_scans_with_area(item, &config, include_ancestors, max_ops)
				})
				.filter(|(ops, _)| !ops.is_empty())
				.min_by(|(ops_a, area_a), (ops_b, area_b)| {
					area_a.total_cmp(area_b).then_with(|| ops_a.len().cmp(&ops_b.len()))
				})
				.unwrap_or((Vec::new(), 1.0)),
			SpatialPredicate::Cap {
				center: crate::val::Geometry::Point(center),
				radius,
				..
			} => plan_cap_scans_with_area(*center, *radius, &config, include_ancestors, max_ops),
			SpatialPredicate::Cap {
				..
			} => (Vec::new(), 1.0),
		}
	}

	fn to_point_filter(&self, column: Idiom) -> Option<SpatialPointFilter> {
		if !cfg!(feature = "geo") {
			return None;
		}
		match self {
			SpatialPredicate::Cap {
				center: crate::val::Geometry::Point(center),
				radius,
				inclusive,
			} => Some(SpatialPointFilter::Cap {
				column,
				center: (center.x(), center.y()),
				radius: *radius,
				inclusive: *inclusive,
			}),
			SpatialPredicate::Geometry {
				geom,
				exact_kind: Some(kind),
			} => {
				let (min_x, max_x, min_y, max_y) =
					surrealdb_idx::spatial::geometry_bounding_box(geom)?;
				match kind {
					SpatialExactKind::Inside => Some(SpatialPointFilter::Inside {
						column,
						min_x,
						max_x,
						min_y,
						max_y,
						geom: geom.clone(),
					}),
					SpatialExactKind::Intersects => Some(SpatialPointFilter::Intersects {
						column,
						min_x,
						max_x,
						min_y,
						max_y,
						geom: geom.clone(),
					}),
				}
			}
			SpatialPredicate::Geometry {
				exact_kind: None,
				..
			}
			| SpatialPredicate::CollectionAll(_)
			| SpatialPredicate::Cap {
				..
			} => None,
		}
	}
}

/// Whether `expr` is a single spatial comparison that matches `filter` on its indexed column.
pub(crate) fn expr_matches_spatial_point_filter(
	expr: &LogicalExpr,
	filter: &SpatialPointFilter,
) -> bool {
	if !matches!(
		expr,
		LogicalExpr::Binary {
			op,
			..
		} if !matches!(op, BinaryOperator::And)
	) {
		return false;
	}
	let mut spatial = Vec::new();
	IndexAnalyzer::collect_spatial_expressions(expr, &mut spatial);
	spatial.into_iter().any(|(path, pred)| {
		path_matches_column(&path, filter.column(), None)
			&& pred.to_point_filter(filter.column().clone()).as_ref() == Some(filter)
	})
}

/// Whether the expression path takes every element of a value (`.*` /
/// `[*]`). That step maps a stored scalar or set to `NONE`, but keeps an
/// object and follows a record link (see `IndexAnalyzer::seeks_containment`).
fn path_takes_each(mut path: &LogicalExpr) -> bool {
	loop {
		if let LogicalExpr::Each(_) = path {
			return true;
		} else if let LogicalExpr::Broadcast {
			body,
			..
		} = path && path_takes_each(body)
		{
			return true;
		} else if let LogicalExpr::Optional {
			then,
			..
		} = path && path_takes_each(then)
		{
			return true;
		}
		match path.step_base() {
			Some(base) => path = base,
			None => return false,
		}
	}
}

/// The least string greater than every string carrying `prefix`.
///
/// Incrementing the last scalar value that can be incremented, and dropping
/// the ones after it, gives the immediate successor of the whole prefix
/// stretch. `None` when every scalar value is already the last one, which
/// leaves the stretch unbounded above.
fn next_string_after_prefix(prefix: &str) -> Option<String> {
	let mut chars: Vec<char> = prefix.chars().collect();
	while let Some(last) = chars.pop() {
		// `char::from_u32` rejects the surrogate range, so step over it
		// rather than treating it as the end of the scalar values.
		let next = (last as u32 + 1..=char::MAX as u32).find_map(char::from_u32);
		if let Some(next) = next {
			chars.push(next);
			return Some(chars.into_iter().collect());
		}
	}
	None
}

/// Whether `candidate` admits fewer values than `kept`, both bounding one side
/// of a column: a higher lower bound or a lower upper bound, and on equal
/// values the exclusive operator. A bound that is not a literal cannot be
/// compared, so a literal replaces it and it never replaces a literal.
fn bound_is_tighter(
	is_lower: bool,
	candidate: (&BinaryOperator, &BoundOperand),
	kept: &(BinaryOperator, &BoundOperand),
) -> bool {
	let (Some(new), Some(old)) = (candidate.1.as_literal(), kept.1.as_literal()) else {
		return candidate.1.as_literal().is_some() && kept.1.as_literal().is_none();
	};
	match new.cmp(old) {
		std::cmp::Ordering::Equal => {
			matches!(candidate.0, BinaryOperator::MoreThan | BinaryOperator::LessThan)
				&& !matches!(kept.0, BinaryOperator::MoreThan | BinaryOperator::LessThan)
		}
		std::cmp::Ordering::Greater => is_lower,
		std::cmp::Ordering::Less => !is_lower,
	}
}

/// Normalize a range operator based on the position of the path in the
/// comparison expression.
///
/// When the path is on the left (`field > value`), the operator is already
/// correct. When on the right (`value < field`), the operator must be
/// flipped so it describes the condition from the field's perspective.
///
/// Returns `None` for non-range operators (equality, MATCHES, etc.).
fn normalize_range_op(op: &BinaryOperator, position: IdiomPosition) -> Option<BinaryOperator> {
	match position {
		IdiomPosition::Left => {
			if matches!(
				op,
				BinaryOperator::MoreThan
					| BinaryOperator::MoreThanEqual
					| BinaryOperator::LessThan
					| BinaryOperator::LessThanEqual
			) {
				Some(op.clone())
			} else {
				None
			}
		}
		IdiomPosition::Right => {
			if let BinaryOperator::LessThan = op {
				Some(BinaryOperator::MoreThan)
			} else if let BinaryOperator::LessThanEqual = op {
				Some(BinaryOperator::MoreThanEqual)
			} else if let BinaryOperator::MoreThan = op {
				Some(BinaryOperator::LessThan)
			} else if let BinaryOperator::MoreThanEqual = op {
				Some(BinaryOperator::LessThanEqual)
			} else {
				None
			}
		}
	}
}

#[cfg(test)]
mod tests {
	//! Unit tests for the IndexAnalyzer.
	//!
	//! These lock in plan-choice behaviour against intentional changes and
	//! act as regression cover for the analyzer + `select_access_path`
	//! helpers.  They use small `IndexDefinition` fixtures and parse
	//! WHERE / ORDER BY snippets via the SurrealQL parser, then drive the
	//! analyzer directly and assert about the candidate set or the access
	//! path that `select_access_path` picks.
	//!
	//! Tests are grouped by concern in nested modules so a failure tells you
	//! which category regressed at a glance.
	use std::str::FromStr;
	use std::sync::Arc;

	use surrealdb_strand::Strand;

	use super::*;
	use crate::catalog::{FullTextParams, Index, IndexDefinition, IndexId, Scoring};
	use crate::expr::Idiom;
	use crate::expr::logical::{LogicalFilter, LogicalOp, LogicalSort, LogicalSource, lower};
	use crate::expr::with::With;

	// ------------------------------------------------------------------
	// Fixture helpers
	// ------------------------------------------------------------------

	/// Build a minimal `IndexDefinition`.  Tests use synthetic
	/// `index_id`s that don't have to match any real catalog state.
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
			nulls_not_distinct: false,
		}
	}

	fn idx_basic(id: u32, name: &str, cols: &[&str]) -> IndexDefinition {
		idx_def(id, name, cols, Index::Idx)
	}

	/// An index in the current key format, which spells decimals without
	/// trailing zeros, so a number exact across kinds is keyed where it compares.
	fn idx_current(id: u32, name: &str, cols: &[&str]) -> IndexDefinition {
		let mut def = idx_basic(id, name, cols);
		def.format_version = crate::catalog::INDEX_FORMAT_VERSION;
		def
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

	fn analyzer<'a>(defs: Vec<IndexDefinition>, with: Option<&'a With>) -> IndexAnalyzer<'a> {
		static REGISTRY: std::sync::LazyLock<crate::exec::function::FunctionRegistry> =
			std::sync::LazyLock::new(crate::exec::function::FunctionRegistry::with_builtins);
		IndexAnalyzer::new(Arc::<[_]>::from(defs.into_boxed_slice()), with, &REGISTRY)
	}

	/// An analyzer that knows every column of `defs` to hold no array, as an
	/// index that has recorded storing none offers it.
	fn scalar_analyzer<'a>(
		defs: Vec<IndexDefinition>,
		with: Option<&'a With>,
	) -> IndexAnalyzer<'a> {
		let flags: Arc<[crate::exec::index::access_path::ColumnFlags]> =
			defs.iter().map(|ix| ix.cols.iter().map(|_| true).collect()).collect();
		analyzer(defs, with).with_scalar_columns(flags)
	}

	/// Parse a snippet wrapped in `SELECT * FROM t <snippet>`, lower it, and
	/// extract `(cond, order, with)` from the logical plan. Useful for driving
	/// the analyzer with realistic expression trees without hand-building
	/// them.
	fn parse_select_parts(
		snippet: &str,
	) -> (Option<LogicalExpr>, Option<LogicalOrdering>, Option<With>) {
		let src = format!("SELECT * FROM t {snippet}");
		let ast = crate::syn::parse(&src).expect("parse");
		let mut exprs = ast.expressions;
		assert_eq!(exprs.len(), 1, "expected one statement from {src:?}");
		let first_expr = exprs.remove(0);
		let surrealdb_expr::parser::TopLevelExpr::Expr(select @ crate::expr::Expr::Select(_)) =
			first_expr
		else {
			panic!("expected SELECT, got {first_expr:?}");
		};
		let plan = lower(select).expect("SELECT lowers");
		let (mut cond, mut order, mut with) = (None, None, None);
		let mut stack = vec![&plan];
		while let Some(op) = stack.pop() {
			if let LogicalOp::Filter(LogicalFilter {
				predicate,
				..
			}) = op
			{
				cond = Some(predicate.clone());
			} else if let LogicalOp::Sort(LogicalSort {
				ordering,
				..
			}) = op
			{
				order = Some(ordering.clone());
			} else if let LogicalOp::Source(LogicalSource {
				with: hint,
				..
			}) = op
			{
				with = hint.clone();
			}
			stack.extend(op.children());
		}
		(cond, order, with)
	}

	fn parse_cond(snippet: &str) -> LogicalExpr {
		let (cond, _, _) = parse_select_parts(&format!("WHERE {snippet}"));
		cond.expect("WHERE produced a predicate")
	}

	fn parse_cond_order(
		where_snippet: &str,
		order_snippet: &str,
	) -> (LogicalExpr, LogicalOrdering) {
		let (cond, order, _) =
			parse_select_parts(&format!("WHERE {where_snippet} ORDER BY {order_snippet}"));
		(cond.expect("WHERE"), order.expect("ORDER BY"))
	}

	// ------------------------------------------------------------------
	// Candidate-shape matchers
	// ------------------------------------------------------------------

	/// Find the candidate for a given index name, returning `None` if absent.
	fn find_for<'a>(cands: &'a [IndexCandidate], index_name: &str) -> Option<&'a IndexCandidate> {
		cands.iter().find(|c| c.index_ref.name.as_str() == index_name)
	}

	fn assert_no_candidate(cands: &[IndexCandidate], index_name: &str) {
		assert!(
			find_for(cands, index_name).is_none(),
			"expected no candidate for index {index_name:?}, got {:?}",
			cands.iter().map(|c| c.index_ref.name.as_str()).collect::<Vec<_>>()
		);
	}

	// ------------------------------------------------------------------
	// 1. Equality / single-column
	// ------------------------------------------------------------------
	// 0. Bitmap fusion
	// ------------------------------------------------------------------
	mod bitmap_fusion {
		use super::*;
		use crate::catalog::INDEX_FORMAT_VERSION;

		/// A b-tree index of the current format: bitmap-capable, and keyed
		/// where each number it holds compares.
		pub(super) fn idx_v2(id: u32, name: &str, cols: &[&str]) -> IndexDefinition {
			let mut def = idx_basic(id, name, cols);
			def.format_version = INDEX_FORMAT_VERSION;
			def
		}

		/// Run `try_bitmap_fusion` the way `resolve_access_path` does: analyze
		/// without ORDER BY, then attempt the fusion. `exact` stands in for
		/// the per-column check, whether the column's entries are its records'
		/// values, that gates NOT subtraction.
		fn fuse(az: &IndexAnalyzer<'_>, cond: &LogicalExpr, exact: bool) -> Option<BitmapPlan> {
			let candidates = az.analyze(Some(cond), None);
			az.try_bitmap_fusion(Some(cond), &candidates, &move |_, _| exact)
		}

		#[test]
		fn two_equality_branches_fuse() {
			let az = analyzer(vec![idx_v2(1, "idx_a", &["a"]), idx_v2(2, "idx_b", &["b"])], None);
			let cond = parse_cond("a = 1 AND b = 2");
			let plan = fuse(&az, &cond, false).expect("two capable branches fuse");
			assert!(matches!(plan, BitmapPlan::And(ref children) if children.len() == 2));
		}

		/// Run `try_bitmap_union_fusion` the way `resolve_access_path` does.
		fn fuse_union(az: &IndexAnalyzer<'_>, cond: &LogicalExpr) -> Option<BitmapPlan> {
			az.try_bitmap_union_fusion(Some(cond), Direction::Forward).map(|(plan, _)| plan)
		}

		#[test]
		fn top_level_or_of_capable_branches_fuses() {
			let az = analyzer(vec![idx_v2(1, "idx_a", &["a"]), idx_v2(2, "idx_b", &["b"])], None);
			let cond = parse_cond("a = 1 OR b = 2");
			let plan = fuse_union(&az, &cond).expect("two capable branches union");
			assert!(matches!(plan, BitmapPlan::Or(ref children) if children.len() == 2));
		}

		#[test]
		fn or_trees_flatten_into_one_union() {
			let az = analyzer(
				vec![
					idx_v2(1, "idx_a", &["a"]),
					idx_v2(2, "idx_b", &["b"]),
					idx_v2(3, "idx_c", &["c"]),
				],
				None,
			);
			let cond = parse_cond("a = 1 OR b = 2 OR c = 3");
			let plan = fuse_union(&az, &cond).expect("nested OR flattens");
			assert!(matches!(plan, BitmapPlan::Or(ref children) if children.len() == 3));
		}

		#[test]
		fn union_fusion_declines_conjunctions() {
			let az = analyzer(vec![idx_v2(1, "idx_a", &["a"]), idx_v2(2, "idx_b", &["b"])], None);
			let cond = parse_cond("a = 1 AND b = 2");
			assert!(fuse_union(&az, &cond).is_none(), "AND is try_bitmap_fusion's shape");
		}

		#[test]
		fn disjoint_same_index_equalities_decline() {
			// Same-index equality branches with distinct values are
			// record-disjoint: no duplicate fetches to save, so the
			// streaming union keeps the query.
			let az = analyzer(vec![idx_v2(1, "idx_a", &["a"])], None);
			let cond = parse_cond("a = 1 OR a = 2");
			assert!(fuse_union(&az, &cond).is_none());
		}

		#[test]
		fn same_index_range_branches_still_fuse() {
			// A range branch can overlap an equality branch on the same
			// index, so the disjointness decline does not apply.
			let az = scalar_analyzer(vec![idx_v2(1, "idx_a", &["a"])], None);
			let cond = parse_cond("a = 1 OR a > 5");
			let plan = fuse_union(&az, &cond).expect("overlappable branches union");
			assert!(matches!(plan, BitmapPlan::Or(ref children) if children.len() == 2));
		}

		#[test]
		fn a_pre_doc_id_branch_blocks_the_union() {
			// One branch's index predates entry doc-IDs: a union missing a
			// branch would silently drop rows, so nothing fuses.
			let az =
				analyzer(vec![idx_v2(1, "idx_a", &["a"]), idx_basic(2, "idx_b", &["b"])], None);
			let cond = parse_cond("a = 1 OR b = 2");
			assert!(fuse_union(&az, &cond).is_none());
		}

		#[test]
		fn an_unindexed_branch_blocks_the_union() {
			let az = analyzer(vec![idx_v2(1, "idx_a", &["a"])], None);
			let cond = parse_cond("a = 1 OR unindexed = 2");
			assert!(fuse_union(&az, &cond).is_none());
		}

		#[test]
		fn a_with_hint_pins_the_union_plan() {
			let with = With::Index(vec!["idx_a".to_owned()]);
			let az =
				analyzer(vec![idx_v2(1, "idx_a", &["a"]), idx_v2(2, "idx_b", &["b"])], Some(&with));
			let cond = parse_cond("a = 1 OR b = 2");
			assert!(fuse_union(&az, &cond).is_none());
		}

		/// The graph semi-join leaf lands after the index leaves, in both
		/// containment spellings, traversing the inverse direction.
		#[test]
		fn a_graph_conjunct_joins_the_intersection() {
			let az = analyzer(vec![idx_v2(1, "idx_a", &["a"])], None);
			for cond in ["a = 1 AND ->wrote->doc CONTAINS doc:x", "a = 1 AND doc:x IN ->wrote->doc"]
			{
				let plan = fuse(&az, &parse_cond(cond), false).expect("graph conjunct fuses");
				let BitmapPlan::And(children) = plan else {
					panic!("expected an intersection for `{cond}`")
				};
				assert_eq!(children.len(), 2, "`{cond}`");
				assert!(
					matches!(
						&children[1],
						BitmapPlan::Graph {
							direction: crate::expr::Dir::In,
							..
						}
					),
					"graph leaf must follow the index leaf for `{cond}`"
				);
			}
		}

		/// A unique index of the current format, as [`idx_v2`].
		fn idx_uniq_v2(id: u32, name: &str, cols: &[&str]) -> IndexDefinition {
			let mut def = idx_uniq(id, name, cols);
			def.format_version = INDEX_FORMAT_VERSION;
			def
		}

		/// A unique-equality anchor guarantees at most one candidate row, so
		/// the graph conjunct must not join the intersection — draining an
		/// adjacency band to intersect against a ≤1-row anchor can never beat
		/// the residual per-row traversal. With no other member the fusion
		/// declines entirely, keeping the streaming single-index plan.
		#[test]
		fn a_unique_anchor_skips_the_graph_leaf() {
			let az = analyzer(vec![idx_uniq_v2(1, "idx_a", &["a"])], None);
			let cond = parse_cond("a = 1 AND ->wrote->doc CONTAINS doc:x");
			assert!(fuse(&az, &cond, false).is_none(), "unique anchor keeps the streaming plan");
			// `NONE` and `NULL` are exempt from uniqueness on a default UNIQUE
			// index, so they can match multiple rows and must keep the graph leaf.
			for nullish in ["a = NONE", "a = NULL"] {
				let cond = parse_cond(&format!("{nullish} AND ->wrote->doc CONTAINS doc:x"));
				let plan = fuse(&az, &cond, false).expect("exempt equality keeps the graph leaf");
				assert!(matches!(plan, BitmapPlan::And(ref c) if c.len() == 2), "{nullish}");
			}
			// Under `NULLS NOT DISTINCT`, `NULL` is unique (≤1 row) while `NONE`
			// remains exempt.
			let mut nnd = idx_uniq_v2(1, "idx_a", &["a"]);
			nnd.nulls_not_distinct = true;
			let az_nnd = analyzer(vec![nnd], None);
			let cond_null = parse_cond("a = NULL AND ->wrote->doc CONTAINS doc:x");
			assert!(
				fuse(&az_nnd, &cond_null, false).is_none(),
				"NULL on UNIQUE NULLS NOT DISTINCT guarantees ≤1 row"
			);
			let cond_none = parse_cond("a = NONE AND ->wrote->doc CONTAINS doc:x");
			let plan = fuse(&az_nnd, &cond_none, false).expect("NONE remains exempt under NND");
			assert!(matches!(plan, BitmapPlan::And(ref c) if c.len() == 2));
		}

		/// The guard only drops the graph leaf: sibling index branches still
		/// fuse around the unique anchor.
		#[test]
		fn a_unique_anchor_with_a_sibling_fuses_without_the_graph_leaf() {
			let az =
				analyzer(vec![idx_uniq_v2(1, "idx_a", &["a"]), idx_v2(2, "idx_b", &["b"])], None);
			let cond = parse_cond("a = 1 AND b = 2 AND ->wrote->doc CONTAINS doc:x");
			let plan = fuse(&az, &cond, false).expect("two b-tree branches fuse");
			let BitmapPlan::And(children) = plan else {
				panic!("expected an intersection")
			};
			assert_eq!(children.len(), 2);
			assert!(
				children.iter().all(|c| !matches!(c, BitmapPlan::Graph { .. })),
				"the graph leaf must stay out of a unique-anchored intersection"
			);
		}

		/// A compound prefix on a unique index is a ≤1-row anchor only when
		/// it pins every column with no range; a partial prefix can match
		/// many rows and keeps the graph leaf.
		#[test]
		fn only_a_fully_pinned_unique_compound_anchor_skips_the_graph_leaf() {
			let az = analyzer(vec![idx_uniq_v2(1, "idx_ab", &["a", "b"])], None);
			let full = parse_cond("a = 1 AND b = 2 AND ->wrote->doc CONTAINS doc:x");
			assert!(fuse(&az, &full, false).is_none(), "fully pinned unique prefix declines");
			let partial = parse_cond("a = 1 AND ->wrote->doc CONTAINS doc:x");
			let plan = fuse(&az, &partial, false).expect("partial prefix keeps the graph leaf");
			assert!(matches!(plan, BitmapPlan::And(ref c) if c.len() == 2));
		}

		/// Every shape outside the single-hop literal-anchored contract
		/// declines, and a graph conjunct alone cannot anchor a plan.
		#[test]
		fn graph_conjuncts_outside_the_contract_decline() {
			let az = analyzer(vec![idx_v2(1, "idx_a", &["a"])], None);
			for cond in [
				// No index-backed sibling to anchor the intersection.
				"->wrote->doc CONTAINS doc:x AND unindexed = 2",
				// Multi-hop.
				"a = 1 AND ->wrote->doc->wrote->doc CONTAINS doc:x",
				// Both-direction traversal.
				"a = 1 AND <->wrote<->doc CONTAINS doc:x",
				// The anchor's table is not a vertex table the hop names.
				"a = 1 AND ->wrote->doc CONTAINS person:1",
				// A clause on the lookup changes its truth set.
				"a = 1 AND ->(wrote WHERE weight > 1)->doc CONTAINS doc:x",
			] {
				assert!(fuse(&az, &parse_cond(cond), false).is_none(), "`{cond}`");
			}
		}

		#[test]
		fn pre_doc_id_indexes_do_not_fuse() {
			// format_version 1 b-tree entries carry no doc-IDs.
			let az =
				analyzer(vec![idx_basic(1, "idx_a", &["a"]), idx_basic(2, "idx_b", &["b"])], None);
			let cond = parse_cond("a = 1 AND b = 2");
			assert!(fuse(&az, &cond, false).is_none());
		}

		#[test]
		fn single_branch_does_not_fuse() {
			let az = analyzer(vec![idx_v2(1, "idx_a", &["a"])], None);
			let cond = parse_cond("a = 1 AND unindexed = 2");
			assert!(fuse(&az, &cond, false).is_none(), "one branch is a streaming plan");
		}

		#[test]
		fn with_hint_pins_the_plan() {
			let with = With::Index(vec!["idx_a".to_owned()]);
			let az =
				analyzer(vec![idx_v2(1, "idx_a", &["a"]), idx_v2(2, "idx_b", &["b"])], Some(&with));
			let cond = parse_cond("a = 1 AND b = 2");
			assert!(fuse(&az, &cond, false).is_none());
		}

		#[test]
		fn conjunctive_not_subtracts_when_exact() {
			let az = analyzer(vec![idx_v2(1, "idx_a", &["a"]), idx_v2(2, "idx_b", &["b"])], None);
			let cond = parse_cond("a = 1 AND !(b = 2)");
			let plan = fuse(&az, &cond, true).expect("exact NOT subtracts");
			assert!(matches!(plan, BitmapPlan::AndNot { .. }));
			// Where no column's entries are known to be its records' values
			// the NOT stays a residual-only filter, and a single positive
			// branch is not worth fusing.
			assert!(fuse(&az, &cond, false).is_none());
		}

		#[test]
		fn standalone_not_never_fuses() {
			let az = analyzer(vec![idx_v2(1, "idx_a", &["a"]), idx_v2(2, "idx_b", &["b"])], None);
			let cond = parse_cond("!(a = 1) AND !(b = 2)");
			assert!(fuse(&az, &cond, true).is_none(), "a subtraction needs a positive anchor");
		}

		#[test]
		fn not_of_conjunction_is_not_subtracted() {
			// `NOT (b = 2 AND c = 3)` cannot be subtracted from partial
			// candidates — a partially-covering bitmap over-subtracts.
			let az = analyzer(
				vec![
					idx_v2(1, "idx_a", &["a"]),
					idx_v2(2, "idx_b", &["b"]),
					idx_v2(3, "idx_c", &["c"]),
				],
				None,
			);
			let cond = parse_cond("a = 1 AND !(b = 2 AND c = 3)");
			assert!(fuse(&az, &cond, true).is_none());
		}

		#[test]
		fn or_conjunct_becomes_union_member() {
			let az = analyzer(vec![idx_v2(1, "idx_a", &["a"]), idx_v2(2, "idx_b", &["b"])], None);
			let cond = parse_cond("a = 1 AND (b = 2 OR b = 3)");
			let plan = fuse(&az, &cond, false).expect("indexable OR conjunct fuses");
			let BitmapPlan::And(children) = plan else {
				panic!("expected And root");
			};
			assert!(
				children
					.iter()
					.any(|c| matches!(c, BitmapPlan::Or(branches) if branches.len() == 2))
			);
		}

		#[test]
		fn or_with_unindexed_branch_stays_residual() {
			let az = analyzer(vec![idx_v2(1, "idx_a", &["a"]), idx_v2(2, "idx_b", &["b"])], None);
			let cond = parse_cond("a = 1 AND (b = 2 OR unindexed = 3)");
			assert!(fuse(&az, &cond, false).is_none(), "partial OR would drop rows");
		}

		#[test]
		fn knn_condition_never_fuses() {
			let az = analyzer(vec![idx_v2(1, "idx_a", &["a"]), idx_v2(2, "idx_b", &["b"])], None);
			let cond = parse_cond("a = 1 AND b = 2 AND vec <|2|> [1, 2]");
			assert!(fuse(&az, &cond, false).is_none());
		}

		#[test]
		fn redundant_same_column_branch_is_skipped() {
			// Two indexes with the same leading column: intersecting both is
			// pointless; the second is skipped so only one branch remains,
			// which is below the fusion threshold.
			let az =
				analyzer(vec![idx_v2(1, "idx_a1", &["a"]), idx_v2(2, "idx_a2", &["a", "b"])], None);
			let cond = parse_cond("a = 1 AND unindexed = 2");
			assert!(fuse(&az, &cond, false).is_none());
		}

		#[test]
		fn count_fusion_requires_every_conjunct_exact() {
			let az = analyzer(vec![idx_v2(1, "idx_a", &["a"]), idx_v2(2, "idx_b", &["b"])], None);
			// Fully covered: eligible.
			let cond = parse_cond("a = 1 AND b = 2");
			assert!(az.try_bitmap_count_fusion(&cond, &|_, _| true).is_some());
			// One conjunct not index-covered: ineligible (no residual filter
			// exists to repair the count).
			let cond = parse_cond("a = 1 AND b = 2 AND unindexed = 3");
			assert!(az.try_bitmap_count_fusion(&cond, &|_, _| true).is_none());
			// A column not known to hold one entry per record is ineligible
			// (an array's entry fan-out would inflate the count).
			let cond = parse_cond("a = 1 AND b = 2");
			assert!(az.try_bitmap_count_fusion(&cond, &|_, _| false).is_none());
		}
	}

	// ------------------------------------------------------------------
	mod nullish_containment {
		use super::*;

		/// The keys of the union a containment expansion over `cols` answers
		/// `cond` with, the leading column flagged as listing every record
		/// under its elements or a nullish key (and withheld, when asked).
		fn union_keys(
			cols: &[&str],
			cond: &str,
			flagged: bool,
			withheld: bool,
		) -> Option<Vec<Value>> {
			let mut a = analyzer(vec![idx_basic(1, "ix", cols)], None);
			if flagged {
				a = a.with_elements_or_nullish(vec![vec![true].into_boxed_slice()].into());
			}
			if withheld {
				a = a.without_containment_on(vec![vec![true].into_boxed_slice()].into());
			}
			let cond = parse_cond(cond);
			let Some(AccessPath::Union {
				paths,
				dedupe,
			}) = a.try_nullish_containment(Some(&cond), crate::kvs::Direction::Forward)
			else {
				return None;
			};
			assert!(dedupe);
			Some(
				paths
					.iter()
					.map(|p| {
						let AccessPath::BTreeScan {
							access,
							..
						} = p
						else {
							panic!("unexpected branch {p:?}");
						};
						let operand = if let BTreeAccess::Compound {
							prefix,
							range: None,
							..
						} = access && let [operand] = prefix.as_slice()
						{
							operand
						} else if let BTreeAccess::Equality(operand) = access {
							operand
						} else {
							panic!("unexpected access {access:?}");
						};
						let BoundOperand::Literal(v) = operand else {
							panic!("unexpected operand {operand:?}");
						};
						v.clone()
					})
					.collect(),
			)
		}

		#[test]
		fn a_containment_over_a_flagged_element_column_seeks_the_value_and_the_nullish_keys() {
			let keys = vec![Value::from("x"), Value::None, Value::Null];
			for cond in ["tags CONTAINS 'x'", "'x' INSIDE tags", "n = 1 AND tags CONTAINS 'x'"] {
				assert_eq!(
					union_keys(&["tags.*", "created_at"], cond, true, false),
					Some(keys.clone()),
					"{cond}"
				);
				assert_eq!(
					union_keys(&["tags.*"], cond, true, false),
					Some(keys.clone()),
					"{cond}"
				);
			}
		}

		#[test]
		fn a_multi_value_containment_seeks_each_value_and_the_nullish_keys() {
			assert_eq!(
				union_keys(&["tags.*"], "tags CONTAINSANY ['x', 'y']", true, false),
				Some(vec![Value::from("x"), Value::from("y"), Value::None, Value::Null])
			);
		}

		#[test]
		fn an_unflagged_or_withheld_column_is_not_sought() {
			for cond in ["tags CONTAINS 'x'", "'x' INSIDE tags", "tags CONTAINSANY ['x', 'y']"] {
				assert_eq!(union_keys(&["tags.*"], cond, false, false), None, "{cond}");
				assert_eq!(union_keys(&["tags.*"], cond, true, true), None, "{cond}");
			}
		}

		#[test]
		fn a_path_taking_each_element_keeps_its_plain_seek() {
			// `tags.* CONTAINS 'x'` cannot match a scalar, so its seek needs no
			// nullish key and stays a candidate of its own.
			let a = analyzer(vec![idx_basic(1, "ix", &["tags.*"])], None)
				.with_elements_or_nullish(vec![vec![true].into_boxed_slice()].into());
			let cond = parse_cond("tags.* CONTAINS 'x'");
			assert!(find_for(&a.analyze(Some(&cond), None), "ix").is_some());
			assert!(
				a.try_nullish_containment(Some(&cond), crate::kvs::Direction::Forward).is_none()
			);
		}

		#[test]
		fn a_negated_containment_is_not_sought() {
			assert_eq!(union_keys(&["tags.*"], "!(tags CONTAINS 'x')", true, false), None);
			// A cast to a string is truthy whether or not the array holds `x`.
			assert_eq!(union_keys(&["tags.*"], "<string> (tags CONTAINS 'x')", true, false), None);
			assert_eq!(
				union_keys(&["tags.*"], "<string> (tags CONTAINSANY ['x', 'y'])", true, false),
				None
			);
			assert_eq!(union_keys(&["tags.*"], "tags CONTAINS 'x' OR n = 1", true, false), None);
		}
	}

	// ------------------------------------------------------------------
	mod withheld_containment {
		use super::*;

		/// Whether a containment leaf finds a candidate on `ix`, with the
		/// first column withheld or not.
		fn seeks(cols: &[&str], cond: &str, withheld: bool) -> bool {
			let mut a = analyzer(vec![idx_basic(1, "ix", cols)], None);
			if withheld {
				a = a.without_containment_on(vec![vec![true].into_boxed_slice()].into());
			}
			let cond = parse_cond(cond);
			find_for(&a.analyze(Some(&cond), None), "ix").is_some()
				|| a.try_containment_expansion(Some(&cond), crate::kvs::Direction::Forward)
					.is_some()
		}

		#[test]
		fn a_withheld_element_column_is_not_sought_for_containment() {
			for cond in ["acl.* CONTAINS 'a'", "'a' INSIDE acl.*", "acl.* CONTAINSANY ['a', 'b']"] {
				assert!(seeks(&["acl.*"], cond, false), "{cond}");
				assert!(!seeks(&["acl.*"], cond, true), "{cond}");
			}
		}

		#[test]
		fn a_withheld_column_still_serves_a_leading_equality() {
			// Only the containment seek on the withheld column goes; the
			// compound prefix over the column before it stays.
			let mut a = analyzer(vec![idx_basic(1, "ix", &["grp", "acl.*"])], None);
			a = a.without_containment_on(vec![vec![false, true].into_boxed_slice()].into());
			let cond = parse_cond("grp = 1 AND acl CONTAINS 'a'");
			let cands = a.analyze(Some(&cond), None);
			let c = find_for(&cands, "ix").expect("prefix candidate");
			assert!(matches!(
				&c.access,
				BTreeAccess::Compound { prefix, .. } if prefix.len() == 1
			));
		}
	}

	// ------------------------------------------------------------------
	mod equality {
		use super::*;

		#[test]
		fn idx_equality_left_idiom() {
			let a = analyzer(vec![idx_basic(1, "ix_a", &["a"])], None);
			let cond = parse_cond("a = 5");
			let cands = a.analyze(Some(&cond), None);
			let c = find_for(&cands, "ix_a").expect("ix_a candidate");
			assert!(matches!(c.access, BTreeAccess::Equality(_)));
		}

		#[test]
		fn idx_equality_right_idiom() {
			// `value = idiom` should still match (idiom position handled).
			let a = analyzer(vec![idx_basic(1, "ix_a", &["a"])], None);
			let cond = parse_cond("5 = a");
			let cands = a.analyze(Some(&cond), None);
			let c = find_for(&cands, "ix_a").expect("ix_a candidate");
			assert!(matches!(c.access, BTreeAccess::Equality(_)));
		}

		#[test]
		fn uniq_equality_outranks_non_unique() {
			// Same column has both a non-unique and a unique index.
			// `select_access_path` must prefer the unique one.
			let a = analyzer(
				vec![idx_basic(1, "ix_a_basic", &["a"]), idx_uniq(2, "ix_a_uniq", &["a"])],
				None,
			);
			let cond = parse_cond("a = 5");
			let cands = a.analyze(Some(&cond), None);
			let path = super::super::super::access_path::select_access_path(
				cands,
				None,
				crate::kvs::Direction::Forward,
			);
			let AccessPath::BTreeScan {
				index_ref,
				..
			} = path
			else {
				panic!("expected BTreeScan, got {path:?}");
			};
			assert_eq!(index_ref.name.as_str(), "ix_a_uniq", "uniq must win");
		}

		#[test]
		fn first_col_of_compound_becomes_prefix() {
			// Single-column equality on the first column of a compound index
			// must be lifted to `BTreeAccess::Compound { prefix: [v], .. }`.
			let a = analyzer(vec![idx_basic(1, "ix_ab", &["a", "b"])], None);
			let cond = parse_cond("a = 5");
			let cands = a.analyze(Some(&cond), None);
			let c = find_for(&cands, "ix_ab").expect("ix_ab candidate");
			let BTreeAccess::Compound {
				prefix,
				range,
				..
			} = &c.access
			else {
				panic!("expected Compound, got {:?}", c.access);
			};
			assert_eq!(prefix.len(), 1);
			assert!(range.is_none());
		}

		#[test]
		fn idiom_mismatch_yields_no_candidate() {
			let a = analyzer(vec![idx_basic(1, "ix_a", &["a"])], None);
			let cond = parse_cond("b = 5");
			let cands = a.analyze(Some(&cond), None);
			assert!(cands.is_empty(), "no index matches column b");
		}

		#[test]
		fn regex_and_geometry_literals_decline_btree_index() {
			let a = scalar_analyzer(vec![idx_basic(1, "ix_a", &["a"])], None);
			for text in [
				"a = /foo/",
				"a = (1.0, 2.0)",
				"a > (1.0, 2.0)",
				"a IN [/foo/, /bar/]",
				"a IN [(1.0, 2.0), (3.0, 4.0)]",
				"a = r't:[(1.0, 2.0)]'",
			] {
				let cond = parse_cond(text);
				let cands = a.analyze(Some(&cond), None);
				assert_no_candidate(&cands, "ix_a");
				assert!(
					a.try_in_expansion(Some(&cond), crate::kvs::Direction::Forward).is_none(),
					"IN expansion must also decline `{text}`"
				);
			}
		}
	}

	// ------------------------------------------------------------------
	// 2. Range / inequality
	// ------------------------------------------------------------------
	mod range {
		use std::ops::Bound;

		use super::*;

		#[test]
		fn half_bounded_gt() {
			let a = scalar_analyzer(vec![idx_basic(1, "ix_a", &["a"])], None);
			let cond = parse_cond("a > 5");
			let cands = a.analyze(Some(&cond), None);
			let c = find_for(&cands, "ix_a").expect("ix_a");
			let BTreeAccess::Range {
				range,
			} = &c.access
			else {
				panic!("expected Range, got {:?}", c.access);
			};
			assert!(matches!(range.start, Bound::Excluded(_)));
			assert!(matches!(range.end, Bound::Unbounded));
		}

		#[test]
		fn half_bounded_gte() {
			let a = scalar_analyzer(vec![idx_basic(1, "ix_a", &["a"])], None);
			let cond = parse_cond("a >= 5");
			let cands = a.analyze(Some(&cond), None);
			let c = find_for(&cands, "ix_a").expect("ix_a");
			let BTreeAccess::Range {
				range,
				..
			} = &c.access
			else {
				panic!("expected Range, got {:?}", c.access);
			};
			assert!(matches!(range.start, Bound::Included(_)));
		}

		#[test]
		fn bounded_after_merge() {
			// `a > 5 AND a < 10` must merge to a bounded range.
			let a = analyzer(vec![idx_basic(1, "ix_a", &["a"])], None);
			let cond = parse_cond("a > 5 AND a < 10");
			let cands = a.analyze(Some(&cond), None);
			let c = find_for(&cands, "ix_a").expect("ix_a");
			let BTreeAccess::Range {
				range,
			} = &c.access
			else {
				panic!("expected merged Range, got {:?}", c.access);
			};
			assert!(!matches!(range.start, Bound::Unbounded));
			assert!(!matches!(range.end, Bound::Unbounded));
		}

		#[test]
		fn value_lt_idiom_normalises() {
			// `5 < a` should be treated as `a > 5`.
			let a = scalar_analyzer(vec![idx_basic(1, "ix_a", &["a"])], None);
			let cond = parse_cond("5 < a");
			let cands = a.analyze(Some(&cond), None);
			let c = find_for(&cands, "ix_a").expect("ix_a");
			let BTreeAccess::Range {
				range,
			} = &c.access
			else {
				panic!("expected Range from-bound, got {:?}", c.access);
			};
			assert!(!matches!(range.start, Bound::Unbounded));
			assert!(matches!(range.end, Bound::Unbounded));
		}
	}

	// ------------------------------------------------------------------
	// 3. Compound prefix building
	// ------------------------------------------------------------------
	mod prefix_successor {
		use super::super::next_string_after_prefix;

		/// The contract: every string carrying the prefix sorts before the
		/// successor, and the successor itself does not carry the prefix.
		fn bounds_hold(prefix: &str, carrying: &[&str]) {
			let next = next_string_after_prefix(prefix).expect("a successor exists");
			assert!(!next.starts_with(prefix), "{next:?} still carries {prefix:?}");
			assert!(prefix < next.as_str(), "{prefix:?} must sort before {next:?}");
			for s in carrying {
				assert!(s.starts_with(prefix), "fixture {s:?} must carry {prefix:?}");
				assert!(*s < next.as_str(), "{s:?} must sort before {next:?}");
			}
		}

		#[test]
		fn an_ascii_prefix_increments_its_last_character() {
			assert_eq!(next_string_after_prefix("alpha").as_deref(), Some("alphb"));
			bounds_hold("alpha", &["alpha", "alphabet", "alpha\u{10FFFF}"]);
		}

		#[test]
		fn a_multibyte_prefix_increments_its_last_scalar_value() {
			assert_eq!(next_string_after_prefix("hé").as_deref(), Some("hê"));
			bounds_hold("hé", &["héllo", "héz"]);
		}

		#[test]
		fn a_prefix_ending_at_the_last_scalar_value_carries_into_the_one_before() {
			let next = next_string_after_prefix("a\u{10FFFF}").expect("carries into 'a'");
			assert_eq!(next, "b");
			bounds_hold("a\u{10FFFF}", &["a\u{10FFFF}z"]);
		}

		#[test]
		fn a_prefix_of_only_last_scalar_values_has_no_successor() {
			// Nothing sorts after it, so the range stays open above rather
			// than gaining a bound that would cut the stretch short.
			assert_eq!(next_string_after_prefix("\u{10FFFF}"), None);
			assert_eq!(next_string_after_prefix("\u{10FFFF}\u{10FFFF}"), None);
		}

		#[test]
		fn the_successor_steps_over_the_surrogate_range() {
			// D800..DFFF are not scalar values; incrementing D7FF must land
			// on E000 rather than producing nothing.
			let next = next_string_after_prefix("\u{D7FF}").expect("a successor exists");
			assert_eq!(next, "\u{E000}");
		}

		#[test]
		fn an_empty_prefix_has_no_successor() {
			assert_eq!(next_string_after_prefix(""), None);
		}
	}

	mod compound {
		use super::*;

		#[test]
		fn two_equalities_form_prefix() {
			let a = analyzer(vec![idx_basic(1, "ix_abc", &["a", "b", "c"])], None);
			let cond = parse_cond("a = 1 AND b = 2");
			let cands = a.analyze(Some(&cond), None);
			let c = find_for(&cands, "ix_abc").expect("ix_abc");
			let BTreeAccess::Compound {
				prefix,
				range,
				..
			} = &c.access
			else {
				panic!("expected Compound, got {:?}", c.access);
			};
			assert_eq!(prefix.len(), 2, "two equalities → prefix length 2");
			assert!(range.is_none());
		}

		#[test]
		fn equality_then_range() {
			let a = scalar_analyzer(vec![idx_current(1, "ix_abc", &["a", "b", "c"])], None);
			let cond = parse_cond("a = 1 AND b > 5");
			let cands = a.analyze(Some(&cond), None);
			let c = find_for(&cands, "ix_abc").expect("ix_abc");
			let BTreeAccess::Compound {
				prefix,
				range,
				..
			} = &c.access
			else {
				panic!("expected Compound with range, got {:?}", c.access);
			};
			assert_eq!(prefix.len(), 1);
			assert!(range.is_some(), "range on b captured");
		}

		#[test]
		fn equality_then_closed_range_bounds_both_ends() {
			// Both bounds on the column after the prefix are pushed into the
			// key range, whichever order the conjuncts are written in and
			// whichever side of the operator the field is on.
			let a = scalar_analyzer(vec![idx_current(1, "ix_ab", &["a", "b"])], None);
			for text in ["a = 1 AND b >= 5 AND b < 10", "b < 10 AND a = 1 AND 5 <= b"] {
				let cond = parse_cond(text);
				let cands = a.analyze(Some(&cond), None);
				let c = find_for(&cands, "ix_ab").expect("ix_ab");
				assert_eq!(
					c.access,
					BTreeAccess::Compound {
						prefix: vec![Value::from(1).into()],
						range: Some((BinaryOperator::MoreThanEqual, Value::from(5).into())),
						upper: Some((BinaryOperator::LessThan, Value::from(10).into())),
					},
					"{text}"
				);
			}
		}

		#[test]
		fn equality_then_closed_range_keeps_tightest_bounds() {
			// Several bounds on one side narrow to the tightest, whatever the
			// order of the conjuncts; on an equal value the exclusive one wins.
			let a = scalar_analyzer(vec![idx_current(1, "ix_ab", &["a", "b"])], None);
			for text in [
				"a = 1 AND b > 0 AND b < 1000000 AND b < 10 AND b >= 3 AND b > 3",
				"b > 3 AND b < 10 AND a = 1 AND b >= 3 AND b < 1000000 AND b > 0",
				"a = 1 AND b <= 10 AND b < 10 AND 3 < b",
			] {
				let cond = parse_cond(text);
				let cands = a.analyze(Some(&cond), None);
				let c = find_for(&cands, "ix_ab").expect("ix_ab");
				assert_eq!(
					c.access,
					BTreeAccess::Compound {
						prefix: vec![Value::from(1).into()],
						range: Some((BinaryOperator::MoreThan, Value::from(3).into())),
						upper: Some((BinaryOperator::LessThan, Value::from(10).into())),
					},
					"{text}"
				);
			}
		}

		#[test]
		fn equality_then_inclusive_singleton_range_bounds_both_ends() {
			// Equal inclusive bounds admit exactly their value, so both are pushed.
			let a = scalar_analyzer(vec![idx_current(1, "ix_ab", &["a", "b"])], None);
			let cond = parse_cond("a = 1 AND b >= 5 AND b <= 5");
			let cands = a.analyze(Some(&cond), None);
			let c = find_for(&cands, "ix_ab").expect("ix_ab");
			assert_eq!(
				c.access,
				BTreeAccess::Compound {
					prefix: vec![Value::from(1).into()],
					range: Some((BinaryOperator::MoreThanEqual, Value::from(5).into())),
					upper: Some((BinaryOperator::LessThanEqual, Value::from(5).into())),
				}
			);
		}

		#[test]
		fn equality_then_range_is_bounded_only_by_exactly_keyed_numbers() {
			// An index that keeps decimals' trailing zeros keys `2.50dec` apart
			// from `2.5dec`, and `5.0dec` apart from `5`, though each pair
			// compares equal: a bound naming such a number could end the scan
			// before records the comparison accepts, so none bounds the key
			// range, whatever the order of the conjuncts, and the residual filter
			// judges each.
			let a = scalar_analyzer(vec![idx_basic(1, "ix_ab", &["a", "b"])], None);
			for text in [
				"a = 1 AND b >= 2.5dec AND b <= 2.5dec",
				"a = 1 AND b <= 2.5dec AND b >= 2.5dec",
				"a = 1 AND b >= 2 AND b <= 5",
				"a = 1 AND b <= 5 AND b >= 2",
				"a = 1 AND b < 5 AND b <= 4",
				"a = 1 AND b > 2 AND b >= 3",
			] {
				let cond = parse_cond(text);
				let cands = a.analyze(Some(&cond), None);
				let c = find_for(&cands, "ix_ab").expect("ix_ab");
				assert_eq!(
					c.access,
					BTreeAccess::Compound {
						prefix: vec![Value::from(1).into()],
						range: None,
						upper: None,
					},
					"{text}"
				);
			}
			// On the current format a float no decimal holds exactly is keyed
			// apart too: the exact bound on the other side bounds the scan alone.
			let a = scalar_analyzer(vec![idx_current(1, "ix_ab", &["a", "b"])], None);
			for text in ["a = 1 AND b < 0.1f AND b > 0", "a = 1 AND b > 0 AND b < 0.1f"] {
				let cond = parse_cond(text);
				let cands = a.analyze(Some(&cond), None);
				let c = find_for(&cands, "ix_ab").expect("ix_ab");
				assert_eq!(
					c.access,
					BTreeAccess::Compound {
						prefix: vec![Value::from(1).into()],
						range: Some((BinaryOperator::MoreThan, Value::from(0).into())),
						upper: None,
					},
					"{text}"
				);
			}
			// The current format keys an exact number where it compares, and closes.
			let cond = parse_cond("a = 1 AND b >= 2.5dec AND b <= 2.5dec");
			let cands = a.analyze(Some(&cond), None);
			let c = find_for(&cands, "ix_ab").expect("ix_ab");
			assert!(
				matches!(
					&c.access,
					BTreeAccess::Compound {
						upper: Some(_),
						..
					}
				),
				"{:?}",
				c.access
			);
		}

		#[test]
		fn equality_then_one_sided_range_keeps_tightest_bound() {
			// Several bounds on one side alone narrow to the tightest too, and an
			// empty interval keeps the tightest bound of the first conjunct's side.
			let a = scalar_analyzer(vec![idx_current(1, "ix_ab", &["a", "b"])], None);
			for (text, op, v) in [
				("a = 1 AND b < 1000000 AND b < 4", BinaryOperator::LessThan, 4),
				("a = 1 AND b > 0 AND b > 10", BinaryOperator::MoreThan, 10),
				("a = 1 AND b > 0 AND b > 10 AND b < 5", BinaryOperator::MoreThan, 10),
			] {
				let cond = parse_cond(text);
				let cands = a.analyze(Some(&cond), None);
				let c = find_for(&cands, "ix_ab").expect("ix_ab");
				assert_eq!(
					c.access,
					BTreeAccess::Compound {
						prefix: vec![Value::from(1).into()],
						range: Some((op, Value::from(v).into())),
						upper: None,
					},
					"{text}"
				);
			}
		}

		#[test]
		fn equality_then_range_ignores_an_array_bound() {
			// No key is a whole array, so a bound comparing with one is left to
			// the residual filter rather than pushed into the key range.
			let a = scalar_analyzer(vec![idx_current(1, "ix_ab", &["a", "b"])], None);
			let cond = parse_cond("a = 1 AND b > 5 AND b < [1, 2]");
			let cands = a.analyze(Some(&cond), None);
			let c = find_for(&cands, "ix_ab").expect("ix_ab");
			assert_eq!(
				c.access,
				BTreeAccess::Compound {
					prefix: vec![Value::from(1).into()],
					range: Some((BinaryOperator::MoreThan, Value::from(5).into())),
					upper: None,
				}
			);
		}

		#[test]
		fn equality_then_empty_closed_range_keeps_one_bound() {
			// Bounds that admit nothing leave the scan to the first bound and
			// the other to the residual filter, rather than a reversed key range.
			let a = scalar_analyzer(vec![idx_current(1, "ix_ab", &["a", "b"])], None);
			let cond = parse_cond("a = 1 AND b > 10 AND b < 5");
			let cands = a.analyze(Some(&cond), None);
			let c = find_for(&cands, "ix_ab").expect("ix_ab");
			assert_eq!(
				c.access,
				BTreeAccess::Compound {
					prefix: vec![Value::from(1).into()],
					range: Some((BinaryOperator::MoreThan, Value::from(10).into())),
					upper: None,
				}
			);
		}

		#[test]
		fn trailing_equality_after_range_is_dropped() {
			// `a = 1 AND b > 5 AND c = 2` on (a,b,c): the c=2 must NOT be part
			// of the index prefix (BTree limitation); becomes residual filter.
			let a = scalar_analyzer(vec![idx_current(1, "ix_abc", &["a", "b", "c"])], None);
			let cond = parse_cond("a = 1 AND b > 5 AND c = 2");
			let cands = a.analyze(Some(&cond), None);
			let c = find_for(&cands, "ix_abc").expect("ix_abc");
			let BTreeAccess::Compound {
				prefix,
				range,
				..
			} = &c.access
			else {
				panic!("expected Compound, got {:?}", c.access);
			};
			assert_eq!(prefix.len(), 1);
			assert!(range.is_some());
		}

		#[test]
		fn a_containment_extends_the_prefix_over_a_fanning_column() {
			// `path_matches_column` vetoes a column holding `*`, so the
			// containment matcher is the only way `emails.*.norm` is reached.
			let a = analyzer(vec![idx_basic(1, "ix_ae", &["account", "emails.*.norm"])], None);
			let cond = parse_cond("account = 1 AND emails.*.norm CONTAINS 'x'");
			let cands = a.analyze(Some(&cond), None);
			let c = find_for(&cands, "ix_ae").expect("ix_ae");
			let BTreeAccess::Compound {
				prefix,
				range,
				..
			} = &c.access
			else {
				panic!("expected Compound, got {:?}", c.access);
			};
			assert_eq!(prefix.len(), 2, "both columns pinned, not just the account");
			assert!(range.is_none());
		}

		#[test]
		fn the_reversed_containment_spelling_extends_it_too() {
			let a = analyzer(vec![idx_basic(1, "ix_ae", &["account", "emails.*.norm"])], None);
			let cond = parse_cond("account = 1 AND 'x' INSIDE emails.*.norm");
			let cands = a.analyze(Some(&cond), None);
			let c = find_for(&cands, "ix_ae").expect("ix_ae");
			let BTreeAccess::Compound {
				prefix,
				..
			} = &c.access
			else {
				panic!("expected Compound, got {:?}", c.access);
			};
			assert_eq!(prefix.len(), 2);
		}

		#[test]
		fn a_containment_pins_a_fanning_column_before_a_trailing_range() {
			let a = scalar_analyzer(
				vec![idx_current(1, "ix_aeb", &["account", "emails.*.norm", "b"])],
				None,
			);
			let cond = parse_cond("account = 1 AND emails.*.norm CONTAINS 'x' AND b > 5");
			let cands = a.analyze(Some(&cond), None);
			let c = find_for(&cands, "ix_aeb").expect("ix_aeb");
			let BTreeAccess::Compound {
				prefix,
				range,
				..
			} = &c.access
			else {
				panic!("expected Compound, got {:?}", c.access);
			};
			assert_eq!(prefix.len(), 2);
			assert!(range.is_some(), "the range on b terminates the prefix");
		}

		#[test]
		fn a_containment_on_each_of_two_fanning_columns_pins_both() {
			// Every column of the index fans out, and a containment leaf
			// pins each: the prefix addresses one key per record, the same
			// as an all-equality prefix over scalar columns.
			let a = analyzer(vec![idx_basic(1, "ix_tn", &["tags.*", "nums.*"])], None);
			let cond = parse_cond("tags.* CONTAINS 'x' AND nums.* CONTAINS 1");
			let cands = a.analyze(Some(&cond), None);
			let c = find_for(&cands, "ix_tn").expect("ix_tn");
			let BTreeAccess::Compound {
				prefix,
				range,
				..
			} = &c.access
			else {
				panic!("expected Compound, got {:?}", c.access);
			};
			assert_eq!(prefix.len(), 2, "both fanning columns pinned");
			assert!(range.is_none());
		}

		#[test]
		fn an_unpinned_second_fanning_column_still_extends_what_it_can() {
			// Only the leading column has a containment leaf, so the prefix
			// stops there and `nums.*` keeps contributing a key per element
			// — which is what the scan's own fan-out check answers for.
			let a = analyzer(vec![idx_basic(1, "ix_tn", &["tags.*", "nums.*"])], None);
			let cond = parse_cond("tags CONTAINS 'x' AND other = 7");
			let cands = a.analyze(Some(&cond), None);
			for c in &cands {
				if let BTreeAccess::Compound {
					prefix,
					..
				} = &c.access
				{
					assert!(prefix.len() < 2, "an unmatched column cannot be pinned");
				}
			}
		}

		#[test]
		fn a_multi_value_containment_does_not_extend_the_prefix() {
			// CONTAINSANY names several values; one prefix cannot hold them,
			// and the union expansion is the shape that serves it.
			let a = analyzer(vec![idx_basic(1, "ix_ae", &["account", "emails.*.norm"])], None);
			let cond = parse_cond("account = 1 AND emails.*.norm CONTAINSANY ['x', 'y']");
			let cands = a.analyze(Some(&cond), None);
			for c in &cands {
				if let BTreeAccess::Compound {
					prefix,
					..
				} = &c.access
				{
					assert!(prefix.len() < 2, "a multi-value leaf pins no single element");
				}
			}
		}

		#[test]
		fn a_containment_on_a_non_fanning_column_does_not_extend_the_prefix() {
			// `b CONTAINS 'x'` on a scalar column is a predicate about the
			// column's own value, not about an element the index keyed.
			let a = analyzer(vec![idx_basic(1, "ix_ab", &["a", "b"])], None);
			let cond = parse_cond("a = 1 AND b CONTAINS 'x'");
			let cands = a.analyze(Some(&cond), None);
			for c in &cands {
				if let BTreeAccess::Compound {
					prefix,
					..
				} = &c.access
				{
					assert!(prefix.len() < 2, "a scalar column is not keyed per element");
				}
			}
		}

		#[test]
		fn middle_column_only_no_candidate() {
			// `b = 2` on INDEX(a, b) — can't use the index because the leading
			// column `a` is not constrained.
			let a = analyzer(vec![idx_basic(1, "ix_ab", &["a", "b"])], None);
			let cond = parse_cond("b = 2");
			let cands = a.analyze(Some(&cond), None);
			// Compound analysis rejects (no leading column); single-column
			// analysis also rejects (b is not the first column of the index).
			assert_no_candidate(&cands, "ix_ab");
		}

		#[test]
		fn non_equality_before_equality_or_range_does_not_shadow_compound_access() {
			let a = scalar_analyzer(vec![idx_current(1, "ix_abc", &["a", "b", "c"])], None);

			// `b != 99` before `b >= 10 AND b < 20` must still capture the closed range on `b`.
			let cond = parse_cond("a = 1 AND b != 99 AND b >= 10 AND b < 20");
			let cands = a.analyze(Some(&cond), None);
			let c = find_for(&cands, "ix_abc").expect("ix_abc");
			assert_eq!(
				c.access,
				BTreeAccess::Compound {
					prefix: vec![Value::from(1).into()],
					range: Some((BinaryOperator::MoreThanEqual, Value::from(10).into())),
					upper: Some((BinaryOperator::LessThan, Value::from(20).into())),
				}
			);

			// `b != 99` or `b > 0` before `b = 5` must still extend the equality prefix through `b`
			// and `c`.
			for text in [
				"a != 99 AND a = 1 AND b != 99 AND b = 5 AND c = 3",
				"a = 1 AND b > 0 AND b = 5 AND c = 3",
			] {
				let cond = parse_cond(text);
				let cands = a.analyze(Some(&cond), None);
				let c = find_for(&cands, "ix_abc").expect("ix_abc");
				assert_eq!(
					c.access,
					BTreeAccess::Compound {
						prefix: vec![
							Value::from(1).into(),
							Value::from(5).into(),
							Value::from(3).into(),
						],
						range: None,
						upper: None,
					},
					"{text}"
				);
			}
		}
	}

	// ------------------------------------------------------------------
	// Wildcard index containment matching
	// ------------------------------------------------------------------
	mod wildcard_containment {
		use super::*;

		#[test]
		fn untyped_column_with_string_scalar_declines_index() {
			let a = analyzer(vec![idx_basic(1, "by_tag", &["tags.*"])], None);
			let cond = parse_cond("tags CONTAINS 'x'");
			let cands = a.analyze(Some(&cond), None);
			assert_no_candidate(&cands, "by_tag");
		}

		#[test]
		fn untyped_column_with_inside_string_scalar_declines_index() {
			let a = analyzer(vec![idx_basic(1, "by_tag", &["tags.*"])], None);
			let cond = parse_cond("'x' INSIDE tags");
			let cands = a.analyze(Some(&cond), None);
			assert_no_candidate(&cands, "by_tag");
		}

		#[test]
		fn untyped_column_with_containsany_strings_declines_expansion() {
			let a = analyzer(vec![idx_basic(1, "by_tag", &["tags.*"])], None);
			let cond = parse_cond("tags CONTAINSANY ['x', 'y']");
			assert!(a.try_containment_expansion(Some(&cond), Direction::Forward).is_none());
		}

		#[test]
		fn untyped_column_with_flattened_query_matches_index() {
			let a = analyzer(vec![idx_basic(1, "by_tag", &["tags.*"])], None);
			let cond = parse_cond("tags.* CONTAINS 'x'");
			let cands = a.analyze(Some(&cond), None);
			let c = find_for(&cands, "by_tag").expect("by_tag");
			let BTreeAccess::Equality(v) = &c.access else {
				panic!("expected Equality, got {:?}", c.access);
			};
			assert_eq!(v.as_literal(), Some(&Value::from("x")));
		}

		#[test]
		fn untyped_column_with_bracket_flattened_query_matches_index() {
			let a = analyzer(vec![idx_basic(1, "by_tag", &["tags.*"])], None);
			let cond = parse_cond("tags[*] CONTAINS 'x'");
			let cands = a.analyze(Some(&cond), None);
			let c = find_for(&cands, "by_tag").expect("by_tag");
			let BTreeAccess::Equality(v) = &c.access else {
				panic!("expected Equality, got {:?}", c.access);
			};
			assert_eq!(v.as_literal(), Some(&Value::from("x")));
		}

		#[test]
		fn untyped_column_with_flatten_step_query_declines_index() {
			// `tags...` returns a stored string unchanged, so it can still
			// match by substring with no element entry.
			let a = analyzer(vec![idx_basic(1, "by_tag", &["tags.*..."])], None);
			for cond in ["tags... CONTAINS 'x'", "'x' INSIDE tags..."] {
				let cands = a.analyze(Some(&parse_cond(cond)), None);
				assert_no_candidate(&cands, "by_tag");
			}
		}

		#[test]
		fn untyped_column_with_non_string_declines_index() {
			// A stored range spans a number, and a stored geometry contains
			// a geometry, without either being an element of the column.
			let a = analyzer(vec![idx_basic(1, "by_tag", &["tags.*"])], None);
			for cond in ["tags CONTAINS 42", "42 INSIDE tags", "tags CONTAINS (1.0, 2.0)"] {
				let cands = a.analyze(Some(&parse_cond(cond)), None);
				assert_no_candidate(&cands, "by_tag");
			}
		}
	}

	// ------------------------------------------------------------------
	// 4. ORDER BY coverage
	// ------------------------------------------------------------------
	mod order_by {
		use super::*;

		#[test]
		fn equality_then_order_by_id_covers() {
			let a = scalar_analyzer(vec![idx_basic(1, "ix_a", &["a"])], None);
			let (cond, order) = parse_cond_order("a = 1", "id");
			let cands = a.analyze(Some(&cond), Some(&order));
			let c = find_for(&cands, "ix_a").expect("ix_a");
			assert!(c.covers_order, "WHERE a=1 ORDER BY id on INDEX(a) covers order");
		}

		#[test]
		fn equality_then_unrelated_order_not_covered() {
			// WHERE a = 1 ORDER BY b on INDEX(a) — the index can't satisfy
			// the requested ordering.
			let a = scalar_analyzer(vec![idx_basic(1, "ix_a", &["a"])], None);
			let (cond, order) = parse_cond_order("a = 1", "b");
			let cands = a.analyze(Some(&cond), Some(&order));
			let c = find_for(&cands, "ix_a").expect("ix_a");
			assert!(!c.covers_order, "ORDER BY non-id non-prefix must not be covered");
		}

		#[test]
		fn order_by_extending_past_index_not_covered() {
			// WHERE a = 1 ORDER BY a, b, c on INDEX(a, b). The index
			// only guarantees (b, id) ordering once a is pinned; the trailing
			// `c` is not in the index, so sort elimination is unsafe.
			let a = scalar_analyzer(vec![idx_basic(1, "ix_ab", &["a", "b"])], None);
			let (cond, order) = parse_cond_order("a = 1", "a, b, c");
			let cands = a.analyze(Some(&cond), Some(&order));
			let c = find_for(&cands, "ix_ab").expect("ix_ab");
			assert!(
				!c.covers_order,
				"ORDER BY extends past the index — sort elimination is unsafe"
			);
		}

		#[test]
		fn order_by_single_col_extending_past_index() {
			// WHERE a > 5 ORDER BY a, b on INDEX(a). The Range candidate
			// only delivers (a, id) order; `b` is unindexed and not in the
			// scan output ordering.
			let a = scalar_analyzer(vec![idx_basic(1, "ix_a", &["a"])], None);
			let (cond, order) = parse_cond_order("a > 5", "a, b");
			let cands = a.analyze(Some(&cond), Some(&order));
			let c = find_for(&cands, "ix_a").expect("ix_a");
			assert!(!c.covers_order, "ORDER BY a, b on INDEX(a) must NOT claim sort elimination");
		}

		#[test]
		fn mixed_asc_desc_not_covered() {
			// WHERE a = 1 ORDER BY a ASC, b DESC on INDEX(a, b). Neither
			// scan direction produces (b ASC) or (b DESC) after `a` is
			// pinned because direction adjustment is whole-scan only.
			//
			// `a = 1` pins the prefix so the leading ASC is trivially
			// satisfied, but the remaining `b DESC` requirement still has
			// to match the scan direction. A forward scan gives `b ASC`;
			// neither direction can mix.
			let a = scalar_analyzer(vec![idx_basic(1, "ix_ab", &["a", "b"])], None);
			let (cond, order) = parse_cond_order("a = 1", "a ASC, b DESC");
			let cands = a.analyze(Some(&cond), Some(&order));
			let c = find_for(&cands, "ix_ab").expect("ix_ab");
			// `index_covers_ordering` returns true here because the leading
			// ASC field references a pinned column and is stripped, then
			// the remaining `b DESC` is checked against a backward scan
			// (which produces `b DESC`) — so this DOES cover. Lock that
			// in: it's correct because direction adjustment can pick the
			// backward scan.
			assert!(c.covers_order, "ORDER BY pinned ASC + col DESC IS coverable by backward scan");
		}

		#[test]
		fn order_by_desc_on_indexed_col_covered() {
			// WHERE a > 5 ORDER BY a DESC on INDEX(a). The analyzer
			// shouldn't lock the direction yet, but should report that
			// some scan direction (Backward here) covers the order.
			let a = scalar_analyzer(vec![idx_basic(1, "ix_a", &["a"])], None);
			let (cond, order) = parse_cond_order("a > 5", "a DESC");
			let cands = a.analyze(Some(&cond), Some(&order));
			let c = find_for(&cands, "ix_a").expect("ix_a");
			assert!(c.covers_order, "ORDER BY DESC coverable via backward scan");
		}

		#[test]
		fn order_by_only_no_where() {
			// ORDER BY a (no WHERE) on INDEX(a) should synthesize a
			// full-range scan with covers_order = true.
			let a = scalar_analyzer(vec![idx_basic(1, "ix_a", &["a"])], None);
			let (_, order, _) = parse_select_parts("ORDER BY a");
			let order = order.expect("ORDER BY");
			let cands = a.analyze(None, Some(&order));
			let c = find_for(&cands, "ix_a").expect("synth ix_a candidate");
			assert!(c.covers_order);
			assert!(matches!(
				c.access,
				BTreeAccess::Range {
					range: OperandRange {
						start: Bound::Unbounded,
						end: Bound::Unbounded,
						..
					}
				}
			));
		}

		#[test]
		fn order_by_a_column_that_may_hold_an_array_is_not_covered() {
			// ORDER BY a on INDEX(a) where `a` is declared no kind: a record
			// holding an array is stored once per element, so the index's key
			// order is element order and covers no ORDER BY on the column.
			let a = analyzer(vec![idx_basic(1, "ix_a", &["a"])], None);
			let (cond, order) = parse_cond_order("a < 5", "a");
			let cands = a.analyze(Some(&cond), Some(&order));
			let c = find_for(&cands, "ix_a").expect("ix_a");
			assert!(!c.covers_order, "a column that may hold an array delivers no order");
			// A range reaching the band arrays sort in is not offered at all: an
			// array whose whole value is in range may have no element inside it.
			let (cond, order) = parse_cond_order("a > 5", "a");
			let cands = a.analyze(Some(&cond), Some(&order));
			assert!(
				find_for(&cands, "ix_a")
					.is_none_or(|c| !matches!(c.access, BTreeAccess::Range { .. })),
				"no range reaching the array band is offered over such a column"
			);
			let (_, order, _) = parse_select_parts("ORDER BY a");
			let cands = a.analyze(None, order.as_ref());
			assert!(
				find_for(&cands, "ix_a").is_none_or(|c| !c.covers_order),
				"no order-covering scan is synthesized over such a column"
			);
		}
	}

	// ------------------------------------------------------------------
	// 5. Hints (WITH INDEX / WITH NOINDEX)
	// ------------------------------------------------------------------
	mod hints {
		use super::*;

		#[test]
		fn with_index_filters_to_named() {
			let defs = vec![idx_basic(1, "ix_a", &["a"]), idx_basic(2, "ix_b", &["a"])];
			let (cond, _, with) = parse_select_parts("WITH INDEX ix_b WHERE a = 1");
			let cond = cond.expect("WHERE");
			let with = with.expect("WITH");
			let a = analyzer(defs, Some(&with));
			let cands = a.analyze(Some(&cond), None);
			// Only the hinted index should remain after WITH INDEX filtering.
			assert_eq!(cands.len(), 1);
			assert_eq!(cands[0].index_ref.name.as_str(), "ix_b");
		}

		#[test]
		fn with_noindex_returns_empty_candidates() {
			// WITH NOINDEX doesn't filter candidates inside analyze(); it
			// short-circuits `select_access_path` to TableScan.  The
			// candidates list is still produced.
			let (cond, _, with) = parse_select_parts("WITH NOINDEX WHERE a = 1");
			let cond = cond.expect("WHERE");
			let with = with.expect("WITH");
			let a = analyzer(vec![idx_basic(1, "ix_a", &["a"])], Some(&with));
			let cands = a.analyze(Some(&cond), None);
			let path = super::super::super::access_path::select_access_path(
				cands,
				Some(&with),
				crate::kvs::Direction::Forward,
			);
			assert!(matches!(path, AccessPath::TableScan), "NOINDEX → TableScan");
		}
	}

	// ------------------------------------------------------------------
	// 6. Negation
	// ------------------------------------------------------------------
	mod negation {
		use super::*;

		#[test]
		fn not_equal_does_not_index() {
			// `a != 5` on a regular Idx index must not produce an Equality
			// or Range candidate; negation inverts the result set.
			let a = analyzer(vec![idx_basic(1, "ix_a", &["a"])], None);
			let cond = parse_cond("a != 5");
			let cands = a.analyze(Some(&cond), None);
			assert_no_candidate(&cands, "ix_a");
		}

		#[test]
		fn negated_predicate_not_indexed() {
			// `NOT (a = 5)` must not generate any candidate for `a`.
			let a = analyzer(vec![idx_basic(1, "ix_a", &["a"])], None);
			let cond = parse_cond("!(a = 5)");
			let cands = a.analyze(Some(&cond), None);
			assert_no_candidate(&cands, "ix_a");
		}

		#[test]
		fn is_not_null_is_not_indexed() {
			// `a != NULL` MUST NOT produce a candidate. NONE sorts before
			// NULL in BTree keys, so an exclusive `> NULL` range would
			// silently drop NONE rows even though `NONE != NULL` is true
			// under SurrealQL semantics. Leaving the predicate to the
			// filter pipeline keeps results correct.
			let a = analyzer(vec![idx_basic(1, "ix_a", &["a"])], None);
			let cond = parse_cond("a != NULL");
			let cands = a.analyze(Some(&cond), None);
			assert_no_candidate(&cands, "ix_a");
		}

		#[test]
		fn is_not_none_uses_index_range() {
			// `a != NONE` is exact: NONE sorts first, so `> NONE` yields
			// every NULL and concrete value, matching filter semantics.
			let a = scalar_analyzer(vec![idx_basic(1, "ix_a", &["a"])], None);
			let cond = parse_cond("a != NONE");
			let cands = a.analyze(Some(&cond), None);
			let c = find_for(&cands, "ix_a").expect("ix_a candidate for != NONE");
			let BTreeAccess::Range {
				range: OperandRange {
					start,
					end,
					..
				},
			} = &c.access
			else {
				panic!("expected exclusive Range from NONE, got {:?}", c.access);
			};
			assert!(matches!(
				start,
				Bound::Excluded(BoundOperand::Literal(crate::val::Value::None))
			));
			assert!(matches!(end, Bound::Unbounded));
		}
	}

	// ------------------------------------------------------------------
	// 7. IN expansion
	// ------------------------------------------------------------------
	mod in_expansion {
		use super::*;

		#[test]
		fn small_in_expands_to_union() {
			let a = analyzer(vec![idx_basic(1, "ix_a", &["a"])], None);
			let cond = parse_cond("a IN [1, 2, 3]");
			let path = a
				.try_in_expansion(Some(&cond), crate::kvs::Direction::Forward)
				.expect("IN expansion");
			let AccessPath::Union {
				paths,
				dedupe,
			} = path
			else {
				panic!("expected Union, got {path:?}");
			};
			assert_eq!(paths.len(), 3);
			assert!(!dedupe, "scalar IN-expansion branches are record-disjoint");
		}

		#[test]
		fn oversized_in_skips_expansion() {
			let a = analyzer(vec![idx_basic(1, "ix_a", &["a"])], None);
			// 33 elements exceeds MAX_IN_EXPANSION_SIZE (32).
			let lit = (1..=33).map(|n| n.to_string()).collect::<Vec<_>>().join(", ");
			let cond = parse_cond(&format!("a IN [{lit}]"));
			let path = a.try_in_expansion(Some(&cond), crate::kvs::Direction::Forward);
			assert!(path.is_none(), "33-element IN should not expand");
		}

		#[test]
		fn in_threshold_boundary_at_32() {
			let a = analyzer(vec![idx_basic(1, "ix_a", &["a"])], None);
			let lit = (1..=32).map(|n| n.to_string()).collect::<Vec<_>>().join(", ");
			let cond = parse_cond(&format!("a IN [{lit}]"));
			let path = a
				.try_in_expansion(Some(&cond), crate::kvs::Direction::Forward)
				.expect("32-element IN expands");
			let AccessPath::Union {
				paths,
				dedupe,
			} = path
			else {
				panic!("expected Union, got {path:?}");
			};
			assert_eq!(paths.len(), 32);
			assert!(!dedupe);
		}
	}

	// ------------------------------------------------------------------
	// 8. OR union
	// ------------------------------------------------------------------
	mod or_union {
		use super::*;

		#[test]
		fn or_both_indexed() {
			let defs = vec![idx_basic(1, "ix_a", &["a"]), idx_basic(2, "ix_b", &["b"])];
			let a = analyzer(defs, None);
			let cond = parse_cond("a = 1 OR b = 2");
			let path = a.try_or_union(Some(&cond), crate::kvs::Direction::Forward).expect("union");
			let AccessPath::Union {
				paths,
				dedupe,
			} = path
			else {
				panic!("expected Union, got {path:?}");
			};
			assert_eq!(paths.len(), 2);
			assert!(dedupe, "OR branches may both hold on the same row");
		}

		#[test]
		fn or_one_branch_unindexed_no_union() {
			// b has no index → union fails → caller falls back to TableScan.
			let a = analyzer(vec![idx_basic(1, "ix_a", &["a"])], None);
			let cond = parse_cond("a = 1 OR b = 2");
			let path = a.try_or_union(Some(&cond), crate::kvs::Direction::Forward);
			assert!(path.is_none(), "unindexed branch defeats union");
		}

		#[test]
		fn or_drops_empty_branch_from_union() {
			// `(a > 10 AND a < 5) OR b = 1` — the first branch is
			// provably empty after range merging, so it must be dropped
			// from the union (a zero-row branch contributes nothing to
			// OR semantics). With only one surviving branch the union
			// degenerates to a plain BTreeScan on `idx_b`.
			let a = scalar_analyzer(
				vec![idx_basic(1, "ix_a", &["a"]), idx_basic(2, "ix_b", &["b"])],
				None,
			);
			let cond = parse_cond("(a > 10 AND a < 5) OR b = 1");
			let path = a
				.try_or_union(Some(&cond), crate::kvs::Direction::Forward)
				.expect("union or degenerate path");
			let AccessPath::BTreeScan {
				index_ref,
				access,
				..
			} = path
			else {
				if matches!(path, AccessPath::Union { .. }) {
					panic!("empty branch should have been dropped, leaving a single path");
				}
				panic!("expected BTreeScan(ix_b), got {path:?}");
			};
			assert_eq!(index_ref.name.as_str(), "ix_b");
			assert!(matches!(access, BTreeAccess::Equality(_)));
		}

		#[test]
		fn or_all_branches_empty_yields_empty_scan() {
			// Every branch's range contradicts, so the OR is empty.
			let a = scalar_analyzer(vec![idx_basic(1, "ix_a", &["a"])], None);
			let cond = parse_cond("(a > 10 AND a < 5) OR (a > 100 AND a < 50)");
			let path =
				a.try_or_union(Some(&cond), crate::kvs::Direction::Forward).expect("union path");
			assert!(matches!(path, AccessPath::EmptyScan));
		}

		#[test]
		fn nested_or_in_and_builds_ft_union() {
			// `type = 'doc' AND (title @@ 'q' OR body @@ 'q')`: the OR of two
			// full-text matches nested inside the AND must still form a union
			// over the two FT indexes. `try_or_union` alone misses it because
			// the top level is an AND, not an OR.
			let defs = vec![
				idx_basic(1, "ix_type", &["type"]),
				idx_ft(2, "ft_title", &["title"]),
				idx_ft(3, "ft_body", &["body"]),
			];
			let a = analyzer(defs, None);
			let cond = parse_cond("type = 'doc' AND (title @@ 'q' OR body @@ 'q')");
			let (path, score) = a
				.try_and_nested_or_union(Some(&cond), crate::kvs::Direction::Forward)
				.expect("nested OR union");
			let AccessPath::Union {
				paths,
				dedupe,
			} = path
			else {
				panic!("expected Union, got {path:?}");
			};
			assert_eq!(paths.len(), 2, "one branch per FT match");
			assert!(dedupe, "a row may match both FT branches");
			// Effective score is the min branch score. Both branches are
			// full-text (800), which outranks the non-unique `type` equality
			// (500), so the SELECT planner switches to the union.
			assert_eq!(score, 800, "min branch score is the FT score");
		}

		#[test]
		fn nested_or_skipped_under_with_index_hint() {
			// WITH INDEX pins the plan; the nested-OR optimisation must bow out.
			let with = With::Index(vec!["ft_title".to_string(), "ft_body".to_string()]);
			let defs = vec![
				idx_basic(1, "ix_type", &["type"]),
				idx_ft(2, "ft_title", &["title"]),
				idx_ft(3, "ft_body", &["body"]),
			];
			let a = analyzer(defs, Some(&with));
			let cond = parse_cond("type = 'doc' AND (title @@ 'q' OR body @@ 'q')");
			assert!(
				a.try_and_nested_or_union(Some(&cond), crate::kvs::Direction::Forward).is_none(),
				"WITH INDEX hint disables the nested-OR union"
			);
		}

		#[test]
		fn nested_or_unindexed_branch_no_union() {
			// One OR branch (`note @@ 'q'`) has no FT index → the OR cannot be
			// unioned, so no nested-OR union is offered (the SELECT planner
			// keeps driving from the `type` equality and filters the OR).
			let defs = vec![idx_basic(1, "ix_type", &["type"]), idx_ft(2, "ft_title", &["title"])];
			let a = analyzer(defs, None);
			let cond = parse_cond("type = 'doc' AND (title @@ 'q' OR note @@ 'q')");
			assert!(
				a.try_and_nested_or_union(Some(&cond), crate::kvs::Direction::Forward).is_none(),
				"an unindexed OR branch defeats the union"
			);
		}

		#[test]
		fn and_without_or_conjunct_no_union() {
			// A pure conjunction has no OR conjunct to drive a union.
			let defs = vec![idx_basic(1, "ix_a", &["a"]), idx_basic(2, "ix_b", &["b"])];
			let a = analyzer(defs, None);
			let cond = parse_cond("a = 1 AND b = 2");
			assert!(
				a.try_and_nested_or_union(Some(&cond), crate::kvs::Direction::Forward).is_none(),
				"no OR conjunct means no nested-OR union"
			);
		}

		#[test]
		fn nested_or_skipped_when_knn_present() {
			// A KNN operator must drive from its KnnScan (it is stripped from
			// the WHERE, not evaluated as a residual). Even though the email OR
			// would otherwise union (unique index, score 1000 > KNN's 800), the
			// presence of the KNN operator must disable the substitution so the
			// nearest-neighbour restriction is not silently dropped.
			let defs = vec![idx_uniq(1, "ix_email", &["email"])];
			let a = analyzer(defs, None);
			let cond = parse_cond("vec <|2,100|> [0.0, 0.0] AND (email = 'a' OR email = 'b')");
			assert!(
				a.try_and_nested_or_union(Some(&cond), crate::kvs::Direction::Forward).is_none(),
				"a KNN operator in the condition must disable the nested-OR union"
			);
		}
	}

	// ------------------------------------------------------------------
	// KNN prefilter split
	// ------------------------------------------------------------------
	mod knn_prefilter {
		use super::bitmap_fusion::idx_v2;
		use super::*;
		use crate::catalog::{Distance, HnswParams, VectorType};
		use crate::kvs::Direction;

		fn idx_hnsw(id: u32, name: &str, cols: &[&str]) -> IndexDefinition {
			idx_def(
				id,
				name,
				cols,
				Index::Hnsw(HnswParams {
					dimension: 2,
					distance: Distance::Euclidean,
					vector_type: VectorType::F32,
					m: 12,
					m0: 24,
					ml: 0.4.into(),
					ef_construction: 150,
					extend_candidates: false,
					keep_pruned_connections: false,
					use_hashed_vector: false,
				}),
			)
		}

		/// Split the KNN-stripped WHERE the way `try_knn_prefilter_plan` does.
		fn split(
			az: &IndexAnalyzer<'_>,
			cond: &LogicalExpr,
			exact: bool,
		) -> Option<KnnPrefilterPlan> {
			let stripped = crate::exec::planner::util::strip_knn_from_condition(cond)
				.expect("a non-KNN residual remains");
			az.try_knn_prefilter(&stripped, &move |_, _| exact)
		}

		#[test]
		fn covered_and_residual_conjuncts_split() {
			let az =
				analyzer(vec![idx_v2(1, "idx_a", &["a"]), idx_hnsw(2, "hn_vec", &["vec"])], None);
			let cond = parse_cond("a = 1 AND unindexed = 2 AND vec <|2,100|> [0.0, 0.0]");
			let plan = split(&az, &cond, true).expect("the equality on `a` is coverable");
			assert!(
				matches!(plan.root, BitmapPlan::BTree { .. }),
				"single covered conjunct forms the bitmap root: {:?}",
				plan.root
			);
			let residual = plan.residual.expect("the unindexed conjunct stays residual");
			assert_eq!(surrealdb_types::ToSql::to_sql(&residual), "unindexed = 2");
		}

		#[test]
		fn inexact_column_joins_as_a_superset_and_stays_residual() {
			// A column not known to hold one entry per record answers the
			// equality only as a superset: it narrows the allow-list and is
			// still judged.
			let az =
				analyzer(vec![idx_v2(1, "idx_a", &["a"]), idx_hnsw(2, "hn_vec", &["vec"])], None);
			let cond = parse_cond("a = 1 AND vec <|2,100|> [0.0, 0.0]");
			let plan = split(&az, &cond, false).expect("the equality narrows the allow-list");
			assert!(
				matches!(plan.root, BitmapPlan::BTree { .. }),
				"the superset leaf forms the bitmap root: {:?}",
				plan.root
			);
			let residual = plan.residual.expect("the superset conjunct stays residual");
			assert_eq!(surrealdb_types::ToSql::to_sql(&residual), "a = 1");
		}

		#[test]
		fn a_number_over_an_index_that_keeps_trailing_zeros_stays_residual() {
			// Such an index keys `1.0dec` apart from `1`, so its entries at `1`
			// hold neither the truth set of `a = 1` nor a superset of it.
			let mut older = idx_v2(1, "idx_a", &["a"]);
			older.format_version = crate::catalog::BTREE_NORMALISED_DECIMAL_KEYS_FORMAT_VERSION - 1;
			let az = scalar_analyzer(vec![older, idx_hnsw(2, "hn_vec", &["vec"])], None);
			let cond = parse_cond("a = 1 AND vec <|2,100|> [0.0, 0.0]");
			assert!(split(&az, &cond, true).is_none());
			assert!(split(&az, &cond, false).is_none());
			// A string is keyed where it compares in every format.
			let cond = parse_cond("a = 'x' AND vec <|2,100|> [0.0, 0.0]");
			let plan = split(&az, &cond, true).expect("the equality covers the conjunct");
			assert!(plan.residual.is_none(), "{:?}", plan.residual);
		}

		#[test]
		fn inexact_range_stays_residual() {
			// A range over a column that may hold an array is no superset of
			// its truth set (`[] > 0` holds, an empty array is not stored
			// above 0), so it neither narrows the allow-list nor leaves the
			// residual.
			let az =
				analyzer(vec![idx_v2(1, "idx_a", &["a"]), idx_hnsw(2, "hn_vec", &["vec"])], None);
			let cond = parse_cond("a > 1 AND vec <|2,100|> [0.0, 0.0]");
			assert!(split(&az, &cond, false).is_none());
			assert!(split(&az, &cond, true).is_none());
			// Over a column its index has recorded storing no array, the range
			// is exact and covers the conjunct.
			let az = scalar_analyzer(
				vec![idx_v2(1, "idx_a", &["a"]), idx_hnsw(2, "hn_vec", &["vec"])],
				None,
			);
			let plan = split(&az, &cond, true).expect("an exact range covers the conjunct");
			assert!(plan.residual.is_none(), "{:?}", plan.residual);
		}

		#[test]
		fn negation_stays_residual() {
			let az =
				analyzer(vec![idx_v2(1, "idx_a", &["a"]), idx_hnsw(2, "hn_vec", &["vec"])], None);
			let cond = parse_cond("a = 1 AND !(a = 2) AND vec <|2,100|> [0.0, 0.0]");
			let plan = split(&az, &cond, true).expect("the positive equality is coverable");
			let residual = plan.residual.expect("the negation stays residual");
			assert!(
				surrealdb_types::ToSql::to_sql(&residual).contains('!'),
				"negation kept: {}",
				surrealdb_types::ToSql::to_sql(&residual)
			);
		}

		#[test]
		fn uncoverable_matches_conjunct_excluded_from_residual() {
			// MATCHES is not evaluable inside the ANN traversal; with no
			// full-text index it cannot be covered either, so it must vanish
			// from the split entirely (the outer Filter enforces it).
			let az =
				analyzer(vec![idx_v2(1, "idx_a", &["a"]), idx_hnsw(2, "hn_vec", &["vec"])], None);
			let cond = parse_cond("a = 1 AND body @@ 'x' AND vec <|2,100|> [0.0, 0.0]");
			let plan = split(&az, &cond, true).expect("the equality is coverable");
			assert!(
				plan.residual.is_none(),
				"an uncoverable MATCHES must not become an in-traversal residual: {:?}",
				plan.residual
			);
		}

		#[test]
		fn knn_candidate_drives_over_unique_equality() {
			// The unique equality outscores KNN (1000 > 800),
			// but a KNN operator can only be computed by its KnnScan, so the
			// KNN candidate must drive unconditionally.
			let az = analyzer(
				vec![idx_uniq(1, "ix_email", &["email"]), idx_hnsw(2, "hn_vec", &["vec"])],
				None,
			);
			let cond = parse_cond("email = 'a@x.com' AND vec <|2,100|> [0.0, 0.0]");
			let candidates = az.analyze(Some(&cond), None);
			let path = crate::exec::index::access_path::select_access_path(
				candidates,
				None,
				Direction::Forward,
			);
			assert!(
				matches!(path, AccessPath::KnnSearch { .. }),
				"the KNN candidate must drive the plan: {path:?}"
			);
		}

		#[test]
		fn a_hint_feeding_the_knn_prefilter_is_not_overridden() {
			use crate::exec::index::access_path::{knn_overrides_hint, select_access_path};
			let cond = parse_cond("a = 1 AND vec <|2,100|> [0.0, 0.0]");
			let stripped = parse_cond("a = 1");
			// `prefiltered`: whether the prefilter is built, as it is unless
			// prefiltering is off or no hinted index can supply the allow-list.
			for (hinted, prefiltered, overridden) in [
				("idx_a", true, false),
				("idx_a", false, true),
				("hn_vec", false, false),
				("idx_z", true, true),
			] {
				let names = [hinted.to_owned()];
				let hint = crate::expr::With::Index(names.to_vec());
				let az = analyzer(
					vec![idx_v2(1, "idx_a", &["a"]), idx_hnsw(2, "hn_vec", &["vec"])],
					Some(&hint),
				);
				let mut path = select_access_path(
					az.analyze(Some(&cond), None),
					Some(&hint),
					Direction::Forward,
				);
				let AccessPath::KnnSearch {
					prefilter,
					..
				} = &mut path
				else {
					panic!("the KNN candidate drives: {path:?}");
				};
				if prefiltered {
					*prefilter = az.try_knn_prefilter(&stripped, &|_, _| true);
				}
				assert_eq!(
					knn_overrides_hint(&path, &names),
					overridden,
					"`{hinted}`, prefiltered: {prefiltered}"
				);
			}
		}

		#[test]
		fn with_index_hint_composes_with_knn_and_prefilter() {
			let hint = crate::expr::With::Index(vec!["idx_a".to_owned()]);
			let az = analyzer(
				vec![
					idx_v2(1, "idx_a", &["a"]),
					idx_v2(2, "idx_b", &["b"]),
					idx_hnsw(3, "hn_vec", &["vec"]),
				],
				Some(&hint),
			);
			let cond = parse_cond("a = 1 AND b = 2 AND vec <|2,100|> [0.0, 0.0]");
			let candidates = az.analyze(Some(&cond), None);
			let path = crate::exec::index::access_path::select_access_path(
				candidates,
				Some(&hint),
				Direction::Forward,
			);
			assert!(
				matches!(&path, AccessPath::KnnSearch { index_ref, .. } if index_ref.name == "hn_vec"),
				"KNN must still be chosen when WITH INDEX names a pre-filter index: {path:?}"
			);
			let plan = split(&az, &cond, true).expect("hinted idx_a covers a = 1");
			assert!(
				matches!(&plan.root, BitmapPlan::BTree { index_ref, .. } if index_ref.name == "idx_a"),
				"only hinted idx_a should be in the prefilter bitmap: {:?}",
				plan.root
			);
			let residual = plan.residual.expect("unhinted b = 2 stays residual");
			assert_eq!(surrealdb_types::ToSql::to_sql(&residual), "b = 2");

			// When multiple ANN indexes exist on `vec` and WITH INDEX names the second one,
			// `select_access_path` must choose the hinted ANN index rather than the first one.
			let hint_ann = crate::expr::With::Index(vec!["hn_vec_2".to_owned()]);
			let az_multi_ann = analyzer(
				vec![idx_hnsw(3, "hn_vec_1", &["vec"]), idx_hnsw(4, "hn_vec_2", &["vec"])],
				Some(&hint_ann),
			);
			let multi_cands = az_multi_ann.analyze(Some(&cond), None);
			let multi_path = crate::exec::index::access_path::select_access_path(
				multi_cands,
				Some(&hint_ann),
				Direction::Forward,
			);
			assert!(
				matches!(&multi_path, AccessPath::KnnSearch { index_ref, .. } if index_ref.name == "hn_vec_2"),
				"WITH INDEX naming a specific ANN index must select it: {multi_path:?}"
			);
		}

		#[test]
		fn order_by_vector_distance_limit_plans_knn_candidate() {
			let mut ix_cos = idx_hnsw(3, "hn_cos", &["cos_vec"]);
			if let Index::Hnsw(ref mut p) = ix_cos.index {
				p.distance = Distance::Cosine;
			}
			let indexes = vec![idx_v2(1, "idx_a", &["a"]), idx_hnsw(2, "hn_vec", &["vec"]), ix_cos];
			let az = analyzer(indexes.clone(), None)
				.with_limit(Some(5))
				.with_complete_vector(Arc::from([false, true, true]));

			// 1. Standard ORDER BY vector::distance::euclidean(vec, [0.0, 0.0]) ASC LIMIT 5
			let (cond, order) =
				parse_cond_order("a = 1", "vector::distance::euclidean(vec, [0.0, 0.0]) ASC");
			let cands = az.analyze(Some(&cond), Some(&order));
			let knn = find_for(&cands, "hn_vec").expect("hn_vec should match euclidean distance");
			assert!(knn.covers_order);
			assert!(matches!(
				knn.access,
				BTreeAccess::Knn {
					k: 5,
					..
				}
			));

			// 2. Commuted arguments: vector::distance::cosine([1.0, 0.0], cos_vec)
			let (_, cos_order) =
				parse_cond_order("a = 1", "vector::distance::cosine([1.0, 0.0], cos_vec)");
			let cos_cands = az.analyze(Some(&cond), Some(&cos_order));
			let cos_knn = find_for(&cos_cands, "hn_cos").expect("hn_cos should match cosine");
			assert!(cos_knn.covers_order);
			assert!(matches!(
				cos_knn.access,
				BTreeAccess::Knn {
					k: 5,
					..
				}
			));

			// 3. DESC or mismatched metric or missing LIMIT does not plan KnnScan
			let (_, desc_order) =
				parse_cond_order("a = 1", "vector::distance::euclidean(vec, [0.0, 0.0]) DESC");
			assert_no_candidate(&az.analyze(Some(&cond), Some(&desc_order)), "hn_vec");
			let (_, mismatch_order) =
				parse_cond_order("a = 1", "vector::distance::manhattan(vec, [0.0, 0.0]) ASC");
			assert_no_candidate(&az.analyze(Some(&cond), Some(&mismatch_order)), "hn_vec");
			let az_no_limit =
				analyzer(vec![idx_hnsw(2, "hn_vec", &["vec"])], None).with_limit(None);
			assert_no_candidate(&az_no_limit.analyze(Some(&cond), Some(&order)), "hn_vec");

			// 4. Empty array, wrong dimension, NaN, or non-array literal does not plan KnnScan
			for invalid_order in [
				"vector::distance::euclidean(vec, []) ASC",
				"vector::distance::euclidean(vec, [0.0, 0.0, 0.0]) ASC",
				"vector::distance::euclidean(vec, [0.0, NaN]) ASC",
				"vector::distance::euclidean(vec, 'not_a_vec') ASC",
			] {
				let (_, bad_order) = parse_cond_order("a = 1", invalid_order);
				assert_no_candidate(&az.analyze(Some(&cond), Some(&bad_order)), "hn_vec");
			}

			// 5. An index that may hold no entry for some record (one whose vector is `NONE` or
			//    `NULL`, or one whose shapes are untracked) does not answer the ordering.
			let az_incomplete = analyzer(indexes.clone(), None).with_limit(Some(5));
			assert_no_candidate(&az_incomplete.analyze(Some(&cond), Some(&order)), "hn_vec");
			let az_cos_only = analyzer(indexes, None)
				.with_limit(Some(5))
				.with_complete_vector(Arc::from([false, false, true]));
			assert_no_candidate(&az_cos_only.analyze(Some(&cond), Some(&order)), "hn_vec");
			assert!(
				find_for(&az_cos_only.analyze(Some(&cond), Some(&cos_order)), "hn_cos").is_some(),
				"a complete index still answers"
			);
		}
	}

	// ------------------------------------------------------------------
	// 9. Range merging — same-index, contradictions, tightening
	// ------------------------------------------------------------------
	mod range_merge {
		use super::*;
		use crate::kvs::Direction;

		fn select_path(cands: Vec<IndexCandidate>) -> AccessPath {
			super::super::super::access_path::select_access_path(cands, None, Direction::Forward)
		}

		#[test]
		fn contradictory_range_yields_empty_scan() {
			// `a > 10 AND a < 5` — contradiction must short-circuit to
			// AccessPath::EmptyScan.
			let a = scalar_analyzer(vec![idx_basic(1, "ix_a", &["a"])], None);
			let cond = parse_cond("a > 10 AND a < 5");
			let cands = a.analyze(Some(&cond), None);
			let c = find_for(&cands, "ix_a").expect("ix_a candidate");
			assert!(c.empty, "contradiction must mark candidate empty");
			assert!(matches!(select_path(cands), AccessPath::EmptyScan));
		}

		#[test]
		fn singleton_range_inclusive_both_sides() {
			// `a >= 5 AND a <= 5` is a singleton, NOT empty.
			let a = analyzer(vec![idx_basic(1, "ix_a", &["a"])], None);
			let cond = parse_cond("a >= 5 AND a <= 5");
			let cands = a.analyze(Some(&cond), None);
			let c = find_for(&cands, "ix_a").expect("ix_a candidate");
			assert!(!c.empty, "inclusive both sides on same value is non-empty");
			let BTreeAccess::Range {
				range: OperandRange {
					start,
					end,
					..
				},
			} = &c.access
			else {
				panic!("expected Range, got {:?}", c.access);
			};
			assert!(matches!(start, Bound::Included(_)));
			assert!(matches!(end, Bound::Included(_)));
		}

		#[test]
		fn equal_values_with_one_exclusive_is_empty() {
			// `a > 5 AND a <= 5` — exclusive lower equal to inclusive upper
			// has no satisfying value.
			let a = scalar_analyzer(vec![idx_basic(1, "ix_a", &["a"])], None);
			let cond = parse_cond("a > 5 AND a <= 5");
			let cands = a.analyze(Some(&cond), None);
			let c = find_for(&cands, "ix_a").expect("ix_a candidate");
			assert!(c.empty, "x > 5 AND x <= 5 is empty");
		}

		#[test]
		fn same_side_from_bounds_keep_tighter() {
			// `a > 5 AND a > 10` → from bound tightens to > 10.
			let a = scalar_analyzer(vec![idx_basic(1, "ix_a", &["a"])], None);
			let cond = parse_cond("a > 5 AND a > 10");
			let cands = a.analyze(Some(&cond), None);
			let c = find_for(&cands, "ix_a").expect("ix_a");
			let BTreeAccess::Range {
				range,
			} = &c.access
			else {
				panic!("expected single tightened Range, got {:?}", c.access);
			};
			assert!(
				matches!(range.start.clone(), Bound::Excluded(BoundOperand::Literal(crate::val::Value::Number(n))) if n.to_int() == 10)
			);
			assert!(matches!(range.end, Bound::Unbounded));
		}

		#[test]
		fn same_side_to_bounds_keep_tighter() {
			// `a < 100 AND a < 50` → to bound tightens to < 50.
			let a = analyzer(vec![idx_basic(1, "ix_a", &["a"])], None);
			let cond = parse_cond("a < 100 AND a < 50");
			let cands = a.analyze(Some(&cond), None);
			let c = find_for(&cands, "ix_a").expect("ix_a");
			let BTreeAccess::Range {
				range,
			} = &c.access
			else {
				panic!("expected single tightened Range, got {:?}", c.access);
			};
			assert!(matches!(range.start, Bound::Unbounded));
			assert!(
				matches!(range.end.clone(), Bound::Excluded(BoundOperand::Literal(crate::val::Value::Number(n))) if n.to_int() == 50)
			);
		}

		#[test]
		fn same_side_mixed_inclusive_picks_exclusive() {
			// `a > 5 AND a >= 5` — same value, exclusive is tighter.
			let a = scalar_analyzer(vec![idx_basic(1, "ix_a", &["a"])], None);
			let cond = parse_cond("a > 5 AND a >= 5");
			let cands = a.analyze(Some(&cond), None);
			let c = find_for(&cands, "ix_a").expect("ix_a");
			let BTreeAccess::Range {
				range: OperandRange {
					start: Bound::Excluded(_),
					..
				},
			} = &c.access
			else {
				panic!("expected Range with inclusive start bound, got {:?}", c.access);
			};
		}
	}

	// ------------------------------------------------------------------
	// 10. Scoring monotonicity
	// ------------------------------------------------------------------
	mod scoring {
		use super::*;

		#[test]
		fn unique_equality_beats_non_unique() {
			let uniq_cand = IndexCandidate {
				index_ref: IndexRef::new(&[idx_uniq(1, "u", &["a"])], 0),
				access: BTreeAccess::Equality(
					crate::val::Value::Number(crate::val::Number::Int(1)).into(),
				),
				covers_order: false,
				empty: false,
			};
			let nonunique_cand = IndexCandidate {
				index_ref: IndexRef::new(&[idx_basic(1, "i", &["a"])], 0),
				access: BTreeAccess::Equality(
					crate::val::Value::Number(crate::val::Number::Int(1)).into(),
				),
				covers_order: false,
				empty: false,
			};
			assert!(
				uniq_cand.score() > nonunique_cand.score(),
				"unique equality must outscore non-unique"
			);
			// `NONE` and `NULL` are not unique on a default UNIQUE index, while
			// `NULL` is unique on a `UNIQUE NULLS NOT DISTINCT` index.
			let uniq_null = IndexCandidate {
				index_ref: IndexRef::new(&[idx_uniq(1, "u", &["a"])], 0),
				access: BTreeAccess::Equality(crate::val::Value::Null.into()),
				covers_order: false,
				empty: false,
			};
			assert_eq!(uniq_null.score(), nonunique_cand.score());
			let mut nnd_def = idx_uniq(1, "u_nnd", &["a"]);
			nnd_def.nulls_not_distinct = true;
			let nnd_null = IndexCandidate {
				index_ref: IndexRef::new(&[nnd_def.clone()], 0),
				access: BTreeAccess::Equality(crate::val::Value::Null.into()),
				covers_order: false,
				empty: false,
			};
			let nnd_none = IndexCandidate {
				index_ref: IndexRef::new(&[nnd_def], 0),
				access: BTreeAccess::Equality(crate::val::Value::None.into()),
				covers_order: false,
				empty: false,
			};
			assert_eq!(nnd_null.score(), uniq_cand.score());
			assert_eq!(nnd_none.score(), nonunique_cand.score());

			// Compound unique prefix: `(1, NULL)` is ≤1 row only under
			// `NULLS NOT DISTINCT`, and `(1, NONE)` is never ≤1 row.
			let comp_uniq_def = idx_uniq(1, "cu", &["a", "b"]);
			let mut comp_nnd_def = idx_uniq(1, "cu_nnd", &["a", "b"]);
			comp_nnd_def.nulls_not_distinct = true;
			let make_comp = |def: IndexDefinition, b: crate::val::Value| IndexCandidate {
				index_ref: IndexRef::new(&[def], 0),
				access: BTreeAccess::Compound {
					prefix: vec![crate::val::Value::from(1).into(), b.into()],
					range: None,
					upper: None,
				},
				covers_order: false,
				empty: false,
			};
			assert!(!IndexAnalyzer::candidate_returns_at_most_one_row(&make_comp(
				comp_uniq_def,
				crate::val::Value::Null,
			)));
			assert!(IndexAnalyzer::candidate_returns_at_most_one_row(&make_comp(
				comp_nnd_def.clone(),
				crate::val::Value::Null,
			)));
			assert!(!IndexAnalyzer::candidate_returns_at_most_one_row(&make_comp(
				comp_nnd_def,
				crate::val::Value::None,
			)));

			// An object key is not exempt from uniqueness.
			let uniq_obj = IndexCandidate {
				index_ref: IndexRef::new(&[idx_uniq(1, "u", &["a"])], 0),
				access: BTreeAccess::Equality(crate::val::Value::Object(Default::default()).into()),
				covers_order: false,
				empty: false,
			};
			assert_eq!(uniq_obj.score(), uniq_cand.score());
		}

		#[test]
		fn outranks_key_range_respects_nulls_not_distinct() {
			let mut nnd_def = idx_uniq(1, "u_nnd", &["a"]);
			nnd_def.nulls_not_distinct = true;
			let make = |def: IndexDefinition, v: crate::val::Value| IndexCandidate {
				index_ref: IndexRef::new(&[def], 0),
				access: BTreeAccess::Equality(v.into()),
				covers_order: false,
				empty: false,
			};
			// Against a 1-component key range (`pinned = 1`), an equality only
			// outranks when it returns at most one row.
			let uniq_null = make(idx_uniq(1, "u", &["a"]), crate::val::Value::Null);
			let nnd_null = make(nnd_def.clone(), crate::val::Value::Null);
			let nnd_none = make(nnd_def, crate::val::Value::None);
			assert!(!uniq_null.outranks_key_range(1, false, false));
			assert!(nnd_null.outranks_key_range(1, false, false));
			assert!(!nnd_none.outranks_key_range(1, false, false));
		}

		#[test]
		fn bounded_range_beats_half_bounded() {
			let make = |from_some, to_some| IndexCandidate {
				index_ref: IndexRef::new(&[idx_basic(1, "i", &["a"])], 0),
				access: BTreeAccess::Range {
					range: Range {
						start: if from_some {
							Bound::Included(crate::val::Value::Number(crate::val::Number::Int(0)))
						} else {
							Bound::Unbounded
						},
						end: if to_some {
							Bound::Included(crate::val::Value::Number(crate::val::Number::Int(10)))
						} else {
							Bound::Unbounded
						},
					}
					.into(),
				},
				covers_order: false,
				empty: false,
			};
			assert!(make(true, true).score() > make(true, false).score());
			assert!(make(true, false).score() > make(false, false).score());
		}

		#[test]
		fn compound_prefix_bonus_is_capped() {
			// A 12-column compound prefix must not silently outscore a
			// unique equality (which is at most one row).
			let big_prefix = (0..12u32)
				.map(|i| crate::val::Value::Number(crate::val::Number::Int(i as i64)).into())
				.collect::<Vec<_>>();
			let wide = IndexCandidate {
				index_ref: IndexRef::new(
					&[idx_basic(
						1,
						"i_wide",
						&["a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l"],
					)],
					0,
				),
				access: BTreeAccess::Compound {
					prefix: big_prefix,
					range: None,
					upper: None,
				},
				covers_order: false,
				empty: false,
			};
			let unique_eq = IndexCandidate {
				index_ref: IndexRef::new(&[idx_uniq(2, "u_a", &["a"])], 0),
				access: BTreeAccess::Equality(
					crate::val::Value::Number(crate::val::Number::Int(1)).into(),
				),
				covers_order: false,
				empty: false,
			};
			assert!(
				unique_eq.score() > wide.score(),
				"unique equality must outscore wide compound prefix"
			);
		}

		#[test]
		fn covers_order_provides_positive_bonus() {
			let make = |covers| IndexCandidate {
				index_ref: IndexRef::new(&[idx_basic(1, "i", &["a"])], 0),
				access: BTreeAccess::Range {
					range: Range {
						start: Bound::Included(crate::val::Value::Number(crate::val::Number::Int(
							0,
						))),
						end: Bound::Unbounded,
					}
					.into(),
				},
				covers_order: covers,
				empty: false,
			};
			assert!(make(true).score() > make(false).score());
		}

		#[test]
		fn under_a_limit_an_order_covering_scan_outranks_a_range_but_not_an_equality() {
			let int = |n| crate::val::Value::Number(crate::val::Number::Int(n));
			let ordered = IndexCandidate {
				index_ref: IndexRef::new(&[idx_basic(1, "by_b", &["b"])], 0),
				access: BTreeAccess::Range {
					range: Range::unbounded().into(),
				},
				covers_order: true,
				empty: false,
			};
			let range = IndexCandidate {
				index_ref: IndexRef::new(&[idx_basic(2, "by_a", &["a"])], 0),
				access: BTreeAccess::Range {
					range: Range {
						start: Bound::Included(int(0)),
						end: Bound::Included(int(10)),
					}
					.into(),
				},
				covers_order: false,
				empty: false,
			};
			let equality = IndexCandidate {
				index_ref: IndexRef::new(&[idx_basic(3, "by_c", &["c"])], 0),
				access: BTreeAccess::Equality(int(1).into()),
				covers_order: false,
				empty: false,
			};
			let prefix = IndexCandidate {
				index_ref: IndexRef::new(&[idx_basic(4, "by_cd", &["c", "d"])], 0),
				access: BTreeAccess::Compound {
					prefix: vec![int(1).into()],
					range: None,
					upper: None,
				},
				covers_order: false,
				empty: false,
			};
			assert!(range.rank(false) > ordered.rank(false));
			assert!(ordered.rank(true) > range.rank(true));
			assert!(equality.rank(true) > ordered.rank(true));
			assert!(prefix.rank(true) > ordered.rank(true));
			assert_eq!(range.rank(true), range.score());
		}

		#[test]
		fn under_a_limit_a_fanning_order_scan_earns_nothing() {
			let fanning = IndexCandidate {
				index_ref: IndexRef::new(&[idx_basic(1, "by_tags", &["tags[*]"])], 0),
				access: BTreeAccess::Range {
					range: Range::unbounded().into(),
				},
				covers_order: true,
				empty: false,
			};
			assert_eq!(fanning.rank(true), fanning.score());
		}
	}

	// ------------------------------------------------------------------
	// 5. Columns that address components of the record id
	// ------------------------------------------------------------------
	mod derived_columns {
		use std::ops::Bound;

		use super::*;
		use crate::exec::index::derived_columns::range_is_widened;
		use crate::expr::{KeyNames, Kind, KindLiteral};
		use crate::val::Datetime;

		/// `[feed: string, at: datetime, n: int]`.
		fn key() -> Arc<KeyStructure> {
			let names = ["feed", "at", "n"].iter().map(|name| Strand::new(*name)).collect();
			Arc::new(
				KeyStructure::from_id_field(
					&Kind::Literal(KindLiteral::Array(vec![
						Kind::String,
						Kind::Datetime,
						Kind::Int,
					])),
					&KeyNames::new(names, 3).expect("valid names"),
				)
				.expect("a keyed type"),
			)
		}

		/// An analyzer over a table whose id names its components, and whose
		/// indexed columns are ones the index has recorded storing no array in
		/// (a component never holds one).
		fn keyed<'a>(defs: Vec<IndexDefinition>) -> IndexAnalyzer<'a> {
			scalar_analyzer(defs, None).with_key(Some(key()))
		}

		fn datetime(text: &str) -> Value {
			Value::Datetime(Datetime(
				chrono::DateTime::parse_from_rfc3339(text)
					.expect("an RFC 3339 datetime")
					.with_timezone(&chrono::Utc),
			))
		}

		#[test]
		fn a_range_over_a_datetime_component_reads_the_whole_seconds_of_its_bounds() {
			let a = keyed(vec![idx_basic(1, "by_at", &["at"])]);
			let cond =
				parse_cond("at > d'2025-01-01T10:00:01Z' AND at <= d'2025-01-01T10:00:05.500Z'");
			let cands = a.analyze(Some(&cond), None);
			let c = find_for(&cands, "by_at").expect("by_at");
			let BTreeAccess::Range {
				range,
			} = &c.access
			else {
				panic!("expected Range, got {:?}", c.access);
			};
			assert_eq!(
				range.start,
				Bound::Included(datetime("2025-01-01T10:00:00.999999999Z").into())
			);
			assert_eq!(range.end, Bound::Included(datetime("2025-01-01T10:00:05Z").into()));
			assert!(range_is_widened(&c.index_ref, &c.access), "the scan answers no conjunct");
		}

		#[test]
		fn a_component_spelled_as_an_element_of_the_id_is_widened_alike() {
			let a = keyed(vec![idx_basic(1, "by_at", &["at"])]);
			let cond = parse_cond("id[1] >= d'2025-01-01T10:00:01.250Z'");
			let cands = a.analyze(Some(&cond), None);
			let c = find_for(&cands, "by_at").expect("by_at");
			let BTreeAccess::Range {
				range,
			} = &c.access
			else {
				panic!("expected Range, got {:?}", c.access);
			};
			assert_eq!(
				range.start,
				Bound::Included(datetime("2025-01-01T10:00:00.999999999Z").into())
			);
			assert_eq!(range.end, Bound::Unbounded);
		}

		#[test]
		fn an_equality_over_a_datetime_component_is_exact() {
			let a = keyed(vec![idx_basic(1, "by_at", &["at"])]);
			let cond = parse_cond("at = d'2025-01-01T10:00:05.500Z'");
			let cands = a.analyze(Some(&cond), None);
			let c = find_for(&cands, "by_at").expect("by_at");
			let BTreeAccess::Equality(value) = &c.access else {
				panic!("expected Equality, got {:?}", c.access);
			};
			assert_eq!(value, &BoundOperand::from(datetime("2025-01-01T10:00:05.500Z")));
			assert!(!range_is_widened(&c.index_ref, &c.access));
		}

		#[test]
		fn the_range_after_a_pinned_prefix_is_widened() {
			let a = keyed(vec![idx_basic(1, "by_feed_at", &["feed", "at"])]);
			let cond = parse_cond("feed = 'f' AND at <= d'2025-01-01T10:00:05.500Z'");
			let cands = a.analyze(Some(&cond), None);
			let c = find_for(&cands, "by_feed_at").expect("by_feed_at");
			let BTreeAccess::Compound {
				prefix,
				range,
				..
			} = &c.access
			else {
				panic!("expected Compound, got {:?}", c.access);
			};
			assert_eq!(prefix, &vec![BoundOperand::from(Value::from("f"))]);
			assert_eq!(
				range,
				&Some((BinaryOperator::LessThanEqual, datetime("2025-01-01T10:00:05Z").into()))
			);
			assert!(range_is_widened(&c.index_ref, &c.access));
		}

		#[test]
		fn an_ordering_by_a_datetime_component_is_not_covered() {
			let a = keyed(vec![idx_basic(1, "by_at", &["at"])]);
			let (cond, order) = parse_cond_order("at > d'2025-01-01T10:00:01Z'", "at");
			let cands = a.analyze(Some(&cond), Some(&order));
			let c = find_for(&cands, "by_at").expect("by_at");
			assert!(!c.covers_order, "the entries order by the whole second only");

			// The same query over a table whose ids name no components.
			let a = scalar_analyzer(vec![idx_basic(1, "by_at", &["at"])], None);
			let cands = a.analyze(Some(&cond), Some(&order));
			let c = find_for(&cands, "by_at").expect("by_at");
			assert!(c.covers_order);
			let BTreeAccess::Range {
				range,
			} = &c.access
			else {
				panic!("expected Range, got {:?}", c.access);
			};
			assert_eq!(range.start, Bound::Excluded(datetime("2025-01-01T10:00:01Z").into()));
			assert!(!range_is_widened(&c.index_ref, &c.access));
		}

		#[test]
		fn an_ordering_by_id_is_not_covered_where_the_id_holds_a_datetime() {
			let a = keyed(vec![idx_basic(1, "by_feed", &["feed"])]);
			let (cond, order) = parse_cond_order("feed = 'f'", "id");
			let cands = a.analyze(Some(&cond), Some(&order));
			let c = find_for(&cands, "by_feed").expect("by_feed");
			assert!(!c.covers_order, "the ids an entry ends with order by the whole second");

			// The same query over a table whose ids name no components.
			let a = scalar_analyzer(vec![idx_basic(1, "by_feed", &["feed"])], None);
			let cands = a.analyze(Some(&cond), Some(&order));
			let c = find_for(&cands, "by_feed").expect("by_feed");
			assert!(c.covers_order);
		}

		#[test]
		fn a_range_over_an_int_component_is_exact() {
			let a = keyed(vec![idx_basic(1, "by_n", &["n"])]);
			let (cond, order) = parse_cond_order("n > 3", "n");
			let cands = a.analyze(Some(&cond), Some(&order));
			let c = find_for(&cands, "by_n").expect("by_n");
			let BTreeAccess::Range {
				range,
			} = &c.access
			else {
				panic!("expected Range, got {:?}", c.access);
			};
			assert_eq!(range.start, Bound::Excluded(Value::from(3).into()));
			assert!(!range_is_widened(&c.index_ref, &c.access));
			assert!(c.covers_order, "an int component orders by value");
		}

		#[test]
		fn a_range_over_a_stored_datetime_field_is_not_widened() {
			let a = keyed(vec![idx_basic(1, "by_seen", &["seen"])]);
			let cond = parse_cond("seen > d'2025-01-01T10:00:01Z'");
			let cands = a.analyze(Some(&cond), None);
			let c = find_for(&cands, "by_seen").expect("by_seen");
			let BTreeAccess::Range {
				range,
			} = &c.access
			else {
				panic!("expected Range, got {:?}", c.access);
			};
			assert_eq!(range.start, Bound::Excluded(datetime("2025-01-01T10:00:01Z").into()));
			assert!(!range_is_widened(&c.index_ref, &c.access));
		}

		#[test]
		fn a_clock_bound_over_a_datetime_component_of_the_id_is_discarded() {
			// Widening to whole seconds needs the instant, which is known only
			// when the scan opens, so the candidate is not offered.
			let a = keyed(vec![idx_basic(1, "by_at", &["at"])]).with_clock(true);
			let cands = a.analyze(Some(&parse_cond("at > time::now() - 1h")), None);
			assert_no_candidate(&cands, "by_at");
			// A stored datetime field is no component, so its bound is offered.
			let a = keyed(vec![idx_basic(1, "by_seen", &["seen"])]).with_clock(true);
			let cands = a.analyze(Some(&parse_cond("seen > time::now() - 1h")), None);
			assert!(find_for(&cands, "by_seen").is_some());
		}
	}

	// ------------------------------------------------------------------
	// Bounds that read the statement clock
	// ------------------------------------------------------------------
	mod clock_bounds {
		use super::*;
		use crate::exec::index::access_path::{ClockBound, ClockSign};

		fn clock(offset: Option<(ClockSign, std::time::Duration)>) -> BoundOperand {
			BoundOperand::StatementClock(ClockBound {
				offset: offset.map(|(sign, by)| (sign, crate::val::Duration::from(by))),
			})
		}

		fn day() -> std::time::Duration {
			std::time::Duration::from_secs(86_400)
		}

		/// Every column is known to hold no array: a range over a column that may
		/// hold one is dropped from the candidates, and these tests read the
		/// candidates a clock-bounded range yields.
		fn permitted<'a>(defs: Vec<IndexDefinition>) -> IndexAnalyzer<'a> {
			scalar_analyzer(defs, None).with_clock(true)
		}

		#[test]
		fn a_comparison_with_the_clock_minus_a_duration_is_a_range_that_reads_it() {
			let a = permitted(vec![idx_basic(1, "ix_at", &["at"])]);
			let cands = a.analyze(Some(&parse_cond("at > time::now() - 1d")), None);
			let c = find_for(&cands, "ix_at").expect("ix_at");
			let BTreeAccess::Range {
				range,
			} = &c.access
			else {
				panic!("expected Range, got {:?}", c.access);
			};
			assert_eq!(range.start, Bound::Excluded(clock(Some((ClockSign::Minus, day())))));
			assert_eq!(range.end, Bound::Unbounded);
			assert_eq!(c.access.describe(), ">time::now() - 1d");
		}

		#[test]
		fn the_clock_alone_and_plus_a_duration_are_bounds() {
			let a = permitted(vec![idx_basic(1, "ix_at", &["at"])]);
			let cands = a.analyze(Some(&parse_cond("at <= time::now()")), None);
			let c = find_for(&cands, "ix_at").expect("ix_at");
			assert_eq!(c.access.describe(), "<=time::now()");
			let cands = a.analyze(Some(&parse_cond("time::now() + 1d >= at")), None);
			let c = find_for(&cands, "ix_at").expect("ix_at");
			assert_eq!(c.access.describe(), "<=time::now() + 1d");
			let cands = a.analyze(Some(&parse_cond("at = time::now()")), None);
			let c = find_for(&cands, "ix_at").expect("ix_at");
			assert_eq!(c.access, BTreeAccess::Equality(clock(None)));
		}

		#[test]
		fn a_caller_denied_the_function_is_offered_no_bound() {
			let a = analyzer(vec![idx_basic(1, "ix_at", &["at"])], None);
			let cands = a.analyze(Some(&parse_cond("at > time::now() - 1d")), None);
			assert_no_candidate(&cands, "ix_at");
		}

		#[test]
		fn only_equality_and_order_comparisons_take_the_clock() {
			let a = permitted(vec![idx_basic(1, "ix_at", &["at"])]);
			for cond in ["at != time::now()", "at INSIDE [time::now()]", "at CONTAINS time::now()"]
			{
				let cands = a.analyze(Some(&parse_cond(cond)), None);
				assert_no_candidate(&cands, "ix_at");
			}
		}

		#[test]
		fn an_offset_that_is_not_one_duration_literal_stays_a_filter() {
			let a = permitted(vec![idx_basic(1, "ix_at", &["at"])]);
			for cond in [
				"at > time::now() - 1d - 1h",
				"at > time::now() * 2",
				"at > 1d + time::now()",
				"at > time::now(d'2020-01-01T00:00:00Z')",
				"at > time::unix()",
			] {
				let cands = a.analyze(Some(&parse_cond(cond)), None);
				assert_no_candidate(&cands, "ix_at");
			}
		}

		#[test]
		fn bounds_on_opposite_sides_merge_without_comparing_the_clock() {
			let a = permitted(vec![idx_basic(1, "ix_at", &["at"])]);
			let cond = parse_cond("at > time::now() - 1d AND at < time::now()");
			let cands = a.analyze(Some(&cond), None);
			assert_eq!(cands.len(), 1, "{cands:?}");
			assert!(!cands[0].empty);
			assert_eq!(cands[0].access.describe(), ">time::now() - 1d <time::now()");
			// A clock bound and a literal bound on one side are not compared,
			// so neither can be proved the tighter and the pair is never empty.
			let cond = parse_cond("at > time::now() AND at < d'2020-01-01T00:00:00Z'");
			let cands = a.analyze(Some(&cond), None);
			assert_eq!(cands.len(), 1, "{cands:?}");
			assert!(!cands[0].empty, "a clock bound is never proved empty");
			let cond = parse_cond("at > time::now() AND at > d'2020-01-01T00:00:00Z'");
			let cands = a.analyze(Some(&cond), None);
			assert_eq!(cands.len(), 1, "{cands:?}");
			assert!(!cands[0].empty);
		}

		#[test]
		fn the_clock_bounds_the_range_after_an_equality_prefix() {
			let a = permitted(vec![idx_basic(1, "ix_city_at", &["city", "at"])]);
			let cond = parse_cond("city = 'london' AND at > time::now() - 1d");
			let cands = a.analyze(Some(&cond), None);
			let c = find_for(&cands, "ix_city_at").expect("ix_city_at");
			let BTreeAccess::Compound {
				prefix,
				range,
				..
			} = &c.access
			else {
				panic!("expected Compound, got {:?}", c.access);
			};
			assert_eq!(prefix, &vec![BoundOperand::from(Value::from("london"))]);
			assert_eq!(
				range,
				&Some((BinaryOperator::MoreThan, clock(Some((ClockSign::Minus, day())))))
			);
		}

		#[test]
		fn a_clock_equality_does_not_prove_union_branches_disjoint() {
			let a = permitted(vec![idx_basic(1, "ix_at", &["at"])]);
			let cond = parse_cond("at = time::now() OR at = d'2020-01-01T00:00:00Z'");
			// The branches may read the same instant, so no fusion is declined
			// on the strength of distinct values.
			let plan = a.try_bitmap_union_fusion(Some(&cond), Direction::Forward);
			if let Some((BitmapPlan::Or(children), _)) = plan {
				assert_eq!(children.len(), 2);
			}
		}

		#[test]
		fn regex_literal_is_not_keyed_as_compared() {
			let a = permitted(vec![idx_basic(1, "ix_name", &["name"])]);
			for sql in ["name = /a/", "name IN [/a/, 'b']"] {
				let cond = parse_cond(sql);
				let mut cands = Vec::new();
				a.analyze_condition(&cond, &mut cands);
				assert!(
					find_for(&cands, "ix_name").is_none(),
					"expected no B-tree index candidate for `{sql}`, got {cands:?}"
				);
			}
		}

		#[test]
		fn temporal_literals_are_keyed_as_compared() {
			use crate::val::{Date, Interval, Time, Timestamp};
			for value in [
				Value::Date(Date(0)),
				Value::Time(Time(0)),
				Value::Timestamp(Timestamp::from_parts(0, 0).unwrap()),
				Value::Interval(Interval::new(1, 0, 0)),
			] {
				assert!(keyed_as_compared(&value, false), "{value:?}");
				assert!(keyed_as_compared(&value, true), "{value:?}");
			}
			let interval_id = Value::RecordId(crate::val::RecordId::new(
				"t".into(),
				RecordIdKey::Array(vec![Value::Interval(Interval::new(1, 0, 0))].into()),
			));
			assert!(!keyed_as_compared(&interval_id, false));
		}
	}

	#[tokio::test]
	async fn spatial_index_end_to_end_queries() {
		use surrealdb_types::ToSql;

		let ds = crate::kvs::Datastore::builder()
			.with_capabilities(crate::dbs::Capabilities::all())
			.with_auth(false)
			.build_with_path("memory")
			.await
			.unwrap();
		let ses = crate::dbs::Session::owner().with_ns("test").with_db("test");

		let setup_sql = r#"
			DEFINE NAMESPACE test;
			DEFINE DATABASE test;
			DEFINE TABLE places;
			DEFINE INDEX idx_places_pt ON places FIELDS pt SPATIAL;
			CREATE places:london SET name = 'London', pt = (-0.118092, 51.509865);
			CREATE places:paris SET name = 'Paris', pt = (2.352222, 48.856614);
			CREATE places:tokyo SET name = 'Tokyo', pt = (139.691706, 35.689487);
			CREATE places:ny SET name = 'New York', pt = (-74.005973, 40.712775);
		"#;
		let res = ds.execute(setup_sql, &ses, None).await.unwrap();
		for r in res {
			r.output().unwrap();
		}

		// 1. Point INSIDE Polygon (London box)
		let inside_sql = r#"
			SELECT VALUE id FROM places
			WHERE pt INSIDE {
				type: "Polygon",
				coordinates: [[
					[-1.0, 51.0],
					[1.0, 51.0],
					[1.0, 52.0],
					[-1.0, 52.0],
					[-1.0, 51.0]
				]]
			};
		"#;
		let mut res = ds.execute(inside_sql, &ses, None).await.unwrap();
		let val = res.remove(0).output().unwrap();
		assert_eq!(
			val.to_sql(),
			"[places:london]",
			"Point INSIDE polygon should use SPATIAL index and return London"
		);

		// 2. EXPLAIN verifies UnionIndexScan on idx_places_pt
		let explain_sql = r#"
			SELECT VALUE id FROM places
			WHERE pt INSIDE {
				type: "Polygon",
				coordinates: [[
					[-1.0, 51.0],
					[1.0, 51.0],
					[1.0, 52.0],
					[-1.0, 52.0],
					[-1.0, 51.0]
				]]
			} EXPLAIN;
		"#;
		let mut explain_res = ds.execute(explain_sql, &ses, None).await.unwrap();
		let explain_str = explain_res.remove(0).output().unwrap().to_sql();
		assert!(
			explain_str.contains("UnionIndexScan") && explain_str.contains("idx_places_pt"),
			"Expected UnionIndexScan on idx_places_pt in EXPLAIN output, got: {explain_str}"
		);

		// 3. geo::distance(pt, center) < radius_meters (within 10km of London)
		let dist_sql = r#"
			SELECT VALUE id FROM places
			WHERE geo::distance(pt, (-0.12, 51.51)) < 10000;
		"#;
		let mut dist_res = ds.execute(dist_sql, &ses, None).await.unwrap();
		let dist_val = dist_res.remove(0).output().unwrap();
		assert_eq!(
			dist_val.to_sql(),
			"[places:london]",
			"geo::distance < 10000m should return London"
		);

		// 4. UPDATE and DELETE maintain the spatial index entries
		let update_sql = r#"
			UPDATE places:paris SET pt = (-0.10, 51.52);
			DELETE places:london;
			SELECT VALUE id FROM places
			WHERE pt INSIDE {
				type: "Polygon",
				coordinates: [[
					[-1.0, 51.0],
					[1.0, 51.0],
					[1.0, 52.0],
					[-1.0, 52.0],
					[-1.0, 51.0]
				]]
			};
		"#;
		let mut upd_res = ds.execute(update_sql, &ses, None).await.unwrap();
		let upd_val = upd_res.remove(2).output().unwrap();
		assert_eq!(
			upd_val.to_sql(),
			"[places:paris]",
			"After moving Paris into box and deleting London, only Paris should match"
		);

		// 5. Polygon table indexed with SPATIAL queried by CONTAINS point
		let poly_sql = r#"
			DEFINE TABLE zones;
			DEFINE INDEX idx_zones_area ON zones FIELDS area SPATIAL;
			CREATE zones:uk SET area = {
				type: "Polygon",
				coordinates: [[
					[-1.0, 51.0],
					[1.0, 51.0],
					[1.0, 52.0],
					[-1.0, 52.0],
					[-1.0, 51.0]
				]]
			};
			CREATE zones:fr SET area = {
				type: "Polygon",
				coordinates: [[
					[2.0, 48.0],
					[3.0, 48.0],
					[3.0, 49.0],
					[2.0, 49.0],
					[2.0, 48.0]
				]]
			};
			SELECT VALUE id FROM zones WHERE area CONTAINS (2.35, 48.85);
		"#;
		let mut poly_res = ds.execute(poly_sql, &ses, None).await.unwrap();
		let poly_val = poly_res.remove(4).output().unwrap();
		assert_eq!(
			poly_val.to_sql(),
			"[zones:fr]",
			"Polygon CONTAINS Point should match zones:fr via SPATIAL index"
		);

		// 6. Array of geometries (`FIELDS pts[*] SPATIAL`) with CONTAINSANY
		let array_sql = r#"
			DEFINE TABLE routes;
			DEFINE INDEX idx_routes_pts ON routes FIELDS pts[*] SPATIAL;
			CREATE routes:eurostar SET pts = [(-0.118092, 51.509865), (2.352222, 48.856614)];
			CREATE routes:shinkansen SET pts = [(139.691706, 35.689487), (135.502165, 34.693738)];
			SELECT VALUE id FROM routes WHERE pts CONTAINSANY [(-0.118092, 51.509865)];
		"#;
		let mut array_res = ds.execute(array_sql, &ses, None).await.unwrap();
		let array_val = array_res.remove(4).output().unwrap();
		assert_eq!(
			array_val.to_sql(),
			"[routes:eurostar]",
			"Array SPATIAL index with CONTAINSANY should match routes:eurostar"
		);

		// 7. Set of geometries (`TYPE set<geometry>`) with `FIELDS pts[*] SPATIAL`
		let set_sql = r#"
			DEFINE TABLE hubs;
			DEFINE FIELD pts ON hubs TYPE set<geometry<point>>;
			DEFINE INDEX idx_hubs_pts ON hubs FIELDS pts[*] SPATIAL;
			CREATE hubs:eu SET pts = [(-0.118092, 51.509865), (2.352222, 48.856614)];
			CREATE hubs:jp SET pts = [(139.691706, 35.689487)];
			SELECT VALUE id FROM hubs WHERE pts CONTAINS (-0.118092, 51.509865);
		"#;
		let mut set_res = ds.execute(set_sql, &ses, None).await.unwrap();
		let set_val = set_res.remove(5).output().unwrap();
		assert_eq!(
			set_val.to_sql(),
			"[hubs:eu]",
			"Set SPATIAL index with CONTAINS should match hubs:eu"
		);

		// 8. OR union of two spatial predicates + WITH INDEX hint + CONTAINSALL + ORDER BY id DESC
		//    + Hybrid HNSW + Spatial
		let adv_sql = r#"
			SELECT VALUE id FROM places
				WHERE pt INSIDE {
					type: "Polygon",
					coordinates: [[[-0.5, 51.0], [0.5, 51.0], [0.5, 52.0], [-0.5, 52.0], [-0.5, 51.0]]]
				}
				OR pt INSIDE {
					type: "Polygon",
					coordinates: [[[139.0, 35.0], [140.0, 35.0], [140.0, 36.0], [139.0, 36.0], [139.0, 35.0]]]
				}
				ORDER BY id;
			SELECT VALUE id FROM zones WITH INDEX idx_zones_area
				WHERE area CONTAINSALL [(2.35, 48.85), (2.40, 48.90)];
			SELECT VALUE id FROM places
				WHERE geo::distance(pt, (0.0, 50.0)) < 500000.0
				ORDER BY id DESC;
			DEFINE TABLE pois SCHEMAFULL;
			DEFINE FIELD pt ON pois TYPE geometry<point>;
			DEFINE FIELD vec ON pois TYPE array<float>;
			DEFINE INDEX idx_pois_pt ON pois FIELDS pt SPATIAL;
			DEFINE INDEX idx_pois_vec ON pois FIELDS vec HNSW DIMENSION 2 DIST EUCLIDEAN;
			CREATE pois:london SET pt = (-0.12, 51.5), vec = [1.0, 0.0];
			CREATE pois:paris SET pt = (2.35, 48.85), vec = [0.9, 0.1];
			CREATE pois:tokyo SET pt = (139.69, 35.68), vec = [1.0, 0.0];
			SELECT VALUE id FROM pois
				WHERE vec <|1, 10|> [1.0, 0.0]
				AND pt INSIDE {
					type: "Polygon",
					coordinates: [[[-1.0, 48.0], [3.0, 48.0], [3.0, 52.0], [-1.0, 52.0], [-1.0, 48.0]]]
				};
		"#;
		let mut adv_res = ds.execute(adv_sql, &ses, None).await.unwrap();
		let or_val = adv_res.remove(0).output().unwrap();
		assert_eq!(
			or_val.to_sql(),
			"[places:paris, places:tokyo]",
			"OR union of two spatial predicates should match paris and tokyo"
		);
		let contains_all_val = adv_res.remove(0).output().unwrap();
		assert_eq!(
			contains_all_val.to_sql(),
			"[zones:fr]",
			"CONTAINSALL on GeometryCollection should match zones:fr"
		);
		let desc_val = adv_res.remove(0).output().unwrap();
		assert_eq!(
			desc_val.to_sql(),
			"[places:paris]",
			"ORDER BY id DESC on spatial range query should return matching records in descending order"
		);
		let knn_spatial_val = adv_res.remove(8).output().unwrap();
		assert_eq!(
			knn_spatial_val.to_sql(),
			"[pois:london]",
			"Hybrid HNSW + SPATIAL prefilter should return pois:london"
		);

		// 9. Compound SPATIAL index (`FIELDS tenant, loc SPATIAL`) + parameterized queries
		//    (`$origin`, `$radius`, `$poly`)
		let compound_sql = r#"
			DEFINE TABLE fleet SCHEMAFULL;
			DEFINE FIELD tenant ON fleet TYPE string;
			DEFINE FIELD loc ON fleet TYPE geometry<point>;
			DEFINE INDEX idx_fleet_tenant_loc ON fleet FIELDS tenant, loc SPATIAL;
			CREATE fleet:a1 SET tenant = 'acme', loc = (-0.12, 51.51);
			CREATE fleet:b1 SET tenant = 'globex', loc = (-0.12, 51.51);
			CREATE fleet:a2 SET tenant = 'acme', loc = (2.35, 48.85);
			LET $origin = (-0.12, 51.51);
			LET $radius = 10000;
			LET $poly = {
				type: "Polygon",
				coordinates: [[[-1.0, 51.0], [1.0, 51.0], [1.0, 52.0], [-1.0, 52.0], [-1.0, 51.0]]]
			};
			SELECT VALUE id FROM fleet WHERE tenant = 'acme' AND geo::distance(loc, $origin) < $radius;
			SELECT VALUE id FROM fleet WHERE tenant = 'acme' AND loc INSIDE $poly EXPLAIN;
		"#;
		let mut comp_res = ds.execute(compound_sql, &ses, None).await.unwrap();
		let comp_dist_val = comp_res.remove(10).output().unwrap();
		assert_eq!(
			comp_dist_val.to_sql(),
			"[fleet:a1]",
			"Compound SPATIAL index + parameterized geo::distance should return [fleet:a1]"
		);
		let comp_explain_str = comp_res.remove(10).output().unwrap().to_sql();
		assert!(
			comp_explain_str.contains("UnionIndexScan")
				&& comp_explain_str.contains("idx_fleet_tenant_loc"),
			"Expected UnionIndexScan on compound index idx_fleet_tenant_loc for parameterized query, got: {comp_explain_str}"
		);

		// 10. Vacuous-truth and Geometry::contains correctness on indexed tables:
		//     - Empty array `pts = []` matches `pts ALLINSIDE [...]`
		//     - Empty `GeometryCollection([])` indexed at root level 0 matches `col INSIDE $poly`
		//     - `Polygon CONTAINS MultiPolygon` rejects a MultiPolygon outside the polygon
		let edge_sql = r#"
			CREATE routes:empty SET pts = [];
			SELECT VALUE id FROM routes
				WHERE pts ALLINSIDE [(-0.118092, 51.509865), (2.352222, 48.856614)]
				ORDER BY id;
			DEFINE TABLE shapes;
			DEFINE INDEX idx_shapes_col ON shapes FIELDS col SPATIAL;
			CREATE shapes:empty_col SET col = { type: "GeometryCollection", geometries: [] };
			CREATE shapes:london_col SET col = {
				type: "GeometryCollection",
				geometries: [{ type: "Point", coordinates: [-0.12, 51.5] }]
			};
			SELECT VALUE id FROM shapes
				WHERE col INSIDE {
					type: "Polygon",
					coordinates: [[[2.0, 48.0], [3.0, 48.0], [3.0, 49.0], [2.0, 49.0], [2.0, 48.0]]]
				}
				ORDER BY id;
			SELECT VALUE id FROM zones
				WHERE area CONTAINS {
					type: "MultiPolygon",
					coordinates: [[[[139.0, 35.0], [140.0, 35.0], [140.0, 36.0], [139.0, 36.0], [139.0, 35.0]]]]
				};
		"#;
		let mut edge_res = ds.execute(edge_sql, &ses, None).await.unwrap();
		let empty_arr_val = edge_res.remove(1).output().unwrap();
		assert_eq!(
			empty_arr_val.to_sql(),
			"[routes:empty, routes:eurostar]",
			"pts ALLINSIDE [...] must include both routes:empty (vacuously true) and routes:eurostar"
		);
		let empty_col_val = edge_res.remove(5).output().unwrap();
		assert_eq!(
			empty_col_val.to_sql(),
			"[shapes:empty_col]",
			"Empty GeometryCollection([]) indexed at root cell must be returned when vacuously INSIDE query polygon"
		);
		let poly_contains_mp_val = edge_res.remove(5).output().unwrap();
		assert_eq!(
			poly_contains_mp_val.to_sql(),
			"[]",
			"Polygon CONTAINS MultiPolygon must be false when the MultiPolygon is outside the polygon"
		);

		// 11. Hybrid HNSW KNN + `geo::distance(loc, <point> $prior) < r` SPATIAL pre-filter:
		//     verifies that `KnnScan` attaches a `BitmapOr` of `BitmapIndexScan` nodes on the
		//     `SPATIAL` index while retaining `geo::distance` in `residual`.
		let knn_spatial_sql = r#"
			REMOVE TABLE places;
			DEFINE TABLE places;
			DEFINE FIELD loc ON places TYPE geometry<point>;
			DEFINE FIELD vec ON places TYPE array<float>;
			DEFINE INDEX idx_places_loc ON places FIELDS loc SPATIAL;
			DEFINE INDEX idx_places_vec ON places FIELDS vec HNSW DIMENSION 2 DIST COSINE TYPE F32;
			CREATE places:near_close SET loc = (6.365, 49.469), vec = [1.0, 0.0];
			CREATE places:near_far   SET loc = (6.366, 49.469), vec = [0.6, 0.8];
			CREATE places:far_close  SET loc = (2.352, 48.856), vec = [1.0, 0.0];
			LET $prior = [6.365, 49.469];
			LET $photo = [1.0, 0.0];
			SELECT VALUE id FROM places
				WHERE geo::distance(loc, <point> $prior) < 400 AND vec <|1, 40|> $photo;
			SELECT VALUE id FROM places
				WHERE geo::distance(loc, <point> $prior) < 400 AND vec <|1, 40|> $photo
				EXPLAIN;
		"#;
		let mut knn_res = ds.execute(knn_spatial_sql, &ses, None).await.unwrap();
		let knn_val = knn_res.remove(11).output().unwrap();
		assert_eq!(
			knn_val.to_sql(),
			"[places:near_close]",
			"Hybrid HNSW + SPATIAL geo::distance prefilter must return [places:near_close]"
		);
		let knn_explain = knn_res.remove(11).output().unwrap().to_sql();
		assert!(
			knn_explain.contains("KnnScan")
				&& knn_explain.contains("idx_places_vec")
				&& knn_explain.contains("BitmapIndexScan")
				&& knn_explain.contains("idx_places_loc"),
			"Expected KnnScan on idx_places_vec with BitmapIndexScan prefilter on idx_places_loc, got: {knn_explain}"
		);

		// 12. End-to-end verification on point-only SPATIAL index (`places`):
		//     - Zero-fetch `SELECT count() ... GROUP ALL` fuses into `Count` with `BitmapIndexScan`
		//       and inline `SpatialPointFilter` (excluding `places:far_close` and boundary false
		//       positives without fetching records).
		//     - `SELECT id, geo::distance(loc, <point> $prior) AS dist ... ORDER BY dist` computes
		//       exact Haversine distances via `MeasureKind::GeoDistance` and partial row
		//       projection.
		//     - `DELETE` / `UPDATE` on a point-indexed record cleanly removes the old index entry
		//       whose `IndexEntryValue` carries inline `(lon, lat)` coordinates.
		let p2_sql = r#"
			LET $prior = [6.365, 49.469];
			SELECT count() FROM places WHERE geo::distance(loc, <point> $prior) < 400 GROUP ALL;
			SELECT count() FROM places WHERE geo::distance(loc, <point> $prior) < 400 GROUP ALL EXPLAIN;
			SELECT VALUE id FROM (
				SELECT id, geo::distance(loc, <point> $prior) AS dist
				FROM places
				WHERE geo::distance(loc, <point> $prior) < 400
				ORDER BY dist ASC
			);
			DELETE places:near_far;
			SELECT count() FROM places WHERE geo::distance(loc, <point> $prior) < 400 GROUP ALL;
		"#;
		let mut p2_res = ds.execute(p2_sql, &ses, None).await.unwrap();
		let count_before = p2_res.remove(1).output().unwrap();
		assert_eq!(
			count_before.to_sql(),
			"[{ count: 2 }]",
			"Point-only spatial bitmap count should return exact count 2"
		);
		let count_explain = p2_res.remove(1).output().unwrap().to_sql();
		assert!(
			count_explain.contains("Count")
				&& count_explain.contains("BitmapIndexScan")
				&& count_explain.contains("idx_places_loc")
				&& count_explain.contains("point_filter"),
			"Expected zero-fetch Count with BitmapIndexScan and point_filter on idx_places_loc, got: {count_explain}"
		);
		let ordered_ids = p2_res.remove(1).output().unwrap();
		assert_eq!(
			ordered_ids.to_sql(),
			"[places:near_close, places:near_far]",
			"Expected ORDER BY dist ASC via MeasureKind::GeoDistance to order near_close before near_far"
		);
		let count_after_delete = p2_res.remove(2).output().unwrap();
		assert_eq!(
			count_after_delete.to_sql(),
			"[{ count: 1 }]",
			"Deleting places:near_far should remove its inline-coord spatial index entry and leave count 1"
		);

		// 12b. Half-ULP decimal radius boundary on point-only SPATIAL index (`places`):
		//      re-create `places:near_far` at `(6.366, 49.469)` and construct two decimals
		//      `dec_lo < d < dec_hi` around the exact Haversine `f64` distance `d` that both
		//      round to `d` as `f64` (`as_float() == d`), verifying that `< dec_hi` admits
		//      `places:near_far` and `<= dec_lo` excludes it across both `UnionIndexScan` and
		//      zero-fetch `BitmapIndexScan` count.
		{
			let mut d_res = ds
				.execute("RETURN geo::distance((6.366, 49.469), (6.365, 49.469));", &ses, None)
				.await
				.unwrap();
			let surrealdb_types::Value::Number(surrealdb_types::Number::Float(d)) =
				d_res.remove(0).output().unwrap()
			else {
				panic!("Expected Float from geo::distance");
			};
			let step = rust_decimal::Decimal::new(1, 15);
			let mut dec_hi = rust_decimal::Decimal::try_from(d).unwrap();
			while crate::val::Number::Decimal(dec_hi) <= crate::val::Number::Float(d) {
				dec_hi += step;
			}
			while crate::val::Number::Decimal(dec_hi - step) > crate::val::Number::Float(d) {
				dec_hi -= step;
			}
			let mut dec_lo = dec_hi - step;
			if crate::val::Number::Decimal(dec_lo) == crate::val::Number::Float(d) {
				dec_lo -= step;
			}
			assert_eq!(crate::val::Number::Decimal(dec_hi).as_float(), d);
			assert_eq!(crate::val::Number::Decimal(dec_lo).as_float(), d);
			assert!(crate::val::Number::Float(d) < crate::val::Number::Decimal(dec_hi));
			assert!(crate::val::Number::Float(d) > crate::val::Number::Decimal(dec_lo));

			let dec_sql = format!(
				r#"
				CREATE places:near_far SET loc = (6.366, 49.469), vec = [0.6, 0.8];
				SELECT VALUE id FROM places WHERE geo::distance(loc, (6.365, 49.469)) < {dec_hi}dec ORDER BY id;
				SELECT VALUE id FROM places WHERE geo::distance(loc, (6.365, 49.469)) <= {dec_lo}dec ORDER BY id;
				SELECT count() FROM places WHERE geo::distance(loc, (6.365, 49.469)) < {dec_hi}dec GROUP ALL;
				SELECT count() FROM places WHERE geo::distance(loc, (6.365, 49.469)) <= {dec_lo}dec GROUP ALL;
				DELETE places:near_far;
				"#
			);
			let mut dec_res = ds.execute(&dec_sql, &ses, None).await.unwrap();
			let lt_hi_ids = dec_res.remove(1).output().unwrap();
			assert_eq!(
				lt_hi_ids.to_sql(),
				"[places:near_close, places:near_far]",
				"geo::distance < dec_hi (within +0.5 ULP of d) must include places:near_far"
			);
			let le_lo_ids = dec_res.remove(1).output().unwrap();
			assert_eq!(
				le_lo_ids.to_sql(),
				"[places:near_close]",
				"geo::distance <= dec_lo (within -0.5 ULP of d) must exclude places:near_far"
			);
			let lt_hi_cnt = dec_res.remove(1).output().unwrap();
			assert_eq!(lt_hi_cnt.to_sql(), "[{ count: 2 }]");
			let le_lo_cnt = dec_res.remove(1).output().unwrap();
			assert_eq!(le_lo_cnt.to_sql(), "[{ count: 1 }]");
		}

		// 13. Array literal on the RHS of `INSIDE` (`loc INSIDE [$disjoint_pt, $match_pt]` and
		//     `SELECT count() ... WHERE loc INSIDE [$poly, $poly] GROUP ALL`): verifies that
		//     `SpatialPointFilter` and index-only bitmap count are not attached to array literals
		//     (where `Array::contains` evaluates disjunctive element equality).
		let arr_inside_sql = r#"
			LET $box = {
				type: "Polygon",
				coordinates: [[[6.0, 49.0], [7.0, 49.0], [7.0, 50.0], [6.0, 50.0], [6.0, 49.0]]]
			};
			SELECT VALUE id FROM places WHERE loc INSIDE [$box, (6.365, 49.469)] ORDER BY id;
			SELECT count() FROM places WHERE loc INSIDE [$box, $box] GROUP ALL;
		"#;
		let mut arr_res = ds.execute(arr_inside_sql, &ses, None).await.unwrap();
		let arr_inside_ids = arr_res.remove(1).output().unwrap();
		assert_eq!(
			arr_inside_ids.to_sql(),
			"[places:near_close]",
			"loc INSIDE [$disjoint_poly, (6.365, 49.469)] must return [places:near_close]"
		);
		let arr_poly_count = arr_res.remove(1).output().unwrap();
		assert_eq!(
			arr_poly_count.to_sql(),
			"[{ count: 0 }]",
			"SELECT count() FROM places WHERE loc INSIDE [$box, $box] GROUP ALL must return [{{ count: 0 }}]"
		);

		// 14. Non-truth-preserving prefix operators (`<string> (loc INSIDE $box)` and `..(loc
		//     INSIDE $box)`) must not push the inner spatial comparison down to the spatial index,
		//     because `<string>` casts both `true` and `false` to the non-empty strings `'true'`
		//     and `'false'` (both truthy for every row in `places`).
		let prefix_sql = r#"
			LET $box = {
				type: "Polygon",
				coordinates: [[[6.0, 49.0], [7.0, 49.0], [7.0, 50.0], [6.0, 50.0], [6.0, 49.0]]]
			};
			SELECT VALUE id FROM places WHERE <string> (loc INSIDE $box) ORDER BY id;
			SELECT VALUE id FROM places WHERE <string> (loc INSIDE $box) EXPLAIN;
			SELECT VALUE id FROM places WHERE <bool> (loc INSIDE $box) ORDER BY id;
		"#;
		let mut prefix_res = ds.execute(prefix_sql, &ses, None).await.unwrap();
		let str_cast_ids = prefix_res.remove(1).output().unwrap();
		assert_eq!(
			str_cast_ids.to_sql(),
			"[places:far_close, places:near_close]",
			"<string> (loc INSIDE $box) evaluates to 'true' or 'false' (both truthy) and must return all rows"
		);
		let str_cast_explain = prefix_res.remove(1).output().unwrap().to_sql();
		assert!(
			str_cast_explain.contains("TableScan") && !str_cast_explain.contains("UnionIndexScan"),
			"Expected TableScan for <string> (loc INSIDE $box), got: {str_cast_explain}"
		);
		let bool_cast_ids = prefix_res.remove(1).output().unwrap();
		assert_eq!(
			bool_cast_ids.to_sql(),
			"[places:near_close]",
			"<bool> (loc INSIDE $box) preserves boolean truthiness and must return [places:near_close]"
		);
	}

	#[test]
	fn test_spatial_point_only_and_contains_all_and_or_union_analysis() {
		let mut ix_def = idx_current(1, "idx_loc", &["loc"]);
		ix_def.index = crate::catalog::Index::Spatial;

		// 1. Point-only spatial schema (`with_point_only_spatial([true])`) emits 1 scan op
		// (no coarse ancestor probes) for a point query.
		let analyzer_pt =
			analyzer(vec![ix_def], None).with_point_only_spatial(Arc::from(vec![true]));
		let pt_cond = parse_cond("loc INSIDE (-0.12, 51.5)");
		let path_pt =
			analyzer_pt.try_spatial_expansion(Some(&pt_cond), Direction::Forward).unwrap();
		let AccessPath::Union {
			paths,
			..
		} = path_pt
		else {
			panic!("Expected AccessPath::Union, got {path_pt:?}");
		};
		assert_eq!(paths.len(), 1, "Point-only spatial index should emit 1 leaf scan op");

		// 2. CONTAINSALL on a GeometryCollection of two points emits the single-item scan count
		// (1 op on point-only index), not the union of both points.
		let col_cond = parse_cond(
			r#"loc CONTAINSALL {
				type: "GeometryCollection",
				geometries: [
					{ type: "Point", coordinates: [-0.12, 51.5] },
					{ type: "Point", coordinates: [139.69, 35.68] }
				]
			}"#,
		);
		let path_col =
			analyzer_pt.try_spatial_expansion(Some(&col_cond), Direction::Forward).unwrap();
		let AccessPath::Union {
			paths,
			..
		} = path_col
		else {
			panic!("Expected AccessPath::Union, got {path_col:?}");
		};
		assert_eq!(
			paths.len(),
			1,
			"CONTAINSALL on GeometryCollection should select a single item's covering"
		);

		// 3. OR union of two spatial predicates produces a flat AccessPath::Union
		let or_cond = parse_cond("loc INSIDE (-0.12, 51.5) OR loc INSIDE (139.69, 35.68)");
		let or_union = analyzer_pt
			.try_or_union(Some(&or_cond), Direction::Forward)
			.expect("try_or_union should succeed for OR of spatial predicates");
		let AccessPath::Union {
			paths,
			..
		} = or_union
		else {
			panic!("Expected flat AccessPath::Union, got {or_union:?}");
		};
		assert_eq!(paths.len(), 2);
		assert!(paths.iter().all(|p| matches!(p, AccessPath::BTreeScan { .. })));

		// 4. `try_spatial_expansion` forces `Direction::Forward` when any scan op is `Range`
		// even if the caller passes `Direction::Backward` (`ORDER BY id DESC`).
		let backward_path =
			analyzer_pt.try_spatial_expansion(Some(&pt_cond), Direction::Backward).unwrap();
		let AccessPath::Union {
			paths,
			..
		} = backward_path
		else {
			panic!("Expected AccessPath::Union, got {backward_path:?}");
		};
		assert!(paths.iter().all(|p| matches!(
			p,
			AccessPath::BTreeScan {
				direction: Direction::Forward,
				..
			}
		)));

		// 5. Mixed OR branches (`score > 0 AND loc INSIDE ...`) prefer the spatial index
		// (score 350) over the 1D range index on `score` (score 200).
		let ix_score = idx_current(2, "idx_score", &["score"]);
		let mut ix_loc = idx_current(1, "idx_loc", &["loc"]);
		ix_loc.index = crate::catalog::Index::Spatial;
		let analyzer_mixed = scalar_analyzer(vec![ix_loc, ix_score], None)
			.with_point_only_spatial(Arc::from(vec![true, false]));
		let mixed_or = parse_cond(
			"(score > 0 AND loc INSIDE (-0.12, 51.5)) OR (score > 0 AND loc INSIDE (139.69, 35.68))",
		);
		let mixed_union = analyzer_mixed
			.try_or_union(Some(&mixed_or), Direction::Forward)
			.expect("Mixed OR with spatial predicates should use spatial union");
		let AccessPath::Union {
			paths,
			..
		} = mixed_union
		else {
			panic!("Expected spatial Union, got {mixed_union:?}");
		};
		assert_eq!(paths.len(), 2);
		assert!(paths.iter().all(|p| matches!(
			p,
			AccessPath::BTreeScan {
				index_ref,
				..
			} if index_ref.name == "idx_loc"
		)));

		// 6. `try_knn_prefilter`:
		// - On a general (non-point-only) `SPATIAL` index, uses `bitmap_superset_plan` and retains
		//   the spatial predicate in `residual` for exact boundary verification.
		// - On a point-only `SPATIAL` index, attaches `SpatialPointFilter` to each cell range and
		//   produces an exact bitmap prefilter (`residual.is_none()`).
		let mut ix_gen = idx_current(1, "idx_loc", &["loc"]);
		ix_gen.index = crate::catalog::Index::Spatial;
		let analyzer_gen = analyzer(vec![ix_gen], None);
		let prefilter_gen = analyzer_gen
			.try_knn_prefilter(&pt_cond, &|ix, col| ix.one_entry_per_record(col))
			.expect("General spatial predicate should produce a KNN superset prefilter");
		assert!(
			prefilter_gen.residual.is_some(),
			"General spatial prefilter is a cell-covering superset and must keep its residual"
		);
		let prefilter_pt = analyzer_pt
			.try_knn_prefilter(&pt_cond, &|ix, col| ix.one_entry_per_record(col))
			.expect("Point-only spatial predicate should produce an exact KNN prefilter");
		assert!(
			prefilter_pt.residual.is_none(),
			"Point-only spatial prefilter evaluates exact inline coordinates and drops residual"
		);

		// 7. `access_repeats_records` is true for an unfiltered range scan on `Index::Spatial`,
		// and false when `point_filter` is attached (point-only disjoint covering).
		let spatial_ref = analyzer_pt.index_ref(0);
		let dummy_range = BTreeAccess::literal_range(Range {
			start: Bound::Included(Value::from(1i64)),
			end: Bound::Included(Value::from(10i64)),
		});
		assert!(crate::exec::index::access_path::access_repeats_records(
			&spatial_ref,
			&dummy_range
		));
		let AccessPath::BTreeScan {
			access: ref pt_branch_access,
			..
		} = paths[0]
		else {
			panic!("Expected BTreeScan branch");
		};
		assert!(pt_branch_access.point_filter().is_some());
		assert!(!crate::exec::index::access_path::access_repeats_records(
			&spatial_ref,
			pt_branch_access
		));

		// 8. Compound SPATIAL index (`FIELDS tenant, loc SPATIAL`) outscores both a
		// single-column B-Tree index (`FIELDS tenant`, score 500) and a single-column
		// SPATIAL index (`FIELDS loc SPATIAL`, score 350), emitting `BTreeAccess::Compound`
		// with score 550.
		let mut ix_compound = idx_current(1, "idx_tenant_loc", &["tenant", "loc"]);
		ix_compound.index = crate::catalog::Index::Spatial;
		let mut ix_single_loc = idx_current(2, "idx_loc", &["loc"]);
		ix_single_loc.index = crate::catalog::Index::Spatial;
		let ix_tenant = idx_current(3, "idx_tenant", &["tenant"]);
		let analyzer_comp = analyzer(vec![ix_single_loc, ix_compound, ix_tenant], None)
			.with_point_only_spatial(Arc::from(vec![true, true, false]));
		let comp_cond = parse_cond("tenant = 'acme' AND loc INSIDE (-0.12, 51.5)");
		let (comp_path, comp_score, comp_pinned) = analyzer_comp
			.try_spatial_expansion_scored(Some(&comp_cond), Direction::Forward)
			.expect("Compound spatial index should match tenant = 'acme' AND loc INSIDE ...");
		assert_eq!(
			comp_score, 550,
			"Compound spatial index with 1 equality prefix should score 550"
		);
		assert_eq!(comp_pinned, 1);
		let AccessPath::Union {
			paths: comp_paths,
			..
		} = comp_path
		else {
			panic!("Expected AccessPath::Union for compound spatial index, got {comp_path:?}");
		};
		assert_eq!(comp_paths.len(), 1);
		assert!(matches!(
			&comp_paths[0],
			AccessPath::BTreeScan {
				index_ref,
				access: BTreeAccess::Compound { prefix, .. },
				..
			} if index_ref.name == "idx_tenant_loc" && prefix.len() == 1
		));

		// 9. Multi-predicate spatial selection chooses the tightest covering (fewest scan ops)
		// when two spatial predicates appear in an AND conjunction.
		let multi_cond = parse_cond(
			r#"loc INSIDE {
				type: "Polygon",
				coordinates: [[[-10.0, 40.0], [10.0, 40.0], [10.0, 60.0], [-10.0, 60.0], [-10.0, 40.0]]]
			} AND loc INSIDE (-0.12, 51.5)"#,
		);
		let multi_path = analyzer_pt
			.try_spatial_expansion(Some(&multi_cond), Direction::Forward)
			.expect("Multi-predicate spatial query should succeed");
		let AccessPath::Union {
			paths: multi_paths,
			..
		} = multi_path
		else {
			panic!("Expected AccessPath::Union, got {multi_path:?}");
		};
		assert_eq!(
			multi_paths.len(),
			1,
			"Planner should choose the point predicate (1 op) over the wide polygon in an AND conjunction"
		);

		// 10. `or_union_from_expr` uses compound SPATIAL index (score 550) in OR branches even
		// when a single-column equality index (`idx_tenant`, score 500) is present.
		let comp_or_cond = parse_cond(
			"(tenant = 'acme' AND loc INSIDE (-0.12, 51.5)) OR (tenant = 'beta' AND loc INSIDE (2.35, 48.85))",
		);
		let (comp_or_path, comp_or_score) = analyzer_comp
			.or_union_from_expr(&comp_or_cond, Direction::Forward)
			.expect("OR of compound spatial branches should produce a Union");
		assert_eq!(comp_or_score, 550);
		let AccessPath::Union {
			paths: comp_or_paths,
			..
		} = comp_or_path
		else {
			panic!("Expected AccessPath::Union for compound spatial OR, got {comp_or_path:?}");
		};
		assert_eq!(comp_or_paths.len(), 2);
		assert!(comp_or_paths.iter().all(|p| matches!(
			p,
			AccessPath::BTreeScan {
				index_ref,
				access: BTreeAccess::Compound { prefix, .. },
				..
			} if index_ref.name == "idx_tenant_loc" && prefix.len() == 1
		)));

		// 11. `try_knn_prefilter` uses compound SPATIAL index when `stripped` contains both the
		// prefix equality (`tenant = 'acme'`) and the spatial predicate (`loc INSIDE ...`).
		let comp_knn_prefilter = analyzer_comp
			.try_knn_prefilter(&comp_cond, &|ix, col| ix.one_entry_per_record(col))
			.expect("Compound spatial predicate should produce a KNN prefilter");
		assert!(comp_knn_prefilter.residual.is_some());
		let has_compound_spatial_leaf = match &comp_knn_prefilter.root {
			BitmapPlan::And(children) | BitmapPlan::Or(children) => children.iter().any(|c| {
				matches!(
					c,
					BitmapPlan::BTree {
						index_ref,
						access: BTreeAccess::Compound { prefix, .. },
					} if index_ref.name == "idx_tenant_loc" && prefix.len() == 1
				) || matches!(
					c,
					BitmapPlan::Or(inner) if inner.iter().any(|ic| matches!(
						ic,
						BitmapPlan::BTree {
							index_ref,
							access: BTreeAccess::Compound { prefix, .. },
						} if index_ref.name == "idx_tenant_loc" && prefix.len() == 1
					))
				)
			}),
			BitmapPlan::BTree {
				..
			}
			| BitmapPlan::FullText {
				..
			}
			| BitmapPlan::Graph {
				..
			}
			| BitmapPlan::AndNot {
				..
			} => false,
		};
		assert!(
			has_compound_spatial_leaf,
			"KNN prefilter should use compound spatial index `idx_tenant_loc`, got {:?}",
			comp_knn_prefilter.root
		);

		// 12. `access_repeats_records` is false for an exact compound cell lookup (`range: None`
		// with scalar prefix) and true for a compound cell range (`range: Some(_)`).
		let comp_spatial_ref =
			analyzer_comp.index_ref(1).with_scalar_columns(vec![true, false].into_boxed_slice());
		let exact_compound = BTreeAccess::Compound {
			prefix: vec![
				BoundOperand::Literal(Value::from("acme")),
				BoundOperand::Literal(Value::from(42i64)),
			],
			range: None,
			upper: None,
		};
		assert!(!crate::exec::index::access_path::access_repeats_records(
			&comp_spatial_ref,
			&exact_compound
		));
		let range_compound = BTreeAccess::Compound {
			prefix: vec![BoundOperand::Literal(Value::from("acme"))],
			range: Some((BinaryOperator::MoreThanEqual, BoundOperand::Literal(Value::from(10i64)))),
			upper: Some((BinaryOperator::LessThanEqual, BoundOperand::Literal(Value::from(20i64)))),
		};
		assert!(crate::exec::index::access_path::access_repeats_records(
			&comp_spatial_ref,
			&range_compound
		));

		// 13. Vacuous-truth guard: `loc ALLINSIDE [pt1, pt2]` and `[pt1, pt2] CONTAINSALL loc`
		// must NOT expand via the spatial index because an empty array/set row (`loc = []`)
		// evaluates to `true` at runtime while emitting zero index entries.
		let loc_all_inside_arr = parse_cond("loc ALLINSIDE [(-0.12, 51.5), (139.69, 35.68)]");
		assert!(
			analyzer_pt
				.try_spatial_expansion(Some(&loc_all_inside_arr), Direction::Forward)
				.is_none()
		);
		let arr_contains_all_loc = parse_cond("[(-0.12, 51.5), (139.69, 35.68)] CONTAINSALL loc");
		assert!(
			analyzer_pt
				.try_spatial_expansion(Some(&arr_contains_all_loc), Direction::Forward)
				.is_none()
		);
		// Conversely, `loc CONTAINSALL [pt1, pt2]` and `[pt1, pt2] ALLINSIDE loc` DO expand.
		let loc_contains_all_arr = parse_cond("loc CONTAINSALL [(-0.12, 51.5), (139.69, 35.68)]");
		assert!(
			analyzer_pt
				.try_spatial_expansion(Some(&loc_contains_all_arr), Direction::Forward)
				.is_some()
		);
		let arr_all_inside_loc = parse_cond("[(-0.12, 51.5), (139.69, 35.68)] ALLINSIDE loc");
		assert!(
			analyzer_pt
				.try_spatial_expansion(Some(&arr_all_inside_loc), Direction::Forward)
				.is_some()
		);

		// 14. Query array containing an empty geometry component (`MultiPoint([])`) is rejected
		// so `CONTAINSANY` cannot miss rows that match the empty component vacuously.
		let contains_any_empty = parse_cond(
			r#"loc CONTAINSANY [(-0.12, 51.5), { type: "MultiPoint", coordinates: [] }]"#,
		);
		assert!(
			analyzer_pt
				.try_spatial_expansion(Some(&contains_any_empty), Direction::Forward)
				.is_none()
		);

		// 15. `scan_ordering` stops at the trailing Hilbert cell column of `Index::Spatial` even
		// if `scalar` is set on the `IndexRef`.
		let spatial_ref_all_scalar =
			analyzer_comp.index_ref(1).with_scalar_columns(vec![true, true].into_boxed_slice());
		let pinned_prefix_only = BTreeAccess::Compound {
			prefix: vec![BoundOperand::Literal(Value::from("acme"))],
			range: None,
			upper: None,
		};
		assert!(
			crate::exec::index::access_path::scan_ordering(
				&spatial_ref_all_scalar,
				&pinned_prefix_only,
				Direction::Forward,
			)
			.is_empty()
		);

		// 16. `<point> [lon, lat]` cast on a 2-element array literal in `geo::distance` expands
		// via the spatial index, whereas an invalid `<geometry<polygon>> (0.0, 0.0)` or
		// `<point> [1.0, 2.0, 3.0]` cast does not.
		let dist_cast_array = parse_cond("geo::distance(loc, <point> [6.365, 49.469]) < 400");
		assert!(
			analyzer_pt.try_spatial_expansion(Some(&dist_cast_array), Direction::Forward).is_some()
		);
		let invalid_cast_geom = parse_cond("loc INSIDE <geometry<polygon>> (0.0, 0.0)");
		assert!(
			analyzer_pt
				.try_spatial_expansion(Some(&invalid_cast_geom), Direction::Forward)
				.is_none()
		);
		let invalid_cast_point = parse_cond("loc INSIDE <point> [1.0, 2.0, 3.0]");
		assert!(
			analyzer_pt
				.try_spatial_expansion(Some(&invalid_cast_point), Direction::Forward)
				.is_none()
		);

		// 17. A regex equality on a compound prefix column (`tenant = /acme/`) must not match a
		// compound spatial index prefix (`idx_tenant_loc`, score 550) because `=` on a regex
		// matches any string under any key; instead it falls back to the single-column spatial
		// index (`idx_loc`, score 350).
		let regex_prefix_cond = parse_cond("tenant = /acme/ AND loc INSIDE (-0.12, 51.5)");
		let (_, regex_score, regex_pinned) = analyzer_comp
			.try_spatial_expansion_scored(Some(&regex_prefix_cond), Direction::Forward)
			.expect("Should fall back to single-column spatial index");
		assert_eq!(regex_score, 350);
		assert_eq!(regex_pinned, 0);

		// 17b. `geo::distance` does not expand on a non-point-only SPATIAL index (`analyzer_gen`)
		// because `geo::distance` returns `NONE` (`NONE < r` is `true`) for non-point geometries.
		assert!(
			analyzer_gen
				.try_spatial_expansion(Some(&dist_cast_array), Direction::Forward)
				.is_none(),
			"geo::distance must not expand on a non-point-only spatial index"
		);

		// 17c. Wide spatial coverings (continental box `[-10, 35]–[40, 70]`, `1,000 km` cap, and
		// whole-Earth cap) still expand on a single-column SPATIAL index when uncompeted, while
		// their spherical surface-area penalty scales down smoothly:
		// - `1,000 km` cap (`~0.61%` of Earth) scores in `210..300` (below bounded B-tree range
		//   `300`, above half-bounded `200`);
		// - continental Europe box (`[-10, 35]–[40, 70]`, `~3.37%` of Earth) and whole-Earth cap
		//   (`100%` of Earth, score `110`) drop below a half-bounded B-tree range (`200`), and are
		//   skipped by `try_bitmap_fusion` when paired with a B-tree anchor;
		// - `CONTAINSALL [$world, $paris]` selects and scores the `$paris` item rather than the
		//   1-cell `$world` root covering.
		let whole_earth_dist = parse_cond("geo::distance(loc, (0.0, 0.0)) < 25000000");
		let (_, whole_earth_score, _) = analyzer_pt
			.try_spatial_expansion_scored(Some(&whole_earth_dist), Direction::Forward)
			.expect("Whole-Earth cap should still expand on a SPATIAL index");
		assert_eq!(whole_earth_score, 110);
		let continental_poly = parse_cond(
			r#"loc INSIDE {
				type: "Polygon",
				coordinates: [[[-10.0, 35.0], [40.0, 35.0], [40.0, 70.0], [-10.0, 70.0], [-10.0, 35.0]]]
			}"#,
		);
		let (_, continental_score, _) = analyzer_pt
			.try_spatial_expansion_scored(Some(&continental_poly), Direction::Forward)
			.expect("Continental [-10,35]-[40,70] box should expand on a SPATIAL index");
		assert!(
			(170..200).contains(&continental_score),
			"Continental [-10,35]-[40,70] box score ({continental_score}) should drop below a half-bounded B-tree range (200)"
		);
		let cap_1000km = parse_cond("geo::distance(loc, (0.0, 51.5)) < 1000000");
		let (_, cap_1000km_score, _) = analyzer_pt
			.try_spatial_expansion_scored(Some(&cap_1000km), Direction::Forward)
			.expect("1,000 km cap should expand on a SPATIAL index");
		assert!(
			(210..300).contains(&cap_1000km_score),
			"1,000 km cap score ({cap_1000km_score}) should rank below a bounded B-tree range (300) and above a half-bounded range (200)"
		);
		// `try_bitmap_fusion` fuses a regional (`~3.37%` of Earth) spatial covering with a B-tree
		// predicate (`score > 39900`) into `BitmapPlan::And` rather than skipping fusion above
		// `1%`.
		let regional_range_and_btree = parse_cond(
			r#"score > 39900 AND loc INSIDE {
				type: "Polygon",
				coordinates: [[[-10.0, 35.0], [40.0, 35.0], [40.0, 70.0], [-10.0, 70.0], [-10.0, 35.0]]]
			}"#,
		);
		let regional_range_cands = analyzer_mixed.analyze(Some(&regional_range_and_btree), None);
		assert!(
			matches!(
				analyzer_mixed.try_bitmap_fusion(
					Some(&regional_range_and_btree),
					&regional_range_cands,
					&|ix, col| ix.one_entry_per_record(col)
				),
				Some(BitmapPlan::And(ref children)) if children.len() == 2
			),
			"Continental Europe (~3.37% area) covering should fuse into BitmapAnd with a B-tree candidate"
		);
		// `CONTAINSALL [$world, $paris]` chooses and scores `$paris`, whereas
		// `CONTAINSALL [$world, $world]` scores `110 < 200`.
		let contains_all_world_paris = parse_cond(
			r#"loc CONTAINSALL [
				{ type: "Polygon", coordinates: [[[-180.0, -90.0], [180.0, -90.0], [180.0, 90.0], [-180.0, 90.0], [-180.0, -90.0]]] },
				{ type: "Polygon", coordinates: [[[2.2, 48.8], [2.4, 48.8], [2.4, 48.9], [2.2, 48.9], [2.2, 48.8]]] }
			]"#,
		);
		let (_, world_paris_score, _) = analyzer_pt
			.try_spatial_expansion_scored(Some(&contains_all_world_paris), Direction::Forward)
			.expect("CONTAINSALL [$world, $paris] should expand");
		assert!(
			world_paris_score > 340,
			"CONTAINSALL [$world, $paris] should scan and score the selective $paris box, got {world_paris_score}"
		);
		let contains_all_world_world = parse_cond(
			r#"loc CONTAINSALL [
				{ type: "Polygon", coordinates: [[[-180.0, -90.0], [180.0, -90.0], [180.0, 90.0], [-180.0, 90.0], [-180.0, -90.0]]] },
				{ type: "Polygon", coordinates: [[[-180.0, -90.0], [180.0, -90.0], [180.0, 90.0], [-180.0, 90.0], [-180.0, -90.0]]] }
			]"#,
		);
		let (_, world_world_score, _) = analyzer_pt
			.try_spatial_expansion_scored(Some(&contains_all_world_world), Direction::Forward)
			.expect("CONTAINSALL [$world, $world] should expand");
		assert_eq!(world_world_score, 110);
		// Zero-area query geometries spanning the globe (horizontal equator line or vertical
		// meridian line) are scored by their covering cells' spherical surface area (`< 200`)
		// rather than their degenerate 0.0 bounding-box area (`350`), and `CONTAINSALL` picks
		// the selective point over the globe-spanning line.
		let equator_line = parse_cond(
			r#"loc INTERSECTS {
				type: "LineString",
				coordinates: [[-179.0, 0.0], [179.0, 0.0]]
			}"#,
		);
		let (_, equator_line_score, _) = analyzer_pt
			.try_spatial_expansion_scored(Some(&equator_line), Direction::Forward)
			.expect("Equator line should expand on a SPATIAL index");
		assert!(
			equator_line_score < 200,
			"Globe-spanning horizontal line covering must score below half-bounded B-tree range (200), got {equator_line_score}"
		);
		let meridian_line = parse_cond(
			r#"loc INTERSECTS {
				type: "LineString",
				coordinates: [[0.0, -89.0], [0.0, 89.0]]
			}"#,
		);
		let (_, meridian_line_score, _) = analyzer_pt
			.try_spatial_expansion_scored(Some(&meridian_line), Direction::Forward)
			.expect("Meridian line should expand on a SPATIAL index");
		assert!(
			meridian_line_score < 200,
			"Pole-to-pole meridian line covering must score below half-bounded B-tree range (200), got {meridian_line_score}"
		);
		let contains_all_paris_and_line = parse_cond(
			r#"loc CONTAINSALL [
				(2.35, 48.85),
				{
					type: "LineString",
					coordinates: [[-179.0, 0.0], [179.0, 0.0]]
				}
			]"#,
		);
		let (paris_only_path, _, _) = analyzer_pt
			.try_spatial_expansion_scored(
				Some(&parse_cond("loc CONTAINS (2.35, 48.85)")),
				Direction::Forward,
			)
			.expect("Point CONTAINS should expand");
		let (paris_and_line_path, paris_and_line_score, _) = analyzer_pt
			.try_spatial_expansion_scored(Some(&contains_all_paris_and_line), Direction::Forward)
			.expect("CONTAINSALL [Paris, equator_line] should expand");
		assert_eq!(paris_and_line_score, 350);
		assert_eq!(
			format!("{paris_and_line_path:?}"),
			format!("{paris_only_path:?}"),
			"CONTAINSALL [Paris, equator_line] must drive from Paris's tight covering, not equator_line's"
		);
		// Polar polygon spanning all 360° of longitude near the North Pole covers 16 level-4
		// cells (`78.75°..=90°`), whose spherical surface area is ~0.96% of Earth (versus 6.25%
		// equirectangular, which would clamp to 110), so its score (`266`) stays above 250.
		let polar_poly = parse_cond(
			r#"loc INSIDE {
				type: "Polygon",
				coordinates: [[[-180.0, 89.0], [180.0, 89.0], [180.0, 90.0], [-180.0, 90.0], [-180.0, 89.0]]]
			}"#,
		);
		let (_, polar_poly_score, _) = analyzer_pt
			.try_spatial_expansion_scored(Some(&polar_poly), Direction::Forward)
			.expect("Polar polygon should expand on a SPATIAL index");
		assert!(
			polar_poly_score > 250,
			"Polar polygon covering spherical area is <1% so score ({polar_poly_score}) must exceed 250"
		);
		// Two 1,000 km caps in an OR accumulate their spherical area penalties so the combined
		// union scores lower than either branch alone.
		let or_two_caps = parse_cond(
			"geo::distance(loc, (0.0, 51.5)) < 1000000 OR geo::distance(loc, (10.0, 45.0)) < 1000000",
		);
		let (_, or_caps_score) = analyzer_pt
			.or_union_from_expr(&or_two_caps, Direction::Forward)
			.expect("OR of two 1,000 km caps should expand");
		assert!(
			or_caps_score < cap_1000km_score && or_caps_score < 210,
			"OR of two 1,000 km caps ({or_caps_score}) should score lower than a single 1,000 km cap ({cap_1000km_score})"
		);
		let compound_continental = parse_cond(
			r#"tenant = 'acme' AND loc INSIDE {
				type: "Polygon",
				coordinates: [[[-10.0, 35.0], [40.0, 35.0], [40.0, 70.0], [-10.0, 70.0], [-10.0, 35.0]]]
			}"#,
		);
		let (_, comp_continental_score, comp_pinned) = analyzer_comp
			.try_spatial_expansion_scored(Some(&compound_continental), Direction::Forward)
			.expect(
				"Compound spatial index with matched equality prefix should expand wide polygon",
			);
		assert_eq!(comp_continental_score, 550);
		assert_eq!(comp_pinned, 1);

		// 18. `try_bitmap_fusion` does not fuse two spatial conjuncts on the same spatial index
		// with each other (`multi_cond` has no other index, so `try_bitmap_fusion` returns `None`
		// and lets `try_spatial_expansion` stream the single tightest covering).
		let multi_cands = analyzer_pt.analyze(Some(&multi_cond), None);
		assert!(
			analyzer_pt
				.try_bitmap_fusion(Some(&multi_cond), &multi_cands, &|ix, col| {
					ix.one_entry_per_record(col)
				})
				.is_none(),
			"Two spatial conjuncts on the same spatial column without another index must not self-fuse"
		);

		// 19. When two distinct spatial columns (`pickup`, `dropoff`) each have a `SPATIAL` index,
		// `try_bitmap_fusion` fuses both into a 2-branch `BitmapPlan::And`.
		let mut pickup_def = idx_current(1, "idx_pickup", &["pickup"]);
		pickup_def.index = crate::catalog::Index::Spatial;
		let mut dropoff_def = idx_current(2, "idx_dropoff", &["dropoff"]);
		dropoff_def.index = crate::catalog::Index::Spatial;
		let analyzer_dual = analyzer(vec![pickup_def, dropoff_def], None)
			.with_point_only_spatial(Arc::from(vec![true, true]));
		let dual_cond = parse_cond("pickup INSIDE (-0.12, 51.5) AND dropoff INSIDE (2.35, 48.85)");
		let dual_cands = analyzer_dual.analyze(Some(&dual_cond), None);
		let dual_plan = analyzer_dual
			.try_bitmap_fusion(Some(&dual_cond), &dual_cands, &|ix, col| {
				ix.one_entry_per_record(col)
			})
			.expect(
				"Two spatial conjuncts on distinct spatial columns should fuse via BitmapPlan::And",
			);
		assert!(matches!(dual_plan, BitmapPlan::And(ref children) if children.len() == 2));

		// 20. `Value::Set` literal of geometries in `CONTAINSALL` / `CONTAINSANY` expands via the
		// spatial index just like `Value::Array`.
		let set_contains_all = parse_cond("loc CONTAINSALL {(-0.12, 51.5), (2.35, 48.85)}");
		assert!(
			analyzer_pt
				.try_spatial_expansion(Some(&set_contains_all), Direction::Forward)
				.is_some()
		);

		// 21. `loc INSIDE [$g1, $g2]` (an array literal on the RHS of `INSIDE`) expands to a
		// superset cell union without attaching an inline `SpatialPointFilter` (`point_filter` is
		// `None`), and does not qualify for index-only `try_bitmap_count_fusion`, because runtime
		// `INSIDE` on an array checks disjunctive element equality (`Array::contains`), not
		// conjunctive `Geometry::Collection::contains`. `loc INTERSECTS [$g1]` is rejected
		// outright because `Value::intersects` returns `false` on arrays.
		let inside_array = parse_cond("loc INSIDE [(-0.12, 51.5), (2.35, 48.85)]");
		let Some(AccessPath::Union {
			paths: inside_arr_paths,
			..
		}) = analyzer_pt.try_spatial_expansion(Some(&inside_array), Direction::Forward)
		else {
			panic!("Expected Union access path for loc INSIDE [pt1, pt2]");
		};
		assert!(inside_arr_paths.iter().all(|p| {
			matches!(
				p,
				AccessPath::BTreeScan {
					access: BTreeAccess::Range {
						range: OperandRange {
							point_filter: None,
							..
						},
					},
					..
				}
			)
		}));
		assert!(analyzer_pt.try_bitmap_count_fusion(&inside_array, &|_, _| true).is_none());
		let intersects_array = parse_cond("loc INTERSECTS [(-0.12, 51.5)]");
		assert!(
			analyzer_pt
				.try_spatial_expansion(Some(&intersects_array), Direction::Forward)
				.is_none()
		);

		// 22. `try_bitmap_count_fusion` admits a single exact spatial predicate on a point-only
		// spatial index (`loc INSIDE (-0.12, 51.5)`) and `tenant = 'acme' AND !(loc INSIDE ...)`
		// when `tenant` has an exact B-tree index, but rejects `tenant = 'acme' AND !(loc INSIDE
		// ... AND unindexed = true)` so a negated conjunction is never partially subtracted.
		assert!(analyzer_pt.try_bitmap_count_fusion(&pt_cond, &|_, _| true).is_some());
		let tenant_btree = idx_current(10, "idx_tenant", &["tenant"]);
		let mut loc_spatial = idx_current(11, "idx_loc", &["loc"]);
		loc_spatial.index = crate::catalog::Index::Spatial;
		let analyzer_tenant_loc = analyzer(vec![tenant_btree, loc_spatial], None)
			.with_point_only_spatial(Arc::from(vec![false, true]));
		let exact_and_not = parse_cond("tenant = 'acme' AND !(loc INSIDE (-0.12, 51.5))");
		assert!(matches!(
			analyzer_tenant_loc.try_bitmap_count_fusion(&exact_and_not, &|_, _| true),
			Some(BitmapPlan::AndNot { .. })
		));
		// A regional spatial box covering ~2.5% of Earth's surface still participates in
		// `try_bitmap_fusion` when paired with a B-tree equality leaf (`tenant = 'acme'`).
		let regional_fusion_cond = parse_cond(
			r#"tenant = 'acme' AND loc INSIDE {
				type: "Polygon",
				coordinates: [[[0.0, 0.0], [30.0, 0.0], [30.0, 20.0], [0.0, 20.0], [0.0, 0.0]]]
			}"#,
		);
		let regional_cands = analyzer_tenant_loc.analyze(Some(&regional_fusion_cond), None);
		let regional_fusion = analyzer_tenant_loc
			.try_bitmap_fusion(Some(&regional_fusion_cond), &regional_cands, &|ix, col| {
				ix.one_entry_per_record(col)
			})
			.expect("Regional (> 1% area) single-column spatial predicate should fuse with B-tree equality via BitmapPlan::And");
		assert!(matches!(regional_fusion, BitmapPlan::And(ref children) if children.len() == 2));
		let partial_and_not =
			parse_cond("tenant = 'acme' AND !(loc INSIDE (-0.12, 51.5) AND unindexed = true)");
		assert!(
			analyzer_tenant_loc.try_bitmap_count_fusion(&partial_and_not, &|_, _| true).is_none(),
			"Negated conjunction with an unindexed conjunct must not be partially subtracted"
		);

		// 23. A spatial `IndexDefinition` stamped below `SPATIAL_COVERING_FORMAT_VERSION` is
		// skipped by all planner entry points (`try_spatial_expansion`, `try_bitmap_fusion`,
		// `try_bitmap_count_fusion`, `try_knn_prefilter`, and `try_or_union`) so queries fall
		// back to a table scan until `REBUILD INDEX` restamps the index.
		let mut stale_spatial = idx_current(20, "idx_loc_stale", &["loc"]);
		stale_spatial.index = crate::catalog::Index::Spatial;
		stale_spatial.format_version = crate::catalog::SPATIAL_COVERING_FORMAT_VERSION - 1;
		let tenant_for_stale = idx_current(21, "idx_tenant", &["tenant"]);
		let analyzer_stale = analyzer(vec![stale_spatial, tenant_for_stale], None)
			.with_point_only_spatial(Arc::from(vec![true, false]));
		assert!(
			analyzer_stale.try_spatial_expansion(Some(&pt_cond), Direction::Forward).is_none(),
			"Stale-stamped spatial index must not be offered by try_spatial_expansion"
		);
		assert!(
			analyzer_stale.try_bitmap_count_fusion(&pt_cond, &|_, _| true).is_none(),
			"Stale-stamped spatial index must not be offered by try_bitmap_count_fusion"
		);
		assert!(
			analyzer_stale
				.try_knn_prefilter(&pt_cond, &|ix, col| ix.one_entry_per_record(col))
				.is_none(),
			"Stale-stamped spatial index must not be offered by try_knn_prefilter"
		);
		assert!(
			analyzer_stale.try_or_union(Some(&or_cond), Direction::Forward).is_none(),
			"Stale-stamped spatial index must not be offered by try_or_union"
		);
		let stale_and_cond = parse_cond("tenant = 'acme' AND loc INSIDE (-0.12, 51.5)");
		let stale_cands = analyzer_stale.analyze(Some(&stale_and_cond), None);
		assert!(
			analyzer_stale
				.try_bitmap_fusion(Some(&stale_and_cond), &stale_cands, &|ix, col| {
					ix.one_entry_per_record(col)
				})
				.is_none(),
			"Stale-stamped spatial index must not be offered by try_bitmap_fusion"
		);

		// 24. Multi-branch spatial OR queries share the `MAX_SPATIAL_SCAN_OPS` budget across
		// branches instead of dropping to `None` (`TableScan`):
		// - a 4-branch mixed-index (`analyzer_gen`, with ancestor probes) OR of regional boxes
		//   produces `AccessPath::Union` within the 96-op cap;
		// - a 16-branch point-only (`analyzer_pt`) OR of city-sized boxes (`0.4° × 0.3°`) produces
		//   `AccessPath::Union` within the 96-op cap;
		// - fractional area penalties across 16 boxes are summed in `f64` before rounding rather
		//   than truncated to `0` per branch.
		let mixed_4_or_sql = (0..4)
			.map(|i| {
				let lon0 = (i as f64) * 5.0;
				let lon1 = lon0 + 0.4;
				format!(
					r#"loc INSIDE {{ type: "Polygon", coordinates: [[[{lon0}, 45.0], [{lon1}, 45.0], [{lon1}, 45.3], [{lon0}, 45.3], [{lon0}, 45.0]]] }}"#
				)
			})
			.collect::<Vec<_>>()
			.join(" OR ");
		let mixed_4_or = parse_cond(&mixed_4_or_sql);
		let (mixed_4_union, _) =
			analyzer_gen.or_union_from_expr(&mixed_4_or, Direction::Forward).expect(
				"4-branch mixed-index spatial OR should coarsen per branch instead of returning None",
			);
		let AccessPath::Union {
			paths: mixed_4_paths,
			..
		} = mixed_4_union
		else {
			panic!("Expected AccessPath::Union for 4-branch mixed OR");
		};
		assert!(
			!mixed_4_paths.is_empty()
				&& mixed_4_paths.len() <= surrealdb_idx::spatial::MAX_SPATIAL_SCAN_OPS
		);

		// An 8-branch or 24-branch mixed-index (`analyzer_gen`) OR where one box crosses `lon = 0`
		// (`[-0.1..0.1, 51.4..51.6]`) coarsens ancestor depth without collapsing that branch to
		// the root cell, whereas a 49-branch mixed-index OR where per-branch budget is 1 op
		// declines the OR (`None` -> `TableScan`) instead of emitting root-cell full-index range
		// scans, while a 49-branch point-only OR (`analyzer_pt`) still produces an
		// `AccessPath::Union`.
		let root_cell = surrealdb_idx::spatial::SpatialCellId::root();
		let root_min = Value::from(root_cell.range_min().0);
		let root_max = Value::from(root_cell.range_max().0);
		let path_covers_root = |path: &AccessPath| -> bool {
			let AccessPath::BTreeScan {
				access: BTreeAccess::Range {
					range,
					..
				},
				..
			} = path
			else {
				return false;
			};
			matches!(
				(&range.start, &range.end),
				(
					Bound::Included(BoundOperand::Literal(lo)),
					Bound::Included(BoundOperand::Literal(hi)),
				) if *lo <= root_min && *hi >= root_max
			)
		};
		for branches in [8usize, 24] {
			let mixed_cross_meridian_sql = (0..branches)
				.map(|i| {
					let lon0 = -0.1 + (i as f64) * 2.0;
					let lon1 = lon0 + 0.2;
					format!(
						r#"loc INSIDE {{ type: "Polygon", coordinates: [[[{lon0}, 51.4], [{lon1}, 51.4], [{lon1}, 51.6], [{lon0}, 51.6], [{lon0}, 51.4]]] }}"#
					)
				})
				.collect::<Vec<_>>()
				.join(" OR ");
			let mixed_or = parse_cond(&mixed_cross_meridian_sql);
			let (mixed_union, _) = analyzer_gen
				.or_union_from_expr(&mixed_or, Direction::Forward)
				.unwrap_or_else(|| {
					panic!(
						"{branches}-branch mixed-index OR crossing lon = 0 should produce AccessPath::Union without collapsing to root"
					)
				});
			let AccessPath::Union {
				paths: mixed_paths,
				..
			} = mixed_union
			else {
				panic!("Expected AccessPath::Union for {branches}-branch mixed OR");
			};
			assert!(
				!mixed_paths.is_empty()
					&& mixed_paths.len() <= surrealdb_idx::spatial::MAX_SPATIAL_SCAN_OPS
			);
			assert!(
				!mixed_paths.iter().any(path_covers_root),
				"{branches}-branch mixed-index OR crossing lon = 0 must not emit a full-index root range scan"
			);
		}

		let mixed_49_or_sql = (0..49)
			.map(|i| {
				let lon0 = -0.1 + (i as f64) * 2.0;
				let lon1 = lon0 + 0.2;
				format!(
					r#"loc INSIDE {{ type: "Polygon", coordinates: [[[{lon0}, 51.4], [{lon1}, 51.4], [{lon1}, 51.6], [{lon0}, 51.6], [{lon0}, 51.4]]] }}"#
				)
			})
			.collect::<Vec<_>>()
			.join(" OR ");
		let mixed_49_or = parse_cond(&mixed_49_or_sql);
		assert!(
			analyzer_gen.or_union_from_expr(&mixed_49_or, Direction::Forward).is_none(),
			"49-branch mixed-index OR must decline the union and fall back to TableScan instead of scanning the root cell"
		);
		let (pt_49_union, _) =
			analyzer_pt.or_union_from_expr(&mixed_49_or, Direction::Forward).expect(
				"49-branch point-only OR crossing lon = 0 should still produce AccessPath::Union",
			);
		assert!(matches!(pt_49_union, AccessPath::Union { .. }));

		let pt_16_or_sql = (0..16)
			.map(|i| {
				let lon0 = -30.0 + (i as f64) * 3.0;
				let lon1 = lon0 + 1.2;
				format!(
					r#"loc INSIDE {{ type: "Polygon", coordinates: [[[{lon0}, 10.0], [{lon1}, 10.0], [{lon1}, 11.2], [{lon0}, 11.2], [{lon0}, 10.0]]] }}"#
				)
			})
			.collect::<Vec<_>>()
			.join(" OR ");
		let pt_16_or = parse_cond(&pt_16_or_sql);
		let (pt_16_union, pt_16_score) =
			analyzer_pt.or_union_from_expr(&pt_16_or, Direction::Forward).expect(
				"16-branch point-only spatial OR should coarsen per branch instead of returning None",
			);
		let AccessPath::Union {
			paths: pt_16_paths,
			..
		} = pt_16_union
		else {
			panic!("Expected AccessPath::Union for 16-branch point-only OR");
		};
		assert!(
			!pt_16_paths.is_empty()
				&& pt_16_paths.len() <= surrealdb_idx::spatial::MAX_SPATIAL_SCAN_OPS
		);
		assert!(
			pt_16_score < 350,
			"16-branch OR of 1.2°×1.2° boxes must accumulate fractional area penalties before rounding, got {pt_16_score}"
		);

		// 25. `spatial_plan_cache` verifies predicate equality on hash hit so a colliding hash key
		// never returns another predicate's covering or area.
		let pred_a = SpatialPredicate::Geometry {
			geom: crate::val::Geometry::Point(geo_types::Point::new(-0.12, 51.5)),
			exact_kind: Some(SpatialExactKind::Inside),
		};
		let pred_b = SpatialPredicate::Geometry {
			geom: crate::val::Geometry::Point(geo_types::Point::new(139.69, 35.68)),
			exact_kind: Some(SpatialExactKind::Inside),
		};
		let key_b = pred_b.cache_key(false, surrealdb_idx::spatial::MAX_SPATIAL_SCAN_OPS);
		let (ops_a, _) = analyzer_pt.plan_spatial_scans(
			&pred_a,
			false,
			surrealdb_idx::spatial::MAX_SPATIAL_SCAN_OPS,
		);
		// Force a synthetic hash collision by inserting `pred_a` under `key_b`.
		analyzer_pt.spatial_plan_cache.borrow_mut().insert(
			key_b,
			vec![(
				pred_a,
				false,
				surrealdb_idx::spatial::MAX_SPATIAL_SCAN_OPS,
				Arc::clone(&ops_a),
				0.99,
			)],
		);
		let (ops_b, area_b) = analyzer_pt.plan_spatial_scans(
			&pred_b,
			false,
			surrealdb_idx::spatial::MAX_SPATIAL_SCAN_OPS,
		);
		assert_ne!(
			ops_a.as_ref(),
			ops_b.as_ref(),
			"Hash collision in spatial_plan_cache must not return pred_a's covering for pred_b"
		);
		assert!(area_b < 0.01);

		// 26. `value_to_geometry` rejects geometries containing non-finite coordinates (`inf` or
		// `NaN`) so query predicates with non-finite vertices decline the spatial index plan.
		let non_finite_poly = crate::val::Geometry::Polygon(geo_types::Polygon::new(
			geo_types::LineString::from(vec![
				(0.0, 0.0),
				(50.0, 0.0),
				(f64::INFINITY, 25.0),
				(50.0, 50.0),
				(0.0, 50.0),
				(0.0, 0.0),
			]),
			vec![],
		));
		assert!(IndexAnalyzer::value_to_geometry(Value::Geometry(non_finite_poly)).is_none());

		// 27. `index_scan_is_id_only` and `spatial_union_is_id_only` require `!versioned` and
		// `DecodeFields::is_id_only()` so `explain.rs` and `host.rs` stay in sync under `VERSION`
		// scopes.
		let id_only_decode =
			crate::exec::operators::scan::common::DecodeFields::new(std::iter::empty::<String>());
		assert!(crate::exec::index::access_path::spatial_union_is_id_only(
			true,
			false,
			Some(&id_only_decode),
		));
		assert!(!crate::exec::index::access_path::spatial_union_is_id_only(
			true,
			true,
			Some(&id_only_decode),
		));
		assert!(crate::exec::index::access_path::index_scan_is_id_only(
			false,
			pt_branch_access,
			false,
			Some(&id_only_decode),
		));
		assert!(!crate::exec::index::access_path::index_scan_is_id_only(
			false,
			pt_branch_access,
			true,
			Some(&id_only_decode),
		));

		// 28. Non-exact decimal radii in `geo::distance(loc, center) < dec` / `<= dec` normalize
		// against `Number::cmp` (`(f_hi, false)` where `f_hi.next_down() < dec < f_hi`) so that
		// `SpatialPointFilter::Cap` agrees with `Number::cmp` at half-ULP boundaries.
		let cond_lt_above =
			parse_cond("geo::distance(loc, (0.0, 0.0)) < 100.00000000000000000000000001dec");
		let cond_le_above =
			parse_cond("geo::distance(loc, (0.0, 0.0)) <= 100.00000000000000000000000001dec");
		let cond_lt_below =
			parse_cond("geo::distance(loc, (0.0, 0.0)) < 99.99999999999999999999999999dec");
		let cond_le_below =
			parse_cond("geo::distance(loc, (0.0, 0.0)) <= 99.99999999999999999999999999dec");
		for cond in [&cond_lt_above, &cond_le_above] {
			let mut preds = Vec::new();
			IndexAnalyzer::collect_spatial_expressions(cond, &mut preds);
			assert_eq!(preds.len(), 1);
			assert_eq!(
				preds[0].1,
				SpatialPredicate::Cap {
					center: crate::val::Geometry::Point((0.0, 0.0).into()),
					radius: 100.0_f64.next_up(),
					inclusive: false,
				}
			);
		}
		for cond in [&cond_lt_below, &cond_le_below] {
			let mut preds = Vec::new();
			IndexAnalyzer::collect_spatial_expressions(cond, &mut preds);
			assert_eq!(preds.len(), 1);
			assert_eq!(
				preds[0].1,
				SpatialPredicate::Cap {
					center: crate::val::Geometry::Point((0.0, 0.0).into()),
					radius: 100.0_f64,
					inclusive: false,
				}
			);
		}

		// 29. Only truth-preserving prefix operators (`<bool>` on a statically-boolean inner
		// expression, and `<any>`) recurse into `collect_spatial_expressions` and
		// `collect_conditions`; casts to other types (such as `<string>`) and range prefixes
		// (`..`, `..=`) do not push the inner spatial predicate down.
		for non_truth_preserving in [
			"<string> (loc INSIDE (-0.12, 51.5))",
			"<int> (loc INSIDE (-0.12, 51.5))",
			"..(loc INSIDE (-0.12, 51.5))",
			"..=(loc INSIDE (-0.12, 51.5))",
			"!(loc INSIDE (-0.12, 51.5))",
			"<bool> (NONE AND loc INSIDE (-0.12, 51.5))",
		] {
			let cond = parse_cond(non_truth_preserving);
			assert!(
				analyzer_pt.try_spatial_expansion(Some(&cond), Direction::Forward).is_none(),
				"Non-truth-preserving prefix `{non_truth_preserving}` must not push down to spatial index"
			);
		}
		for truth_preserving in [
			"<bool> (loc INSIDE (-0.12, 51.5))",
			"<any> (loc INSIDE (-0.12, 51.5))",
			"<bool> (tenant = 'acme' AND loc INSIDE (-0.12, 51.5))",
		] {
			let cond = parse_cond(truth_preserving);
			assert!(
				analyzer_pt.try_spatial_expansion(Some(&cond), Direction::Forward).is_some(),
				"Truth-preserving prefix `{truth_preserving}` should push down to spatial index"
			);
		}
	}

	#[tokio::test]
	async fn knn_with_index_subquery_and_order_by_distance_end_to_end() {
		use surrealdb_types::ToSql;

		let ds = crate::kvs::Datastore::builder()
			.with_capabilities(crate::dbs::Capabilities::all())
			.with_auth(false)
			.build_with_path("memory")
			.await
			.unwrap();
		let ses = crate::dbs::Session::owner().with_ns("test").with_db("test");

		let setup_sql = r#"
			DEFINE NAMESPACE test;
			DEFINE DATABASE test;
			DEFINE TABLE places;
			DEFINE FIELD cat ON places TYPE string;
			DEFINE FIELD vec ON places TYPE array<float>;
			DEFINE INDEX idx_places_cat ON places FIELDS cat;
			DEFINE INDEX idx_places_vec ON places FIELDS vec HNSW DIMENSION 2 DIST COSINE TYPE F32;
			CREATE places:near_close SET cat = 'near', vec = [1.0, 0.0];
			CREATE places:near_far   SET cat = 'near', vec = [0.6, 0.8];
			CREATE places:far_close  SET cat = 'far',  vec = [1.0, 0.0];
		"#;
		let res = ds.execute(setup_sql, &ses, None).await.unwrap();
		for r in res {
			r.output().unwrap();
		}

		// WITH INDEX composability with KNN + pre-filter, subquery post-filtering, and standard
		// SQL `ORDER BY vector::distance::<metric>(...) LIMIT k`: Note that `places:near_close`
		// and `places:far_close` both have `vec = [1.0, 0.0]` (cosine distance 0 to `$photo`),
		// while `places:near_far` has `vec = [0.6, 0.8]` (cosine distance 0.4 to `$photo`).
		// - Pre-filtered `k = 2` (whether via `<|2, 40|>`, `WITH INDEX`, or `ORDER BY ... LIMIT 2`)
		//   must return both near places `[places:near_close, places:near_far]`.
		// - Post-filtered `k = 2` via subquery must take the global top-2 (`places:near_close` and
		//   `places:far_close`) and then filter by `cat = 'near'`, returning only
		//   `[places:near_close]`.
		let knn_ergonomics_sql = r#"
			LET $photo = [1.0, 0.0];
			SELECT VALUE id FROM places WITH INDEX idx_places_cat
				WHERE cat = 'near' AND vec <|2, 40|> $photo;
			SELECT VALUE id FROM places WITH INDEX idx_places_cat, idx_places_vec
				WHERE cat = 'near' AND vec <|2, 40|> $photo
				EXPLAIN;
			SELECT VALUE id FROM (
				SELECT id, cat, vec FROM places WHERE vec <|2, 40|> $photo
			) WHERE cat = 'near';
			SELECT VALUE id FROM places
				WHERE cat = 'near'
				ORDER BY vector::distance::cosine(vec, $photo) ASC
				LIMIT 2;
			SELECT VALUE id FROM places
				WHERE cat = 'near'
				ORDER BY vector::distance::cosine(vec, $photo) ASC
				LIMIT 2
				EXPLAIN;
		"#;
		let mut erg_res = ds.execute(knn_ergonomics_sql, &ses, None).await.unwrap();
		let with_index_val = erg_res.remove(1).output().unwrap();
		assert_eq!(
			with_index_val.to_sql(),
			"[places:near_close, places:near_far]",
			"WITH INDEX idx_places_cat must preserve pre-filtered KNN semantics and return both near places"
		);
		let with_index_explain = erg_res.remove(1).output().unwrap().to_sql();
		assert!(
			with_index_explain.contains("KnnScan")
				&& with_index_explain.contains("idx_places_vec")
				&& with_index_explain.contains("BitmapIndexScan")
				&& with_index_explain.contains("idx_places_cat"),
			"WITH INDEX idx_places_cat, idx_places_vec must plan KnnScan + BitmapIndexScan, got: {with_index_explain}"
		);
		let post_filter_val = erg_res.remove(1).output().unwrap();
		assert_eq!(
			post_filter_val.to_sql(),
			"[places:near_close]",
			"Subquery post-filtering must not push outer WHERE into KNN subquery"
		);
		let order_by_val = erg_res.remove(1).output().unwrap();
		assert_eq!(
			order_by_val.to_sql(),
			"[places:near_close, places:near_far]",
			"ORDER BY vector::distance::cosine(vec, $photo) ASC LIMIT 2 must return pre-filtered top-2 in distance order"
		);
		let order_by_explain = erg_res.remove(1).output().unwrap().to_sql();
		assert!(
			order_by_explain.contains("KnnScan")
				&& order_by_explain.contains("idx_places_vec")
				&& order_by_explain.contains("BitmapIndexScan")
				&& order_by_explain.contains("idx_places_cat"),
			"ORDER BY vector::distance::cosine(...) ASC LIMIT 2 must plan KnnScan + BitmapIndexScan, got: {order_by_explain}"
		);
	}
}
