//! Index analysis for matching WHERE conditions to available indexes.
//!
//! The [`IndexAnalyzer`] examines query conditions and ORDER BY clauses to find
//! indexes that can accelerate the query.

use std::collections::HashMap;
use std::ops::Bound;
use std::sync::Arc;

use super::access_path::{
	AccessPath, BTreeAccess, BitmapPlan, BoundOperand, IndexRef, KnnPrefilterPlan, OperandRange,
	select_access_path,
};
use crate::catalog::{Index, IndexDefinition, KeyStructure};
use crate::exec::planner::util::{
	KnnQueryVector, KnnVectorClass, classify_knn_vector, expr_contains_matches,
	idiom_matches_column_in, idiom_matches_containment, try_bound_operand, try_literal_to_value,
};
use crate::expr::logical::{LogicalCall, LogicalExpr, LogicalOrdering};
use crate::expr::operator::{MatchesOperator, NearestNeighbor, PrefixOperator};
use crate::expr::with::With;
use crate::expr::{BinaryOperator, Function, Idiom, Part};
use crate::kvs::Direction;
use crate::val::{Number, Range, Value};

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
	/// Per index of `indexes`, whether each column is declared a kind that
	/// holds no array; a column past the end of its index's flags is not
	/// known to. `None` knows of none. Carried on each [`IndexRef`] made.
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
}

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
		}
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

	/// Know the columns `flags` marks, per index of `indexes`, to be declared
	/// kinds that hold no array.
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
		let index_ref = match super::derived_columns::coarse_columns(key, &self.indexes[idx].cols) {
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
	/// `idx`: the column holds elements for the path, and is not withheld.
	/// Every containment match goes through here.
	fn seeks_containment(&self, idx: usize, col: usize, path: &LogicalExpr) -> bool {
		let withheld = self
			.no_containment
			.as_ref()
			.is_some_and(|flags| flags.get(idx).and_then(|f| f.get(col)).copied().unwrap_or(false));
		!withheld
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
			candidates.retain(|c| {
				matches!(c.access, BTreeAccess::Knn { .. })
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

		// Analyze each branch independently
		let mut branch_paths = Vec::with_capacity(branches.len());
		let mut min_score = u32::MAX;
		for branch_expr in branches {
			let candidates = self.analyze(Some(branch_expr), None);
			if candidates.is_empty() {
				// This branch has no index — cannot use union
				return None;
			}
			let branch_score = candidates.iter().map(|c| c.score()).max().unwrap_or(0);
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
				_ => {
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
	/// responsibility; `exact_col` reports whether a column's declared
	/// field kind guarantees scalar values (array values fan out to one
	/// index entry per element, making b-tree bitmaps over-approximate and
	/// therefore unusable for subtraction).
	pub(crate) fn try_bitmap_fusion(
		&self,
		cond: Option<&LogicalExpr>,
		candidates: &[IndexCandidate],
		exact_col: &dyn Fn(&Idiom) -> bool,
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
			covered.extend(cols);
			if let Some(leaf) = Self::bitmap_leaf_from_candidate(c) {
				if and_children.is_empty() {
					anchor_at_most_one_row = Self::candidate_returns_at_most_one_row(c);
				}
				and_children.push(leaf);
			}
		}

		// OR / IN / containment conjuncts become union members when every
		// branch is independently bitmap-capable.
		for conjunct in &conjuncts {
			let path = match conjunct {
				LogicalExpr::Binary {
					op: BinaryOperator::Or,
					..
				} => self.or_union_from_expr(conjunct, Direction::Forward).map(|(p, _)| p),
				LogicalExpr::Binary {
					op:
						BinaryOperator::Inside
						| BinaryOperator::ContainAll
						| BinaryOperator::ContainAny
						| BinaryOperator::AllInside
						| BinaryOperator::AnyInside,
					..
				} => self
					.try_in_expansion(Some(conjunct), Direction::Forward)
					.or_else(|| self.try_containment_expansion(Some(conjunct), Direction::Forward)),
				_ => None,
			};
			if let Some(path) = path
				&& let Some(node) = Self::bitmap_plan_from_access_path(&path)
			{
				and_children.push(node);
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
		match Self::bitmap_plan_from_access_path(&path)? {
			BitmapPlan::Or(children)
				if children.len() >= 2 && !Self::equality_branches_disjoint(&children) =>
			{
				Some((BitmapPlan::Or(children), path))
			}
			_ => None,
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
		match expr {
			LogicalExpr::Prefix {
				op: PrefixOperator::Not,
				expr: inner,
			} => Some(inner),
			LogicalExpr::Call(call)
				if call.args.len() == 1
					&& matches!(
						&call.receiver,
						crate::expr::function::Function::Normal(name) if name == "not"
					) =>
			{
				Some(&call.args[0])
			}
			_ => None,
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
	/// covering every column.
	fn candidate_returns_at_most_one_row(c: &IndexCandidate) -> bool {
		if !c.index_ref.is_unique() {
			return false;
		}
		match &c.access {
			BTreeAccess::Equality(_) => true,
			BTreeAccess::Compound {
				prefix,
				range,
			} => range.is_none() && prefix.len() >= c.index_ref.definition().cols.len(),
			_ => false,
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
		let (path, lit) = match conjunct {
			LogicalExpr::Binary {
				left,
				op: BinaryOperator::Contain,
				right,
			} => match (&**left, &**right) {
				(path, lit) if path.is_path() => (path, lit),
				_ => return None,
			},
			LogicalExpr::Binary {
				left,
				op: BinaryOperator::Inside,
				right,
			} => match (&**left, &**right) {
				(lit, path) if path.is_path() => (path, lit),
				_ => return None,
			},
			_ => return None,
		};
		let Some(Value::RecordId(anchor)) = try_literal_to_value(lit) else {
			return None;
		};
		// Two hops over the row: lowering places a run of lookups that
		// continues nowhere inside a `Broadcast` over the current value, or
		// bare when nothing follows; both spell the same path.
		let hops = match path {
			LogicalExpr::Broadcast {
				base,
				body,
				..
			} if matches!(**base, LogicalExpr::Current) => &**body,
			other => other,
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
			match s {
				LogicalLookupSubject::Table {
					table,
					referencing_field: None,
				} => tables.push(table.clone()),
				_ => return None,
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
	/// a single non-qualifying conjunct disqualifies the plan. Single-conjunct
	/// WHERE clauses are left to the existing single-index key-only count.
	pub(crate) fn try_bitmap_count_fusion(
		&self,
		cond: &LogicalExpr,
		exact_col: &dyn Fn(&Idiom) -> bool,
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
			return None;
		}
		let mut positives: Vec<BitmapPlan> = Vec::new();
		let mut nots: Vec<BitmapPlan> = Vec::new();
		for conjunct in conjuncts {
			if let Some(inner) = Self::as_negated_expr(conjunct) {
				nots.push(self.bitmap_exact_plan(inner, exact_col)?);
			} else {
				positives.push(self.bitmap_exact_plan(conjunct, exact_col)?);
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
	///   ([`IndexRef::one_entry_per_record`]) or are declared with an array-free field kind
	///   (`exact_col`) — array values fan out to one index entry per element, which would make the
	///   bitmap a superset of the predicate's truth set — and whose range no number bounds
	///   ([`super::access_path::range_matches_comparison`]);
	/// - an OR of such leaves (`B OR C` is exactly `B ∪ C`).
	///
	/// Anything else returns `None`.
	fn bitmap_exact_plan(
		&self,
		inner: &LogicalExpr,
		exact_col: &dyn Fn(&Idiom) -> bool,
	) -> Option<BitmapPlan> {
		// `NOT (B OR C)` — subtract the union when every branch is exact.
		if let LogicalExpr::Binary {
			op: BinaryOperator::Or,
			..
		} = inner
		{
			let mut branches = Vec::new();
			Self::flatten_or(inner, &mut branches);
			let mut children = Vec::with_capacity(branches.len());
			for branch in branches {
				children.push(self.bitmap_exact_plan(branch, exact_col)?);
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
		// exact candidate.
		let candidates = self.analyze(Some(inner), None);
		candidates
			.into_iter()
			.filter(|c| {
				if !Self::bitmap_capable_candidate(c) {
					return false;
				}
				match &c.access {
					BTreeAccess::FullText {
						..
					} => true,
					// A range over a datetime component of the record id lists
					// records whose datetime lies in the second of a bound, not
					// beyond it, so its bitmap is a superset of the truth set.
					access if super::derived_columns::range_is_widened(&c.index_ref, access) => {
						false
					}
					_ => {
						super::access_path::range_matches_comparison(&c.access)
							&& Self::candidate_pinned_columns(c).iter().enumerate().all(
								|(i, col)| c.index_ref.one_entry_per_record(i) || exact_col(col),
							)
					}
				}
			})
			.max_by_key(|c| c.score())
			.as_ref()
			.and_then(Self::bitmap_leaf_from_candidate)
	}

	/// Split a KNN query's stripped WHERE into an exact allow-list bitmap
	/// plan and a true residual (pre-filtered vector search).
	///
	/// Per conjunct of the AND chain:
	/// - a conjunct containing a MATCHES operator joins the prefilter when it is exactly
	///   bitmap-representable (its full-text index is online in the current format); otherwise it
	///   is dropped entirely — MATCHES is not evaluable inside the ANN traversal (no query executor
	///   there, it computes to `false` and would reject every candidate), so an uncovered MATCHES
	///   is enforced only by the outer `Filter` re-applying the KNN-stripped WHERE;
	/// - a negated conjunct always stays residual (no table-universe bitmap is maintained, and
	///   index-NOT is inexact for records missing the field);
	/// - any other conjunct joins the prefilter iff its truth set is *exactly* a candidate bitmap
	///   ([`Self::bitmap_exact_plan`]). Exactness is mandatory: covered conjuncts are dropped from
	///   the in-traversal filter, and a superset bitmap would let non-matching candidates consume
	///   top-K slots (the outer `Filter` would drop them, silently returning fewer than k rows).
	///
	/// One covered conjunct suffices — unlike [`Self::try_bitmap_fusion`]'s
	/// ≥ 2 rule — because it already eliminates in-traversal record fetches.
	///
	/// The covered conjuncts are reported by their index in the `AND` chain of `stripped`
	/// ([`KnnPrefilterPlan::covered`]), never by their text, so a caller maps them back to its own
	/// conjuncts whatever rewriting `stripped` went through.
	pub(crate) fn try_knn_prefilter(
		&self,
		stripped: &LogicalExpr,
		exact_col: &dyn Fn(&Idiom) -> bool,
	) -> Option<KnnPrefilterPlan> {
		// Explicit hints pin the plan.
		if self.with_hints.is_some() {
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
			} else if let Some(leaf) = self.bitmap_exact_plan(conjunct, exact_col) {
				covered.push(leaf);
				covered_at.push(at);
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

	/// Collect CONTAINSALL/CONTAINSANY (field path on left, array literal on
	/// right) and ALLINSIDE/ANYINSIDE (array literal on left, field path on
	/// right) from an AND tree.
	fn collect_containment_expressions(
		expr: &LogicalExpr,
		results: &mut Vec<(LogicalExpr, Vec<Value>)>,
	) {
		match expr {
			LogicalExpr::Binary {
				left,
				op: BinaryOperator::And,
				right,
			} => {
				Self::collect_containment_expressions(left, results);
				Self::collect_containment_expressions(right, results);
			}
			LogicalExpr::Binary {
				left,
				op: BinaryOperator::ContainAll | BinaryOperator::ContainAny,
				right,
			} => {
				if left.is_path()
					&& let Some(Value::Array(arr)) = try_literal_to_value(right)
				{
					results.push((left.as_ref().clone(), arr.0));
				}
			}
			LogicalExpr::Binary {
				left,
				op: BinaryOperator::AllInside | BinaryOperator::AnyInside,
				right,
			} => {
				if right.is_path()
					&& let Some(Value::Array(arr)) = try_literal_to_value(left)
				{
					results.push((right.as_ref().clone(), arr.0));
				}
			}
			LogicalExpr::Prefix {
				op,
				expr: inner,
			} if !matches!(op, PrefixOperator::Not) => {
				Self::collect_containment_expressions(inner, results);
			}
			_ => {}
		}
	}

	/// Collect `field INSIDE [values]` expressions from an AND tree.
	fn collect_in_expressions(expr: &LogicalExpr, results: &mut Vec<(LogicalExpr, Vec<Value>)>) {
		match expr {
			LogicalExpr::Binary {
				left,
				op: BinaryOperator::And,
				right,
			} => {
				Self::collect_in_expressions(left, results);
				Self::collect_in_expressions(right, results);
			}
			LogicalExpr::Binary {
				left,
				op: BinaryOperator::Inside,
				right,
			} => {
				if left.is_path()
					&& let Some(Value::Array(arr)) = try_literal_to_value(right)
				{
					results.push((left.as_ref().clone(), arr.0));
				}
			}
			// Do NOT recurse into NOT — expanding `NOT (field IN [...])`
			// into index lookups would produce the wrong result set.
			LogicalExpr::Prefix {
				op,
				expr: inner,
			} if !matches!(op, PrefixOperator::Not) => {
				Self::collect_in_expressions(inner, results);
			}
			_ => {}
		}
	}

	/// Flatten nested OR expressions into a list of branches.
	///
	/// `A OR B OR C` (parsed as `(A OR B) OR C`) becomes `[A, B, C]`.
	fn flatten_or<'b>(expr: &'b LogicalExpr, branches: &mut Vec<&'b LogicalExpr>) {
		match expr {
			LogicalExpr::Binary {
				left,
				op: BinaryOperator::Or,
				right,
			} => {
				Self::flatten_or(left, branches);
				Self::flatten_or(right, branches);
			}
			_ => {
				branches.push(expr);
			}
		}
	}

	/// Flatten nested AND expressions into a list of conjuncts.
	///
	/// `A AND B AND C` (parsed as `(A AND B) AND C`) becomes `[A, B, C]`.
	/// Non-AND nodes (including OR sub-trees) are returned as single
	/// conjuncts so the caller can inspect them.
	fn flatten_and<'b>(expr: &'b LogicalExpr, conjuncts: &mut Vec<&'b LogicalExpr>) {
		match expr {
			LogicalExpr::Binary {
				left,
				op: BinaryOperator::And,
				right,
			} => {
				Self::flatten_and(left, conjuncts);
				Self::flatten_and(right, conjuncts);
			}
			_ => {
				conjuncts.push(expr);
			}
		}
	}

	/// Returns `true` if the expression tree contains a KNN (nearest-neighbour)
	/// operator. Such an operator is consumed by a `KnnScan` and stripped from
	/// the WHERE before residual filtering, so it must not be left behind as a
	/// residual by an index substitution (see `try_and_nested_or_union`).
	fn expr_contains_knn(expr: &LogicalExpr) -> bool {
		match expr {
			LogicalExpr::Binary {
				left,
				op,
				right,
			} => {
				matches!(op, BinaryOperator::NearestNeighbor(_))
					|| Self::expr_contains_knn(left)
					|| Self::expr_contains_knn(right)
			}
			LogicalExpr::Prefix {
				expr: inner,
				..
			} => Self::expr_contains_knn(inner),
			_ => false,
		}
	}

	/// Collect all simple conditions from an AND tree.
	fn collect_conditions(&self, expr: &LogicalExpr, conditions: &mut Vec<SimpleCondition>) {
		match expr {
			LogicalExpr::Binary {
				left,
				op,
				right,
			} => {
				match op {
					BinaryOperator::And => {
						// Recurse into AND branches
						self.collect_conditions(left, conditions);
						self.collect_conditions(right, conditions);
					}
					BinaryOperator::Or => {
						// Don't collect from OR branches
					}
					_ => {
						// Try to extract a simple condition
						if let Some(cond) = self.extract_simple_condition(left, op, right) {
							conditions.push(cond);
						}
					}
				}
			}
			// Do NOT recurse into NOT — `NOT (field > 5)` must not
			// generate an index candidate for `field > 5`.
			LogicalExpr::Prefix {
				op,
				expr: inner,
			} if !matches!(op, PrefixOperator::Not) => {
				self.collect_conditions(inner, conditions);
			}
			_ => {}
		}
	}

	/// The bound operand `expr` denotes as the value side of a comparison
	/// with `op`: a literal, or the statement clock where the caller may read
	/// it and `op` is an equality or an ordering comparison
	/// ([`try_bound_operand`]).
	fn operand_of(&self, expr: &LogicalExpr, op: &BinaryOperator) -> Option<BoundOperand> {
		try_bound_operand(expr, self.clock_allowed && is_ordering_comparison(op))
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

			for (col_idx, col) in ix_def.cols.iter().enumerate() {
				// Find a condition that matches this column
				let matching_cond = conditions
					.iter()
					.find(|c| path_matches_column(&c.path, col, self.key.as_deref()));

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
				let containment_cond = if matching_cond.is_none() {
					conditions.iter().find(|c| {
						matches!(
							(&c.op, c.position),
							(BinaryOperator::Contain, IdiomPosition::Left)
								| (BinaryOperator::Inside, IdiomPosition::Right)
						) && self.seeks_containment(idx, col_idx, &c.path)
					})
				} else {
					None
				};

				if let Some(cond) = containment_cond {
					prefix_values.push(cond.value.clone());
					continue;
				}

				match matching_cond {
					// No key is a whole array (see `match_operator_to_access`),
					// so a comparison to one ends the prefix.
					Some(cond) if matches!(cond.value.as_literal(), Some(Value::Array(_))) => break,
					Some(cond) => {
						let is_equality =
							matches!(cond.op, BinaryOperator::Equal | BinaryOperator::ExactEqual);

						if is_equality {
							// Equality condition -- add to prefix
							prefix_values.push(cond.value.clone());
						} else {
							// Non-equality (range) -- capture the range condition
							// on this column and stop. The range narrows the scan
							// beyond the equality prefix.
							if let Some(op) = normalize_range_op(&cond.op, cond.position) {
								range_condition = Some((op, cond.value.clone()));
							} else if matches!(cond.op, BinaryOperator::NotEqual)
								&& matches!(cond.value.as_literal(), Some(Value::None))
							{
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
								range_condition =
									Some((BinaryOperator::MoreThan, cond.value.clone()));
							}
							break;
						}
					}
					None => {
						// No condition for this column -- stop looking
						break;
					}
				}
			}

			// Create compound candidate if we have at least 2 equality columns,
			// or at least 1 equality column with a range on the next column.
			if prefix_values.len() >= 2 || (!prefix_values.is_empty() && range_condition.is_some())
			{
				let access = BTreeAccess::Compound {
					prefix: prefix_values,
					range: range_condition,
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
			other => other,
		});

		// Keep only the best candidate per index
		candidates.dedup_by(|a, b| a.index_ref.idx == b.index_ref.idx);
	}

	/// Analyze a single expression for index opportunities.
	fn analyze_condition(&self, expr: &LogicalExpr, candidates: &mut Vec<IndexCandidate>) {
		match expr {
			// Binary expression - check for indexable patterns
			LogicalExpr::Binary {
				left,
				op,
				right,
			} => {
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
					_ => {
						self.try_match_comparison(left, op, right, candidates);
					}
				}
			}
			// A prefix test names a contiguous stretch of the key space.
			LogicalExpr::Call(call) => self.try_match_prefix_call(call, candidates),
			// Nested expression in parentheses (but NOT negation).
			// Do NOT recurse into NOT — negated predicates invert
			// the result set and index candidates would be wrong.
			LogicalExpr::Prefix {
				op,
				expr: inner,
			} if !matches!(op, PrefixOperator::Not) => {
				self.analyze_condition(inner, candidates);
			}
			_ => {}
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
		// are pre-folded into literals before the analyzer runs (see
		// `resolve_condition_params` in the planner and dynamic scan). A bare
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
					match access {
						BTreeAccess::Equality(v) => BTreeAccess::Compound {
							prefix: vec![v],
							range: None,
						},
						other => other,
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
				match value.as_literal()? {
					// Single-element array: treat as equality (field IN [v] → field = v)
					Value::Array(arr) if arr.len() == 1 => {
						Some(BTreeAccess::literal_equality(arr[0].clone()))
					}
					// A range literal states the bounds directly, so it is
					// the same access as the two comparisons that spell it
					// out: `field IN 1..9` reads the keys `>= 1` and `< 9`.
					// The literal's own bounds carry which end is open, so
					// every spelling (`..`, `..=`, `>..`, `>..=`) maps
					// without a case of its own.
					Value::Range(r) => Some(BTreeAccess::literal_range((**r).clone())),
					_ => None,
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
		let (path, value) = match op {
			BinaryOperator::Contain if left.is_path() => match try_literal_to_value(right) {
				Some(v) => (left, v),
				None => return,
			},
			BinaryOperator::Inside if right.is_path() => match try_literal_to_value(left) {
				Some(v) => (right, v),
				None => return,
			},
			_ => return,
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
			NearestNeighbor::K(k, d) => (*k, None, Some(d)),
			_ => return,
		};

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
		let resolved = match right {
			LogicalExpr::Param(param) => self.knn_query_vectors.and_then(|v| v.get(param.as_str())),
			_ => None,
		};
		let vector = match resolved {
			Some(vector) => Arc::clone(vector),
			None => match classify_knn_vector(right, self.registry) {
				KnnVectorClass::Vector(KnnQueryVector::Literal(vector)) => vector,
				KnnVectorClass::Vector(KnnQueryVector::Deferred(_)) => Arc::from(Vec::new()),
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
						&& crate::catalog::Distance::from((*d).clone()) != hnsw.distance
					{
						continue;
					}
					user_ef.unwrap_or_else(|| k.max(hnsw.ef_construction as u32))
				}
				Index::DiskAnn(diskann) => {
					if let Some(d) = required_distance
						&& crate::catalog::Distance::from((*d).clone()) != diskann.distance
					{
						continue;
					}
					user_ef.unwrap_or_else(|| k.max(diskann.l_build as u32))
				}
				_ => continue,
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
	/// | Unique equality                         | 1_000 | Returns at most one row              |
	/// | Non-unique equality                     |   500 | Returns a small bucket               |
	/// | Compound prefix len N (capped at 6)     | 400+50N | Each pinned column shrinks scan    |
	/// | Compound prefix + range on next col     |   +25 | Slight further narrowing             |
	/// | FullText / KNN                          |   800 | Specialised; only applies for MATCHES/<\|N\|> |
	/// | Bounded range                           |   300 | Both sides bounded                   |
	/// | Half-bounded range                      |   200 | One side bounded                     |
	/// | Full range (covers ORDER BY only)       |    50 | Sort-elim worth more than table scan |
	/// | Plus: `covers_order` bonus              |  +100 | Avoids in-memory Sort                |
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
				score += if self.index_ref.is_unique() {
					1000
				} else {
					500
				};
			}
			BTreeAccess::Compound {
				prefix,
				range,
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
	/// - a unique index pinned on every column returns at most one row, and wins;
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
		let (equalities, whole) = match &self.access {
			BTreeAccess::FullText {
				..
			}
			| BTreeAccess::Knn {
				..
			} => return true,
			BTreeAccess::Equality(_) => (1, self.index_ref.cols.len() == 1),
			BTreeAccess::Compound {
				prefix,
				..
			} => (prefix.len(), prefix.len() == self.index_ref.cols.len()),
			BTreeAccess::Range {
				..
			} => (0, false),
		};
		if self.index_ref.is_unique() && whole {
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
			_ => AccessPath::BTreeScan {
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
		IdiomPosition::Left => match op {
			BinaryOperator::MoreThan
			| BinaryOperator::MoreThanEqual
			| BinaryOperator::LessThan
			| BinaryOperator::LessThanEqual => Some(op.clone()),
			_ => None,
		},
		IdiomPosition::Right => match op {
			BinaryOperator::LessThan => Some(BinaryOperator::MoreThan),
			BinaryOperator::LessThanEqual => Some(BinaryOperator::MoreThanEqual),
			BinaryOperator::MoreThan => Some(BinaryOperator::LessThan),
			BinaryOperator::MoreThanEqual => Some(BinaryOperator::LessThanEqual),
			_ => None,
		},
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

	fn analyzer<'a>(defs: Vec<IndexDefinition>, with: Option<&'a With>) -> IndexAnalyzer<'a> {
		static REGISTRY: std::sync::LazyLock<crate::exec::function::FunctionRegistry> =
			std::sync::LazyLock::new(crate::exec::function::FunctionRegistry::with_builtins);
		IndexAnalyzer::new(Arc::<[_]>::from(defs.into_boxed_slice()), with, &REGISTRY)
	}

	/// An analyzer that knows every column of `defs` to be declared a kind
	/// holding no array, as a table defining its indexed fields with scalar
	/// types offers it.
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
		let plan = match exprs.remove(0) {
			surrealdb_expr::parser::TopLevelExpr::Expr(select @ crate::expr::Expr::Select(_)) => {
				lower(select).expect("SELECT lowers")
			}
			other => panic!("expected SELECT, got {other:?}"),
		};
		let (mut cond, mut order, mut with) = (None, None, None);
		let mut stack = vec![&plan];
		while let Some(op) = stack.pop() {
			match op {
				LogicalOp::Filter(LogicalFilter {
					predicate,
					..
				}) => cond = Some(predicate.clone()),
				LogicalOp::Sort(LogicalSort {
					ordering,
					..
				}) => order = Some(ordering.clone()),
				LogicalOp::Source(LogicalSource {
					with: hint,
					..
				}) => with = hint.clone(),
				_ => {}
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
		use crate::catalog::BTREE_ENTRY_DOC_IDS_FORMAT_VERSION;

		/// A b-tree index at the doc-ID entry format (bitmap-capable).
		pub(super) fn idx_v2(id: u32, name: &str, cols: &[&str]) -> IndexDefinition {
			let mut def = idx_basic(id, name, cols);
			def.format_version = BTREE_ENTRY_DOC_IDS_FORMAT_VERSION;
			def
		}

		/// Run `try_bitmap_fusion` the way `resolve_access_path` does: analyze
		/// without ORDER BY, then attempt the fusion. `exact` stands in for
		/// the field-kind check gating NOT subtraction.
		fn fuse(az: &IndexAnalyzer<'_>, cond: &LogicalExpr, exact: bool) -> Option<BitmapPlan> {
			let candidates = az.analyze(Some(cond), None);
			az.try_bitmap_fusion(Some(cond), &candidates, &move |_| exact)
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

		/// A unique index at the doc-ID entry format (bitmap-capable).
		fn idx_uniq_v2(id: u32, name: &str, cols: &[&str]) -> IndexDefinition {
			let mut def = idx_uniq(id, name, cols);
			def.format_version = BTREE_ENTRY_DOC_IDS_FORMAT_VERSION;
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
			// Without the field-kind exactness guarantee the NOT stays a
			// residual-only filter, and a single positive branch is not
			// worth fusing.
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
			assert!(az.try_bitmap_count_fusion(&cond, &|_| true).is_some());
			// One conjunct not index-covered: ineligible (no residual filter
			// exists to repair the count).
			let cond = parse_cond("a = 1 AND b = 2 AND unindexed = 3");
			assert!(az.try_bitmap_count_fusion(&cond, &|_| true).is_none());
			// Array-admitting field kinds are ineligible (entry fan-out
			// would inflate the count).
			let cond = parse_cond("a = 1 AND b = 2");
			assert!(az.try_bitmap_count_fusion(&cond, &|_| false).is_none());
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
			for cond in ["acl CONTAINS 'a'", "'a' INSIDE acl", "acl CONTAINSANY ['a', 'b']"] {
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
			match path {
				AccessPath::BTreeScan {
					index_ref,
					..
				} => {
					assert_eq!(index_ref.name.as_str(), "ix_a_uniq", "uniq must win");
				}
				other => panic!("expected BTreeScan, got {other:?}"),
			}
		}

		#[test]
		fn first_col_of_compound_becomes_prefix() {
			// Single-column equality on the first column of a compound index
			// must be lifted to `BTreeAccess::Compound { prefix: [v], .. }`.
			let a = analyzer(vec![idx_basic(1, "ix_ab", &["a", "b"])], None);
			let cond = parse_cond("a = 5");
			let cands = a.analyze(Some(&cond), None);
			let c = find_for(&cands, "ix_ab").expect("ix_ab candidate");
			match &c.access {
				BTreeAccess::Compound {
					prefix,
					range,
				} => {
					assert_eq!(prefix.len(), 1);
					assert!(range.is_none());
				}
				other => panic!("expected Compound, got {other:?}"),
			}
		}

		#[test]
		fn idiom_mismatch_yields_no_candidate() {
			let a = analyzer(vec![idx_basic(1, "ix_a", &["a"])], None);
			let cond = parse_cond("b = 5");
			let cands = a.analyze(Some(&cond), None);
			assert!(cands.is_empty(), "no index matches column b");
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
			match &c.access {
				BTreeAccess::Range {
					range,
				} => {
					assert!(matches!(range.start, Bound::Excluded(_)));
					assert!(matches!(range.end, Bound::Unbounded));
				}
				other => panic!("expected Range, got {other:?}"),
			}
		}

		#[test]
		fn half_bounded_gte() {
			let a = scalar_analyzer(vec![idx_basic(1, "ix_a", &["a"])], None);
			let cond = parse_cond("a >= 5");
			let cands = a.analyze(Some(&cond), None);
			let c = find_for(&cands, "ix_a").expect("ix_a");
			match &c.access {
				BTreeAccess::Range {
					range,
					..
				} => {
					assert!(matches!(range.start, Bound::Included(_)));
				}
				other => panic!("expected Range, got {other:?}"),
			}
		}

		#[test]
		fn bounded_after_merge() {
			// `a > 5 AND a < 10` must merge to a bounded range.
			let a = analyzer(vec![idx_basic(1, "ix_a", &["a"])], None);
			let cond = parse_cond("a > 5 AND a < 10");
			let cands = a.analyze(Some(&cond), None);
			let c = find_for(&cands, "ix_a").expect("ix_a");
			match &c.access {
				BTreeAccess::Range {
					range,
				} => {
					assert!(!matches!(range.start, Bound::Unbounded));
					assert!(!matches!(range.end, Bound::Unbounded));
				}
				other => panic!("expected merged Range, got {other:?}"),
			}
		}

		#[test]
		fn value_lt_idiom_normalises() {
			// `5 < a` should be treated as `a > 5`.
			let a = scalar_analyzer(vec![idx_basic(1, "ix_a", &["a"])], None);
			let cond = parse_cond("5 < a");
			let cands = a.analyze(Some(&cond), None);
			let c = find_for(&cands, "ix_a").expect("ix_a");
			match &c.access {
				BTreeAccess::Range {
					range,
				} => {
					assert!(!matches!(range.start, Bound::Unbounded));
					assert!(matches!(range.end, Bound::Unbounded));
				}
				other => panic!("expected Range from-bound, got {other:?}"),
			}
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
			match &c.access {
				BTreeAccess::Compound {
					prefix,
					range,
				} => {
					assert_eq!(prefix.len(), 2, "two equalities → prefix length 2");
					assert!(range.is_none());
				}
				other => panic!("expected Compound, got {other:?}"),
			}
		}

		#[test]
		fn equality_then_range() {
			let a = scalar_analyzer(vec![idx_basic(1, "ix_abc", &["a", "b", "c"])], None);
			let cond = parse_cond("a = 1 AND b > 5");
			let cands = a.analyze(Some(&cond), None);
			let c = find_for(&cands, "ix_abc").expect("ix_abc");
			match &c.access {
				BTreeAccess::Compound {
					prefix,
					range,
				} => {
					assert_eq!(prefix.len(), 1);
					assert!(range.is_some(), "range on b captured");
				}
				other => panic!("expected Compound with range, got {other:?}"),
			}
		}

		#[test]
		fn trailing_equality_after_range_is_dropped() {
			// `a = 1 AND b > 5 AND c = 2` on (a,b,c): the c=2 must NOT be part
			// of the index prefix (BTree limitation); becomes residual filter.
			let a = scalar_analyzer(vec![idx_basic(1, "ix_abc", &["a", "b", "c"])], None);
			let cond = parse_cond("a = 1 AND b > 5 AND c = 2");
			let cands = a.analyze(Some(&cond), None);
			let c = find_for(&cands, "ix_abc").expect("ix_abc");
			match &c.access {
				BTreeAccess::Compound {
					prefix,
					range,
				} => {
					assert_eq!(prefix.len(), 1);
					assert!(range.is_some());
				}
				other => panic!("expected Compound, got {other:?}"),
			}
		}

		#[test]
		fn a_containment_extends_the_prefix_over_a_fanning_column() {
			// `path_matches_column` vetoes a column holding `*`, so the
			// containment matcher is the only way `emails.*.norm` is reached.
			let a = analyzer(vec![idx_basic(1, "ix_ae", &["account", "emails.*.norm"])], None);
			let cond = parse_cond("account = 1 AND emails.*.norm CONTAINS 'x'");
			let cands = a.analyze(Some(&cond), None);
			let c = find_for(&cands, "ix_ae").expect("ix_ae");
			match &c.access {
				BTreeAccess::Compound {
					prefix,
					range,
				} => {
					assert_eq!(prefix.len(), 2, "both columns pinned, not just the account");
					assert!(range.is_none());
				}
				other => panic!("expected Compound, got {other:?}"),
			}
		}

		#[test]
		fn the_reversed_containment_spelling_extends_it_too() {
			let a = analyzer(vec![idx_basic(1, "ix_ae", &["account", "emails.*.norm"])], None);
			let cond = parse_cond("account = 1 AND 'x' INSIDE emails.*.norm");
			let cands = a.analyze(Some(&cond), None);
			let c = find_for(&cands, "ix_ae").expect("ix_ae");
			match &c.access {
				BTreeAccess::Compound {
					prefix,
					..
				} => assert_eq!(prefix.len(), 2),
				other => panic!("expected Compound, got {other:?}"),
			}
		}

		#[test]
		fn a_containment_pins_a_fanning_column_before_a_trailing_range() {
			let a = scalar_analyzer(
				vec![idx_basic(1, "ix_aeb", &["account", "emails.*.norm", "b"])],
				None,
			);
			let cond = parse_cond("account = 1 AND emails.*.norm CONTAINS 'x' AND b > 5");
			let cands = a.analyze(Some(&cond), None);
			let c = find_for(&cands, "ix_aeb").expect("ix_aeb");
			match &c.access {
				BTreeAccess::Compound {
					prefix,
					range,
				} => {
					assert_eq!(prefix.len(), 2);
					assert!(range.is_some(), "the range on b terminates the prefix");
				}
				other => panic!("expected Compound, got {other:?}"),
			}
		}

		#[test]
		fn a_containment_on_each_of_two_fanning_columns_pins_both() {
			// Every column of the index fans out, and a containment leaf
			// pins each: the prefix addresses one key per record, the same
			// as an all-equality prefix over scalar columns.
			let a = analyzer(vec![idx_basic(1, "ix_tn", &["tags.*", "nums.*"])], None);
			let cond = parse_cond("tags CONTAINS 'x' AND nums CONTAINS 1");
			let cands = a.analyze(Some(&cond), None);
			let c = find_for(&cands, "ix_tn").expect("ix_tn");
			match &c.access {
				BTreeAccess::Compound {
					prefix,
					range,
				} => {
					assert_eq!(prefix.len(), 2, "both fanning columns pinned");
					assert!(range.is_none());
				}
				other => panic!("expected Compound, got {other:?}"),
			}
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
			match &c.access {
				BTreeAccess::Range {
					range: OperandRange {
						start,
						end,
					},
				} => {
					assert!(matches!(
						start,
						Bound::Excluded(BoundOperand::Literal(crate::val::Value::None))
					));
					assert!(matches!(end, Bound::Unbounded));
				}
				other => panic!("expected exclusive Range from NONE, got {other:?}"),
			}
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
			match path {
				AccessPath::Union {
					paths,
					dedupe,
				} => {
					assert_eq!(paths.len(), 3);
					assert!(!dedupe, "scalar IN-expansion branches are record-disjoint");
				}
				other => panic!("expected Union, got {other:?}"),
			}
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
			match path {
				AccessPath::Union {
					paths,
					dedupe,
				} => {
					assert_eq!(paths.len(), 32);
					assert!(!dedupe);
				}
				other => panic!("expected Union, got {other:?}"),
			}
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
			match path {
				AccessPath::Union {
					paths,
					dedupe,
				} => {
					assert_eq!(paths.len(), 2);
					assert!(dedupe, "OR branches may both hold on the same row");
				}
				other => panic!("expected Union, got {other:?}"),
			}
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
			match path {
				AccessPath::BTreeScan {
					index_ref,
					access,
					..
				} => {
					assert_eq!(index_ref.name.as_str(), "ix_b");
					assert!(matches!(access, BTreeAccess::Equality(_)));
				}
				AccessPath::Union {
					..
				} => {
					panic!("empty branch should have been dropped, leaving a single path")
				}
				other => panic!("expected BTreeScan(ix_b), got {other:?}"),
			}
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
			match path {
				AccessPath::Union {
					paths,
					dedupe,
				} => {
					assert_eq!(paths.len(), 2, "one branch per FT match");
					assert!(dedupe, "a row may match both FT branches");
				}
				other => panic!("expected Union, got {other:?}"),
			}
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
			az.try_knn_prefilter(&stripped, &move |_| exact)
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
		fn inexact_column_stays_residual() {
			// Without a declared array-free field kind the equality is not
			// provably exact, so nothing is covered and no prefilter forms.
			let az =
				analyzer(vec![idx_v2(1, "idx_a", &["a"]), idx_hnsw(2, "hn_vec", &["vec"])], None);
			let cond = parse_cond("a = 1 AND vec <|2,100|> [0.0, 0.0]");
			assert!(split(&az, &cond, false).is_none());
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
			match &c.access {
				BTreeAccess::Range {
					range: OperandRange {
						start,
						end,
					},
				} => {
					assert!(matches!(start, Bound::Included(_)));
					assert!(matches!(end, Bound::Included(_)));
				}
				other => panic!("expected Range, got {other:?}"),
			}
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
			match &c.access {
				BTreeAccess::Range {
					range,
				} => {
					assert!(
						matches!(range.start, Bound::Excluded(BoundOperand::Literal(crate::val::Value::Number(n))) if n.to_int() == 10)
					);
					assert!(matches!(range.end, Bound::Unbounded));
				}
				other => panic!("expected single tightened Range, got {other:?}"),
			}
		}

		#[test]
		fn same_side_to_bounds_keep_tighter() {
			// `a < 100 AND a < 50` → to bound tightens to < 50.
			let a = analyzer(vec![idx_basic(1, "ix_a", &["a"])], None);
			let cond = parse_cond("a < 100 AND a < 50");
			let cands = a.analyze(Some(&cond), None);
			let c = find_for(&cands, "ix_a").expect("ix_a");
			match &c.access {
				BTreeAccess::Range {
					range,
				} => {
					assert!(matches!(range.start, Bound::Unbounded));
					assert!(
						matches!(range.end, Bound::Excluded(BoundOperand::Literal(crate::val::Value::Number(n))) if n.to_int() == 50)
					);
				}
				other => panic!("expected single tightened Range, got {other:?}"),
			}
		}

		#[test]
		fn same_side_mixed_inclusive_picks_exclusive() {
			// `a > 5 AND a >= 5` — same value, exclusive is tighter.
			let a = scalar_analyzer(vec![idx_basic(1, "ix_a", &["a"])], None);
			let cond = parse_cond("a > 5 AND a >= 5");
			let cands = a.analyze(Some(&cond), None);
			let c = find_for(&cands, "ix_a").expect("ix_a");
			match &c.access {
				BTreeAccess::Range {
					range: OperandRange {
						start: Bound::Excluded(_),
						..
					},
				} => {}
				other => panic!("expected Range with inclusive start bound, got {other:?}"),
			}
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
		/// indexed columns are declared kinds holding no array (a component never
		/// holds one).
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
			match &c.access {
				BTreeAccess::Equality(value) => {
					assert_eq!(value, &BoundOperand::from(datetime("2025-01-01T10:00:05.500Z")));
				}
				other => panic!("expected Equality, got {other:?}"),
			}
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

		fn permitted<'a>(defs: Vec<IndexDefinition>) -> IndexAnalyzer<'a> {
			analyzer(defs, None).with_clock(true)
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
	}
}
