//! Index analysis for matching WHERE conditions to available indexes.
//!
//! The [`IndexAnalyzer`] examines query conditions and ORDER BY clauses to find
//! indexes that can accelerate the query.

use std::sync::Arc;

use super::access_path::{AccessPath, BTreeAccess, IndexRef, RangeBound, select_access_path};
use crate::catalog::{Index, IndexDefinition};
use crate::exec::planner::util::try_literal_to_value;
use crate::expr::operator::{MatchesOperator, NearestNeighbor, PrefixOperator};
use crate::expr::order::Ordering;
use crate::expr::with::With;
use crate::expr::{BinaryOperator, Cond, Expr, Idiom};
use crate::idx::planner::ScanDirection;
use crate::val::{Number, Value};

/// Analyzes query conditions to find matching indexes.
pub struct IndexAnalyzer<'a> {
	/// Available indexes for the table
	pub indexes: Arc<[IndexDefinition]>,
	/// Optional WITH INDEX/NOINDEX hints
	pub with_hints: Option<&'a With>,
}

impl<'a> IndexAnalyzer<'a> {
	/// Create a new analyzer for the given table and indexes.
	pub fn new(indexes: Arc<[IndexDefinition]>, with_hints: Option<&'a With>) -> Self {
		Self {
			indexes,
			with_hints,
		}
	}

	/// Analyze conditions and ORDER BY to find candidate access paths.
	///
	/// Returns a list of index candidates that could be used for this query.
	pub fn analyze(&self, cond: Option<&Cond>, order: Option<&Ordering>) -> Vec<IndexCandidate> {
		let mut candidates = Vec::new();

		// Skip analysis if indexes are empty
		if self.indexes.is_empty() {
			return candidates;
		}

		// Analyze WHERE conditions
		if let Some(cond) = cond {
			// First, collect all simple conditions (idiom op value)
			let mut conditions = Vec::new();
			self.collect_conditions(&cond.0, &mut conditions);

			// Try to build compound index access for multi-column indexes
			self.analyze_compound_conditions(&conditions, &mut candidates);

			// Also analyze for single-column matches and special operators
			self.analyze_condition(&cond.0, &mut candidates);
		}

		// Analyze ORDER BY for index-ordered scans
		if let Some(ordering) = order {
			self.analyze_order(ordering, &mut candidates);
		}

		// Filter out indexes not allowed by WITH hints
		if let Some(With::Index(names)) = self.with_hints {
			candidates.retain(|c| names.iter().any(|n| n.as_str() == c.index_ref.name.as_str()));
		}

		// Merge half-bounded ranges on the same index into bounded ranges
		// (e.g. field > 5 AND field < 10 → Range(>5, <10))
		self.merge_range_candidates(&mut candidates);

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
		cond: Option<&Cond>,
		direction: ScanDirection,
	) -> Option<AccessPath> {
		let cond = cond?;

		// Check for WITH NOINDEX
		if matches!(self.with_hints, Some(With::NoIndex)) {
			return None;
		}

		// Flatten OR branches from the condition tree
		let mut branches = Vec::new();
		Self::flatten_or(&cond.0, &mut branches);

		// Need at least 2 branches for a union to make sense
		if branches.len() < 2 {
			return None;
		}

		// Analyze each branch independently
		let mut branch_paths = Vec::with_capacity(branches.len());
		for branch_expr in branches {
			let branch_cond = Cond(branch_expr.clone());
			let candidates = self.analyze(Some(&branch_cond), None);
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
				_ => branch_paths.push(path),
			}
		}

		// If every branch turned out to be empty the whole OR is empty.
		if branch_paths.is_empty() {
			return Some(AccessPath::EmptyScan);
		}

		// A single surviving branch is a degenerate union — return it
		// directly rather than wrapping in `AccessPath::Union(...)`
		// (the planner's union dispatch expects ≥ 2 sub-paths).
		if branch_paths.len() == 1 {
			return branch_paths.into_iter().next();
		}

		// OR branches are independent predicates that may both hold
		// on the same row, so the union can emit the same record
		// from multiple branches — dedupe required.
		Some(AccessPath::Union {
			paths: branch_paths,
			dedupe: true,
		})
	}

	/// Maximum number of array elements to expand for `field IN [...]`.
	///
	/// Beyond this threshold, the per-operator overhead of creating individual
	/// `IndexScan` operators inside a `UnionIndexScan` outweighs the benefit
	/// of targeted lookups. Arrays larger than this fall back to a table scan
	/// with a predicate filter, which performs a single sequential pass.
	///
	/// The value 32 is currently heuristic — not measured against a specific
	/// crossover point. Raising it requires benchmarking the per-element
	/// `UnionIndexScan` overhead vs the table-scan cost for typical row
	/// counts. The `index_analyzer` criterion bench at
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
		cond: Option<&Cond>,
		direction: ScanDirection,
	) -> Option<AccessPath> {
		let cond = cond?;

		if matches!(self.with_hints, Some(With::NoIndex)) {
			return None;
		}

		// Collect IN expressions from the condition
		let mut in_exprs = Vec::new();
		Self::collect_in_expressions(&cond.0, &mut in_exprs);

		for (idiom, values) in &in_exprs {
			if values.len() < 2 || values.len() > Self::MAX_IN_EXPANSION_SIZE {
				continue; // Single-element handled by match_operator_to_access; too-large skipped
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
					&& idiom_matches(idiom, first_col)
				{
					let ncols = ix_def.cols.len();
					if best.is_none_or(|(_, best_ncols)| ncols < best_ncols) {
						best = Some((idx, ncols));
					}
				}
			}

			if let Some((idx, ncols)) = best {
				let index_ref = IndexRef::new(Arc::clone(&self.indexes), idx);
				let paths: Vec<AccessPath> = if ncols == 1 {
					// Single-column index: equality scans
					values
						.iter()
						.map(|v| AccessPath::BTreeScan {
							index_ref: index_ref.clone(),
							access: BTreeAccess::Equality(v.clone()),
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
							access: BTreeAccess::Compound {
								prefix: vec![v.clone()],
								range: None,
							},
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
	/// `Part::All`) using `idiom_matches_containment`.
	pub fn try_containment_expansion(
		&self,
		cond: Option<&Cond>,
		direction: ScanDirection,
	) -> Option<AccessPath> {
		let cond = cond?;

		if matches!(self.with_hints, Some(With::NoIndex)) {
			return None;
		}

		let mut exprs = Vec::new();
		Self::collect_containment_expressions(&cond.0, &mut exprs);

		for (idiom, values) in &exprs {
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

				if let Some(first_col) = ix_def.cols.first()
					&& idiom_matches_containment(idiom, first_col)
				{
					let index_ref = IndexRef::new(Arc::clone(&self.indexes), idx);
					let is_composite = ix_def.cols.len() > 1;
					let paths: Vec<AccessPath> = values
						.iter()
						.map(|v| {
							let access = if is_composite {
								BTreeAccess::Compound {
									prefix: vec![v.clone()],
									range: None,
								}
							} else {
								BTreeAccess::Equality(v.clone())
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

	/// Collect CONTAINSALL/CONTAINSANY (idiom on left, array literal on right)
	/// and ALLINSIDE/ANYINSIDE (array literal on left, idiom on right) from an
	/// AND tree.
	fn collect_containment_expressions(expr: &Expr, results: &mut Vec<(Idiom, Vec<Value>)>) {
		match expr {
			Expr::Binary {
				left,
				op: BinaryOperator::And,
				right,
			} => {
				Self::collect_containment_expressions(left, results);
				Self::collect_containment_expressions(right, results);
			}
			Expr::Binary {
				left,
				op: BinaryOperator::ContainAll | BinaryOperator::ContainAny,
				right,
			} => {
				if let (Expr::Idiom(idiom), Expr::Literal(lit)) = (left.as_ref(), right.as_ref())
					&& let Some(Value::Array(arr)) = try_literal_to_value(lit)
				{
					results.push((idiom.clone(), arr.0));
				}
			}
			Expr::Binary {
				left,
				op: BinaryOperator::AllInside | BinaryOperator::AnyInside,
				right,
			} => {
				if let (Expr::Literal(lit), Expr::Idiom(idiom)) = (left.as_ref(), right.as_ref())
					&& let Some(Value::Array(arr)) = try_literal_to_value(lit)
				{
					results.push((idiom.clone(), arr.0));
				}
			}
			Expr::Prefix {
				op,
				expr: inner,
			} if !matches!(op, PrefixOperator::Not) => {
				Self::collect_containment_expressions(inner, results);
			}
			_ => {}
		}
	}

	/// Collect `field INSIDE [values]` expressions from an AND tree.
	fn collect_in_expressions(expr: &Expr, results: &mut Vec<(Idiom, Vec<Value>)>) {
		match expr {
			Expr::Binary {
				left,
				op: BinaryOperator::And,
				right,
			} => {
				Self::collect_in_expressions(left, results);
				Self::collect_in_expressions(right, results);
			}
			Expr::Binary {
				left,
				op: BinaryOperator::Inside,
				right,
			} => {
				if let (Expr::Idiom(idiom), Expr::Literal(lit)) = (left.as_ref(), right.as_ref())
					&& let Some(Value::Array(arr)) = try_literal_to_value(lit)
				{
					results.push((idiom.clone(), arr.0));
				}
			}
			// Do NOT recurse into NOT — expanding `NOT (field IN [...])`
			// into index lookups would produce the wrong result set.
			Expr::Prefix {
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
	fn flatten_or<'b>(expr: &'b Expr, branches: &mut Vec<&'b Expr>) {
		match expr {
			Expr::Binary {
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

	/// Collect all simple conditions from an AND tree.
	fn collect_conditions(&self, expr: &Expr, conditions: &mut Vec<SimpleCondition>) {
		match expr {
			Expr::Binary {
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
			Expr::Prefix {
				op,
				expr: inner,
			} if !matches!(op, PrefixOperator::Not) => {
				self.collect_conditions(inner, conditions);
			}
			_ => {}
		}
	}

	/// Extract a simple condition (idiom op value) from a binary expression.
	fn extract_simple_condition(
		&self,
		left: &Expr,
		op: &BinaryOperator,
		right: &Expr,
	) -> Option<SimpleCondition> {
		let (idiom, value, position) = match (left, right) {
			(Expr::Idiom(idiom), Expr::Literal(lit)) => {
				if let Some(value) = try_literal_to_value(lit) {
					(idiom.clone(), value, IdiomPosition::Left)
				} else {
					return None;
				}
			}
			(Expr::Literal(lit), Expr::Idiom(idiom)) => {
				if let Some(value) = try_literal_to_value(lit) {
					(idiom.clone(), value, IdiomPosition::Right)
				} else {
					return None;
				}
			}
			_ => return None,
		};

		// Normalise single-element `field IN [v]` to `field = v` so it can
		// participate in compound-prefix building. Without this, a query
		// like `a IN [1] AND b = 2` would lose the leading equality on the
		// compound index `(a, b)`.
		let (op, value) = if matches!(op, BinaryOperator::Inside) && position == IdiomPosition::Left
		{
			if let Value::Array(arr) = &value
				&& arr.len() == 1
			{
				(BinaryOperator::Equal, arr[0].clone())
			} else {
				(op.clone(), value)
			}
		} else {
			(op.clone(), value)
		};

		Some(SimpleCondition {
			idiom,
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
			// The prefix collects leading equality conditions. After the
			// equality prefix, a single range condition on the next column
			// is captured and encoded into the compound key range so the
			// index scan is narrowed at the storage level.
			let mut prefix_values = Vec::new();
			let mut range_condition: Option<(BinaryOperator, Value)> = None;

			for col in &ix_def.cols {
				// Find a condition that matches this column
				let matching_cond = conditions.iter().find(|c| idiom_matches(&c.idiom, col));

				match matching_cond {
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
								&& matches!(cond.value, Value::None)
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

				let index_ref = IndexRef::new(Arc::clone(&self.indexes), idx);
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
				match Self::try_merge_ranges(&candidates[i].access, &candidates[j].access) {
					Some(Ok(merged_access)) => {
						let covers_order = candidates[i].covers_order || candidates[j].covers_order;
						candidates[i].access = merged_access;
						candidates[i].covers_order = covers_order;
						candidates.remove(j);
						// Don't increment j — the next candidate shifted into slot j
					}
					Some(Err(())) => {
						// Contradiction — flag the surviving candidate as
						// empty so it converts to `AccessPath::EmptyScan`.
						candidates[i].empty = true;
						candidates.remove(j);
					}
					None => {
						j += 1;
					}
				}
			}
			i += 1;
		}
	}

	/// Try to merge two BTreeAccess::Range values into a single tighter range.
	///
	/// Handles three cases:
	/// - One side provides `from`, the other provides `to` → produce a bounded range with both.
	/// - Both sides provide `from` → keep the tighter (larger) bound.
	/// - Both sides provide `to`   → keep the tighter (smaller) bound.
	///
	/// Returns:
	/// - `Some(Ok(merged))` — a strictly tighter range than either input.
	/// - `Some(Err(()))`    — the bounds are mutually unsatisfiable; the caller should set the
	///   candidate's `empty` flag so the planner short-circuits to [`AccessPath::EmptyScan`].
	/// - `None`             — the inputs cannot be merged (different value kinds, NaN, non-Range
	///   variants); leave the candidates as-is.
	#[allow(clippy::result_unit_err)]
	fn try_merge_ranges(a: &BTreeAccess, b: &BTreeAccess) -> Option<Result<BTreeAccess, ()>> {
		let (
			BTreeAccess::Range {
				from: from_a,
				to: to_a,
			},
			BTreeAccess::Range {
				from: from_b,
				to: to_b,
			},
		) = (a, b)
		else {
			return None;
		};

		// Skip merges where neither input contributes any bound — that
		// would just produce a second copy of `Range { None, None }`.
		if from_a.is_none() && to_a.is_none() && from_b.is_none() && to_b.is_none() {
			return None;
		}

		// Tighten `from` bounds: keep the larger value (exclusive wins on
		// ties so `> 5 AND >= 5` becomes `> 5`).
		let merged_from = match (from_a, from_b) {
			(Some(fa), Some(fb)) => Some(Self::tighter_from(fa, fb)?),
			(Some(f), None) | (None, Some(f)) => Some(f.clone()),
			(None, None) => None,
		};

		// Tighten `to` bounds: keep the smaller value (exclusive wins).
		let merged_to = match (to_a, to_b) {
			(Some(ta), Some(tb)) => Some(Self::tighter_to(ta, tb)?),
			(Some(t), None) | (None, Some(t)) => Some(t.clone()),
			(None, None) => None,
		};

		// Detect contradiction: from > to (or equal with at least one exclusive).
		if let (Some(f), Some(t)) = (merged_from.as_ref(), merged_to.as_ref())
			&& bounds_are_unsatisfiable(f, t)
		{
			return Some(Err(()));
		}

		Some(Ok(BTreeAccess::Range {
			from: merged_from,
			to: merged_to,
		}))
	}

	/// Pick the tighter (larger) of two `from` bounds. Returns `None` if
	/// the two values are not comparable (e.g. different `Value::kind`).
	fn tighter_from(a: &RangeBound, b: &RangeBound) -> Option<RangeBound> {
		let cmp = a.value.partial_cmp(&b.value)?;
		Some(match cmp {
			std::cmp::Ordering::Less => b.clone(),
			std::cmp::Ordering::Greater => a.clone(),
			std::cmp::Ordering::Equal => {
				// Equal values: exclusive (non-inclusive) is tighter.
				if !a.inclusive {
					a.clone()
				} else {
					b.clone()
				}
			}
		})
	}

	/// Pick the tighter (smaller) of two `to` bounds. Returns `None` if
	/// the two values are not comparable.
	fn tighter_to(a: &RangeBound, b: &RangeBound) -> Option<RangeBound> {
		let cmp = a.value.partial_cmp(&b.value)?;
		Some(match cmp {
			std::cmp::Ordering::Less => a.clone(),
			std::cmp::Ordering::Greater => b.clone(),
			std::cmp::Ordering::Equal => {
				if !a.inclusive {
					a.clone()
				} else {
					b.clone()
				}
			}
		})
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
	fn analyze_condition(&self, expr: &Expr, candidates: &mut Vec<IndexCandidate>) {
		match expr {
			// Binary expression - check for indexable patterns
			Expr::Binary {
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
						// This is more complex - for now, don't index OR conditions
						// (can be enhanced later)
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
			// Nested expression in parentheses (but NOT negation).
			// Do NOT recurse into NOT — negated predicates invert
			// the result set and index candidates would be wrong.
			Expr::Prefix {
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
		left: &Expr,
		op: &BinaryOperator,
		right: &Expr,
		candidates: &mut Vec<IndexCandidate>,
	) {
		// Extract idiom and value from the comparison
		let (idiom, value, position) = match (left, right) {
			(Expr::Idiom(idiom), Expr::Literal(lit)) => {
				if let Some(value) = try_literal_to_value(lit) {
					(idiom, value, IdiomPosition::Left)
				} else {
					return;
				}
			}
			(Expr::Literal(lit), Expr::Idiom(idiom)) => {
				if let Some(value) = try_literal_to_value(lit) {
					(idiom, value, IdiomPosition::Right)
				} else {
					return;
				}
			}
			// Parameters are pre-folded into literals before the analyzer
			// runs (see `resolve_condition_params` in the planner and dynamic
			// scan). If a bare `Expr::Param` reaches the analyzer it means
			// the param could not be resolved at plan time, so we cannot push
			// it down to the index and fall through to the table-scan path.
			_ => return,
		};

		// Find indexes that match this idiom
		for (idx, ix_def) in self.indexes.iter().enumerate() {
			// Skip indexes being removed
			if ix_def.prepare_remove {
				continue;
			}

			// Check if the idiom matches the first column of the index
			if let Some(first_col) = ix_def.cols.first()
				&& idiom_matches(idiom, first_col)
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
						BTreeAccess::Range {
							from,
							to,
						} => {
							// A range on the first column of a compound index
							// cannot use Compound prefix+range (that's for
							// equality prefix + range on next column).
							// Keep it as a simple range -- the IndexScan compound
							// path won't be reached, but deduplication may
							// prefer a compound candidate if one exists.
							BTreeAccess::Range {
								from,
								to,
							}
						}
						other => other,
					}
				} else {
					access
				};

				let index_ref = IndexRef::new(Arc::clone(&self.indexes), idx);
				candidates.push(IndexCandidate::new(index_ref, access));
			}
		}
	}

	/// Convert an operator and value to a BTreeAccess pattern.
	fn match_operator_to_access(
		&self,
		op: &BinaryOperator,
		value: &Value,
		position: IdiomPosition,
		index_type: &Index,
	) -> Option<BTreeAccess> {
		// Only Idx and Uniq support these access patterns
		if !matches!(index_type, Index::Idx | Index::Uniq) {
			return None;
		}

		match (op, position) {
			// Equality
			(BinaryOperator::Equal | BinaryOperator::ExactEqual, _) => {
				Some(BTreeAccess::Equality(value.clone()))
			}

			// Less than (field < value)
			(BinaryOperator::LessThan, IdiomPosition::Left) => Some(BTreeAccess::Range {
				from: None,
				to: Some(RangeBound::exclusive(value.clone())),
			}),

			// Less than or equal (field <= value)
			(BinaryOperator::LessThanEqual, IdiomPosition::Left) => Some(BTreeAccess::Range {
				from: None,
				to: Some(RangeBound::inclusive(value.clone())),
			}),

			// Greater than (field > value)
			(BinaryOperator::MoreThan, IdiomPosition::Left) => Some(BTreeAccess::Range {
				from: Some(RangeBound::exclusive(value.clone())),
				to: None,
			}),

			// Greater than or equal (field >= value)
			(BinaryOperator::MoreThanEqual, IdiomPosition::Left) => Some(BTreeAccess::Range {
				from: Some(RangeBound::inclusive(value.clone())),
				to: None,
			}),

			// Handle reversed comparisons (value < field means field > value)
			(BinaryOperator::LessThan, IdiomPosition::Right) => Some(BTreeAccess::Range {
				from: Some(RangeBound::exclusive(value.clone())),
				to: None,
			}),
			(BinaryOperator::LessThanEqual, IdiomPosition::Right) => Some(BTreeAccess::Range {
				from: Some(RangeBound::inclusive(value.clone())),
				to: None,
			}),
			(BinaryOperator::MoreThan, IdiomPosition::Right) => Some(BTreeAccess::Range {
				from: None,
				to: Some(RangeBound::exclusive(value.clone())),
			}),
			(BinaryOperator::MoreThanEqual, IdiomPosition::Right) => Some(BTreeAccess::Range {
				from: None,
				to: Some(RangeBound::inclusive(value.clone())),
			}),

			// IN clause (field IN [values])
			(BinaryOperator::Inside, IdiomPosition::Left) => {
				// Single-element array: treat as equality (field IN [v] → field = v)
				if let Value::Array(arr) = value
					&& arr.len() == 1
				{
					Some(BTreeAccess::Equality(arr[0].clone()))
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
			// Until the access-path model can express the union
			// `< NULL OR > NULL` we fall through and let the table-scan
			// + filter path handle `!= NULL` correctly.
			(BinaryOperator::NotEqual, _) if matches!(value, Value::None) => {
				Some(BTreeAccess::Range {
					from: Some(RangeBound::exclusive(value.clone())),
					to: None,
				})
			}

			_ => None,
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
		left: &Expr,
		op: &BinaryOperator,
		right: &Expr,
		candidates: &mut Vec<IndexCandidate>,
	) {
		let (idiom, value) = match op {
			BinaryOperator::Contain => match (left, right) {
				(Expr::Idiom(idiom), Expr::Literal(lit)) => {
					if let Some(v) = try_literal_to_value(lit) {
						(idiom, v)
					} else {
						return;
					}
				}
				_ => return,
			},
			BinaryOperator::Inside => match (left, right) {
				(Expr::Literal(lit), Expr::Idiom(idiom)) => {
					if let Some(v) = try_literal_to_value(lit) {
						(idiom, v)
					} else {
						return;
					}
				}
				_ => return,
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
			if let Some(first_col) = ix_def.cols.first()
				&& idiom_matches_containment(idiom, first_col)
			{
				// Compound indexes need a prefix scan rather than a point
				// lookup, because the on-disk key includes all columns. The
				// trailing columns can then be used by `index_covers_ordering`
				// to satisfy ORDER BY without a post-iteration sort. Mirrors
				// the equality rewrite in `try_match_comparison`.
				let access = if ix_def.cols.len() > 1 {
					BTreeAccess::Compound {
						prefix: vec![value.clone()],
						range: None,
					}
				} else {
					BTreeAccess::Equality(value.clone())
				};
				let index_ref = IndexRef::new(Arc::clone(&self.indexes), idx);
				candidates.push(IndexCandidate::new(index_ref, access));
			}
		}
	}

	/// Try to match a MATCHES expression to a full-text index.
	fn try_match_fulltext(
		&self,
		left: &Expr,
		operator: &MatchesOperator,
		right: &Expr,
		candidates: &mut Vec<IndexCandidate>,
	) {
		// Extract idiom from left side and query string from right side
		let (idiom, query) = match (left, right) {
			(Expr::Idiom(idiom), Expr::Literal(lit)) => {
				if let Some(Value::String(s)) = try_literal_to_value(lit) {
					(idiom, s)
				} else {
					return;
				}
			}
			_ => return,
		};

		// Find full-text indexes that match this idiom
		for (idx, ix_def) in self.indexes.iter().enumerate() {
			if ix_def.prepare_remove {
				continue;
			}

			// Only FullText indexes support MATCHES
			if !matches!(ix_def.index, Index::FullText(_)) {
				continue;
			}

			if let Some(first_col) = ix_def.cols.first()
				&& idiom_matches(idiom, first_col)
			{
				let index_ref = IndexRef::new(Arc::clone(&self.indexes), idx);
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
		left: &Expr,
		right: &Expr,
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

		// Extract idiom from left side
		let idiom = match left {
			Expr::Idiom(idiom) => idiom,
			_ => return,
		};

		// Extract numeric vector from right side
		let vector = match right {
			Expr::Literal(lit) => {
				if let Some(Value::Array(arr)) = try_literal_to_value(lit) {
					let nums: Vec<Number> = arr
						.iter()
						.filter_map(|v| match v {
							Value::Number(n) => Some(*n),
							_ => None,
						})
						.collect();
					if nums.len() != arr.len() {
						// Not all elements are numbers
						return;
					}
					nums
				} else {
					return;
				}
			}
			_ => return,
		};

		// Find ANN indexes that match this idiom
		for (idx, ix_def) in self.indexes.iter().enumerate() {
			if ix_def.prepare_remove {
				continue;
			}

			let ef = match &ix_def.index {
				Index::Hnsw(hnsw) => {
					if let Some(d) = required_distance
						&& *d != hnsw.distance
					{
						continue;
					}
					user_ef.unwrap_or_else(|| k.max(hnsw.ef_construction as u32))
				}
				Index::DiskAnn(diskann) => {
					if let Some(d) = required_distance
						&& *d != diskann.distance
					{
						continue;
					}
					user_ef.unwrap_or_else(|| k.max(diskann.l_build as u32))
				}
				_ => continue,
			};

			if let Some(first_col) = ix_def.cols.first()
				&& idiom_matches(idiom, first_col)
			{
				let index_ref = IndexRef::new(Arc::clone(&self.indexes), idx);
				candidates.push(IndexCandidate::new(
					index_ref,
					BTreeAccess::Knn {
						vector: vector.clone(),
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
	fn analyze_order(&self, ordering: &Ordering, candidates: &mut Vec<IndexCandidate>) {
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
			let index_ref = IndexRef::new(Arc::clone(&self.indexes), idx);
			let full_range = BTreeAccess::Range {
				from: None,
				to: None,
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
	ordering: &Ordering,
	covers: F,
) -> bool
where
	F: Fn(&IndexRef, &BTreeAccess, ScanDirection, &Ordering) -> bool,
{
	covers(index_ref, access, ScanDirection::Forward, ordering)
		|| covers(index_ref, access, ScanDirection::Backward, ordering)
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
				from,
				to,
			} => {
				score += match (from.is_some(), to.is_some()) {
					(true, true) => 300,
					(true, false) | (false, true) => 200,
					(false, false) => 50,
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

	/// Convert this candidate to an AccessPath.
	pub fn to_access_path(&self, direction: ScanDirection) -> AccessPath {
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
				vector: vector.clone(),
				k: *k,
				ef: *ef,
			},
			_ => AccessPath::BTreeScan {
				index_ref: self.index_ref.clone(),
				access: self.access.clone(),
				direction,
			},
		}
	}
}

/// Position of the idiom in a comparison expression.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IdiomPosition {
	/// Idiom is on the left: `field = value`
	Left,
	/// Idiom is on the right: `value = field`
	Right,
}

/// A simple condition extracted from the WHERE clause.
#[derive(Debug, Clone)]
struct SimpleCondition {
	idiom: Idiom,
	op: BinaryOperator,
	value: Value,
	position: IdiomPosition,
}

/// Check if an idiom matches an index column.
///
/// Idioms containing `Part::All` (flattened field paths like `marks.*.mark`)
/// are excluded because the Scan predicate filter cannot correctly evaluate
/// comparison operators on flattened paths — `[40] = 40` evaluates to false.
/// Users should use CONTAINS/INSIDE operators for array-aware queries.
fn idiom_matches(expr_idiom: &Idiom, index_col: &Idiom) -> bool {
	use crate::expr::Part;

	if expr_idiom != index_col {
		return false;
	}

	// Skip flattened field paths — comparison predicates don't evaluate
	// correctly on array-valued paths (e.g., marks.*.mark = 40 becomes
	// [40] = 40 which is false).
	if index_col.0.iter().any(|p| matches!(p, Part::All)) {
		return false;
	}

	true
}

/// Check if an idiom matches an index column for containment operators.
///
/// Unlike `idiom_matches`, this allows `Part::All` in the index column.
/// The query idiom `tags` matches index column `tags.*` (or `tags[*]`)
/// because each array element is indexed individually, and the containment
/// operator checks membership of a scalar in the indexed array.
///
/// Also handles nested array paths like `marks.*.subject` where both the
/// expression idiom and the index column contain `Part::All`. The comparison
/// strips `Part::All` from both sides before checking equality.
///
/// Only matches when the index column actually contains `Part::All` --
/// regular scalar indexes are not valid for containment lookups.
pub(crate) fn idiom_matches_containment(expr_idiom: &Idiom, index_col: &Idiom) -> bool {
	use crate::expr::Part;

	if !index_col.0.iter().any(|p| matches!(p, Part::All)) {
		return false;
	}

	let col_without_all: Vec<&Part> =
		index_col.0.iter().filter(|p| !matches!(p, Part::All)).collect();
	let expr_without_all: Vec<&Part> =
		expr_idiom.0.iter().filter(|p| !matches!(p, Part::All)).collect();
	col_without_all == expr_without_all
}

/// Decide whether a `from`/`to` pair describes an unsatisfiable range.
///
/// Returns `true` when no value can satisfy both bounds simultaneously:
/// either `from.value > to.value`, or the values are equal but at least
/// one bound is exclusive. Returns `false` when at least one value can
/// satisfy both, **and also** when the values are not comparable — the
/// caller treats incomparable bounds as "leave it alone" rather than
/// silently emitting an EmptyScan that could be wrong.
fn bounds_are_unsatisfiable(from: &RangeBound, to: &RangeBound) -> bool {
	use std::cmp::Ordering;
	match from.value.partial_cmp(&to.value) {
		Some(Ordering::Greater) => true,
		Some(Ordering::Equal) => !(from.inclusive && to.inclusive),
		Some(Ordering::Less) => false,
		None => false,
	}
}

/// Normalize a range operator based on the position of the idiom in the
/// comparison expression.
///
/// When the idiom is on the left (`field > value`), the operator is already
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

// literal_to_value and expr_to_value are imported from crate::exec::planner::util
// as try_literal_to_value and try_expr_to_value.

#[cfg(test)]
mod tests {
	//! Unit tests for the IndexAnalyzer.
	//!
	//! These lock in plan-choice behaviour against intentional changes and
	//! act as regression cover for the analyzer + `select_access_path`
	//! helpers.  They use small `IndexDefinition` fixtures and parse WHERE /
	//! ORDER BY snippets via the SurrealQL parser, then drive the analyzer
	//! directly and assert about the candidate set or the access path that
	//! `select_access_path` picks.
	//!
	//! Tests are grouped by concern in nested modules so a failure tells you
	//! which category regressed at a glance.
	use std::str::FromStr;
	use std::sync::Arc;

	use surrealdb_strand::Strand;

	use super::*;
	use crate::catalog::{Index, IndexDefinition, IndexId};
	use crate::expr::order::Ordering;
	use crate::expr::with::With;
	use crate::expr::{Cond, Expr, Idiom};
	use crate::val::TableName;

	// ------------------------------------------------------------------
	// Fixture helpers
	// ------------------------------------------------------------------

	/// Build a minimal `IndexDefinition`.  Tests use synthetic `index_id`s
	/// that don't have to match any real catalog state.
	fn idx_def(id: u32, name: &str, cols: &[&str], kind: Index) -> IndexDefinition {
		IndexDefinition {
			index_id: IndexId(id),
			name: Strand::from(name),
			table_name: TableName::from("t"),
			cols: cols.iter().map(|c| Idiom::from_str(c).expect("valid idiom")).collect(),
			index: kind,
			comment: None,
			prepare_remove: false,
		}
	}

	fn idx_basic(id: u32, name: &str, cols: &[&str]) -> IndexDefinition {
		idx_def(id, name, cols, Index::Idx)
	}

	fn idx_uniq(id: u32, name: &str, cols: &[&str]) -> IndexDefinition {
		idx_def(id, name, cols, Index::Uniq)
	}

	fn analyzer<'a>(defs: Vec<IndexDefinition>, with: Option<&'a With>) -> IndexAnalyzer<'a> {
		IndexAnalyzer::new(Arc::<[_]>::from(defs.into_boxed_slice()), with)
	}

	/// Parse a snippet wrapped in `SELECT * FROM t <snippet>` and extract
	/// `(cond, order, with)`.  Useful for driving the analyzer with realistic
	/// expression trees without hand-building AST nodes.
	fn parse_select_parts(snippet: &str) -> (Option<Cond>, Option<Ordering>, Option<With>) {
		let src = format!("SELECT * FROM t {snippet}");
		let ast = crate::syn::parse(&src).expect("parse");
		let mut exprs = ast.expressions;
		assert_eq!(exprs.len(), 1, "expected one statement from {src:?}");
		let top: crate::expr::TopLevelExpr = exprs.remove(0).into();
		match top {
			crate::expr::TopLevelExpr::Expr(Expr::Select(s)) => (s.cond, s.order, s.with),
			other => panic!("expected SELECT, got {other:?}"),
		}
	}

	fn parse_cond(snippet: &str) -> Cond {
		let (cond, _, _) = parse_select_parts(&format!("WHERE {snippet}"));
		cond.expect("WHERE produced a Cond")
	}

	fn parse_cond_order(where_snippet: &str, order_snippet: &str) -> (Cond, Ordering) {
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
				crate::idx::planner::ScanDirection::Forward,
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
		use super::*;

		#[test]
		fn half_bounded_gt() {
			let a = analyzer(vec![idx_basic(1, "ix_a", &["a"])], None);
			let cond = parse_cond("a > 5");
			let cands = a.analyze(Some(&cond), None);
			let c = find_for(&cands, "ix_a").expect("ix_a");
			match &c.access {
				BTreeAccess::Range {
					from,
					to,
				} => {
					assert!(from.is_some());
					assert!(to.is_none());
					assert!(!from.as_ref().unwrap().inclusive, "MoreThan is exclusive");
				}
				other => panic!("expected Range, got {other:?}"),
			}
		}

		#[test]
		fn half_bounded_gte() {
			let a = analyzer(vec![idx_basic(1, "ix_a", &["a"])], None);
			let cond = parse_cond("a >= 5");
			let cands = a.analyze(Some(&cond), None);
			let c = find_for(&cands, "ix_a").expect("ix_a");
			match &c.access {
				BTreeAccess::Range {
					from,
					..
				} => {
					assert!(from.as_ref().unwrap().inclusive, "MoreThanEqual is inclusive");
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
					from,
					to,
				} => {
					assert!(from.is_some());
					assert!(to.is_some());
				}
				other => panic!("expected merged Range, got {other:?}"),
			}
		}

		#[test]
		fn value_lt_idiom_normalises() {
			// `5 < a` should be treated as `a > 5`.
			let a = analyzer(vec![idx_basic(1, "ix_a", &["a"])], None);
			let cond = parse_cond("5 < a");
			let cands = a.analyze(Some(&cond), None);
			let c = find_for(&cands, "ix_a").expect("ix_a");
			match &c.access {
				BTreeAccess::Range {
					from,
					to,
				} => {
					assert!(from.is_some());
					assert!(to.is_none());
				}
				other => panic!("expected Range from-bound, got {other:?}"),
			}
		}
	}

	// ------------------------------------------------------------------
	// 3. Compound prefix building
	// ------------------------------------------------------------------
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
			let a = analyzer(vec![idx_basic(1, "ix_abc", &["a", "b", "c"])], None);
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
			let a = analyzer(vec![idx_basic(1, "ix_abc", &["a", "b", "c"])], None);
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
			let a = analyzer(vec![idx_basic(1, "ix_a", &["a"])], None);
			let (cond, order) = parse_cond_order("a = 1", "id");
			let cands = a.analyze(Some(&cond), Some(&order));
			let c = find_for(&cands, "ix_a").expect("ix_a");
			assert!(c.covers_order, "WHERE a=1 ORDER BY id on INDEX(a) covers order");
		}

		#[test]
		fn equality_then_unrelated_order_not_covered() {
			// WHERE a = 1 ORDER BY b on INDEX(a) — the index can't satisfy
			// the requested ordering.
			let a = analyzer(vec![idx_basic(1, "ix_a", &["a"])], None);
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
			let a = analyzer(vec![idx_basic(1, "ix_ab", &["a", "b"])], None);
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
			let a = analyzer(vec![idx_basic(1, "ix_a", &["a"])], None);
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
			let a = analyzer(vec![idx_basic(1, "ix_ab", &["a", "b"])], None);
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
			let a = analyzer(vec![idx_basic(1, "ix_a", &["a"])], None);
			let (cond, order) = parse_cond_order("a > 5", "a DESC");
			let cands = a.analyze(Some(&cond), Some(&order));
			let c = find_for(&cands, "ix_a").expect("ix_a");
			assert!(c.covers_order, "ORDER BY DESC coverable via backward scan");
		}

		#[test]
		fn order_by_only_no_where() {
			// ORDER BY a (no WHERE) on INDEX(a) should synthesize a
			// full-range scan with covers_order = true.
			let a = analyzer(vec![idx_basic(1, "ix_a", &["a"])], None);
			let (_, order, _) = parse_select_parts("ORDER BY a");
			let order = order.expect("ORDER BY");
			let cands = a.analyze(None, Some(&order));
			let c = find_for(&cands, "ix_a").expect("synth ix_a candidate");
			assert!(c.covers_order);
			assert!(matches!(
				c.access,
				BTreeAccess::Range {
					from: None,
					to: None
				}
			));
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
				crate::idx::planner::ScanDirection::Forward,
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
			let a = analyzer(vec![idx_basic(1, "ix_a", &["a"])], None);
			let cond = parse_cond("a != NONE");
			let cands = a.analyze(Some(&cond), None);
			let c = find_for(&cands, "ix_a").expect("ix_a candidate for != NONE");
			match &c.access {
				BTreeAccess::Range {
					from,
					to,
				} => {
					let from = from.as_ref().expect("from bound");
					assert!(matches!(from.value, crate::val::Value::None));
					assert!(!from.inclusive);
					assert!(to.is_none());
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
				.try_in_expansion(Some(&cond), crate::idx::planner::ScanDirection::Forward)
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
			let path = a.try_in_expansion(Some(&cond), crate::idx::planner::ScanDirection::Forward);
			assert!(path.is_none(), "33-element IN should not expand");
		}

		#[test]
		fn in_threshold_boundary_at_32() {
			let a = analyzer(vec![idx_basic(1, "ix_a", &["a"])], None);
			let lit = (1..=32).map(|n| n.to_string()).collect::<Vec<_>>().join(", ");
			let cond = parse_cond(&format!("a IN [{lit}]"));
			let path = a
				.try_in_expansion(Some(&cond), crate::idx::planner::ScanDirection::Forward)
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
			let path = a
				.try_or_union(Some(&cond), crate::idx::planner::ScanDirection::Forward)
				.expect("union");
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
			let path = a.try_or_union(Some(&cond), crate::idx::planner::ScanDirection::Forward);
			assert!(path.is_none(), "unindexed branch defeats union");
		}

		#[test]
		fn or_drops_empty_branch_from_union() {
			// `(a > 10 AND a < 5) OR b = 1` — the first branch is
			// provably empty after range merging, so it must be dropped
			// from the union (a zero-row branch contributes nothing to
			// OR semantics). With only one surviving branch the union
			// degenerates to a plain BTreeScan on `idx_b`.
			let a =
				analyzer(vec![idx_basic(1, "ix_a", &["a"]), idx_basic(2, "ix_b", &["b"])], None);
			let cond = parse_cond("(a > 10 AND a < 5) OR b = 1");
			let path = a
				.try_or_union(Some(&cond), crate::idx::planner::ScanDirection::Forward)
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
			let a = analyzer(vec![idx_basic(1, "ix_a", &["a"])], None);
			let cond = parse_cond("(a > 10 AND a < 5) OR (a > 100 AND a < 50)");
			let path = a
				.try_or_union(Some(&cond), crate::idx::planner::ScanDirection::Forward)
				.expect("union path");
			assert!(matches!(path, AccessPath::EmptyScan));
		}
	}

	// ------------------------------------------------------------------
	// 9. Range merging — same-index, contradictions, tightening
	// ------------------------------------------------------------------
	mod range_merge {
		use super::*;
		use crate::idx::planner::ScanDirection;

		fn select_path(cands: Vec<IndexCandidate>) -> AccessPath {
			super::super::super::access_path::select_access_path(
				cands,
				None,
				ScanDirection::Forward,
			)
		}

		#[test]
		fn contradictory_range_yields_empty_scan() {
			// `a > 10 AND a < 5` — contradiction must short-circuit to
			// AccessPath::EmptyScan.
			let a = analyzer(vec![idx_basic(1, "ix_a", &["a"])], None);
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
					from,
					to,
				} => {
					assert!(from.as_ref().unwrap().inclusive);
					assert!(to.as_ref().unwrap().inclusive);
				}
				other => panic!("expected Range, got {other:?}"),
			}
		}

		#[test]
		fn equal_values_with_one_exclusive_is_empty() {
			// `a > 5 AND a <= 5` — exclusive lower equal to inclusive upper
			// has no satisfying value.
			let a = analyzer(vec![idx_basic(1, "ix_a", &["a"])], None);
			let cond = parse_cond("a > 5 AND a <= 5");
			let cands = a.analyze(Some(&cond), None);
			let c = find_for(&cands, "ix_a").expect("ix_a candidate");
			assert!(c.empty, "x > 5 AND x <= 5 is empty");
		}

		#[test]
		fn same_side_from_bounds_keep_tighter() {
			// `a > 5 AND a > 10` → from bound tightens to > 10.
			let a = analyzer(vec![idx_basic(1, "ix_a", &["a"])], None);
			let cond = parse_cond("a > 5 AND a > 10");
			let cands = a.analyze(Some(&cond), None);
			let c = find_for(&cands, "ix_a").expect("ix_a");
			match &c.access {
				BTreeAccess::Range {
					from,
					to,
				} => {
					let from = from.as_ref().expect("from bound present");
					assert!(matches!(&from.value,
						crate::val::Value::Number(n) if n.to_int() == 10
					));
					assert!(!from.inclusive, "exclusive `>` survives");
					assert!(to.is_none());
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
					from,
					to,
				} => {
					assert!(from.is_none());
					let to = to.as_ref().expect("to bound present");
					assert!(matches!(&to.value,
						crate::val::Value::Number(n) if n.to_int() == 50
					));
					assert!(!to.inclusive);
				}
				other => panic!("expected single tightened Range, got {other:?}"),
			}
		}

		#[test]
		fn same_side_mixed_inclusive_picks_exclusive() {
			// `a > 5 AND a >= 5` — same value, exclusive is tighter.
			let a = analyzer(vec![idx_basic(1, "ix_a", &["a"])], None);
			let cond = parse_cond("a > 5 AND a >= 5");
			let cands = a.analyze(Some(&cond), None);
			let c = find_for(&cands, "ix_a").expect("ix_a");
			match &c.access {
				BTreeAccess::Range {
					from,
					..
				} => {
					let from = from.as_ref().expect("from");
					assert!(!from.inclusive, "exclusive wins on ties");
				}
				other => panic!("expected Range, got {other:?}"),
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
				index_ref: IndexRef::new(
					Arc::<[_]>::from(vec![idx_uniq(1, "u", &["a"])].into_boxed_slice()),
					0,
				),
				access: BTreeAccess::Equality(crate::val::Value::Number(crate::val::Number::Int(
					1,
				))),
				covers_order: false,
				empty: false,
			};
			let nonunique_cand = IndexCandidate {
				index_ref: IndexRef::new(
					Arc::<[_]>::from(vec![idx_basic(1, "i", &["a"])].into_boxed_slice()),
					0,
				),
				access: BTreeAccess::Equality(crate::val::Value::Number(crate::val::Number::Int(
					1,
				))),
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
				index_ref: IndexRef::new(
					Arc::<[_]>::from(vec![idx_basic(1, "i", &["a"])].into_boxed_slice()),
					0,
				),
				access: BTreeAccess::Range {
					from: if from_some {
						Some(RangeBound::inclusive(crate::val::Value::Number(
							crate::val::Number::Int(0),
						)))
					} else {
						None
					},
					to: if to_some {
						Some(RangeBound::inclusive(crate::val::Value::Number(
							crate::val::Number::Int(10),
						)))
					} else {
						None
					},
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
				.map(|i| crate::val::Value::Number(crate::val::Number::Int(i as i64)))
				.collect::<Vec<_>>();
			let wide = IndexCandidate {
				index_ref: IndexRef::new(
					Arc::<[_]>::from(
						vec![idx_basic(
							1,
							"i_wide",
							&["a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l"],
						)]
						.into_boxed_slice(),
					),
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
				index_ref: IndexRef::new(
					Arc::<[_]>::from(vec![idx_uniq(2, "u_a", &["a"])].into_boxed_slice()),
					0,
				),
				access: BTreeAccess::Equality(crate::val::Value::Number(crate::val::Number::Int(
					1,
				))),
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
				index_ref: IndexRef::new(
					Arc::<[_]>::from(vec![idx_basic(1, "i", &["a"])].into_boxed_slice()),
					0,
				),
				access: BTreeAccess::Range {
					from: Some(RangeBound::inclusive(crate::val::Value::Number(
						crate::val::Number::Int(0),
					))),
					to: None,
				},
				covers_order: covers,
				empty: false,
			};
			assert!(make(true).score() > make(false).score());
		}
	}
}
