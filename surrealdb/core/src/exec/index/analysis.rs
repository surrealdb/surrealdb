//! Index analysis for matching WHERE conditions to available indexes.
//!
//! The [`IndexAnalyzer`] examines query conditions and ORDER BY clauses to find
//! indexes that can accelerate the query.

use std::sync::Arc;

use super::access_path::{AccessPath, BTreeAccess, IndexRef, RangeBound, select_access_path};
use crate::catalog::{Index, IndexDefinition};
use crate::exec::planner::util::try_literal_to_value;
use crate::expr::operator::{MatchesOperator, NearestNeighbor};
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
			candidates.retain(|c| names.contains(&c.index_ref.name));
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
			if matches!(path, AccessPath::TableScan) {
				// WITH hints rejected all candidates for this branch
				return None;
			}
			branch_paths.push(path);
		}

		Some(AccessPath::Union(branch_paths))
	}

	/// Maximum number of array elements to expand for `field IN [...]`.
	///
	/// Beyond this threshold, the per-operator overhead of creating individual
	/// `IndexScan` operators inside a `UnionIndexScan` outweighs the benefit
	/// of targeted lookups. Arrays larger than this fall back to a table scan
	/// with a predicate filter, which performs a single sequential pass.
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

			for (idx, ix_def) in self.indexes.iter().enumerate() {
				if ix_def.prepare_remove {
					continue;
				}
				if !matches!(ix_def.index, crate::catalog::Index::Idx | crate::catalog::Index::Uniq)
				{
					continue;
				}
				// Only handle single-column indexes for IN expansion.
				// Compound indexes would need prefix handling.
				if ix_def.cols.len() != 1 {
					continue;
				}

				if let Some(With::Index(names)) = self.with_hints
					&& !names.contains(&ix_def.name)
				{
					continue;
				}

				if let Some(first_col) = ix_def.cols.first()
					&& idiom_matches(idiom, first_col)
				{
					let index_ref = IndexRef::new(self.indexes.clone(), idx);
					let paths: Vec<AccessPath> = values
						.iter()
						.map(|v| AccessPath::BTreeScan {
							index_ref: index_ref.clone(),
							access: BTreeAccess::Equality(v.clone()),
							direction,
						})
						.collect();
					return Some(AccessPath::Union(paths));
				}
			}
		}

		None
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
			Expr::Prefix {
				expr: inner,
				..
			} => {
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
			Expr::Prefix {
				expr: inner,
				..
			} => {
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

		Some(SimpleCondition {
			idiom,
			op: op.clone(),
			value,
			position,
		})
	}

	/// Analyze conditions to find compound index opportunities.
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
			// The prefix collects leading equality conditions. Non-equality
			// (range) conditions on later columns are NOT encoded into the
			// compound key because the key encoding uses variable-length
			// arrays and mixing different array sizes in range bounds
			// produces incorrect scan ranges. Range conditions are instead
			// handled by the Scan operator's predicate filter.
			let mut prefix_values = Vec::new();

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
							// Non-equality -- stop adding to prefix.
							// This column's range will be filtered by the predicate.
							break;
						}
					}
					None => {
						// No condition for this column -- stop looking
						break;
					}
				}
			}

			// Create compound candidate if we have at least 2 equality columns
			if prefix_values.len() >= 2 {
				let access = BTreeAccess::Compound {
					prefix: prefix_values,
					range: None,
				};

				let index_ref = IndexRef::new(self.indexes.clone(), idx);
				let candidate = IndexCandidate {
					index_ref,
					access,

					covers_order: false,
				};
				candidates.push(candidate);
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
				// Try to merge candidates[i] and candidates[j]
				let merged = Self::try_merge_ranges(&candidates[i].access, &candidates[j].access);
				if let Some(merged_access) = merged {
					// Keep the merged result in slot i, remove slot j
					let covers_order = candidates[i].covers_order || candidates[j].covers_order;
					candidates[i].access = merged_access;
					candidates[i].covers_order = covers_order;
					candidates.remove(j);
					// Don't increment j — the next candidate shifted into slot j
				} else {
					j += 1;
				}
			}
			i += 1;
		}
	}

	/// Try to merge two BTreeAccess::Range values into a single bounded range.
	///
	/// Returns `Some(merged)` if one provides a `from` bound and the other a
	/// `to` bound. Returns `None` if the ranges cannot be merged (e.g. both
	/// have `from` bounds, or they are not Range variants).
	fn try_merge_ranges(a: &BTreeAccess, b: &BTreeAccess) -> Option<BTreeAccess> {
		match (a, b) {
			(
				BTreeAccess::Range {
					from: from_a,
					to: to_a,
				},
				BTreeAccess::Range {
					from: from_b,
					to: to_b,
				},
			) => {
				// Merge when one has from and the other has to
				let merged_from = match (from_a, from_b) {
					(Some(_), Some(_)) => return None, // both have from — can't merge
					(Some(f), None) => Some(f.clone()),
					(None, Some(f)) => Some(f.clone()),
					(None, None) => None,
				};
				let merged_to = match (to_a, to_b) {
					(Some(_), Some(_)) => return None, // both have to — can't merge
					(Some(t), None) => Some(t.clone()),
					(None, Some(t)) => Some(t.clone()),
					(None, None) => None,
				};
				// Only produce a merge if the result is strictly more bounded
				// than either input (i.e. we actually combined something).
				if merged_from.is_some()
					&& merged_to.is_some()
					&& (from_a.is_none() || to_a.is_none() || from_b.is_none() || to_b.is_none())
				{
					Some(BTreeAccess::Range {
						from: merged_from,
						to: merged_to,
					})
				} else {
					None
				}
			}
			_ => None,
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
					_ => {
						// Check if this is an indexable comparison
						self.try_match_comparison(left, op, right, candidates);
					}
				}
			}
			// Nested expression in parentheses
			Expr::Prefix {
				op: _,
				expr: inner,
			} => {
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
			(Expr::Idiom(idiom), Expr::Param(_param)) => {
				// Parameters need to be resolved at execution time
				// For now, skip index matching on parameters
				// TODO: Support parameter-based index access
				let _ = idiom;
				return;
			}
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

				let index_ref = IndexRef::new(self.indexes.clone(), idx);
				let candidate = IndexCandidate {
					index_ref,
					access,
					covers_order: false,
				};
				candidates.push(candidate);
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

			_ => None,
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
				let index_ref = IndexRef::new(self.indexes.clone(), idx);
				let candidate = IndexCandidate {
					index_ref,
					access: BTreeAccess::FullText {
						query: query.clone(),
						operator: operator.clone(),
					},

					covers_order: false,
				};
				candidates.push(candidate);
			}
		}
	}

	/// Try to match a KNN expression to an HNSW index.
	fn try_match_knn(
		&self,
		left: &Expr,
		right: &Expr,
		nn: &NearestNeighbor,
		candidates: &mut Vec<IndexCandidate>,
	) {
		// Only HNSW-backed (Approximate) KNN uses index scan
		let (k, ef) = match nn {
			NearestNeighbor::Approximate(k, ef) => (*k, *ef),
			// K (brute-force) and KTree don't use index analysis
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

		// Find HNSW indexes that match this idiom
		for (idx, ix_def) in self.indexes.iter().enumerate() {
			if ix_def.prepare_remove {
				continue;
			}

			// Only HNSW indexes support KNN
			if !matches!(ix_def.index, Index::Hnsw(_)) {
				continue;
			}

			if let Some(first_col) = ix_def.cols.first()
				&& idiom_matches(idiom, first_col)
			{
				let index_ref = IndexRef::new(self.indexes.clone(), idx);
				let candidate = IndexCandidate {
					index_ref,
					access: BTreeAccess::Knn {
						vector: vector.clone(),
						k,
						ef,
					},
					covers_order: false,
				};
				candidates.push(candidate);
			}
		}
	}

	/// Analyze ORDER BY for index-ordered scan opportunities.
	fn analyze_order(&self, ordering: &Ordering, candidates: &mut Vec<IndexCandidate>) {
		let Ordering::Order(order_list) = ordering else {
			return;
		};

		// Check the first order field
		let Some(first_order) = order_list.0.first() else {
			return;
		};

		// The order value is already an Idiom
		let idiom = &first_order.value;

		// Find indexes that match this idiom as first column
		for (idx, ix_def) in self.indexes.iter().enumerate() {
			if ix_def.prepare_remove {
				continue;
			}

			// Only Idx and Uniq support ordered iteration
			if !ix_def.index.supports_order() {
				continue;
			}

			if let Some(first_col) = ix_def.cols.first()
				&& idiom_matches(idiom, first_col)
			{
				let index_ref = IndexRef::new(self.indexes.clone(), idx);

				// Mark existing candidate as covering order, or add new one
				let existing = candidates.iter_mut().find(|c| c.index_ref == index_ref);
				if let Some(candidate) = existing {
					candidate.covers_order = true;
				} else {
					// Create a full-range scan candidate that covers order
					let candidate = IndexCandidate {
						index_ref,
						access: BTreeAccess::Range {
							from: None,
							to: None,
						},

						covers_order: true,
					};
					candidates.push(candidate);
				}
			}
		}
	}
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
}

impl IndexCandidate {
	/// Score this candidate for comparison (higher is better).
	///
	/// Scoring heuristics:
	/// - Unique index equality: highest (returns 1 row)
	/// - Non-unique equality: high
	/// - Range scan: medium
	/// - Full scan with order: low
	/// - Order coverage: bonus points
	/// - FullText and KNN: high (specialized search)
	pub fn score(&self) -> u32 {
		let mut score = 0u32;

		// Base score by access type
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
				..
			} => {
				// More prefix columns = better selectivity
				score += 400 + (prefix.len() as u32 * 50);
			}
			BTreeAccess::Range {
				from,
				to,
			} => {
				// Bounded range is better than unbounded
				score += match (from.is_some(), to.is_some()) {
					(true, true) => 300,
					(true, false) | (false, true) => 200,
					(false, false) => 50, // Full scan via index
				};
			}
			BTreeAccess::FullText {
				..
			} => {
				// Full-text search is specialized and should be preferred
				// when the query uses MATCHES
				score += 800;
			}
			BTreeAccess::Knn {
				..
			} => {
				// KNN search is specialized and should be preferred
				// when the query uses nearest neighbor operators
				score += 800;
			}
		}

		// Bonus for covering ORDER BY
		if self.covers_order {
			score += 100;
		}

		score
	}

	/// Convert this candidate to an AccessPath.
	pub fn to_access_path(&self, direction: ScanDirection) -> AccessPath {
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
	#[allow(dead_code)]
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

// literal_to_value and expr_to_value are imported from crate::exec::planner::util
// as try_literal_to_value and try_expr_to_value.
