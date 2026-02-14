//! Index analysis for matching WHERE conditions to available indexes.
//!
//! The [`IndexAnalyzer`] examines query conditions and ORDER BY clauses to find
//! indexes that can accelerate the query.

use std::sync::Arc;

use super::access_path::{AccessPath, BTreeAccess, IndexRef, RangeBound};
use crate::catalog::{Index, IndexDefinition};
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

		// Deduplicate candidates - prefer compound over simple
		self.deduplicate_candidates(&mut candidates);

		candidates
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
				if let Some(value) = literal_to_value(lit) {
					(idiom.clone(), value, IdiomPosition::Left)
				} else {
					return None;
				}
			}
			(Expr::Literal(lit), Expr::Idiom(idiom)) => {
				if let Some(value) = literal_to_value(lit) {
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
			// The prefix collects leading equality conditions; once a
			// non-equality (range) condition is seen we record it and
			// stop -- columns after a range cannot be used.
			let mut prefix_values = Vec::new();
			let mut range_condition: Option<(BinaryOperator, Value)> = None;

			for col in &ix_def.cols {
				// Find a condition that matches this column
				let matching_cond = conditions.iter().find(|c| idiom_matches(&c.idiom, col));

				match matching_cond {
					Some(cond) => {
						// Check if this is an equality condition
						let is_equality =
							matches!(cond.op, BinaryOperator::Equal | BinaryOperator::ExactEqual);

						if range_condition.is_none() && is_equality {
							// Still in the equality prefix -- add to prefix
							prefix_values.push(cond.value.clone());
						} else if range_condition.is_none() {
							// First non-equality -- becomes range condition
							range_condition = Some((cond.op.clone(), cond.value.clone()));
							// Stop -- can't use columns after a range
							break;
						} else {
							// Already have a range condition, stop
							break;
						}
					}
					None => {
						// No condition for this column -- stop looking
						break;
					}
				}
			}

			// Create compound candidate if we have at least 2 matched columns,
			// or 1 prefix + 1 range condition
			let matched_count = prefix_values.len()
				+ if range_condition.is_some() {
					1
				} else {
					0
				};
			if matched_count >= 2 || (!prefix_values.is_empty() && range_condition.is_some()) {
				let access = BTreeAccess::Compound {
					prefix: prefix_values,
					range: range_condition,
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
				if let Some(value) = literal_to_value(lit) {
					(idiom, value, IdiomPosition::Left)
				} else {
					return;
				}
			}
			(Expr::Literal(lit), Expr::Idiom(idiom)) => {
				if let Some(value) = literal_to_value(lit) {
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
				// Value should be an array - return None, let union handling deal with it
				None
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
				if let Some(Value::String(s)) = literal_to_value(lit) {
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
				if let Some(Value::Array(arr)) = literal_to_value(lit) {
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
fn idiom_matches(expr_idiom: &Idiom, index_col: &Idiom) -> bool {
	// Simple equality check for now
	// TODO: Handle array field matching (Part::All)
	expr_idiom == index_col
}

/// Convert a literal expression to a Value.
fn literal_to_value(lit: &crate::expr::Literal) -> Option<Value> {
	use crate::expr::Literal;
	match lit {
		Literal::Integer(i) => Some(Value::from(*i)),
		Literal::Float(f) => Some(Value::from(*f)),
		Literal::Decimal(d) => Some(Value::from(*d)),
		Literal::String(s) => Some(Value::from(s.clone())),
		Literal::Bool(b) => Some(Value::from(*b)),
		Literal::Uuid(u) => Some(Value::from(*u)),
		Literal::Datetime(dt) => Some(Value::from(dt.clone())),
		Literal::Duration(d) => Some(Value::from(*d)),
		Literal::None => Some(Value::None),
		Literal::Null => Some(Value::Null),
		Literal::RecordId(_rid) => {
			// RecordIdLit requires async computation to convert to RecordId
			// For now, skip index matching on record ID literals
			None
		}
		Literal::Array(arr) => {
			// Convert array elements
			let values: Option<Vec<Value>> = arr.iter().map(expr_to_value).collect();
			values.map(|v| Value::Array(v.into()))
		}
		Literal::Object(_) => None, // Complex objects not supported for index matching
		Literal::Regex(_) => None,
		Literal::Bytes(_) => None,
		Literal::Set(_) => None,
		Literal::UnboundedRange => None,
		Literal::File(_) => None,
		Literal::Geometry(_) => None,
	}
}

/// Try to convert an expression to a constant value.
fn expr_to_value(expr: &Expr) -> Option<Value> {
	match expr {
		Expr::Literal(lit) => literal_to_value(lit),
		_ => None,
	}
}
