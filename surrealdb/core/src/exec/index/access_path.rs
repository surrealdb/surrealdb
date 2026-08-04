//! Access path types for index-based record retrieval.
//!
//! An [`AccessPath`] represents a specific way to retrieve records from a table,
//! whether through a full table scan, point lookup, or index scan.

use std::ops::Bound;
use std::sync::Arc;

use super::IndexCandidate;
use crate::catalog::IndexDefinition;
use crate::expr::BinaryOperator;
use crate::expr::operator::MatchesOperator;
use crate::expr::with::With;
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
	},

	/// Roaring-bitmap candidate fusion over the table's shared doc-ID space
	/// (issue #547).
	///
	/// Chosen for an AND-composed WHERE clause with at least two index-backed
	/// members (or one plus a subtractable `NOT`), when every participating
	/// index carries doc-IDs, no index covers the ORDER BY, and no
	/// early-termination (LIMIT without ORDER BY) or VERSION constraints
	/// apply. Surfaces as a `BitmapResolve` operator over a `BitmapNode`
	/// tree; the whole WHERE clause stays as the residual filter.
	BitmapFusion {
		root: BitmapPlan,
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

/// Select the best access path from candidates based on hints and heuristics.
///
/// Selection priority:
/// 1. WITH NOINDEX - always use table scan
/// 2. WITH INDEX names - use specified index(es)
/// 3. Best effort heuristics:
///    - Prefer unique index for equality (returns 1 row)
///    - Prefer compound index that matches more columns
///    - Prefer index that covers ORDER BY
///    - Otherwise, pick first matching index
pub fn select_access_path(
	candidates: Vec<IndexCandidate>,
	with_hints: Option<&With>,
	direction: Direction,
) -> AccessPath {
	// WITH NOINDEX forces table scan
	if matches!(with_hints, Some(With::NoIndex)) {
		return AccessPath::TableScan;
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

	// Best effort: score and pick the best candidate
	candidates
		.into_iter()
		.max_by_key(|c| c.score())
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
		fn a_score_tie_resolves_to_the_last_candidate() {
			// The selection rustdoc says "pick first matching index", but
			// `max_by_key` keeps the last of equally-scoring elements. Both
			// candidates are non-unique equalities, so they tie at 500.
			let defs = vec![idx_basic(1, "ix_a1", &["a"]), idx_basic(2, "ix_a2", &["a"])];
			let candidates = vec![
				candidate(defs.clone(), 0, BTreeAccess::Equality(num(1))),
				candidate(defs, 1, BTreeAccess::Equality(num(1))),
			];
			let path = select_access_path(candidates, None, Direction::Forward);
			assert_eq!(scan_index(&path), "ix_a2");
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
}
