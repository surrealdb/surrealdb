//! Access path types for index-based record retrieval.
//!
//! An [`AccessPath`] represents a specific way to retrieve records from a table,
//! whether through a full table scan, point lookup, or index scan.

use std::sync::Arc;

use super::IndexCandidate;
use crate::catalog::IndexDefinition;
use crate::expr::BinaryOperator;
use crate::expr::operator::MatchesOperator;
use crate::expr::with::With;
use crate::idx::planner::ScanDirection;
use crate::val::{Number, RecordId, TableName, Value};

/// A reference to an index definition with its position in the schema.
///
/// This is a lightweight reference that can be cloned efficiently.
#[derive(Debug, Clone)]
pub struct IndexRef {
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
	TableScan {
		table: TableName,
		direction: ScanDirection,
	},

	/// Point lookup by record ID - single record retrieval.
	PointLookup {
		rid: RecordId,
	},

	/// B-tree index scan (Idx or Uniq).
	///
	/// Supports equality lookups, range scans, and compound key access.
	BTreeScan {
		index_ref: IndexRef,
		access: BTreeAccess,
		direction: ScanDirection,
	},

	/// Union of multiple values using the same index.
	///
	/// Used for IN clauses and OR conditions on the same indexed field.
	IndexUnion {
		index_ref: IndexRef,
		values: Vec<Value>,
	},

	/// Full-text search using MATCHES operator.
	FullTextSearch {
		index_ref: IndexRef,
		query: String,
		operator: MatchesOperator,
	},

	/// K-nearest neighbor vector search.
	KnnSearch {
		index_ref: IndexRef,
		vector: Vec<Number>,
		k: u32,
		ef: u32,
	},

	/// Count index for COUNT(*) optimization.
	CountIndex {
		index_ref: IndexRef,
	},
}

/// How to access an index.
#[derive(Debug, Clone)]
pub enum BTreeAccess {
	/// Single value equality: `field = value`
	Equality(Value),

	/// Range scan with optional bounds: `field > a AND field < b`
	Range {
		from: Option<RangeBound>,
		to: Option<RangeBound>,
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

	/// KNN vector search access
	Knn {
		/// The query vector
		vector: Vec<crate::val::Number>,
		/// Number of nearest neighbors to return
		k: u32,
		/// Exploration factor for HNSW search
		ef: u32,
	},
}

/// A bound for a range scan.
#[derive(Debug, Clone)]
pub struct RangeBound {
	/// The bound value
	pub value: Value,
	/// Whether the bound is inclusive
	pub inclusive: bool,
}

impl RangeBound {
	/// Create an inclusive bound.
	pub fn inclusive(value: Value) -> Self {
		Self {
			value,
			inclusive: true,
		}
	}

	/// Create an exclusive bound.
	pub fn exclusive(value: Value) -> Self {
		Self {
			value,
			inclusive: false,
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
	table: TableName,
	candidates: Vec<IndexCandidate>,
	with_hints: Option<&With>,
	direction: ScanDirection,
) -> AccessPath {
	// WITH NOINDEX forces table scan
	if matches!(with_hints, Some(With::NoIndex)) {
		return AccessPath::TableScan {
			table,
			direction,
		};
	}

	// WITH INDEX names - find the hinted index
	if let Some(With::Index(names)) = with_hints
		&& let Some(candidate) = find_hinted_index(&candidates, names)
	{
		return candidate.to_access_path(direction);
	}
	// If hinted index not found, fall through to best effort
	// (could also error here, but being lenient)

	// No candidates - table scan
	if candidates.is_empty() {
		return AccessPath::TableScan {
			table,
			direction,
		};
	}

	// Best effort: score and pick the best candidate
	candidates.into_iter().max_by_key(|c| c.score()).map(|c| c.to_access_path(direction)).unwrap_or(
		AccessPath::TableScan {
			table,
			direction,
		},
	)
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
