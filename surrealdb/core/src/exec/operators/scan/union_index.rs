//! Union index scan operator for multi-index OR conditions.
//!
//! Created by the planner when the access path is `AccessPath::Union`,
//! meaning the WHERE clause has top-level OR branches that can each be
//! served by a different index. Each sub-operator handles one branch;
//! results are deduplicated by record ID at execution time.
//!
//! Follows the same permission pattern as [`super::TableScan`]: resolves
//! table-level and field-level SELECT permissions, builds computed fields,
//! and applies the full [`ScanPipeline`](super::pipeline::ScanPipeline).

use crate::exec::field_path::FieldPath;
use crate::exec::operators::SortDirection;

/// How [`UnionIndexScan`] combines per-branch sub-streams.
#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::enum_variant_names)]
pub(crate) enum MergeMode {
	/// K-way merge by record ID. Used when each sub-stream is already
	/// sorted by record ID (e.g. single-column equality index scans) and
	/// the downstream wants `ORDER BY id`.  Duplicate record IDs across
	/// branches are deduplicated.
	ById(SortDirection),
	/// K-way merge by an indexed field value (e.g. composite index's
	/// second column when each branch pins the first column to a
	/// distinct equality value).  Each sub-stream must already be sorted
	/// by `path` in `direction`; branches must be prefix-disjoint so no
	/// record appears twice.  Enables ORDER BY-by-suffix-column with
	/// early-stop on a LIMIT downstream.
	ByIndexKey {
		path: FieldPath,
		direction: SortDirection,
	},
	/// Like `ByIndexKey` but with record-ID deduplication via a
	/// `HashSet<RecordId>`.  Used when branches can overlap on the same
	/// record — for example, `field CONTAINSANY [a, b]` on an
	/// array-element index, where a row whose `field` array contains
	/// both `a` and `b` sits in both branches' prefix ranges.  The
	/// non-dedup `ByIndexKey` variant assumes prefix-disjoint branches
	/// and would emit such a row twice.
	ByIndexKeyDedup {
		path: FieldPath,
		direction: SortDirection,
	},
}

impl MergeMode {
	pub(crate) fn direction(&self) -> SortDirection {
		match self {
			MergeMode::ById(d)
			| MergeMode::ByIndexKey {
				direction: d,
				..
			}
			| MergeMode::ByIndexKeyDedup {
				direction: d,
				..
			} => *d,
		}
	}
}
