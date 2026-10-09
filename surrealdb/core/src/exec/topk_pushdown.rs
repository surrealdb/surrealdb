//! TopK dynamic threshold pushdown — **reject-only** optimisation during KV
//! scans for `ORDER BY <field> … LIMIT n` queries.
//!
//! Once the downstream [`SortTopKByKey`](crate::exec::operators::SortTopKByKey)
//! heap is full, its worst entry's **first** sort key is a monotonically-
//! tightening threshold: a row whose first ORDER BY key cannot strictly beat it
//! can never enter the final top-K, so the scan can skip full record decode for
//! it — the same place, and the same conservative reject-only contract, as the
//! [`PreDecodeFilter`](crate::exec::pre_decode_filter::PreDecodeFilter).
//!
//! ## Soundness
//!
//! * The threshold is published only from rows already admitted to the heap, which have passed the
//!   authoritative post-decode pipeline (WHERE re-check, permissions, computed fields). Rejecting a
//!   row that is not strictly better than the heap's worst can therefore never evict a row that
//!   belongs in the final top-K.
//! * The threshold only tightens. The scan snapshots it once per cursor batch; a stale snapshot is
//!   merely looser — it under-rejects, never over-rejects.
//! * The heap admits a candidate only on strict [`Ordering::Less`] against its worst entry
//!   (insertion-stable `seq` tie-break — earlier rows win ties), so the probe rejects on `Greater`
//!   always and on `Equal` only for single-key ORDER BY; with multiple keys an `Equal` first key
//!   must fall through to full decode because later keys could still make the row strictly better.
//! * Raw-bytes reads must match engine-visible values: the probe's ORDER BY root field must not be
//!   read-time computed (`DEFINE FIELD … COMPUTED …`, directly or nested beneath the root) and must
//!   carry an `Allow` SELECT permission when permission checks apply — the exact conditions
//!   enforced by [`field_state_blocks_raw_read`], shared with the pre-decode WHERE filter.
//!   (Write-time `VALUE` clauses are materialised into the stored bytes and stay eligible.)
//! * Wire-level ordered comparison only covers `Int` numbers (`pre_decode_filter::wire_cmp`), so
//!   the probe decodes just the leaf [`Value`] (cheap — e.g. two varints for a datetime) and
//!   compares with [`compare_values`], the same function the sort operator uses, including
//!   `Value::None` sorting below everything for missing fields.
//!
//! ## One threshold per execution
//!
//! A compiled plan can run more than once: a subquery once per outer row, with
//! runs overlapping where those rows are evaluated concurrently, and a `FOR`
//! body once per iteration. Each run's threshold is that run's alone. The
//! single-key `Equal` rejection above is sound only against the heap of the run
//! doing the scanning, which already holds the tied row, and a correlated run
//! has rows of its own that another run's threshold would reject.
//!
//! So the planner gives the scan's probe and the sort the same pushdown id
//! ([`next_id`]) rather than a cell. Each execution of the sort creates a fresh
//! [`TopKThresholdCell`] and runs its input in a context holding it under that
//! id ([`ExecutionContext::with_topk_threshold`]). The scan arms its probe with
//! the cell it finds there ([`topk_probe_for_execute`]), and a scan that finds
//! none reads no threshold.
//!
//! ## Degradation
//!
//! The pushdown gets a publisher only when the planner installs its id into a
//! [`SortTopKByKey`](crate::exec::operators::SortTopKByKey) whose computed sort
//! keys match the probe (see the install guard in
//! `plan_sort_consolidated`). Any divergence — sort eliminated, GROUP BY,
//! TEMPFILES, oversized limit, registry-computed first key — leaves no cell in
//! the scan's context, and the scan runs without a probe.

use std::cmp::Ordering;
use std::sync::{Arc, RwLock};

use crate::exec::OperatorMetrics;
use crate::exec::field_path::{FieldPath, FieldPathPart};
use crate::exec::object_extract::{Extracted, PathSegment, extract_field_from_record_bytes};
use crate::exec::operators::{SortDirection, compare_values};
use crate::val::Value;

/// Monotonically-tightening threshold shared, for one execution, between the
/// sort operator (top of the buffered pipeline, possibly a separate task) and
/// the KV scan visitor (bottom).
///
/// `std::sync::RwLock` rather than a lock-free cell: the scan reads once per
/// cursor batch (≤ 1 per [`crate::kvs::NORMAL_BATCH_SIZE`] rows) and the sort
/// writes once per heap admission — both rare, so an uncontended lock is
/// already far off the hot path.
#[derive(Debug, Default)]
pub(crate) struct TopKThresholdCell {
	value: RwLock<Option<Arc<Value>>>,
}

impl TopKThresholdCell {
	/// Publish a new (tighter) threshold. Both sides of the lock are
	/// poison-tolerant: a panicked peer can only have left a stale-but-valid
	/// threshold, which is sound (under-rejects).
	pub(crate) fn publish(&self, v: Value) {
		let v = Some(Arc::new(v));
		match self.value.write() {
			Ok(mut guard) => *guard = v,
			Err(poisoned) => *poisoned.into_inner() = v,
		}
	}

	/// Current threshold, if the sort has published one yet.
	pub(crate) fn snapshot(&self) -> Option<Arc<Value>> {
		match self.value.read() {
			Ok(guard) => guard.clone(),
			Err(poisoned) => poisoned.into_inner().clone(),
		}
	}
}

/// Scan-side probe: rejects rows from raw revision-encoded record bytes when
/// their first ORDER BY key cannot beat the current threshold.
///
/// Built at plan time by the select planner, naming its pushdown but holding no
/// threshold. [`topk_probe_for_execute`] arms a copy for each execution with
/// the cell that execution's sort created and, optionally, a metrics handle.
#[derive(Debug, Clone)]
pub(crate) struct TopKThresholdProbe {
	/// First ORDER BY key as pre-encoded wire segments (all
	/// [`FieldPathPart::Field`] parts — enforced by
	/// [`field_path_wire_segments`]).
	path: Arc<[PathSegment]>,
	direction: SortDirection,
	/// `true` when the ORDER BY has exactly one key: an `Equal` comparison
	/// against the threshold also rejects (the heap admits only on strict
	/// `Less`). With multiple keys, `Equal` must fall through to full decode.
	single_key: bool,
	/// The pushdown whose sort publishes the threshold this probe reads.
	pushdown: u64,
	/// The threshold of the execution this probe is armed for. `None` in the
	/// plan-time probe, which [`topk_probe_for_execute`] never hands to a
	/// scan.
	cell: Option<Arc<TopKThresholdCell>>,
	/// Bounds walker descent; sourced from `ctx.config.exec.idiom_recursion_limit`.
	depth_limit: u32,
	/// Present only when EXPLAIN ANALYZE enabled the scan's metrics; the
	/// visitor flushes per-batch skip counts through it.
	metrics: Option<Arc<OperatorMetrics>>,
}

impl TopKThresholdProbe {
	/// Current threshold snapshot; [`None`] until this execution's sort heap
	/// fills.
	pub(crate) fn snapshot(&self) -> Option<Arc<Value>> {
		self.cell.as_ref()?.snapshot()
	}

	/// A probe armed with `cell` directly, for a scan that holds the cell of
	/// the sort it feeds itself rather than finding it by pushdown id. Its
	/// pushdown id is `0`, which [`next_id`] never hands out.
	pub(crate) fn armed(
		path: Arc<[PathSegment]>,
		direction: SortDirection,
		single_key: bool,
		depth_limit: u32,
		cell: Arc<TopKThresholdCell>,
	) -> Arc<Self> {
		Arc::new(Self {
			path,
			direction,
			single_key,
			pushdown: 0,
			cell: Some(cell),
			depth_limit,
			metrics: None,
		})
	}

	/// Metrics handle for flushing skip counts (EXPLAIN ANALYZE only).
	pub(crate) fn metrics(&self) -> Option<&Arc<OperatorMetrics>> {
		self.metrics.as_ref()
	}

	/// Decide from raw record bytes whether this row can be skipped without
	/// decode. `true` only when the row's first ORDER BY key provably cannot
	/// beat `threshold`; any uncertainty (wire bail, decode error) returns
	/// `false` so the row takes the ordinary full-decode path.
	pub(crate) fn rejects(&self, threshold: &Value, record_bytes: &[u8]) -> bool {
		let candidate =
			match extract_field_from_record_bytes(record_bytes, &self.path, self.depth_limit) {
				Extracted::Found(v) => v,
				// Absent field reads as None, exactly like FieldPath::extract
				// in compare_records_by_keys.
				Extracted::Missing => Value::None,
				Extracted::Bail => return false,
			};
		// Same comparison the sort heap uses (collate/numeric keys are
		// ineligible for pushdown, so plain Value ordering applies).
		let ord = compare_values(&candidate, threshold, false, false);
		let ord = match self.direction {
			SortDirection::Asc => ord,
			SortDirection::Desc => ord.reverse(),
		};
		match ord {
			// Strictly worse than the heap's worst: can never be admitted.
			Ordering::Greater => true,
			// Ties with the heap's worst: the heap admits only on strict
			// Less, so a single-key tie can never be admitted either. With
			// more keys the tail could still win — fall through.
			Ordering::Equal => self.single_key,
			Ordering::Less => false,
		}
	}
}

impl PartialEq for TopKThresholdProbe {
	fn eq(&self, other: &Self) -> bool {
		self.pushdown == other.pushdown
			&& self.direction == other.direction
			&& self.single_key == other.single_key
			&& self.path == other.path
	}
}

/// Convert a [`FieldPath`] into pre-encoded wire segments for raw-bytes
/// descent.
///
/// Returns [`None`] when the path cannot be probed on the wire: empty paths,
/// any non-[`FieldPathPart::Field`] part (indices, `[$]`, graph lookups — the
/// walker descends object keys only), or a root of `id` (synthetic, derived
/// from the KV key rather than stored in the record body, so a raw read would
/// see `Missing` and mis-reject).
pub(crate) fn field_path_wire_segments(path: &FieldPath) -> Option<Vec<PathSegment>> {
	if path.is_empty() {
		return None;
	}
	let mut segments = Vec::with_capacity(path.len());
	for part in &path.0 {
		match part {
			FieldPathPart::Field(name) => segments.push(PathSegment::new(name.as_str())),
			_ => return None,
		}
	}
	if segments[0].as_str() == "id" {
		return None;
	}
	Some(segments)
}

#[cfg(test)]
mod tests {

	use super::*;
	use crate::val::Number;

	#[test]
	fn cell_starts_empty_publishes_and_overwrites() {
		let cell = TopKThresholdCell::default();
		assert!(cell.snapshot().is_none());
		cell.publish(Value::Number(Number::Int(5)));
		assert_eq!(cell.snapshot().as_deref(), Some(&Value::Number(Number::Int(5))));
		cell.publish(Value::Number(Number::Int(7)));
		assert_eq!(cell.snapshot().as_deref(), Some(&Value::Number(Number::Int(7))));
	}

	#[test]
	fn wire_segments_accept_field_chains_only() {
		use crate::exec::field_path::{FieldPath, FieldPathPart};
		let fields =
			FieldPath(vec![FieldPathPart::Field("a".into()), FieldPathPart::Field("b".into())]);
		let segs = field_path_wire_segments(&fields).expect("plain field chain");
		assert_eq!(segs.len(), 2);
		assert_eq!(segs[0].as_str(), "a");
		assert_eq!(segs[1].as_str(), "b");

		for bad in [
			FieldPath(vec![]),
			FieldPath(vec![FieldPathPart::Field("a".into()), FieldPathPart::Index(0)]),
			FieldPath(vec![FieldPathPart::First]),
			FieldPath(vec![FieldPathPart::Last]),
			FieldPath(vec![FieldPathPart::Lookup("->edge".into())]),
			// `id` is synthetic — not present in the record body.
			FieldPath(vec![FieldPathPart::Field("id".into())]),
			FieldPath(vec![FieldPathPart::Field("id".into()), FieldPathPart::Field("x".into())]),
		] {
			assert!(field_path_wire_segments(&bad).is_none(), "expected None for {bad:?}");
		}
	}
}
