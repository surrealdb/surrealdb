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
//! ## Degradation
//!
//! The cell gets a writer only when the planner installs it into a
//! [`SortTopKByKey`](crate::exec::operators::SortTopKByKey) whose computed sort
//! keys match the probe (see the install guard in
//! `plan_sort_consolidated`). Any divergence — sort eliminated, GROUP BY,
//! TEMPFILES, oversized limit, registry-computed first key — leaves the cell
//! unpublished and the probe dormant: one `RwLock` read per cursor batch,
//! nothing else.

use std::cmp::Ordering;
use std::sync::{Arc, RwLock};

use crate::exec::OperatorMetrics;
use crate::exec::field_path::{FieldPath, FieldPathPart};
use crate::exec::object_extract::{Extracted, PathSegment, extract_field_from_record_bytes};
use crate::exec::operators::scan::pipeline::FieldState;
use crate::exec::operators::{SortDirection, SortKey, compare_values};
use crate::exec::pre_decode_filter::{PreDecodeFilterReason, field_state_blocks_raw_read};
use crate::val::Value;

/// Shared, monotonically-tightening threshold between the sort operator (top
/// of the buffered pipeline, possibly a separate task) and the KV scan visitor
/// (bottom).
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
/// Built at plan time by the select planner; armed (and optionally given a
/// metrics handle) at execute time via [`topk_probe_for_execute`].
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
	cell: Arc<TopKThresholdCell>,
	/// Bounds walker descent; sourced from `ctx.config.idiom_recursion_limit`.
	depth_limit: u32,
	/// Present only when EXPLAIN ANALYZE enabled the scan's metrics; the
	/// visitor flushes per-batch skip counts through it.
	metrics: Option<Arc<OperatorMetrics>>,
}

impl TopKThresholdProbe {
	pub(crate) fn new(
		path: Vec<PathSegment>,
		direction: SortDirection,
		single_key: bool,
		cell: Arc<TopKThresholdCell>,
		depth_limit: u32,
	) -> Self {
		Self {
			path: path.into(),
			direction,
			single_key,
			cell,
			depth_limit,
			metrics: None,
		}
	}

	/// Root field name the probe reads from raw bytes — the subject of the
	/// computed-field / field-permission eligibility checks.
	pub(crate) fn root_field(&self) -> &str {
		// `field_path_wire_segments` rejects empty paths, so index 0 exists.
		self.path[0].as_str()
	}

	/// Current threshold snapshot; [`None`] until the sort heap fills.
	pub(crate) fn snapshot(&self) -> Option<Arc<Value>> {
		self.cell.snapshot()
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

/// Why TopK threshold pushdown cannot be applied to a scan.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(crate) enum TopKPushdownReason {
	/// ORDER BY shape unsupported: COLLATE / NUMERIC modifiers, alias
	/// resolving to an expression, non-`Field` path parts (indices, lookups),
	/// or a first key the planner must compute post-decode.
	UnsupportedOrder,
	/// No literal effective limit, or it exceeds
	/// `max_order_limit_priority_queue_size` — the plan uses a full sort, so
	/// no heap threshold exists.
	LimitTooLarge,
	/// `TEMPFILES` routes the sort to disk; no in-memory heap.
	Tempfiles,
	/// The ORDER BY root field is read-time computed (`DEFINE FIELD …
	/// COMPUTED …`, directly or nested beneath the root); raw bytes diverge
	/// from the engine-visible value.
	ComputedFields,
	/// The ORDER BY root field carries a non-`Allow` SELECT permission;
	/// reading it raw would bypass per-field authorisation.
	FieldPermissions,
}

/// Plan / execute-time status of TopK threshold pushdown for a KV table scan.
/// Mirrors [`PreDecodeFilterStatus`](crate::exec::pre_decode_filter::PreDecodeFilterStatus).
#[derive(Debug, Clone)]
pub(crate) enum TopKPushdownStatus {
	/// No eligible ORDER BY + LIMIT opportunity — omit the attribute in
	/// EXPLAIN entirely.
	NotApplicable,
	/// Probe built and field checks passed at plan time.
	Active(Arc<TopKThresholdProbe>),
	/// Probe built; field state or permission checks need runtime context.
	Deferred(Arc<TopKThresholdProbe>),
	/// Opportunity exists but the shape or field state disqualifies it.
	Ineligible(TopKPushdownReason),
}

impl TopKPushdownStatus {
	/// Human-readable value for the `topk_pushdown` attribute in EXPLAIN
	/// output; [`None`] means the attribute is omitted.
	pub(crate) fn explain_text(&self) -> Option<&'static str> {
		match self {
			Self::NotApplicable => None,
			Self::Active(_) => Some("yes"),
			Self::Deferred(_) => Some("deferred (runtime field state)"),
			Self::Ineligible(TopKPushdownReason::UnsupportedOrder) => {
				Some("no (unsupported order)")
			}
			Self::Ineligible(TopKPushdownReason::LimitTooLarge) => Some("no (limit too large)"),
			Self::Ineligible(TopKPushdownReason::Tempfiles) => Some("no (tempfiles)"),
			Self::Ineligible(TopKPushdownReason::ComputedFields) => Some("no (computed fields)"),
			Self::Ineligible(TopKPushdownReason::FieldPermissions) => {
				Some("no (field permissions)")
			}
		}
	}
}

/// Planner-side handle from scan construction to sort construction.
///
/// `plan_table_scan_source` creates the cell and probe; `plan_sort_consolidated`
/// installs the publish side into [`SortTopKByKey`](crate::exec::operators::SortTopKByKey)
/// **only** when the sort plan it actually built matches what the probe was
/// compiled against — `sort_keys.first() == Some(&expected_first_key)` and
/// `sort_keys.len() == expected_key_count`. A mismatch (alias resolution or
/// registry compute diverging from the request-time analysis) simply leaves
/// the cell unpublished.
#[derive(Debug, Clone)]
pub(crate) struct TopKPushdownHandle {
	pub(crate) cell: Arc<TopKThresholdCell>,
	pub(crate) expected_first_key: SortKey,
	pub(crate) expected_key_count: usize,
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

/// Map the shared raw-read eligibility reason onto the pushdown's reason enum.
fn map_reason(reason: PreDecodeFilterReason) -> TopKPushdownReason {
	match reason {
		PreDecodeFilterReason::ComputedFields => TopKPushdownReason::ComputedFields,
		PreDecodeFilterReason::FieldPermissions => TopKPushdownReason::FieldPermissions,
		// field_state_blocks_raw_read never returns this; conservative map.
		PreDecodeFilterReason::UnsupportedPredicate => TopKPushdownReason::UnsupportedOrder,
	}
}

/// Resolve pushdown status at plan time given an optional plan-time
/// [`FieldState`]. Mirrors
/// [`pre_decode_filter_status_at_plan_time`](crate::exec::pre_decode_filter::pre_decode_filter_status_at_plan_time):
/// full check passes → `Active`; only the permission check fails (the active
/// session's permission level is an execute-time fact) or no field state →
/// `Deferred`; otherwise `Ineligible`.
pub(crate) fn topk_pushdown_status_at_plan_time(
	probe: Arc<TopKThresholdProbe>,
	field_state: Option<&FieldState>,
) -> TopKPushdownStatus {
	match field_state {
		Some(fs) => match field_state_blocks_raw_read(fs, probe.root_field(), true) {
			None => TopKPushdownStatus::Active(probe),
			Some(PreDecodeFilterReason::FieldPermissions) => {
				match field_state_blocks_raw_read(fs, probe.root_field(), false) {
					None => TopKPushdownStatus::Deferred(probe),
					Some(reason) => TopKPushdownStatus::Ineligible(map_reason(reason)),
				}
			}
			Some(reason) => TopKPushdownStatus::Ineligible(map_reason(reason)),
		},
		None => TopKPushdownStatus::Deferred(probe),
	}
}

/// Resolve the optional probe for a KV scan at execution time. Mirrors
/// [`pre_decode_filter_for_execute`](crate::exec::pre_decode_filter::pre_decode_filter_for_execute):
/// `Active` arms directly, `Deferred` re-checks against the runtime
/// [`FieldState`] with the session's `check_perms`, everything else is
/// [`None`]. The armed probe carries the scan's metrics handle only when
/// EXPLAIN ANALYZE enabled it, so the common path keeps the plan-time `Arc`.
///
/// The metrics handle is additionally withheld when permission checks apply
/// (`check_perms`): the `skipped` counter tallies rows rejected *before*
/// table/row permission filtering, so exposing it to a permission-checked
/// session would leak a lower bound on the number of rows that exist but are
/// not visible to that session. The rejection itself stays active — only the
/// observable count is suppressed.
pub(crate) fn topk_probe_for_execute(
	status: &TopKPushdownStatus,
	field_state: &FieldState,
	check_perms: bool,
	metrics: &Arc<OperatorMetrics>,
) -> Option<Arc<TopKThresholdProbe>> {
	let probe = match status {
		TopKPushdownStatus::NotApplicable | TopKPushdownStatus::Ineligible(_) => return None,
		TopKPushdownStatus::Active(p) => p,
		TopKPushdownStatus::Deferred(p) => {
			if field_state_blocks_raw_read(field_state, p.root_field(), check_perms).is_some() {
				return None;
			}
			p
		}
	};
	if metrics.is_enabled() && !check_perms {
		Some(Arc::new(TopKThresholdProbe {
			metrics: Some(Arc::clone(metrics)),
			..probe.as_ref().clone()
		}))
	} else {
		Some(Arc::clone(probe))
	}
}

#[cfg(test)]
mod tests {
	use std::collections::BTreeMap;

	use chrono::{TimeZone, Utc};
	use revision::SerializeRevisioned;
	use surrealdb_strand::Strand;

	use super::*;
	use crate::catalog::Record;
	use crate::val::{Datetime, Number, Object};

	/// Matches the default `ctx.config.idiom_recursion_limit` (256).
	const TEST_DEPTH_LIMIT: u32 = 256;

	fn wire_record(obj: Object) -> Vec<u8> {
		// Route through the macro-generated serializer so the bytes always
		// match the latest revision's wire layout.
		let rec = Record {
			metadata: None,
			data: Value::Object(obj),
		};
		let mut out = Vec::new();
		rec.serialize_revisioned(&mut out).unwrap();
		out
	}

	fn record_with_int(field: &str, n: i64) -> Vec<u8> {
		wire_record(Object::from(BTreeMap::from([(
			Strand::from(field),
			Value::Number(Number::Int(n)),
		)])))
	}

	fn probe(direction: SortDirection, single_key: bool, path: &[&str]) -> TopKThresholdProbe {
		TopKThresholdProbe::new(
			path.iter().map(|s| PathSegment::new(*s)).collect(),
			direction,
			single_key,
			Arc::new(TopKThresholdCell::default()),
			TEST_DEPTH_LIMIT,
		)
	}

	#[test]
	fn cell_starts_empty_publishes_and_overwrites() {
		let cell = TopKThresholdCell::default();
		assert!(cell.snapshot().is_none());
		cell.publish(Value::Number(Number::Int(5)));
		assert_eq!(cell.snapshot().as_deref(), Some(&Value::Number(Number::Int(5))));
		cell.publish(Value::Number(Number::Int(7)));
		assert_eq!(cell.snapshot().as_deref(), Some(&Value::Number(Number::Int(7))));
	}

	/// Full reject matrix: {Asc, Desc} × {candidate better, tie, worse} ×
	/// {single-key, multi-key}. Candidate field value is fixed at 5; the
	/// threshold varies.
	#[test]
	fn reject_matrix() {
		let rec = record_with_int("a", 5);
		let t = |n: i64| Value::Number(Number::Int(n));
		// (direction, threshold, single_key, expect_reject)
		let cases = [
			// Asc: smaller is better. 5 vs 6 → better → pass.
			(SortDirection::Asc, 6, true, false),
			(SortDirection::Asc, 6, false, false),
			// Asc tie: single rejects, multi passes.
			(SortDirection::Asc, 5, true, true),
			(SortDirection::Asc, 5, false, false),
			// Asc worse: always rejects.
			(SortDirection::Asc, 4, true, true),
			(SortDirection::Asc, 4, false, true),
			// Desc: larger is better. 5 vs 4 → better → pass.
			(SortDirection::Desc, 4, true, false),
			(SortDirection::Desc, 4, false, false),
			// Desc tie: single rejects, multi passes.
			(SortDirection::Desc, 5, true, true),
			(SortDirection::Desc, 5, false, false),
			// Desc worse: always rejects.
			(SortDirection::Desc, 6, true, true),
			(SortDirection::Desc, 6, false, true),
		];
		for (direction, threshold, single_key, expect) in cases {
			let p = probe(direction, single_key, &["a"]);
			assert_eq!(
				p.rejects(&t(threshold), &rec),
				expect,
				"direction={direction:?} threshold={threshold} single_key={single_key}",
			);
		}
	}

	/// Missing field reads as Value::None. Under DESC, None is the worst
	/// possible key — rejected against any real threshold. Under ASC, None is
	/// the best possible key — always passes.
	#[test]
	fn missing_field_reads_as_none() {
		let rec = record_with_int("other", 1);
		let threshold = Value::Number(Number::Int(5));
		assert!(probe(SortDirection::Desc, true, &["a"]).rejects(&threshold, &rec));
		assert!(!probe(SortDirection::Asc, true, &["a"]).rejects(&threshold, &rec));
		// None == None tie: single-key rejects, multi-key passes.
		assert!(probe(SortDirection::Desc, true, &["a"]).rejects(&Value::None, &rec));
		assert!(!probe(SortDirection::Desc, false, &["a"]).rejects(&Value::None, &rec));
	}

	/// Wire-level bail (corrupt bytes / non-object intermediate) must pass the
	/// row through to full decode, never reject.
	#[test]
	fn bail_passes_through() {
		let threshold = Value::Number(Number::Int(5));
		let p = probe(SortDirection::Desc, true, &["a"]);
		assert!(!p.rejects(&threshold, b"\xff\xff\xff\xff"));
		// Descending through a non-object intermediate (`a.b` where `a` is an
		// int) bails rather than treating the leaf as missing.
		let rec = record_with_int("a", 5);
		let nested = probe(SortDirection::Desc, true, &["a", "b"]);
		assert!(!nested.rejects(&threshold, &rec));
	}

	/// Datetime keys take the leaf-decode path (wire-level ordered compare is
	/// Int-only) and must still order correctly.
	#[test]
	fn datetime_leaf_compares() {
		let older = Datetime(Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap());
		let newer = Datetime(Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap());
		let rec = wire_record(Object::from(BTreeMap::from([(
			Strand::from("created_at"),
			Value::Datetime(older),
		)])));
		let p = probe(SortDirection::Desc, true, &["created_at"]);
		// Row is older than the heap's worst → cannot enter the top-K.
		assert!(p.rejects(&Value::Datetime(newer), &rec));
		// Row ties with the heap's worst → single-key reject.
		assert!(p.rejects(&Value::Datetime(older), &rec));
		// Threshold older than the row → row wins → pass.
		let rec_newer = wire_record(Object::from(BTreeMap::from([(
			Strand::from("created_at"),
			Value::Datetime(newer),
		)])));
		assert!(!p.rejects(&Value::Datetime(older), &rec_newer));
	}

	/// Nested Field chains descend; the leaf compares like a root field.
	#[test]
	fn nested_field_path_descends() {
		let inner =
			Object::from(BTreeMap::from([(Strand::from("score"), Value::Number(Number::Int(3)))]));
		let rec = wire_record(Object::from(BTreeMap::from([(
			Strand::from("stats"),
			Value::Object(inner),
		)])));
		let p = probe(SortDirection::Desc, true, &["stats", "score"]);
		assert!(p.rejects(&Value::Number(Number::Int(9)), &rec));
		assert!(!p.rejects(&Value::Number(Number::Int(1)), &rec));
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

	#[test]
	fn explain_text_covers_every_variant() {
		let p = Arc::new(probe(SortDirection::Desc, true, &["a"]));
		assert_eq!(TopKPushdownStatus::NotApplicable.explain_text(), None);
		assert_eq!(TopKPushdownStatus::Active(Arc::clone(&p)).explain_text(), Some("yes"));
		assert_eq!(
			TopKPushdownStatus::Deferred(p).explain_text(),
			Some("deferred (runtime field state)")
		);
		// Every reason variant must render — adding a variant breaks this
		// match, forcing the EXPLAIN text (and language tests) to be updated.
		let reasons = [
			TopKPushdownReason::UnsupportedOrder,
			TopKPushdownReason::LimitTooLarge,
			TopKPushdownReason::Tempfiles,
			TopKPushdownReason::ComputedFields,
			TopKPushdownReason::FieldPermissions,
		];
		for reason in reasons {
			match reason {
				TopKPushdownReason::UnsupportedOrder
				| TopKPushdownReason::LimitTooLarge
				| TopKPushdownReason::Tempfiles
				| TopKPushdownReason::ComputedFields
				| TopKPushdownReason::FieldPermissions => {}
			}
			assert!(TopKPushdownStatus::Ineligible(reason).explain_text().is_some());
		}
	}
}
