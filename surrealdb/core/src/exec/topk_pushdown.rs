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
//! * A packed row is rejected only from its checked columns, read as the whole decode holds them
//!   ([`crate::exec::pre_decode_filter::extract_from_packed`]), and only where the scan states that
//!   every op between it and the sort is infallible, deterministic and a function of the row
//!   ([`crate::exec::vm::payload::TopKKey::packed_rows`]): the ops above the scan read a packed row
//!   whole, so a row skipped unread must hide no error and no effect of theirs.
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
use crate::exec::packed_view::AdmittedRow;
use crate::exec::pre_decode_filter::extract_from_packed;
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
	/// Publish `v` unless the threshold held is at least as tight. `tighter(new, held)` says
	/// whether `new` orders strictly before `held` in the sort's order. Several sorts of one
	/// execution publish into one cell (the sorts of a scan's morsels), so what each publishes
	/// must only ever tighten what is held. Both sides of the lock are poison-tolerant: a
	/// panicked peer can only have left a stale-but-valid threshold, which is sound
	/// (under-rejects).
	pub(crate) fn tighten(&self, v: Value, tighter: impl Fn(&Value, &Value) -> bool) {
		let mut guard = match self.value.write() {
			Ok(guard) => guard,
			Err(poisoned) => poisoned.into_inner(),
		};
		if guard.as_deref().is_none_or(|held| tighter(&v, held)) {
			*guard = Some(Arc::new(v));
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

/// Scan-side probe: rejects rows from a record's raw stored bytes, in either
/// encoding, when their first ORDER BY key cannot beat the current threshold.
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
	/// Whether a packed row may be rejected from its checked columns
	/// ([`crate::exec::vm::payload::TopKKey::packed_rows`]). A packed row of a scan that does not
	/// state it is never rejected.
	packed_rows: bool,
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
		packed_rows: bool,
		cell: Arc<TopKThresholdCell>,
	) -> Arc<Self> {
		Arc::new(Self {
			path,
			direction,
			single_key,
			packed_rows,
			pushdown: 0,
			cell: Some(cell),
			depth_limit,
			metrics: None,
		})
	}

	/// Copy of this probe that never rejects a tie, whatever the number of keys. A threshold
	/// published by a sort that did not see every row before the row being judged (each morsel of
	/// a scan publishes what it holds) may belong to rows that come after it, and those lose a tie.
	pub(crate) fn ties_kept(&self) -> Arc<Self> {
		Arc::new(Self {
			single_key: false,
			..self.clone()
		})
	}

	/// Metrics handle for flushing skip counts (EXPLAIN ANALYZE only).
	pub(crate) fn metrics(&self) -> Option<&Arc<OperatorMetrics>> {
		self.metrics.as_ref()
	}

	/// Whether this probe may reject a packed row from its checked columns: the scan states that
	/// no op between it and the sort can hide an error or an effect of a row it skips.
	pub(crate) fn reads_packed_rows(&self) -> bool {
		self.packed_rows
	}

	/// Decide from a record admitted for reading its bytes whether this row can be skipped
	/// without decode. `true` only when the row's first ORDER BY key provably cannot beat
	/// `threshold`; any uncertainty (wire bail, decode error) returns `false` so the row takes the
	/// ordinary full-decode path.
	///
	/// The caller has admitted the record's generation against its table's
	/// layouts ([`crate::kvs::RowGate`]) before this reads any field of it, so
	/// a row of an unlisted generation is refused rather than rejected here. A
	/// checked packed row is rejected from its columns only where the scan
	/// states that no op above it can hide an error or an effect of a row it
	/// skips ([`Self::reads_packed_rows`]); a packed row not checked, or one
	/// the check refused, is never rejected.
	pub(crate) fn rejects(&self, threshold: &Value, row: &AdmittedRow<'_>) -> bool {
		let extracted = match row {
			AdmittedRow::Bytes(record_bytes) => {
				extract_field_from_record_bytes(record_bytes, &self.path, self.depth_limit)
			}
			AdmittedRow::Packed {
				row,
				..
			} if self.packed_rows => extract_from_packed(*row, &self.path, self.depth_limit),
			AdmittedRow::Packed {
				..
			}
			| AdmittedRow::Refused => return false,
		};
		let candidate = match extracted {
			Extracted::Found(v) => v,
			// Absent field reads as None, exactly like FieldPath::extract
			// in compare_records_by_keys.
			Extracted::Missing => Value::None,
			Extracted::Bail => return false,
		};
		let rejected = self.rejects_candidate(threshold, &candidate);
		#[cfg(test)]
		if rejected {
			crate::exec::vm::row_packed::observed::note_probed(matches!(
				row,
				AdmittedRow::Packed { .. }
			));
		}
		rejected
	}

	/// Whether a row whose first ORDER BY key is `candidate` cannot beat `threshold`.
	fn rejects_candidate(&self, threshold: &Value, candidate: &Value) -> bool {
		cannot_beat(threshold, candidate, self.direction, self.single_key)
	}
}

/// Whether a row whose first ORDER BY key is `candidate` cannot beat `threshold`, the first key
/// of the worst row a sort in `direction` keeps: it orders strictly after it, or ties it where
/// the order has `single_key`.
pub(crate) fn cannot_beat(
	threshold: &Value,
	candidate: &Value,
	direction: SortDirection,
	single_key: bool,
) -> bool {
	// Same comparison the sort heap uses (collate/numeric keys are
	// ineligible for pushdown, so plain Value ordering applies).
	let ord = compare_values(candidate, threshold, false, false);
	let ord = match direction {
		SortDirection::Asc => ord,
		SortDirection::Desc => ord.reverse(),
	};
	match ord {
		// Strictly worse than the heap's worst: can never be admitted.
		Ordering::Greater => true,
		// Ties with the heap's worst: the heap admits only on strict
		// Less, so a single-key tie can never be admitted either. With
		// more keys the tail could still win — fall through.
		Ordering::Equal => single_key,
		Ordering::Less => false,
	}
}

impl PartialEq for TopKThresholdProbe {
	fn eq(&self, other: &Self) -> bool {
		self.pushdown == other.pushdown
			&& self.direction == other.direction
			&& self.single_key == other.single_key
			&& self.packed_rows == other.packed_rows
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
		let FieldPathPart::Field(name) = part else {
			return None;
		};
		segments.push(PathSegment::new(name.as_str()));
	}
	if segments[0].as_str() == "id" {
		return None;
	}
	Some(segments)
}

/// Whether wire segments `path` open with the name of a component of record
/// ids with structure `key`.
///
/// A named component is derived from the record id and never stored in the
/// record body, so a raw read of it would see `Missing` and mis-reject, as a
/// read of `id` would: such a path is not probed.
pub(crate) fn reads_key_component(
	path: &[PathSegment],
	key: &crate::catalog::KeyStructure,
) -> bool {
	path.first().is_some_and(|root| {
		key.components.iter().any(|component| {
			component.name.as_ref().is_some_and(|name| name.as_str() == root.as_str())
		})
	})
}

#[cfg(all(test, feature = "kv-mem"))]
mod packed_tests;
#[cfg(test)]
mod tests {
	use std::collections::BTreeMap;

	use chrono::{TimeZone, Utc};
	use revision::SerializeRevisioned;
	use surrealdb_strand::Strand;

	use super::*;
	use crate::catalog::Record;
	use crate::val::{Datetime, Number, Object};

	/// The format the generation-0 samples are written in.
	const DOCUMENT: crate::catalog::RecordFormat = crate::catalog::RecordFormat::Document {
		generation: None,
	};

	/// Matches the default `ctx.config.exec.idiom_recursion_limit` (256).
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

	/// `obj` as a record in the stored-value encoding.
	fn stored_record(obj: Object) -> Vec<u8> {
		crate::catalog::encode_record(DOCUMENT, None, &Value::Object(obj), None).unwrap()
	}

	fn int_object(field: &str, n: i64) -> Object {
		Object::from(BTreeMap::from([(Strand::from(field), Value::Number(Number::Int(n)))]))
	}

	fn record_with_int(field: &str, n: i64) -> Vec<u8> {
		wire_record(int_object(field, n))
	}

	/// A record holding `field: n` in each encoding.
	fn records_with_int(field: &str, n: i64) -> [Vec<u8>; 2] {
		[record_with_int(field, n), stored_record(int_object(field, n))]
	}

	fn probe(direction: SortDirection, single_key: bool, path: &[&str]) -> Arc<TopKThresholdProbe> {
		TopKThresholdProbe::armed(
			path.iter().map(|s| PathSegment::new(*s)).collect(),
			direction,
			single_key,
			TEST_DEPTH_LIMIT,
			false,
			Arc::new(TopKThresholdCell::default()),
		)
	}

	#[test]
	fn a_path_rooted_at_a_named_component_is_not_probed() {
		use crate::expr::{KeyNames, Kind, KindLiteral};
		use crate::val::Strand;
		let names = KeyNames::new(vec![Strand::new("city"), Strand::new("time")], 2).unwrap();
		let kind = Kind::Literal(KindLiteral::Array(vec![Kind::String, Kind::Datetime]));
		let key = crate::catalog::KeyStructure::from_id_field(&kind, &names).expect("a structure");
		assert!(reads_key_component(&[PathSegment::new("time")], &key));
		assert!(reads_key_component(&[PathSegment::new("city"), PathSegment::new("x")], &key));
		assert!(!reads_key_component(&[PathSegment::new("temp")], &key));
		assert!(!reads_key_component(&[], &key));
	}

	/// A cell shared by several sorts keeps only the tightest threshold published to it, as ranked
	/// by the comparison each publisher passes to `tighten`.
	#[test]
	fn a_cell_shared_by_sorts_only_tightens() {
		let t = |n: i64| Value::Number(Number::Int(n));
		let cell = TopKThresholdCell::default();
		assert!(cell.snapshot().is_none());
		let ascending = |new: &Value, held: &Value| new < held;
		cell.tighten(t(50), ascending);
		assert_eq!(cell.snapshot().as_deref(), Some(&t(50)));
		cell.tighten(t(70), ascending);
		assert_eq!(cell.snapshot().as_deref(), Some(&t(50)), "a looser threshold is not kept");
		cell.tighten(t(50), ascending);
		assert_eq!(cell.snapshot().as_deref(), Some(&t(50)));
		cell.tighten(t(20), ascending);
		assert_eq!(cell.snapshot().as_deref(), Some(&t(20)));
		let descending = |new: &Value, held: &Value| new > held;
		let cell = TopKThresholdCell::default();
		cell.tighten(t(20), descending);
		cell.tighten(t(10), descending);
		cell.tighten(t(30), descending);
		assert_eq!(cell.snapshot().as_deref(), Some(&t(30)));
	}

	/// A probe that keeps ties rejects only what is strictly worse than the threshold, however many
	/// keys the order has.
	#[test]
	fn a_probe_that_keeps_ties_rejects_only_the_strictly_worse() {
		use revision::SerializeRevisioned;
		let mut rec = Vec::new();
		crate::catalog::Record {
			metadata: None,
			data: Value::Object(crate::val::Object::from(std::collections::BTreeMap::from([(
				surrealdb_strand::Strand::from("a"),
				Value::Number(Number::Int(5)),
			)]))),
		}
		.serialize_revisioned(&mut rec)
		.unwrap();
		let t = |n: i64| Value::Number(Number::Int(n));
		for direction in [SortDirection::Asc, SortDirection::Desc] {
			let single = TopKThresholdProbe::armed(
				[PathSegment::new("a")].into_iter().collect(),
				direction,
				true,
				256,
				false,
				Arc::new(TopKThresholdCell::default()),
			);
			let kept = single.ties_kept();
			// The row's key equals the threshold.
			assert!(
				single.rejects(&t(5), &AdmittedRow::Bytes(&rec)),
				"{direction:?}: a single key rejects a tie"
			);
			assert!(
				!kept.rejects(&t(5), &AdmittedRow::Bytes(&rec)),
				"{direction:?}: a tie is kept"
			);
			// Strictly worse than the threshold is rejected, strictly better is not.
			let (worse, better) = match direction {
				SortDirection::Asc => (4, 6),
				SortDirection::Desc => (6, 4),
			};
			assert!(kept.rejects(&t(worse), &AdmittedRow::Bytes(&rec)), "{direction:?}");
			assert!(!kept.rejects(&t(better), &AdmittedRow::Bytes(&rec)), "{direction:?}");
		}
	}

	/// Full reject matrix: {Asc, Desc} × {candidate better, tie, worse} ×
	/// {single-key, multi-key}, over a record in each encoding. Candidate field
	/// value is fixed at 5; the threshold varies.
	#[test]
	fn reject_matrix() {
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
		for rec in records_with_int("a", 5) {
			for (direction, threshold, single_key, expect) in cases {
				let p = probe(direction, single_key, &["a"]);
				assert_eq!(
					p.rejects(&t(threshold), &AdmittedRow::Bytes(&rec)),
					expect,
					"direction={direction:?} threshold={threshold} single_key={single_key}",
				);
			}
		}
	}

	/// Missing field reads as Value::None. Under DESC, None is the worst
	/// possible key — rejected against any real threshold. Under ASC, None is
	/// the best possible key — always passes.
	#[test]
	fn missing_field_reads_as_none() {
		for rec in records_with_int("other", 1) {
			let threshold = Value::Number(Number::Int(5));
			assert!(
				probe(SortDirection::Desc, true, &["a"])
					.rejects(&threshold, &AdmittedRow::Bytes(&rec))
			);
			assert!(
				!probe(SortDirection::Asc, true, &["a"])
					.rejects(&threshold, &AdmittedRow::Bytes(&rec))
			);
			// None == None tie: single-key rejects, multi-key passes.
			assert!(
				probe(SortDirection::Desc, true, &["a"])
					.rejects(&Value::None, &AdmittedRow::Bytes(&rec))
			);
			assert!(
				!probe(SortDirection::Desc, false, &["a"])
					.rejects(&Value::None, &AdmittedRow::Bytes(&rec))
			);
		}
	}

	/// Wire-level bail (corrupt bytes / non-object intermediate) must pass the
	/// row through to full decode, never reject.
	#[test]
	fn bail_passes_through() {
		let threshold = Value::Number(Number::Int(5));
		let p = probe(SortDirection::Desc, true, &["a"]);
		assert!(!p.rejects(&threshold, &AdmittedRow::Bytes(b"\xff\xff\xff\xff")));
		// Descending through a non-object intermediate (`a.b` where `a` is an
		// int) bails rather than treating the leaf as missing.
		for rec in records_with_int("a", 5) {
			let nested = probe(SortDirection::Desc, true, &["a", "b"]);
			assert!(!nested.rejects(&threshold, &AdmittedRow::Bytes(&rec)));
		}
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
		assert!(p.rejects(&Value::Datetime(newer), &AdmittedRow::Bytes(&rec)));
		// Row ties with the heap's worst → single-key reject.
		assert!(p.rejects(&Value::Datetime(older), &AdmittedRow::Bytes(&rec)));
		// Threshold older than the row → row wins → pass.
		let rec_newer = wire_record(Object::from(BTreeMap::from([(
			Strand::from("created_at"),
			Value::Datetime(newer),
		)])));
		assert!(!p.rejects(&Value::Datetime(older), &AdmittedRow::Bytes(&rec_newer)));
	}

	/// Nested Field chains descend; the leaf compares like a root field.
	#[test]
	fn nested_field_path_descends() {
		let inner =
			Object::from(BTreeMap::from([(Strand::from("score"), Value::Number(Number::Int(3)))]));
		let object = Object::from(BTreeMap::from([(Strand::from("stats"), Value::Object(inner))]));
		for rec in [wire_record(object.clone()), stored_record(object)] {
			let p = probe(SortDirection::Desc, true, &["stats", "score"]);
			assert!(p.rejects(&Value::Number(Number::Int(9)), &AdmittedRow::Bytes(&rec)));
			assert!(!p.rejects(&Value::Number(Number::Int(1)), &AdmittedRow::Bytes(&rec)));
		}
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
