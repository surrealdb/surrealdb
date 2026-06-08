//! Behavioural tests for the walker-based object extraction.

#![cfg(test)]

use std::collections::BTreeMap;

use revision::{DeserializeRevisioned, SerializeRevisioned};
use surrealdb_strand::Strand;

use super::{
	Extracted, PathSegment, SlotScanResult, TEST_DEPTH_LIMIT, descend_record_value_path,
	descend_value_slice_path, extract_field_from_record_bytes,
	scan_record_object_at_path_with_slots,
};
use crate::catalog::Record;
use crate::val::{Number, Object, Value};

/// Build a `Vec<PathSegment>` from string slices. Mirrors how the planner
/// hands paths to descent (one segment per dotted part).
fn p(segments: &[&str]) -> Vec<PathSegment> {
	segments.iter().copied().map(PathSegment::from).collect()
}

/// Encode a "plain" record (no metadata) wrapping `obj` as `Value::Object(obj)`.
///
/// Routes through the macro-generated [`Record::serialize_revisioned`] so the
/// produced bytes always match the latest revision's wire layout (rev-2
/// optimised + indexed_struct as of DB-655's predecessor). Hand-rolling the
/// envelope here used to work for rev 1 but breaks the moment the revision
/// bumps — the offset prologue and `u32_le payload_length` envelope would be
/// missing.
fn wire_record_plain_object(obj: Object) -> Vec<u8> {
	let rec = Record {
		metadata: None,
		data: Value::Object(obj),
	};
	let mut out = Vec::new();
	rec.serialize_revisioned(&mut out).unwrap();
	out
}

/// Test-only convenience over [`scan_record_object_at_path_with_slots`] that
/// decodes each slot's wire bytes into a [`Value`] (or [`Value::None`] when
/// the slot is absent), giving back the same `Vec<Value>` shape the legacy
/// scan returned. Production callers run their evaluator on borrowed slot
/// bytes inside the closure — the tests just verify the descent + needle
/// pairing logic, so decoding the slots is harmless.
fn scan_slots_decoded(
	record_bytes: &[u8],
	path: &[&PathSegment],
	needles: &[&[u8]],
) -> SlotScanResult<Vec<Value>> {
	scan_record_object_at_path_with_slots(record_bytes, path, needles, TEST_DEPTH_LIMIT, |slots| {
		slots
			.iter()
			.map(|s| match s {
				None => Value::None,
				Some(bytes) => {
					let mut r: &[u8] = bytes;
					<Value as DeserializeRevisioned>::deserialize_revisioned(&mut r)
						.unwrap_or(Value::None)
				}
			})
			.collect()
	})
}

#[test]
fn extract_existing_top_level_field() {
	let obj = Object::from(BTreeMap::from([
		(Strand::from("a"), Value::Number(Number::Int(1))),
		(Strand::from("b"), Value::Bool(true)),
	]));
	let rec = wire_record_plain_object(obj);
	let result = extract_field_from_record_bytes(&rec, &p(&["b"]), TEST_DEPTH_LIMIT);
	match result {
		Extracted::Found(Value::Bool(true)) => {}
		other => panic!("expected Found(Bool(true)), got {:?}", other),
	}
}

#[test]
fn extract_nested_field_descends_correctly() {
	let inner = Object::from(BTreeMap::from([(Strand::from("x"), Value::Number(Number::Int(42)))]));
	let outer = Object::from(BTreeMap::from([(Strand::from("o"), Value::Object(inner))]));
	let rec = wire_record_plain_object(outer);
	let result = extract_field_from_record_bytes(&rec, &p(&["o", "x"]), TEST_DEPTH_LIMIT);
	assert!(matches!(result, Extracted::Found(Value::Number(Number::Int(42)))));
}

#[test]
fn missing_key_yields_missing() {
	let obj = Object::from(BTreeMap::from([(Strand::from("only"), Value::Number(Number::Int(1)))]));
	let rec = wire_record_plain_object(obj);
	let result = extract_field_from_record_bytes(&rec, &p(&["missing"]), TEST_DEPTH_LIMIT);
	assert!(matches!(result, Extracted::Missing));
}

#[test]
fn edge_metadata_extracts_data() {
	// Edge records carry metadata flagging `record_type = Edge`, but the
	// `data` field is still the user-visible payload (graph row with
	// `in` / `out` fields plus user fields). The pre-decode filter should
	// extract from `data` for these.
	use crate::catalog::{Metadata, RecordType};
	let obj = Object::from(BTreeMap::from([
		(Strand::from("kind"), Value::String(Strand::from("knows"))),
		(Strand::from("score"), Value::Number(Number::Int(7))),
	]));
	let rec = Record {
		metadata: Some(Metadata {
			record_type: RecordType::Edge {
				variant: Default::default(),
			},
			aggregation_stats: Vec::new(),
		}),
		data: Value::Object(obj),
	};
	let mut bytes = Vec::new();
	rec.serialize_revisioned(&mut bytes).unwrap();
	let result = extract_field_from_record_bytes(&bytes, &p(&["score"]), TEST_DEPTH_LIMIT);
	match result {
		Extracted::Found(Value::Number(Number::Int(7))) => {}
		other => panic!("expected Found(Int(7)) for edge data, got {:?}", other),
	}
}

#[test]
fn aggregation_view_metadata_extracts_data() {
	// Materialised-view rows carry non-empty `aggregation_stats`, but
	// `record.data` is populated by evaluating the view's SELECT
	// expressions against the aggregated state at write time (see
	// `doc::table::compute` setting `record.data = data`). The pre-decode
	// filter therefore reads `data` directly for these too.
	use crate::catalog::aggregation::AggregationStat;
	use crate::catalog::{Metadata, RecordType};
	let obj =
		Object::from(BTreeMap::from([(Strand::from("total"), Value::Number(Number::Int(42)))]));
	let rec = Record {
		metadata: Some(Metadata {
			record_type: RecordType::Table,
			aggregation_stats: vec![AggregationStat::Count {
				count: 17,
			}],
		}),
		data: Value::Object(obj),
	};
	let mut bytes = Vec::new();
	rec.serialize_revisioned(&mut bytes).unwrap();
	let result = extract_field_from_record_bytes(&bytes, &p(&["total"]), TEST_DEPTH_LIMIT);
	match result {
		Extracted::Found(Value::Number(Number::Int(42))) => {}
		other => panic!("expected Found(Int(42)) for aggregation row data, got {:?}", other),
	}
}

#[test]
fn empty_path_bails() {
	let rec = wire_record_plain_object(Object::default());
	let result = extract_field_from_record_bytes(&rec, &[], TEST_DEPTH_LIMIT);
	assert!(matches!(result, Extracted::Bail));
}

#[test]
fn descend_value_slice_path_decodes_field() {
	// Build a Value::Object wire slice and feed it to descend_value_slice_path.
	let inner =
		Object::from(BTreeMap::from([(Strand::from("k"), Value::String(Strand::from("v")))]));
	let value = Value::Object(inner);
	let mut wire = Vec::new();
	value.serialize_revisioned(&mut wire).unwrap();
	let result = descend_value_slice_path(&wire, &p(&["k"]));
	match result {
		Extracted::Found(Value::String(s)) => assert_eq!(s.as_str(), "v"),
		other => panic!("unexpected: {:?}", other),
	}
}

#[test]
fn descend_record_value_path_round_trips_all_supported_types() {
	let obj = Object::from(BTreeMap::from([
		(Strand::from("alpha"), Value::Number(Number::Int(1))),
		(Strand::from("middle"), Value::Number(Number::Int(2))),
		(Strand::from("zeta"), Value::Number(Number::Int(3))),
	]));
	let rec = wire_record_plain_object(obj);

	for (key, expected) in [("alpha", 1), ("middle", 2), ("zeta", 3)] {
		let result = descend_record_value_path(&rec, &p(&[key]));
		match result {
			Extracted::Found(Value::Number(Number::Int(v))) => assert_eq!(v, expected),
			other => panic!("unexpected for {key}: {other:?}"),
		}
	}
}

/// `scan_value_object_with_slots`'s indexed branch switches from
/// `entries()` iteration to per-needle `find_value_bytes` when needles
/// are sparse against a wide row (M · log N vs N · log M). The threshold
/// is `4 * M < N`; a row with ≥ 12 entries indexed (need ≥ 8 to trigger
/// the indexed prologue) and 2 needles satisfies that.
#[test]
fn scan_sparse_needles_against_wide_indexed_object_returns_correct_slots() {
	// Object with 16 keys (well past the indexed-prologue threshold)
	// — k00..k15 — each holding a distinct integer value.
	let mut entries = BTreeMap::new();
	for i in 0..16u64 {
		entries.insert(Strand::from(format!("k{i:02}")), Value::Number(Number::Int(i as i64 * 10)));
	}
	let obj = Object::from(entries);
	let rec = wire_record_plain_object(obj);

	// 2 needles against 16 entries → 4*2 < 16 → needle-driven branch.
	let needles: &[&[u8]] = &[b"k03", b"k12"];
	let result = scan_slots_decoded(&rec, &[], needles);
	match result {
		SlotScanResult::Found(values) => {
			assert_eq!(values.len(), 2);
			assert!(matches!(values[0], Value::Number(Number::Int(30))));
			assert!(matches!(values[1], Value::Number(Number::Int(120))));
		}
		other => panic!("expected Found, got {other:?}"),
	}

	// Absent needle in the sparse path stays `Value::None`. Keep the
	// needle slice strictly UTF-8-ascending: `k03 < k12 < zzz`.
	let needles: &[&[u8]] = &[b"k03", b"k12", b"zzz"];
	let result = scan_slots_decoded(&rec, &[], needles);
	match result {
		SlotScanResult::Found(values) => {
			assert_eq!(values.len(), 3);
			assert!(matches!(values[0], Value::Number(Number::Int(30))));
			assert!(matches!(values[1], Value::Number(Number::Int(120))));
			assert!(matches!(values[2], Value::None));
		}
		other => panic!("expected Found with sparse-needle miss, got {other:?}"),
	}
}

/// Symmetric coverage: same wide object, but with **dense** needles
/// (M ≈ N) staying on the original `entries()`-iteration branch.
#[test]
fn scan_dense_needles_against_wide_indexed_object_returns_correct_slots() {
	let mut entries = BTreeMap::new();
	for i in 0..16u64 {
		entries.insert(Strand::from(format!("k{i:02}")), Value::Number(Number::Int(i as i64 * 10)));
	}
	let obj = Object::from(entries);
	let rec = wire_record_plain_object(obj);

	// 12 needles against 16 entries → 4*12 = 48, not < 16 → entries()
	// iteration. Result must still align with the input needle order.
	let needle_strings: Vec<String> = (0..12).map(|i| format!("k{i:02}")).collect();
	let needles: Vec<&[u8]> = needle_strings.iter().map(|s| s.as_bytes()).collect();
	let result = scan_slots_decoded(&rec, &[], &needles);
	match result {
		SlotScanResult::Found(values) => {
			assert_eq!(values.len(), 12);
			for (i, v) in values.iter().enumerate() {
				assert!(
					matches!(v, Value::Number(Number::Int(n)) if *n == (i as i64) * 10),
					"slot {i}: {v:?}",
				);
			}
		}
		other => panic!("expected Found, got {other:?}"),
	}
}

#[test]
fn scan_record_root_returns_values_for_present_keys_and_none_for_absent() {
	let obj = Object::from(BTreeMap::from([
		(Strand::from("a"), Value::Bool(false)),
		(Strand::from("m"), Value::Number(Number::Int(7))),
		(Strand::from("z"), Value::Bool(true)),
	]));
	let rec = wire_record_plain_object(obj);

	// Sorted needle list with a missing key in the middle.
	let needles: &[&[u8]] = &[b"a", b"missing", b"z"];
	let result = scan_slots_decoded(&rec, &[], needles);
	match result {
		SlotScanResult::Found(values) => {
			assert_eq!(values.len(), 3);
			assert!(matches!(values[0], Value::Bool(false)));
			assert!(matches!(values[1], Value::None));
			assert!(matches!(values[2], Value::Bool(true)));
		}
		other => panic!("expected Found at root, got {other:?}"),
	}
}

#[test]
fn scan_record_object_at_path_walks_nested_object() {
	// Build a record whose `outer` field is a nested object with three
	// keys; scan at `path = ["outer"]` for two needles and one missing.
	let inner = Object::from(BTreeMap::from([
		(Strand::from("a"), Value::Number(Number::Int(1))),
		(Strand::from("b"), Value::Number(Number::Int(2))),
		(Strand::from("c"), Value::Number(Number::Int(3))),
	]));
	let outer = Object::from(BTreeMap::from([
		(Strand::from("outer"), Value::Object(inner)),
		(Strand::from("sibling"), Value::Bool(false)),
	]));
	let rec = wire_record_plain_object(outer);

	let needles: &[&[u8]] = &[b"a", b"b", b"missing"];
	let path = p(&["outer"]);
	let path_refs: Vec<&PathSegment> = path.iter().collect();
	let result = scan_slots_decoded(&rec, &path_refs, needles);
	match result {
		SlotScanResult::Found(values) => {
			assert_eq!(values.len(), 3);
			assert!(matches!(values[0], Value::Number(Number::Int(1))));
			assert!(matches!(values[1], Value::Number(Number::Int(2))));
			assert!(matches!(values[2], Value::None));
		}
		other => panic!("expected Found, got {other:?}"),
	}
}

#[test]
fn scan_record_object_at_path_missing_intermediate_yields_missing() {
	// `outer` exists but `inner` does not; scanning at
	// `["outer", "inner"]` must yield Missing.
	let outer = Object::from(BTreeMap::from([(
		Strand::from("outer"),
		Value::Object(Object::from(BTreeMap::from([(Strand::from("present"), Value::Bool(true))]))),
	)]));
	let rec = wire_record_plain_object(outer);

	let needles: &[&[u8]] = &[b"a"];
	let path = p(&["outer", "inner"]);
	let path_refs: Vec<&PathSegment> = path.iter().collect();
	let result = scan_slots_decoded(&rec, &path_refs, needles);
	assert!(matches!(result, SlotScanResult::Missing));
}

#[test]
fn scan_record_object_at_path_non_object_intermediate_bails() {
	// `outer` is a Number, not an Object — descending into it must Bail
	// because the walker cannot treat a non-object as a map.
	let outer =
		Object::from(BTreeMap::from([(Strand::from("outer"), Value::Number(Number::Int(7)))]));
	let rec = wire_record_plain_object(outer);

	let needles: &[&[u8]] = &[b"a"];
	let path = p(&["outer", "anything"]);
	let path_refs: Vec<&PathSegment> = path.iter().collect();
	let result = scan_slots_decoded(&rec, &path_refs, needles);
	assert!(matches!(result, SlotScanResult::Bail));
}

/// Non-ASCII keys lock down the load-bearing claim of the byte-borrowed
/// `find_bytes` / `with_key_bytes` path: `Strand` is validated UTF-8, so
/// byte lexicographic order equals codepoint lexicographic order. If the
/// `VecMap<Strand, Value>` sort and the on-wire byte comparator ever
/// diverge, lookups against multi-byte keys would silently miss.
#[test]
fn extract_handles_non_ascii_keys() {
	// Three keys ordered by UTF-8 byte sequence (which equals codepoint
	// order for valid UTF-8):
	//   "café"   = 63 61 66 c3 a9
	//   "naïve"  = 6e 61 c3 af 76 65
	//   "日本"    = e6 97 a5 e6 9c ac
	let obj = Object::from(BTreeMap::from([
		(Strand::from("café"), Value::Number(Number::Int(1))),
		(Strand::from("naïve"), Value::Number(Number::Int(2))),
		(Strand::from("日本"), Value::Number(Number::Int(3))),
	]));
	let rec = wire_record_plain_object(obj);
	for (key, expected) in [("café", 1i64), ("naïve", 2), ("日本", 3)] {
		match extract_field_from_record_bytes(&rec, &p(&[key]), TEST_DEPTH_LIMIT) {
			Extracted::Found(Value::Number(Number::Int(n))) => {
				assert_eq!(n, expected, "wrong value for key {key:?}")
			}
			other => panic!("expected Found(Int({expected})) for {key:?}, got {other:?}"),
		}
	}
	// Multi-needle scan over the same object exercises the byte-borrowed
	// `find_value_bytes` path.
	let needles: &[&[u8]] = &["café".as_bytes(), "naïve".as_bytes(), "日本".as_bytes()];
	let result = scan_slots_decoded(&rec, &[], needles);
	match result {
		SlotScanResult::Found(scanned) => {
			assert_eq!(scanned.len(), 3);
			for (got, expected) in scanned.iter().zip([1i64, 2, 3]) {
				match got {
					Value::Number(Number::Int(n)) => assert_eq!(*n, expected),
					other => panic!("expected Int({expected}), got {other:?}"),
				}
			}
		}
		other => panic!("expected Found, got {other:?}"),
	}
}

/// Build a record whose `data` is a chain of `depth` nested objects, each
/// containing a single field named `"x"` pointing at the next level, with the
/// innermost level holding `Value::Bool(true)`.
fn wire_record_nested_x(depth: usize) -> Vec<u8> {
	let mut value = Value::Bool(true);
	for _ in 0..depth {
		let mut entries = BTreeMap::new();
		entries.insert(Strand::from("x"), value);
		value = Value::Object(Object::from(entries));
	}
	let Value::Object(obj) = value else {
		unreachable!("at least one level wrapped")
	};
	wire_record_plain_object(obj)
}

#[test]
fn descent_bails_past_depth_limit() {
	// Depth-limit guard: a path strictly longer than the configured cap
	// returns `Bail` regardless of whether the data is present — this is
	// the defensive cap that prevents pathological deep paths from
	// pressuring the descent's per-level allocations.
	let depth = 8usize;
	let rec = wire_record_nested_x(depth);
	let path: Vec<PathSegment> = std::iter::repeat_n(PathSegment::from("x"), depth).collect();
	let limit: u32 = (depth as u32) - 1;

	let extracted = super::extract_field_from_record_bytes(&rec, &path, limit);
	assert!(
		matches!(extracted, Extracted::Bail),
		"expected Bail when path length {} > depth_limit {}, got {:?}",
		path.len(),
		limit,
		extracted
	);
}

#[test]
fn descent_succeeds_at_exact_depth_limit() {
	// Boundary: a path with length exactly equal to `depth_limit` must not
	// be rejected by the cap — the check is strict `>` not `>=`.
	let depth = 8usize;
	let rec = wire_record_nested_x(depth);
	let path: Vec<PathSegment> = std::iter::repeat_n(PathSegment::from("x"), depth).collect();
	let limit: u32 = depth as u32;

	let extracted = super::extract_field_from_record_bytes(&rec, &path, limit);
	match extracted {
		Extracted::Found(Value::Bool(true)) => {}
		other => {
			panic!("expected Found(true) at depth == depth_limit == {}, got {:?}", depth, other)
		}
	}
}

#[test]
fn descent_bails_on_zero_depth_limit() {
	// Trivial edge case: a zero limit always bails because every real
	// descent has at least one segment.
	let rec = wire_record_nested_x(1);
	let path = p(&["x"]);
	let extracted = super::extract_field_from_record_bytes(&rec, &path, 0);
	assert!(matches!(extracted, Extracted::Bail));
}

#[test]
fn scan_record_object_at_path_with_empty_path_resolves_against_root() {
	// Empty path resolves needles directly against the row's outer object
	// — same shape as a `path = []` scan, no NavigatePrefix descent needed.
	let obj = Object::from(BTreeMap::from([
		(Strand::from("a"), Value::Bool(false)),
		(Strand::from("z"), Value::Bool(true)),
	]));
	let rec = wire_record_plain_object(obj);

	let needles: &[&[u8]] = &[b"a", b"missing", b"z"];
	let result = scan_slots_decoded(&rec, &[], needles);
	match result {
		SlotScanResult::Found(values) => {
			assert_eq!(values.len(), 3);
			assert!(matches!(values[0], Value::Bool(false)));
			assert!(matches!(values[1], Value::None));
			assert!(matches!(values[2], Value::Bool(true)));
		}
		other => panic!("expected Found, got {other:?}"),
	}
}
