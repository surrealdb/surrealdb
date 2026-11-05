//! Behavioural tests for the walker-based object extraction.

#![cfg(test)]

use std::collections::BTreeMap;

use revision::{DeserializeRevisioned, SerializeRevisioned, WalkRevisioned};
use surrealdb_strand::Strand;

use super::{
	Extracted, PathSegment, SlotScanResult, TEST_DEPTH_LIMIT, descend_record_value_path,
	descend_value_slice_path, extract_field_from_record_bytes, project_packed_record_bytes,
	project_record_bytes, project_record_bytes_excluding, record_data_bytes,
	rev2_record_data_bytes, scan_record_object_at_path_with_slots,
};
use crate::catalog::{Record, RecordFormat, RecordType, StoredRecord, encode_record};
use crate::val::{Number, Object, RecordId, RecordIdKey, Value};

/// The format the generation-0 samples are written in.
const DOCUMENT: RecordFormat = RecordFormat::Document {
	generation: None,
};

/// Build a `Vec<PathSegment>` from string slices. Mirrors how the planner
/// hands paths to descent (one segment per dotted part).
fn p(segments: &[&str]) -> Vec<PathSegment> {
	segments.iter().copied().map(PathSegment::from).collect()
}

/// Encode a "plain" record (no metadata) wrapping `obj` as `Value::Object(obj)`.
///
/// Routes through the macro-generated [`Record::serialize_revisioned`] so the
/// produced bytes always match the latest revision's wire layout (rev-2
/// optimised + indexed_struct). Hand-rolling the envelope here would break
/// the moment the revision bumps — the offset prologue and
/// `u32_le payload_length` envelope would be missing.
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
/// the slot is absent), giving back one `Vec<Value>` entry per slot.
/// Production callers run their evaluator on borrowed slot
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
	let Extracted::Found(Value::Bool(true)) = result else {
		panic!("expected Found(Bool(true)), got {:?}", result);
	};
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
	let Extracted::Found(Value::Number(Number::Int(7))) = result else {
		panic!("expected Found(Int(7)) for edge data, got {:?}", result);
	};
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
	let Extracted::Found(Value::Number(Number::Int(42))) = result else {
		panic!("expected Found(Int(42)) for aggregation row data, got {:?}", result);
	};
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
	let Extracted::Found(Value::String(s)) = result else {
		panic!("unexpected: {:?}", result);
	};
	assert_eq!(s.as_str(), "v");
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
		let Extracted::Found(Value::Number(Number::Int(v))) = result else {
			panic!("unexpected for {key}: {result:?}");
		};
		assert_eq!(v, expected);
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
	let SlotScanResult::Found(values) = result else {
		panic!("expected Found, got {result:?}");
	};
	assert_eq!(values.len(), 2);
	assert!(matches!(values[0], Value::Number(Number::Int(30))));
	assert!(matches!(values[1], Value::Number(Number::Int(120))));

	// Absent needle in the sparse path stays `Value::None`. Keep the
	// needle slice strictly UTF-8-ascending: `k03 < k12 < zzz`.
	let needles: &[&[u8]] = &[b"k03", b"k12", b"zzz"];
	let result = scan_slots_decoded(&rec, &[], needles);
	let SlotScanResult::Found(values) = result else {
		panic!("expected Found with sparse-needle miss, got {result:?}");
	};
	assert_eq!(values.len(), 3);
	assert!(matches!(values[0], Value::Number(Number::Int(30))));
	assert!(matches!(values[1], Value::Number(Number::Int(120))));
	assert!(matches!(values[2], Value::None));
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
	let SlotScanResult::Found(values) = result else {
		panic!("expected Found, got {result:?}");
	};
	assert_eq!(values.len(), 12);
	for (i, v) in values.iter().enumerate() {
		assert!(
			matches!(v, Value::Number(Number::Int(n)) if *n == (i as i64) * 10),
			"slot {i}: {v:?}",
		);
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
	let SlotScanResult::Found(values) = result else {
		panic!("expected Found at root, got {result:?}");
	};
	assert_eq!(values.len(), 3);
	assert!(matches!(values[0], Value::Bool(false)));
	assert!(matches!(values[1], Value::None));
	assert!(matches!(values[2], Value::Bool(true)));
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
	let SlotScanResult::Found(values) = result else {
		panic!("expected Found, got {result:?}");
	};
	assert_eq!(values.len(), 3);
	assert!(matches!(values[0], Value::Number(Number::Int(1))));
	assert!(matches!(values[1], Value::Number(Number::Int(2))));
	assert!(matches!(values[2], Value::None));
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
		let result = extract_field_from_record_bytes(&rec, &p(&[key]), TEST_DEPTH_LIMIT);
		let Extracted::Found(Value::Number(Number::Int(n))) = result else {
			panic!("expected Found(Int({expected})) for {key:?}, got {result:?}");
		};
		assert_eq!(n, expected, "wrong value for key {key:?}");
	}
	// Multi-needle scan over the same object exercises the byte-borrowed
	// `find_value_bytes` path.
	let needles: &[&[u8]] = &["café".as_bytes(), "naïve".as_bytes(), "日本".as_bytes()];
	let result = scan_slots_decoded(&rec, &[], needles);
	let SlotScanResult::Found(scanned) = result else {
		panic!("expected Found, got {result:?}");
	};
	assert_eq!(scanned.len(), 3);
	for (got, expected) in scanned.iter().zip([1i64, 2, 3]) {
		let Value::Number(Number::Int(n)) = got else {
			panic!("expected Int({expected}), got {got:?}");
		};
		assert_eq!(*n, expected);
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
	let Extracted::Found(Value::Bool(true)) = extracted else {
		panic!("expected Found(true) at depth == depth_limit == {}, got {:?}", depth, extracted);
	};
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
	let SlotScanResult::Found(values) = result else {
		panic!("expected Found, got {result:?}");
	};
	assert_eq!(values.len(), 3);
	assert!(matches!(values[0], Value::Bool(false)));
	assert!(matches!(values[1], Value::None));
	assert!(matches!(values[2], Value::Bool(true)));
}

/// Diverse rev-2 records: data-only (small object, indexed-map object,
/// nested, non-object scalars, empty object) and a metadata-bearing record
/// (which shifts `data_off` past the `Option::None` case).
fn equivalence_sample_records() -> Vec<Vec<u8>> {
	let mut small = Object::default();
	small.0.insert(Strand::from("a"), Value::Bool(true));
	small.0.insert(Strand::from("b"), Value::from(7i64));

	// >= 8 keys forces the indexed-map prologue on the inner object.
	let mut wide = Object::default();
	for i in 0..12u8 {
		wide.0.insert(Strand::from(format!("k{i:02}")), Value::from(i as i64));
	}

	let mut nested_inner = Object::default();
	nested_inner.0.insert(Strand::from("city"), Value::String(Strand::from("London")));
	let mut nested = Object::default();
	nested.0.insert(Strand::from("addr"), Value::Object(nested_inner));

	let encode = |data: Value| {
		let rec = Record {
			metadata: None,
			data,
		};
		let mut out = Vec::new();
		rec.serialize_revisioned(&mut out).unwrap();
		out
	};

	// A record carrying metadata: `data_off` is no longer the minimal
	// `Option::None` (1-byte) case, so the fast path must read it, not assume it.
	let with_meta = {
		let mut rec = Record::new(Value::Object(small.clone()));
		rec.set_record_type(RecordType::Table);
		assert!(rec.metadata.is_some(), "expected metadata to be set");
		let mut out = Vec::new();
		rec.serialize_revisioned(&mut out).unwrap();
		out
	};

	vec![
		encode(Value::Object(small)),
		encode(Value::Object(wide)),
		encode(Value::Object(nested)),
		encode(Value::Object(Object::default())),
		encode(Value::Bool(true)),
		encode(Value::from(42i64)),
		encode(Value::String(Strand::from("hello"))),
		encode(Value::None),
		with_meta,
	]
}

/// The samples of [`equivalence_sample_records`] in the stored-value
/// encoding, and two more: a record whose `id` its key replaces, and one that
/// stores an `id` other than its key.
fn stored_sample_records() -> Vec<Vec<u8>> {
	let mut samples: Vec<Vec<u8>> = equivalence_sample_records()
		.iter()
		.map(|bytes| {
			let record: Record = revision::from_slice(bytes).expect("sample decodes");
			encode_record(DOCUMENT, record.metadata.as_ref(), &record.data, None)
				.expect("sample encodes")
		})
		.collect();
	let key = RecordId::new("t".into(), RecordIdKey::Number(1));
	let mut object = Object::default();
	object.0.insert(Strand::from("a"), Value::Bool(true));
	object.0.insert(Strand::from("b"), Value::from(7i64));
	object.0.insert(Strand::from("id"), Value::RecordId(key.clone()));
	let data = Value::Object(object);
	let keyed = encode_record(DOCUMENT, None, &data, Some(&key)).expect("sample encodes");
	assert!(StoredRecord::parse(&keyed).unwrap().unwrap().is_keyed());
	samples.push(keyed);
	let other = RecordId::new("t".into(), RecordIdKey::Number(2));
	let foreign = encode_record(DOCUMENT, None, &data, Some(&other)).expect("sample encodes");
	assert!(!StoredRecord::parse(&foreign).unwrap().unwrap().is_keyed());
	samples.push(foreign);
	samples
}

/// Every sample, in each encoding.
fn all_sample_records() -> Vec<Vec<u8>> {
	let mut all = equivalence_sample_records();
	all.extend(stored_sample_records());
	all
}

/// The data a record's bytes hold, as written: a stored-value record whose
/// key replaces its `id` holds none.
fn data_as_written(bytes: &[u8]) -> Value {
	match StoredRecord::parse(bytes).expect("sample parses") {
		Some(stored) => stored.decode_document().expect("sample decodes").data,
		None => revision::from_slice::<Record>(bytes).expect("sample decodes").data,
	}
}

/// An extraction's outcome as a comparable value: `None` for a bail,
/// `Some(None)` for a missing field.
fn outcome(extracted: Extracted) -> Option<Option<Value>> {
	match extracted {
		Extracted::Found(v) => Some(Some(v)),
		Extracted::Missing => Some(None),
		Extracted::Bail => None,
	}
}

/// A stored-value record answers every descent its `revision` encoding
/// answers, with the same value, absence or bail: through nested objects,
/// past a field that is not an object, along a path into a wide object, and
/// past the depth limit. Only a path starting at `id` differs, which the
/// stored-value record leaves to the key.
#[test]
fn extract_reads_a_stored_value_record_as_a_revision_record() {
	let paths: [&[&str]; 10] = [
		&["a"],
		&["b"],
		&["missing"],
		&["addr"],
		&["addr", "city"],
		&["addr", "zip"],
		&["addr", "city", "x"],
		&["a", "x"],
		&["k07"],
		&["missing", "x"],
	];
	for bytes in equivalence_sample_records() {
		let record: Record = revision::from_slice(&bytes).expect("sample decodes");
		let stored =
			encode_record(DOCUMENT, record.metadata.as_ref(), &record.data, None).expect("encodes");
		for path in paths {
			let path = p(path);
			for limit in [TEST_DEPTH_LIMIT, 1, 0] {
				assert_eq!(
					outcome(extract_field_from_record_bytes(&stored, &path, limit)),
					outcome(extract_field_from_record_bytes(&bytes, &path, limit)),
					"{path:?} (limit {limit}) of {:?}",
					record.data
				);
			}
		}
	}
	let depth = 8;
	let nested = wire_record_nested_x(depth);
	let record: Record = revision::from_slice(&nested).expect("sample decodes");
	let stored = encode_record(DOCUMENT, None, &record.data, None).expect("encodes");
	let path: Vec<PathSegment> = std::iter::repeat_n(PathSegment::from("x"), depth).collect();
	for limit in [depth as u32 - 1, depth as u32] {
		assert_eq!(
			outcome(extract_field_from_record_bytes(&stored, &path, limit)),
			outcome(extract_field_from_record_bytes(&nested, &path, limit)),
			"limit {limit}"
		);
	}
}

/// A stored-value record's `id` is never read from its bytes, which leave it
/// out when the key replaces it: a path that starts there bails, whether or
/// not the record stores one, and the caller reads it from the key.
#[test]
fn extract_leaves_a_stored_value_records_id_to_its_key() {
	let samples = stored_sample_records();
	for bytes in &samples[samples.len() - 2..] {
		for path in [&["id"][..], &["id", "tb"]] {
			assert!(
				matches!(
					extract_field_from_record_bytes(bytes, &p(path), TEST_DEPTH_LIMIT),
					Extracted::Bail
				),
				"{path:?}"
			);
		}
		assert!(matches!(
			extract_field_from_record_bytes(bytes, &p(&["b"]), TEST_DEPTH_LIMIT),
			Extracted::Found(Value::Number(Number::Int(7)))
		));
	}
}

/// Bytes that start as a stored-value record but do not read as one are a
/// bail and project to `None`, so the caller decodes them in full and
/// surfaces the error.
#[test]
fn a_stored_value_record_that_does_not_read_bails() {
	let good =
		encode_record(DOCUMENT, None, &Value::Object(Object::default()), None).expect("encodes");
	for broken in [vec![StoredRecord::MARKER], vec![StoredRecord::MARKER, 0x80, 0x1A, 0x01], {
		let mut b = good;
		b.push(0);
		b
	}] {
		assert!(matches!(
			extract_field_from_record_bytes(&broken, &p(&["a"]), TEST_DEPTH_LIMIT),
			Extracted::Bail
		));
		assert_eq!(project_record_bytes(&broken, &p(&["a"])), None);
		assert_eq!(project_record_bytes_excluding(&broken, &p(&["a"])), None);
	}
}

/// A stored-value record whose object keys are out of order bails, so the
/// caller decodes it in full and surfaces the error, rather than reading a
/// field it stores as absent or projecting a corrupt record.
#[test]
fn a_stored_value_record_with_keys_out_of_order_bails() {
	let data = Value::Object(Object::from(std::collections::BTreeMap::from([
		(Strand::from("aa"), Value::from(1)),
		(Strand::from("bb"), Value::from(2)),
	])));
	let mut bytes = encode_record(DOCUMENT, None, &data, None).expect("encodes");
	let at = |v: &[u8], needle: &[u8]| {
		v.windows(needle.len()).position(|w| w == needle).expect("the key is stored")
	};
	let (a, b) = (at(&bytes, b"aa"), at(&bytes, b"bb"));
	bytes[a..a + 2].copy_from_slice(b"bb");
	bytes[b..b + 2].copy_from_slice(b"aa");
	for field in ["aa", "cc"] {
		assert!(
			matches!(
				extract_field_from_record_bytes(&bytes, &p(&[field]), TEST_DEPTH_LIMIT),
				Extracted::Bail
			),
			"{field}"
		);
	}
	assert_eq!(project_record_bytes(&bytes, &p(&["bb"])), None);
	assert_eq!(project_record_bytes(&bytes, &p(&["cc"])), None);
	// A projection that misses nothing, or names no key, reads no fewer keys
	// out of order.
	assert_eq!(project_record_bytes(&bytes, &p(&["aa"])), None);
	assert_eq!(project_record_bytes(&bytes, &[]), None);
	assert_eq!(project_record_bytes_excluding(&bytes, &p(&["cc"])), None);
}

#[test]
fn rev2_record_data_bytes_matches_walker_path() {
	// The slice-direct rev-2 fast path must return byte-for-byte what the
	// `Record::walk_revisioned(..).into_data_bytes()` walker chain yields.
	for bytes in equivalence_sample_records() {
		let fast = rev2_record_data_bytes(&bytes)
			.expect("serialiser emits rev-2, so the fast path must recognise it");
		let mut reader: &[u8] = &bytes;
		let slow = Record::walk_revisioned(&mut reader)
			.and_then(|w| w.into_data_bytes())
			.expect("walker chain opens a valid record");
		assert_eq!(fast, &*slow, "fast-path data bytes diverge from the walker chain");
		// The unified helper returns the same bytes as both.
		let via_helper = record_data_bytes(&bytes).expect("helper opens a valid record");
		assert_eq!(&*via_helper, fast);
	}
}

#[test]
fn record_data_bytes_falls_back_on_non_rev2_prefix() {
	// A non-rev-2 first byte makes the fast path decline (returns None);
	// the helper must fall through to the walker chain rather than
	// mis-slicing. We can't cheaply forge a valid rev-1 record here, so we
	// assert the two observable properties: the fast path declines, and the
	// helper does not panic (it returns the walker chain's result — here an
	// error, since the forged bytes aren't a valid rev-1 record).
	let mut bytes = equivalence_sample_records().remove(0);
	bytes[0] = 1; // pretend rev = 1
	assert!(rev2_record_data_bytes(&bytes).is_none(), "fast path must decline non-rev-2");
	let _ = record_data_bytes(&bytes); // must not panic
}

/// A projection decodes exactly the listed fields a full decode would yield,
/// for every record encoding the sample set covers: a small object (legacy map
/// body), a wide indexed object (sparse and dense key sets), nested values,
/// records carrying metadata, and keys the record does not store.
#[test]
fn project_record_bytes_matches_a_full_decode_restricted_to_the_keys() {
	let key_sets: [&[&str]; 6] = [
		&[],
		&["a"],
		&["a", "b"],
		&["k03"],
		&["k00", "k05", "k07", "k11", "zz"],
		&["addr", "missing"],
	];
	for bytes in all_sample_records() {
		let data = data_as_written(&bytes);
		for keys in key_sets {
			let mut segments: Vec<PathSegment> =
				keys.iter().copied().map(PathSegment::from).collect();
			segments.sort();
			let projected = project_record_bytes(&bytes, &segments);
			if let Value::Object(object) = &data {
				let mut expected = Object::default();
				for key in keys {
					if let Some(v) = object.get(key) {
						expected.0.insert(Strand::from(*key), v.clone());
					}
				}
				assert_eq!(projected, Some(expected), "keys {keys:?} of {data:?}");
			} else {
				assert_eq!(projected, None, "non-object data {data:?} must not project");
			}
		}
	}
}

/// Bytes that are not a record project to `None`, so the caller decodes them
/// in full and surfaces the error, whether or not any key is listed.
#[test]
fn project_record_bytes_rejects_bytes_it_cannot_walk() {
	let garbage = [0xff_u8; 8];
	assert_eq!(project_record_bytes(&garbage, &[]), None);
	assert_eq!(project_record_bytes(&garbage, &p(&["a"])), None);
}

/// Excluding fields decodes everything else a full decode would yield, for
/// every record encoding the sample set covers, including exclusions the
/// record does not store and an exclusion of every field.
#[test]
fn project_record_bytes_excluding_matches_a_full_decode_without_the_keys() {
	let key_sets: [&[&str]; 6] = [
		&[],
		&["a"],
		&["a", "b"],
		&["k03"],
		&["k00", "k05", "k07", "k11", "zz"],
		&["addr", "missing"],
	];
	for bytes in all_sample_records() {
		let data = data_as_written(&bytes);
		for keys in key_sets {
			let mut segments: Vec<PathSegment> =
				keys.iter().copied().map(PathSegment::from).collect();
			segments.sort();
			let projected = project_record_bytes_excluding(&bytes, &segments);
			if let Value::Object(object) = &data {
				let mut expected = object.clone();
				for key in keys {
					expected.0.remove(*key);
				}
				assert_eq!(projected, Some(expected), "excluding {keys:?} of {data:?}");
			} else {
				assert_eq!(projected, None, "non-object data {data:?} must not project");
			}
		}
	}
}

/// `project_packed_record_bytes` decodes the requested top-level columns of a packed
/// `SCHEMAFULL` row (including fixed scalars, points, nested object nodes, record keys,
/// literal codes, stored value columns, root remainder fields, and absent optional
/// columns) to match `Record::decode_admitted` and declines non-packed records,
/// mismatched tables or missing layouts.
#[test]
fn project_packed_record_bytes_matches_full_decode_for_packed_columns() {
	use std::sync::Arc;

	use crate::catalog::layout::{
		Column, ColumnRole, GenerationLayout, LayoutDescriptor, Location, PackedDescriptor,
		PhysicalKind, Presence, RowLayout,
	};
	use crate::catalog::packed::{Packing, pack};
	use crate::catalog::record::RowFlags;
	use crate::catalog::{
		DatabaseId, Generation, GenerationRing, NamespaceId, Record, TableLayouts,
	};
	use crate::val::{Geometry, TableName};

	let table = TableName::new("t");
	let descriptor = PackedDescriptor {
		columns: vec![
			Column {
				path: Vec::new(),
				kind: PhysicalKind::Value,
				presence: Presence::Optional {
					bit: 0,
				},
				location: Location::Var(0),
				role: ColumnRole::Remainder,
				literals: Vec::new(),
				tables: Vec::new(),
			},
			Column {
				path: vec!["addr".to_owned()],
				kind: PhysicalKind::Object,
				presence: Presence::Required,
				location: Location::None,
				role: ColumnRole::Field,
				literals: Vec::new(),
				tables: Vec::new(),
			},
			Column {
				path: vec!["addr".to_owned(), "city".to_owned()],
				kind: PhysicalKind::String,
				presence: Presence::Required,
				location: Location::Var(1),
				role: ColumnRole::Field,
				literals: Vec::new(),
				tables: Vec::new(),
			},
			Column {
				path: vec!["age".to_owned()],
				kind: PhysicalKind::I64,
				presence: Presence::Optional {
					bit: 1,
				},
				location: Location::Fixed(1),
				role: ColumnRole::Field,
				literals: Vec::new(),
				tables: Vec::new(),
			},
			Column {
				path: vec!["author".to_owned()],
				kind: PhysicalKind::RecordKey,
				presence: Presence::Required,
				location: Location::Var(2),
				role: ColumnRole::Field,
				literals: Vec::new(),
				tables: vec![TableName::new("user")],
			},
			Column {
				path: vec!["name".to_owned()],
				kind: PhysicalKind::String,
				presence: Presence::Required,
				location: Location::Var(3),
				role: ColumnRole::Field,
				literals: Vec::new(),
				tables: Vec::new(),
			},
			Column {
				path: vec!["poly".to_owned()],
				kind: PhysicalKind::Value,
				presence: Presence::Required,
				location: Location::Var(4),
				role: ColumnRole::Field,
				literals: Vec::new(),
				tables: Vec::new(),
			},
			Column {
				path: vec!["pt".to_owned()],
				kind: PhysicalKind::Point,
				presence: Presence::Required,
				location: Location::Fixed(9),
				role: ColumnRole::Field,
				literals: Vec::new(),
				tables: Vec::new(),
			},
			Column {
				path: vec!["score".to_owned()],
				kind: PhysicalKind::I16,
				presence: Presence::Required,
				location: Location::Fixed(25),
				role: ColumnRole::Field,
				literals: Vec::new(),
				tables: Vec::new(),
			},
			Column {
				path: vec!["status".to_owned()],
				kind: PhysicalKind::LiteralCode(1),
				presence: Presence::Required,
				location: Location::Fixed(27),
				role: ColumnRole::Field,
				literals: vec![
					Value::String(Strand::from("active")),
					Value::String(Strand::from("pending")),
				],
				tables: Vec::new(),
			},
		],
		presence_bits: 2,
		bool_bits: 0,
		fixed_size: 28,
		var_count: 5,
	};
	descriptor.validate().expect("valid descriptor");

	let addr = Object::from(BTreeMap::from([(
		Strand::from("city"),
		Value::String(Strand::from("Paris")),
	)]));
	let data = Object::from(BTreeMap::from([
		(Strand::from("addr"), Value::Object(addr)),
		(
			Strand::from("author"),
			Value::RecordId(RecordId::new(TableName::from("user"), Strand::from("alice"))),
		),
		(Strand::from("extra"), Value::Number(Number::Int(42))),
		(Strand::from("name"), Value::String(Strand::from("Ada"))),
		(
			Strand::from("poly"),
			Value::Array(crate::val::Array::from(vec![
				Value::Number(Number::Int(1)),
				Value::Number(Number::Int(2)),
			])),
		),
		(Strand::from("pt"), Value::Geometry(Geometry::Point((2.35, 48.85).into()))),
		(Strand::from("score"), Value::Number(Number::I16(7))),
		(Strand::from("status"), Value::String(Strand::from("pending"))),
	]));
	let generation = Generation::FIRST;
	let Packing::Packed(bytes) =
		pack(&descriptor, generation, RowFlags::NONE, &data).expect("packs")
	else {
		panic!("expected packed row");
	};
	let ring = GenerationRing::new(generation, generation, Some(generation)).expect("valid ring");
	let layouts = TableLayouts::new(
		NamespaceId(1),
		DatabaseId(1),
		table.clone(),
		ring,
		vec![(
			generation,
			GenerationLayout::Readable(Arc::new(LayoutDescriptor {
				table: table.clone(),
				layout: RowLayout::Packed(descriptor),
				migration: None,
				migrated: None,
				verified: None,
			})),
		)],
		0,
	)
	.expect("valid layouts");

	let full = Record::decode_admitted(&bytes, Some(&layouts), RecordId::new(table.clone(), 1))
		.expect("full decode");
	let Value::Object(full_obj) = full.data else {
		panic!("expected object from full decode");
	};

	for keys in [
		&[][..],
		&["name"][..],
		&["addr", "age", "missing", "pt", "score"][..],
		&["author", "extra", "poly", "status"][..],
		&["addr", "age", "author", "extra", "missing", "name", "poly", "pt", "score", "status"][..],
	] {
		let segments = p(keys);
		let projected = project_packed_record_bytes(&bytes, Some(&layouts), &table, &segments)
			.expect("projects packed row");
		let mut expected = Object::default();
		for key in keys {
			if let Some(v) = full_obj.get(key) {
				expected.0.insert(Strand::from(*key), v.clone());
			}
		}
		assert_eq!(projected, expected, "keys {keys:?}");
	}

	assert_eq!(project_packed_record_bytes(&bytes, None, &table, &p(&["name"])), None);
	assert_eq!(
		project_packed_record_bytes(
			&bytes,
			Some(&layouts),
			&TableName::new("other"),
			&p(&["name"])
		),
		None
	);
	let doc_bytes = wire_record_plain_object(data);
	assert_eq!(
		project_packed_record_bytes(&doc_bytes, Some(&layouts), &table, &p(&["name"])),
		None
	);
}
