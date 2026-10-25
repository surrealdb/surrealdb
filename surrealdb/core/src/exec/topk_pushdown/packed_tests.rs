//! The top-k probe over packed rows, held to the same probe over the same records stored as
//! documents.

use std::sync::Arc;

use super::{TopKThresholdCell, TopKThresholdProbe};
use crate::catalog::StoredRecord;
use crate::exec::object_extract::PathSegment;
use crate::exec::operators::SortDirection;
use crate::exec::operators::scan::pipeline::decode_record_value;
use crate::exec::operators::test_util::TestDb;
use crate::exec::packed_view::AdmittedRow;
use crate::exec::vm::tests_packed_reads::{dataset, layouts_of, rows, unverified_layouts_of};
use crate::val::{Datetime, Duration, Number, Uuid, Value};

const DEPTH_LIMIT: u32 = 256;

/// The first ORDER BY keys the probes read: a column of each kind, the nested ones, an open
/// object's key, an absent key and keys that step through a value that is no object.
const KEYS: &[&[&str]] = &[
	&["i"],
	&["f"],
	&["dec"],
	&["b"],
	&["s"],
	&["t"],
	&["du"],
	&["u"],
	&["by"],
	&["o"],
	&["nl"],
	&["tags"],
	&["lit"],
	&["obj"],
	&["pt"],
	&["added"],
	&["absent"],
	&["obj", "x"],
	&["obj", "y"],
	&["flex", "k"],
	&["flex", "a", "b"],
	&["i", "x"],
	&["nl", "x"],
	&["absent", "x"],
];

fn probe(
	direction: SortDirection,
	single_key: bool,
	packed_rows: bool,
	path: &[&str],
) -> Arc<TopKThresholdProbe> {
	TopKThresholdProbe::armed(
		path.iter().map(|segment| PathSegment::new(*segment)).collect(),
		direction,
		single_key,
		DEPTH_LIMIT,
		packed_rows,
		Arc::new(TopKThresholdCell::default()),
	)
}

/// Thresholds below, equal to and above `key`, and of other kinds.
fn thresholds(key: &Value) -> Vec<Value> {
	let int = |i: i64| Value::Number(Number::Int(i));
	vec![
		key.clone(),
		Value::None,
		Value::Null,
		int(i64::MIN),
		int(0),
		int(5),
		int(i64::MAX),
		Value::Number(Number::Float(1.5)),
		Value::String("".into()),
		Value::String("one".into()),
		Value::String("zzzz".into()),
		Value::Bool(false),
		Value::Bool(true),
		Value::Datetime(Datetime(chrono::DateTime::from_timestamp(0, 0).expect("time"))),
		Value::Duration(Duration(std::time::Duration::from_secs(1))),
		Value::Uuid(Uuid(uuid::Uuid::from_u128(1))),
		Value::from(vec![int(1)]),
	]
}

/// The value the whole decode `decoded` holds at `path`, or `None` when it steps through a value
/// that is no object.
fn at(decoded: &Value, path: &[&str]) -> Option<Value> {
	let mut value = decoded.clone();
	for segment in path {
		let Value::Object(object) = value else {
			return None;
		};
		value = object.get(segment).cloned().unwrap_or(Value::None);
	}
	Some(value)
}

fn as_document(decoded: &Value) -> Vec<u8> {
	let Value::Object(object) = decoded else {
		panic!("a record is an object: {decoded:?}");
	};
	let mut object = object.clone();
	object.remove("id");
	let format = crate::catalog::RecordFormat::Document {
		generation: None,
	};
	crate::catalog::encode_record(format, None, &Value::Object(object), None).expect("encodes")
}

/// A packed row is rejected as the same record stored as a document is, in either direction and
/// whether the sort has one key or several, where the scan states that it may be, and never
/// otherwise.
#[tokio::test]
async fn a_packed_row_is_rejected_as_the_same_record_stored_as_a_document() {
	let db = TestDb::new(&dataset()).await;
	let rows = rows(&db, "p").await;
	let layouts = layouts_of(&db, "p", &rows).await;
	let (mut packed, mut rejected, mut kept) = (0usize, 0usize, 0usize);
	for (key, value) in &rows {
		if !StoredRecord::is_packed_bytes(value) {
			continue;
		}
		packed += 1;
		let decoded = decode_record_value(key, value, Some(&layouts)).expect("a row decodes");
		let document = as_document(&decoded);
		let admitted = AdmittedRow::of(key, value, Some(&layouts));
		for path in KEYS {
			let candidate = at(&decoded, path).unwrap_or(Value::None);
			for threshold in thresholds(&candidate) {
				for direction in [SortDirection::Asc, SortDirection::Desc] {
					for single_key in [true, false] {
						let allowed = probe(direction, single_key, true, path);
						let withheld = probe(direction, single_key, false, path);
						let want = allowed.rejects(&threshold, &AdmittedRow::Bytes(&document));
						let got = allowed.rejects(&threshold, &admitted);
						assert_eq!(
							got, want,
							"{path:?} {direction:?} single={single_key} against {threshold:?} over {decoded:?}"
						);
						assert!(!withheld.rejects(&threshold, &admitted), "{path:?}: not stated");
						if got {
							rejected += 1;
						} else {
							kept += 1;
						}
					}
				}
			}
		}
	}
	assert!(packed >= 8, "{packed} packed rows");
	assert!(rejected > 500, "{rejected} rejected");
	assert!(kept > 500, "{kept} kept");
}

/// A packed row whose check refuses it is never rejected, nor one that is not checked, and one
/// the check accepts after a corruption is judged as its whole decode holds it.
#[tokio::test]
async fn a_refused_or_unchecked_packed_row_is_not_rejected() {
	let db = TestDb::new(&dataset()).await;
	let rows = rows(&db, "p").await;
	let layouts = unverified_layouts_of(&db, "p", &rows).await;
	let probes: Vec<_> = KEYS
		.iter()
		.flat_map(|path| {
			[SortDirection::Asc, SortDirection::Desc]
				.map(|direction| (path, probe(direction, true, true, path)))
		})
		.collect();
	let (mut refused, mut judged) = (0usize, 0usize);
	for (key, value) in &rows {
		if !StoredRecord::is_packed_bytes(value) {
			continue;
		}
		// Unchecked packed bytes are never rejected.
		for (_, probe) in &probes {
			assert!(!probe.rejects(&Value::None, &AdmittedRow::Bytes(value)));
		}
		let mut check = |bytes: &[u8]| {
			let admitted = AdmittedRow::of(key, bytes, Some(&layouts));
			let decoded = decode_record_value(key, bytes, Some(&layouts));
			for (path, probe) in &probes {
				for threshold in [Value::None, Value::Number(Number::Int(0)), Value::Null] {
					let rejects = probe.rejects(&threshold, &admitted);
					match (&admitted, &decoded) {
						(AdmittedRow::Refused, Err(_)) => {
							refused += 1;
							assert!(!rejects, "{bytes:02x?}");
						}
						(
							AdmittedRow::Packed {
								..
							},
							Ok(decoded),
						) => {
							judged += 1;
							// A row the check accepts is judged as the record its whole decode
							// holds, stored as a document.
							let document = as_document(decoded);
							assert_eq!(
								rejects,
								probe.rejects(&threshold, &AdmittedRow::Bytes(&document)),
								"{path:?} against {threshold:?} over {decoded:?}"
							);
						}
						(AdmittedRow::Bytes(_), Err(_)) => assert!(!rejects, "{bytes:02x?}"),
						_ => panic!("{admitted:?} where the whole decode is {decoded:?}"),
					}
				}
			}
		};
		for at in 3..value.len() {
			for bit in 0..8 {
				let mut mutant = value.clone();
				mutant[at] ^= 1 << bit;
				check(&mutant);
			}
		}
		for length in 0..value.len() {
			check(&value[..length]);
		}
	}
	assert!(refused > 1_000, "{refused} refused");
	assert!(judged > 1_000, "{judged} judged");
}
