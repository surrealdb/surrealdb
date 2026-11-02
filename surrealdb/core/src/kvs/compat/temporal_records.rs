//! Frozen bytes for records that hold a `date`, `time`, `timestamp` or `interval`.
//!
//! A record holding one is written in the stored-value record envelope, where the value has the
//! tags `DATE` (`0x32`), `TIME` (`0x33`), `TIMESTAMP` (`0x34`) and `INTERVAL` (`0x35`), or in
//! `revision`, where `Value` is at revision 3. Both are permanent: the bytes below decode to the
//! records they were captured from, and the current encoder writes each record as exactly these
//! bytes. The revision cases also pin that a record holding none of the four stays at the revision
//! `Value` has always written it in, so a 3.x release reads every record that holds none of them.
//!
//! Never edit the frozen bytes and never regenerate them to make a failing test pass.

use revision::{DeserializeRevisioned, SerializeRevisioned};
use surrealdb_strand::Strand;

use super::fixtures::test_record_rid;
use crate::catalog::{
	Metadata, Record, RecordContext, RecordFormat, RecordType, encode_record, encode_whole_record,
};
use crate::expr::Kind;
use crate::key::KVValue;
use crate::val::{Date, Interval, Object, RecordId, Time, Timestamp, Value};

/// The format the frozen envelope bytes were written in: documents of generation 0.
const DOCUMENT: RecordFormat = RecordFormat::Document {
	generation: None,
};

fn object(entries: Vec<(&str, Value)>) -> Value {
	let mut object = Object::default();
	for (key, value) in entries {
		object.0.insert(Strand::new(key), value);
	}
	Value::Object(object)
}

fn temporal_object() -> Value {
	object(vec![
		("date", Value::Date(Date(-719_162))),
		("interval", Value::Interval(Interval::new(-1, 30, -4_000_001))),
		("time", Value::Time(Time(Time::NANOS_PER_DAY))),
		(
			"timestamp",
			Value::Timestamp(Timestamp {
				secs: 1_700_000_000,
				nanos: 123_456_789,
			}),
		),
	])
}

/// The records the frozen envelope bytes were captured from, by name.
fn cases() -> Vec<(&'static str, Record)> {
	vec![
		("date", Record::new(Value::Date(Date(20_000)))),
		("date_before_the_epoch", Record::new(Value::Date(Date(-1)))),
		("time", Record::new(Value::Time(Time(45_000_000_123)))),
		("time_end_of_day", Record::new(Value::Time(Time(Time::NANOS_PER_DAY)))),
		(
			"timestamp",
			Record::new(Value::Timestamp(Timestamp {
				secs: 1_700_000_000,
				nanos: 123_456_789,
			})),
		),
		(
			"timestamp_far_future",
			Record::new(Value::Timestamp(Timestamp {
				secs: 8_000_000_000_000,
				nanos: 5,
			})),
		),
		("interval", Record::new(Value::Interval(Interval::new(14, -3, 14_706_500_000)))),
		("object", Record::new(temporal_object())),
	]
}

/// The records written through `revision`, by name.
fn revision_cases() -> Vec<(&'static str, Record)> {
	let mut table = Record::new(temporal_object());
	table.metadata = Some(Metadata {
		record_type: RecordType::Table,
		aggregation_stats: vec![],
	});
	let mut plain = Record::new(object(vec![("name", Value::String(Strand::new("plain")))]));
	plain.metadata = table.metadata.clone();
	vec![("table_with_temporal_values", table), ("table_without_temporal_values", plain)]
}

/// FROZEN. Envelope record bytes, by the name of the case in [`cases`].
#[rustfmt::skip]
const STORED_RECORDS: &[(&str, &[u8])] = &[
	("date", &[255, 0, 0, 50, 32, 78]),
	("date_before_the_epoch", &[255, 0, 0, 50, 255]),
	("time", &[255, 0, 0, 51, 123, 130, 53, 122, 10]),
	("time_end_of_day", &[255, 0, 0, 51, 0, 0, 79, 145, 148, 78]),
	("timestamp", &[255, 0, 0, 52, 21, 205, 133, 61, 254, 156, 151, 23]),
	("timestamp_far_future", &[255, 0, 0, 52, 0, 128, 40, 165, 70, 7, 0, 0, 5, 0, 0, 0]),
	("interval", &[
		255, 0, 0, 53, 14, 0, 0, 0, 253, 255, 255, 255, 160, 97, 147, 108,
		3, 0, 0, 0,
	]),
	("object", &[
		255, 0, 0, 26, 4, 0, 9, 35, 47, 4, 100, 97, 116, 101, 50, 198,
		6, 245, 8, 105, 110, 116, 101, 114, 118, 97, 108, 53, 255, 255, 255, 255,
		30, 0, 0, 0, 255, 246, 194, 255, 255, 255, 255, 255, 4, 116, 105, 109,
		101, 51, 0, 0, 79, 145, 148, 78, 9, 116, 105, 109, 101, 115, 116, 97,
		109, 112, 52, 21, 205, 133, 61, 254, 156, 151, 23,
	]),
];

/// FROZEN. The `revision` bytes of the records in [`revision_cases`], by name.
#[rustfmt::skip]
const REVISION_RECORDS: &[(&str, &[u8])] = &[
	("table_with_temporal_values", &[
		2, 114, 0, 0, 0, 8, 0, 0, 0, 13, 0, 0, 0, 1, 1, 2,
		0, 0, 2, 74, 95, 0, 0, 0, 2, 90, 0, 0, 0, 0, 4, 4,
		100, 97, 116, 101, 3, 83, 6, 0, 0, 0, 1, 252, 115, 242, 21, 0,
		4, 116, 105, 109, 101, 3, 84, 10, 0, 0, 0, 1, 253, 0, 0, 79,
		145, 148, 78, 0, 0, 8, 105, 110, 116, 101, 114, 118, 97, 108, 3, 86,
		8, 0, 0, 0, 1, 1, 60, 252, 1, 18, 122, 0, 9, 116, 105, 109,
		101, 115, 116, 97, 109, 112, 3, 85, 11, 0, 0, 0, 1, 252, 0, 226,
		167, 202, 252, 21, 205, 91, 7,
	]),
	("table_without_temporal_values", &[
		2, 43, 0, 0, 0, 8, 0, 0, 0, 13, 0, 0, 0, 1, 1, 2,
		0, 0, 2, 74, 24, 0, 0, 0, 2, 19, 0, 0, 0, 0, 1, 4,
		110, 97, 109, 101, 2, 68, 6, 0, 0, 0, 5, 112, 108, 97, 105, 110,
	]),
];

/// FROZEN. The `revision` bytes of the four kinds: the revision (2) and the variant number.
#[rustfmt::skip]
const KINDS: &[(&str, Kind, &[u8])] = &[
	("date", Kind::Date, &[2, 34]),
	("time", Kind::Time, &[2, 35]),
	("timestamp", Kind::Timestamp, &[2, 36]),
	("interval", Kind::Interval, &[2, 37]),
];

#[test]
fn frozen_kinds_decode_and_are_written_as_they_were() {
	for (name, kind, bytes) in KINDS {
		let decoded = Kind::deserialize_revisioned(&mut &bytes[..])
			.unwrap_or_else(|e| panic!("BACKWARDS COMPATIBILITY BROKEN: kind {name}: {e}"));
		assert_eq!(&decoded, kind, "{name}");
		let mut written = Vec::new();
		kind.serialize_revisioned(&mut written).expect("encodes");
		assert_eq!(&written, bytes, "kind {name} is written differently");
	}
}

fn frozen<'a>(table: &'a [(&str, &'static [u8])], name: &str) -> &'a [u8] {
	table
		.iter()
		.find(|(n, _)| *n == name)
		.map(|(_, bytes)| *bytes)
		.unwrap_or_else(|| panic!("no frozen bytes for {name}"))
}

#[test]
fn every_case_has_frozen_bytes() {
	let names: Vec<&str> = cases().iter().map(|(name, _)| *name).collect();
	let frozen: Vec<&str> = STORED_RECORDS.iter().map(|(name, _)| *name).collect();
	assert_eq!(names, frozen);
	let names: Vec<&str> = revision_cases().iter().map(|(name, _)| *name).collect();
	let frozen: Vec<&str> = REVISION_RECORDS.iter().map(|(name, _)| *name).collect();
	assert_eq!(names, frozen);
}

#[test]
fn frozen_records_decode() {
	let rid: RecordId = test_record_rid();
	for (name, record) in cases() {
		let bytes = frozen(STORED_RECORDS, name);
		let decoded = Record::decode_as_written(bytes, None, || rid.clone())
			.unwrap_or_else(|e| panic!("BACKWARDS COMPATIBILITY BROKEN: {name} decodes: {e}"));
		assert_eq!(decoded, record, "{name} decodes to another record");
	}
	for (name, record) in revision_cases() {
		let bytes = frozen(REVISION_RECORDS, name);
		let decoded = Record::decode_as_written(bytes, None, || rid.clone())
			.unwrap_or_else(|e| panic!("BACKWARDS COMPATIBILITY BROKEN: {name} decodes: {e}"));
		assert_eq!(decoded, record, "{name} decodes to another record");
		let through = Record::kv_decode_value(bytes, RecordContext::without_layouts(rid.clone()))
			.expect("decodes");
		assert_eq!(through.metadata, record.metadata, "{name}");
	}
}

#[test]
fn a_record_written_now_is_its_frozen_bytes() {
	for (name, record) in cases() {
		let encoded =
			encode_record(DOCUMENT, record.metadata.as_ref(), &record.data, None).expect("encodes");
		assert_eq!(encoded, frozen(STORED_RECORDS, name), "{name} is written differently");
		assert_eq!(
			encode_whole_record(DOCUMENT, &record, None).expect("encodes"),
			frozen(STORED_RECORDS, name),
			"{name}"
		);
	}
	for (name, record) in revision_cases() {
		assert_eq!(
			record.kv_encode_value().expect("encodes"),
			frozen(REVISION_RECORDS, name),
			"{name} is written differently"
		);
	}
}

/// Regenerate [`STORED_RECORDS`] and [`REVISION_RECORDS`]. Ignored so it never runs in CI:
///
/// ```text
/// cargo test -p surrealdb-core --lib kvs::compat::temporal_records::generate -- --ignored --nocapture
/// ```
#[test]
#[ignore = "prints the frozen fixtures; run by hand when a case is added"]
fn generate() {
	fn rows(name: &str, cases: Vec<(&'static str, Vec<u8>)>) -> String {
		let mut out = format!("const {name}: &[(&str, &[u8])] = &[\n");
		for (case, bytes) in cases {
			let rows: Vec<String> = bytes
				.chunks(16)
				.map(|row| row.iter().map(|b| b.to_string()).collect::<Vec<_>>().join(", "))
				.collect();
			if rows.len() == 1 {
				out.push_str(&format!("\t(\"{case}\", &[{}]),\n", rows[0]));
			} else {
				out.push_str(&format!("\t(\"{case}\", &[\n"));
				for row in rows {
					out.push_str(&format!("\t\t{row},\n"));
				}
				out.push_str("\t]),\n");
			}
		}
		out.push_str("];");
		out
	}
	let stored = cases()
		.into_iter()
		.map(|(name, r)| {
			(name, encode_record(DOCUMENT, r.metadata.as_ref(), &r.data, None).expect("encodes"))
		})
		.collect();
	let revision = revision_cases()
		.into_iter()
		.map(|(name, r)| (name, r.kv_encode_value().expect("encodes")))
		.collect();
	println!("{}", rows("STORED_RECORDS", stored));
	println!("{}", rows("REVISION_RECORDS", revision));
}
