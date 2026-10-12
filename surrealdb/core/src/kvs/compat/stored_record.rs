//! Frozen bytes for [`Record`].
//!
//! A record is written either in the stored-value record envelope
//! (`catalog::StoredRecord`) or, when its metadata is more than the envelope
//! holds, in `revision`. The envelope does not change between releases: no
//! revision marks a record's layout, so a release that wrote bytes the next
//! one reads differently would corrupt every record silently. These bytes pin
//! it from both sides. Each decodes to the record it was captured from, so a
//! reader change that misreads what an earlier release of this encoding wrote
//! fails here; and the current encoder writes each byte for byte, so a writer
//! change that an earlier reader of it would misread fails here too.
//!
//! The `revision` bytes every release before the stored-value encoding wrote
//! are the `vX_Y_Z.rs` snapshots next door, which still decode.
//!
//! The envelope cases are every value kind a record holds, written without a
//! key in the generation-0 document format, then the shapes only a write
//! with a key produces: a record whose `id` the key replaces, a record whose
//! `id` is not its key, and the current edge layout, plain and keyed. Each
//! begins with the marker, a header, and the generation `0` of a record
//! written before its table had one.
//!
//! [`revision_cases`] are the records the envelope cannot hold: table
//! metadata, an edge on an older variant, and a view row carrying aggregation
//! state. They are written through `revision`, and [`REVISION_RECORDS`] freezes
//! the bytes this build writes for them, in the same two directions as the
//! envelope cases. The `vX_Y_Z.rs` snapshots pin only that the bytes earlier
//! releases wrote still decode; none holds a view row or table metadata, and
//! `compat/tests.rs` does not pin the bytes a `Record` re-encodes to. So a
//! later bump of the `Record`, `Metadata` or `AggregationStat` revision that
//! changes what these records are written as fails here, before a build that
//! cannot be rolled back to the one before it ships.

use surrealdb_strand::Strand;

use super::fixtures::{self, test_record_rid};
use crate::catalog::aggregation::AggregationStat;
use crate::catalog::{
	Metadata, Record, RecordContext, RecordFormat, RecordType, StoredRecord, V1_EDGE_VARIANT,
	encode_record, encode_whole_record,
};
use crate::key::KVValue;
use crate::val::{Datetime, Number, Object, RecordId, RecordIdKey, TableName, Value};

/// The format the frozen envelope bytes were written in: documents of generation 0.
const DOCUMENT: RecordFormat = RecordFormat::Document {
	generation: None,
};

/// A view row holding one stat of every kind, as a materialised view writes
/// its rows: an object without an `id`, with table metadata.
fn view_row() -> Record {
	let mut data = Object::default();
	data.0.insert(Strand::new("total"), Value::from(10i64));
	Record {
		metadata: Some(Metadata {
			record_type: RecordType::Table,
			aggregation_stats: vec![
				AggregationStat::Count {
					count: 3,
				},
				AggregationStat::CountValue {
					arg: 1,
					count: -4,
				},
				AggregationStat::NumberMax {
					arg: 2,
					max: f64::NEG_INFINITY.into(),
				},
				AggregationStat::NumberMin {
					arg: 3,
					min: Number::Decimal("1.50".parse().unwrap()),
				},
				AggregationStat::Sum {
					arg: 4,
					sum: 7.into(),
				},
				AggregationStat::Mean {
					arg: 5,
					sum: 2.5.into(),
					count: 2,
				},
				AggregationStat::StdDev {
					arg: 6,
					shift: 1.0.into(),
					sum: 3.into(),
					sum_of_squares: 9.0.into(),
					count: 3,
				},
				AggregationStat::Variance {
					arg: 7,
					shift: 0.0.into(),
					sum: 0.0.into(),
					sum_of_squares: 0.0.into(),
					count: 0,
				},
				AggregationStat::TimeMax {
					arg: 8,
					max: Datetime::MIN_UTC,
				},
				AggregationStat::TimeMin {
					arg: 9,
					min: Datetime::MAX_UTC,
				},
				AggregationStat::Accumulate {
					arg: 10,
					values: vec![Value::from(1i64), Value::String(Strand::new("x")), Value::None],
				},
			],
		}),
		data: Value::Object(data),
	}
}

/// A record id other than [`test_record_rid`], the key a record is written
/// under while its data holds that one.
fn other_rid() -> RecordId {
	RecordId {
		table: TableName::from("compat_test"),
		key: RecordIdKey::Number(7),
	}
}

/// An edge on the current adjacency layout, the one the envelope's EDGE bit
/// names.
fn current_edge() -> Record {
	let mut record = fixtures::record_with_metadata();
	record.set_record_type(RecordType::Edge {
		variant: V1_EDGE_VARIANT,
	});
	record
}

/// The records the frozen bytes were captured from, by name, each with the
/// key it is written under, if any.
fn cases() -> Vec<(&'static str, Option<RecordId>, Record)> {
	let keyed = Some(test_record_rid());
	vec![
		("none", None, fixtures::record_none()),
		("null", None, fixtures::record_null()),
		("bool", None, fixtures::record_bool()),
		("number_int", None, fixtures::record_number_int()),
		("number_float", None, fixtures::record_number_float()),
		("number_decimal", None, fixtures::record_number_decimal()),
		("string", None, fixtures::record_string()),
		("bytes", None, fixtures::record_bytes()),
		("duration", None, fixtures::record_duration()),
		("datetime", None, fixtures::record_datetime()),
		("uuid", None, fixtures::record_uuid()),
		("geometry_point", None, fixtures::record_geometry_point()),
		("geometry_line", None, fixtures::record_geometry_line()),
		("geometry_polygon", None, fixtures::record_geometry_polygon()),
		("geometry_multi_point", None, fixtures::record_geometry_multi_point()),
		("geometry_multi_line", None, fixtures::record_geometry_multi_line()),
		("geometry_multi_polygon", None, fixtures::record_geometry_multi_polygon()),
		("geometry_collection", None, fixtures::record_geometry_collection()),
		("table", None, fixtures::record_table()),
		("recordid", None, fixtures::record_recordid()),
		("file", None, fixtures::record_file()),
		("range_unbounded", None, fixtures::record_range_unbounded()),
		("range_bounded", None, fixtures::record_range_bounded()),
		("regex", None, fixtures::record_regex()),
		("array", None, fixtures::record_array()),
		("object", None, fixtures::record_object()),
		("set", None, fixtures::record_set()),
		("object_keyed", keyed.clone(), fixtures::record_object()),
		("object_under_another_key", Some(other_rid()), fixtures::record_object()),
		("edge_current", None, current_edge()),
		("edge_current_keyed", keyed, current_edge()),
	]
}

/// The records written through `revision` because their metadata is more than
/// the envelope holds, by name, each with the key it is written under, if any.
fn revision_cases() -> Vec<(&'static str, Option<RecordId>, Record)> {
	let keyed = Some(test_record_rid());
	vec![
		("edge_variant_1", None, fixtures::record_with_metadata()),
		("table_metadata", None, fixtures::record_with_table_metadata()),
		("edge_variant_1_keyed", keyed.clone(), fixtures::record_with_metadata()),
		("table_metadata_keyed", keyed, fixtures::record_with_table_metadata()),
		("view_row", None, view_row()),
	]
}

/// FROZEN. Envelope record bytes, by the name of the case in [`cases`] they
/// were captured from. Never edit these by hand and never regenerate them to
/// make a failing test pass: a failure here means the current code no longer
/// reads, or no longer writes, what an earlier release of this encoding wrote.
#[rustfmt::skip]
const STORED_RECORDS: &[(&str, &[u8])] = &[
	("none", &[255, 0, 0, 0]),
	("null", &[255, 0, 0, 1]),
	("bool", &[255, 0, 0, 3]),
	("number_int", &[255, 0, 0, 170]),
	("number_float", &[255, 0, 0, 5, 0, 0, 40, 66]),
	("number_decimal", &[255, 0, 0, 6, 0, 42]),
	("string", &[255, 0, 0, 7, 116, 101, 115, 116, 32, 100, 97, 116, 97]),
	("bytes", &[255, 0, 0, 11, 1, 2, 3, 4]),
	("duration", &[255, 0, 0, 8, 0, 160, 184, 48, 70, 3, 0, 0]),
	("datetime", &[255, 0, 0, 9, 0, 238, 13, 115, 107, 248, 255, 255, 0, 0, 0, 0]),
	("uuid", &[
		255, 0, 0, 10, 18, 62, 69, 103, 232, 155, 18, 211, 164, 86, 66, 102,
		20, 23, 64, 0,
	]),
	("geometry_point", &[
		255, 0, 0, 17, 0, 0, 0, 0, 0, 0, 0, 240, 63, 0, 0, 0,
		0, 0, 0, 0, 64,
	]),
	("geometry_line", &[
		255, 0, 0, 17, 1, 0, 0, 0, 0, 0, 0, 240, 63, 0, 0, 0,
		0, 0, 0, 0, 64, 0, 0, 0, 0, 0, 0, 8, 64, 0, 0, 0,
		0, 0, 0, 16, 64,
	]),
	("geometry_polygon", &[
		255, 0, 0, 17, 2, 2, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0,
		0, 0, 0, 240, 63, 0, 0, 0, 0, 0, 0, 0, 64, 0, 0, 0,
		0, 0, 0, 8, 64, 0, 0, 0, 0, 0, 0, 16, 64, 0, 0, 0,
		0, 0, 0, 20, 64, 0, 0, 0, 0, 0, 0, 24, 64, 0, 0, 0,
		0, 0, 0, 240, 63, 0, 0, 0, 0, 0, 0, 0, 64, 4, 0, 0,
		0, 0, 0, 0, 0, 0, 0, 28, 64, 0, 0, 0, 0, 0, 0, 32,
		64, 0, 0, 0, 0, 0, 0, 34, 64, 0, 0, 0, 0, 0, 0, 36,
		64, 0, 0, 0, 0, 0, 0, 38, 64, 0, 0, 0, 0, 0, 0, 40,
		64, 0, 0, 0, 0, 0, 0, 28, 64, 0, 0, 0, 0, 0, 0, 32,
		64,
	]),
	("geometry_multi_point", &[
		255, 0, 0, 17, 3, 0, 0, 0, 0, 0, 0, 240, 63, 0, 0, 0,
		0, 0, 0, 0, 64, 0, 0, 0, 0, 0, 0, 8, 64, 0, 0, 0,
		0, 0, 0, 16, 64,
	]),
	("geometry_multi_line", &[
		255, 0, 0, 17, 4, 2, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0,
		0, 0, 0, 240, 63, 0, 0, 0, 0, 0, 0, 0, 64, 0, 0, 0,
		0, 0, 0, 8, 64, 0, 0, 0, 0, 0, 0, 16, 64, 2, 0, 0,
		0, 0, 0, 0, 0, 0, 0, 20, 64, 0, 0, 0, 0, 0, 0, 24,
		64, 0, 0, 0, 0, 0, 0, 28, 64, 0, 0, 0, 0, 0, 0, 32,
		64,
	]),
	("geometry_multi_polygon", &[
		255, 0, 0, 17, 5, 2, 0, 0, 0, 1, 0, 0, 0, 4, 0, 0,
		0, 0, 0, 0, 0, 0, 0, 240, 63, 0, 0, 0, 0, 0, 0, 0,
		64, 0, 0, 0, 0, 0, 0, 8, 64, 0, 0, 0, 0, 0, 0, 16,
		64, 0, 0, 0, 0, 0, 0, 20, 64, 0, 0, 0, 0, 0, 0, 24,
		64, 0, 0, 0, 0, 0, 0, 240, 63, 0, 0, 0, 0, 0, 0, 0,
		64, 1, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 28,
		64, 0, 0, 0, 0, 0, 0, 32, 64, 0, 0, 0, 0, 0, 0, 34,
		64, 0, 0, 0, 0, 0, 0, 36, 64, 0, 0, 0, 0, 0, 0, 38,
		64, 0, 0, 0, 0, 0, 0, 40, 64, 0, 0, 0, 0, 0, 0, 28,
		64, 0, 0, 0, 0, 0, 0, 32, 64,
	]),
	("geometry_collection", &[
		255, 0, 0, 17, 6, 3, 0, 0, 0, 17, 0, 0, 0, 0, 0, 0,
		0, 240, 63, 0, 0, 0, 0, 0, 0, 0, 64, 33, 1, 0, 0, 0,
		0, 0, 0, 8, 64, 0, 0, 0, 0, 0, 0, 16, 64, 0, 0, 0,
		0, 0, 0, 20, 64, 0, 0, 0, 0, 0, 0, 24, 64, 73, 2, 1,
		0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 28, 64, 0,
		0, 0, 0, 0, 0, 32, 64, 0, 0, 0, 0, 0, 0, 34, 64, 0,
		0, 0, 0, 0, 0, 36, 64, 0, 0, 0, 0, 0, 0, 38, 64, 0,
		0, 0, 0, 0, 0, 40, 64, 0, 0, 0, 0, 0, 0, 28, 64, 0,
		0, 0, 0, 0, 0, 32, 64,
	]),
	("table", &[255, 0, 0, 12, 117, 115, 101, 114, 115]),
	("recordid", &[255, 0, 0, 13, 5, 117, 115, 101, 114, 115, 251]),
	("file", &[255, 0, 0, 14, 6, 98, 117, 99, 107, 101, 116, 47, 107, 101, 121]),
	("range_unbounded", &[255, 0, 0, 16, 0, 0]),
	("range_bounded", &[255, 0, 0, 16, 1, 1, 251, 2, 4, 200, 1]),
	("regex", &[255, 0, 0, 15, 94, 116, 101, 115, 116, 46, 42]),
	("array", &[255, 0, 0, 20, 3, 0, 6, 7, 7, 105, 116, 101, 109, 49, 251, 3]),
	("object", &[
		255, 0, 0, 26, 4, 0, 8, 13, 37, 6, 97, 99, 116, 105, 118, 101,
		3, 3, 97, 103, 101, 158, 2, 105, 100, 13, 11, 99, 111, 109, 112, 97,
		116, 95, 116, 101, 115, 116, 7, 102, 105, 120, 116, 117, 114, 101, 4, 110,
		97, 109, 101, 7, 65, 108, 105, 99, 101,
	]),
	("set", &[
		255, 0, 0, 23, 3, 0, 5, 10, 7, 116, 97, 103, 49, 7, 116, 97,
		103, 50, 7, 116, 97, 103, 51,
	]),
	("object_keyed", &[
		255, 64, 0, 26, 3, 0, 8, 13, 6, 97, 99, 116, 105, 118, 101, 3,
		3, 97, 103, 101, 158, 4, 110, 97, 109, 101, 7, 65, 108, 105, 99, 101,
	]),
	("object_under_another_key", &[
		255, 0, 0, 26, 4, 0, 8, 13, 37, 6, 97, 99, 116, 105, 118, 101,
		3, 3, 97, 103, 101, 158, 2, 105, 100, 13, 11, 99, 111, 109, 112, 97,
		116, 95, 116, 101, 115, 116, 7, 102, 105, 120, 116, 117, 114, 101, 4, 110,
		97, 109, 101, 7, 65, 108, 105, 99, 101,
	]),
	("edge_current", &[
		255, 8, 0, 26, 1, 0, 2, 105, 100, 13, 11, 99, 111, 109, 112, 97,
		116, 95, 116, 101, 115, 116, 7, 102, 105, 120, 116, 117, 114, 101,
	]),
	("edge_current_keyed", &[255, 72, 0, 26, 0]),
];

/// FROZEN. The `revision` bytes of the records in [`revision_cases`], by name.
/// Never edit these by hand and never regenerate them to make a failing test
/// pass: a failure here means the current code no longer reads, or no longer
/// writes, what an earlier build wrote for a record the envelope cannot hold.
#[rustfmt::skip]
const REVISION_RECORDS: &[(&str, &[u8])] = &[
	("edge_variant_1", &[
		2, 59, 0, 0, 0, 8, 0, 0, 0, 14, 0, 0, 0, 1, 1, 2,
		1, 1, 0, 2, 74, 39, 0, 0, 0, 2, 34, 0, 0, 0, 0, 1,
		2, 105, 100, 2, 78, 23, 0, 0, 0, 1, 11, 99, 111, 109, 112, 97,
		116, 95, 116, 101, 115, 116, 1, 1, 7, 102, 105, 120, 116, 117, 114, 101,
	]),
	("table_metadata", &[
		2, 81, 0, 0, 0, 8, 0, 0, 0, 13, 0, 0, 0, 1, 1, 2,
		0, 0, 2, 74, 62, 0, 0, 0, 2, 57, 0, 0, 0, 0, 2, 2,
		105, 100, 2, 78, 23, 0, 0, 0, 1, 11, 99, 111, 109, 112, 97, 116,
		95, 116, 101, 115, 116, 1, 1, 7, 102, 105, 120, 116, 117, 114, 101, 4,
		110, 97, 109, 101, 2, 68, 12, 0, 0, 0, 11, 84, 101, 115, 116, 32,
		82, 101, 99, 111, 114, 100,
	]),
	("edge_variant_1_keyed", &[
		2, 59, 0, 0, 0, 8, 0, 0, 0, 14, 0, 0, 0, 1, 1, 2,
		1, 1, 0, 2, 74, 39, 0, 0, 0, 2, 34, 0, 0, 0, 0, 1,
		2, 105, 100, 2, 78, 23, 0, 0, 0, 1, 11, 99, 111, 109, 112, 97,
		116, 95, 116, 101, 115, 116, 1, 1, 7, 102, 105, 120, 116, 117, 114, 101,
	]),
	("table_metadata_keyed", &[
		2, 81, 0, 0, 0, 8, 0, 0, 0, 13, 0, 0, 0, 1, 1, 2,
		0, 0, 2, 74, 62, 0, 0, 0, 2, 57, 0, 0, 0, 0, 2, 2,
		105, 100, 2, 78, 23, 0, 0, 0, 1, 11, 99, 111, 109, 112, 97, 116,
		95, 116, 101, 115, 116, 1, 1, 7, 102, 105, 120, 116, 117, 114, 101, 4,
		110, 97, 109, 101, 2, 68, 12, 0, 0, 0, 11, 84, 101, 115, 116, 32,
		82, 101, 99, 111, 114, 100,
	]),
	("view_row", &[
		2, 218, 0, 0, 0, 8, 0, 0, 0, 190, 0, 0, 0, 1, 1, 2,
		0, 11, 2, 0, 6, 2, 1, 1, 7, 2, 2, 2, 1, 1, 0, 0,
		0, 0, 0, 0, 240, 255, 2, 3, 3, 1, 2, 0, 0, 2, 0, 150,
		0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 4, 4, 1, 0,
		14, 2, 5, 5, 1, 1, 0, 0, 0, 0, 0, 0, 4, 64, 4, 2,
		6, 6, 1, 1, 0, 0, 0, 0, 0, 0, 240, 63, 1, 0, 6, 1,
		1, 0, 0, 0, 0, 0, 0, 34, 64, 6, 2, 7, 7, 1, 1, 0,
		0, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0,
		0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 8, 8, 1,
		253, 255, 35, 228, 25, 41, 15, 0, 0, 0, 2, 9, 9, 1, 253, 254,
		214, 20, 52, 239, 14, 0, 0, 252, 255, 201, 154, 59, 2, 10, 10, 3,
		2, 67, 3, 0, 0, 0, 1, 0, 2, 2, 68, 2, 0, 0, 0, 1,
		120, 2, 0, 2, 74, 22, 0, 0, 0, 2, 17, 0, 0, 0, 0, 1,
		5, 116, 111, 116, 97, 108, 2, 67, 3, 0, 0, 0, 1, 0, 20,
	]),
];

/// The frozen `revision` bytes of the case named `name`.
fn frozen_revision(name: &str) -> &'static [u8] {
	REVISION_RECORDS
		.iter()
		.find(|(n, _)| *n == name)
		.map(|(_, bytes)| *bytes)
		.unwrap_or_else(|| panic!("no frozen revision bytes for {name}"))
}

/// The frozen bytes of the case named `name`.
fn frozen(name: &str) -> &'static [u8] {
	STORED_RECORDS
		.iter()
		.find(|(n, _)| *n == name)
		.map(|(_, bytes)| *bytes)
		.unwrap_or_else(|| panic!("no frozen bytes for {name}"))
}

/// The frozen tables and [`cases`] and [`revision_cases`] name the same records
/// in the same order, so a case is never added without its bytes nor its bytes
/// dropped.
#[test]
fn every_case_has_frozen_bytes() {
	let names: Vec<&str> = cases().iter().map(|(name, _, _)| *name).collect();
	let frozen: Vec<&str> = STORED_RECORDS.iter().map(|(name, _)| *name).collect();
	assert_eq!(names, frozen);
	let names: Vec<&str> = revision_cases().iter().map(|(name, _, _)| *name).collect();
	let frozen: Vec<&str> = REVISION_RECORDS.iter().map(|(name, _)| *name).collect();
	assert_eq!(names, frozen);
}

/// Each record's frozen bytes decode to the record they were written from:
/// as written, with the `id` a keyed write left out restored from the key, and
/// through the record decoder, which sets an object's `id` to the key.
#[test]
fn frozen_stored_records_decode() {
	for (name, key, record) in cases() {
		let bytes = frozen(name);
		assert_eq!(bytes.first(), Some(&StoredRecord::MARKER), "{name} is not stored-value bytes");
		let rid = key.unwrap_or_else(test_record_rid);
		let as_written =
			Record::decode_as_written(bytes, None, || rid.clone()).unwrap_or_else(|e| {
				panic!(
					"BACKWARDS COMPATIBILITY BROKEN: the stored-value record {name} no longer \
				 decodes.\nError: {e}\n\nEvery record written since the stored-value \
				 encoding was introduced is in it."
				)
			});
		assert_eq!(as_written, record, "{name} decodes to another record");
		let mut expected = record;
		if let Value::Object(object) = &mut expected.data {
			object.0.insert(Strand::new("id"), Value::RecordId(rid.clone()));
		}
		let decoded =
			Record::kv_decode_value(bytes, RecordContext::without_layouts(rid)).expect("decodes");
		assert_eq!(
			decoded, expected,
			"{name} decodes through the record decoder to another record"
		);
	}
}

/// Each revision record's frozen bytes decode to the record they were written
/// from, as written and through the record decoder, which sets an object's `id`
/// to the key.
#[test]
fn frozen_revision_records_decode() {
	for (name, key, record) in revision_cases() {
		let bytes = frozen_revision(name);
		assert_ne!(bytes.first(), Some(&StoredRecord::MARKER), "{name} is not revision bytes");
		let rid = key.unwrap_or_else(test_record_rid);
		let as_written =
			Record::decode_as_written(bytes, None, || rid.clone()).unwrap_or_else(|e| {
				panic!(
					"BACKWARDS COMPATIBILITY BROKEN: the revision record {name} no longer \
				 decodes.\nError: {e}"
				)
			});
		assert_eq!(as_written, record, "{name} decodes to another record");
		let mut expected = record;
		if let Value::Object(object) = &mut expected.data {
			object.0.insert(Strand::new("id"), Value::RecordId(rid.clone()));
		}
		let decoded =
			Record::kv_decode_value(bytes, RecordContext::without_layouts(rid)).expect("decodes");
		assert_eq!(
			decoded, expected,
			"{name} decodes through the record decoder to another record"
		);
	}
}

/// The current encoder writes each record as its frozen bytes, so every
/// reader of the envelope reads what it writes, and writes each record the
/// envelope cannot hold as its frozen `revision` bytes.
#[test]
fn a_record_written_now_is_its_frozen_bytes() {
	for (name, key, record) in cases() {
		let encoded = encode_record(DOCUMENT, record.metadata.as_ref(), &record.data, key.as_ref())
			.expect("encodes");
		assert_eq!(encoded, frozen(name), "{name} is written differently");
		if key.is_none() {
			assert_eq!(
				encode_whole_record(DOCUMENT, &record, None).expect("encodes"),
				frozen(name),
				"{name}"
			);
		}
	}
	for (name, key, record) in revision_cases() {
		let encoded = encode_record(DOCUMENT, record.metadata.as_ref(), &record.data, key.as_ref())
			.expect("encodes");
		assert_eq!(encoded, frozen_revision(name), "{name} is written differently");
		if key.is_none() {
			assert_eq!(record.kv_encode_value().expect("encodes"), frozen_revision(name), "{name}");
		}
	}
}

/// A write with a key leaves out the `id` only when it is the key, and marks
/// the record so: the cases written under their own key are keyed, the one
/// written under another is not, and neither are those written without one.
#[test]
fn only_a_record_whose_id_is_its_key_is_keyed() {
	for (name, key, record) in cases() {
		let stored = StoredRecord::parse(frozen(name)).expect("parses").expect("stored-value");
		let own = matches!(
			(&record.data, &key),
			(Value::Object(o), Some(k)) if o.get("id") == Some(&Value::RecordId(k.clone()))
		);
		assert_eq!(stored.is_keyed(), own, "{name}");
		assert_eq!(stored.is_edge(), record.is_edge(), "{name}");
		assert_eq!(stored.edge_variant(), record.edge_variant(), "{name}");
		assert_eq!(stored.metadata(), record.metadata, "{name}");
	}
}

/// Regenerate [`STORED_RECORDS`] and [`REVISION_RECORDS`]. Ignored so it never
/// runs in CI:
///
/// ```text
/// cargo test -p surrealdb-core --lib kvs::compat::stored_record::generate -- --ignored --nocapture
/// ```
#[test]
#[ignore = "prints the frozen fixtures; run by hand when a case is added"]
fn generate() {
	fn table(name: &str, cases: Vec<(&'static str, Option<RecordId>, Record)>) -> String {
		let mut out = format!("const {name}: &[(&str, &[u8])] = &[\n");
		for (case, key, record) in cases {
			let bytes =
				encode_record(DOCUMENT, record.metadata.as_ref(), &record.data, key.as_ref())
					.expect("encodes");
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
	println!("{}", table("STORED_RECORDS", cases()));
	println!("{}", table("REVISION_RECORDS", revision_cases()));
}
