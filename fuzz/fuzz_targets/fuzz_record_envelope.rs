//! Fuzzing of the record envelope around a stored value.
//!
//! A record is written in one of two encodings: the v1 envelope (`0xFF`, a
//! header, a generation, a stored value) for no metadata or an edge on the
//! current adjacency layout, and `revision` for any other metadata. An input's
//! first byte chooses how the rest is used. Even, the rest is a record's stored
//! bytes, read through `StoredRecord` and `Record::decode_admitted_as_written`, in either
//! encoding: neither may panic, a record read in the v1 envelope reads the same
//! in place, and a record read in either encoding is written again, as an
//! update writes it, to bytes that read back exactly as it. Odd, the rest
//! drives a generator of records, their metadata, their data and the key they
//! are written under, and each record written must read back exactly as it was
//! written, in the encoding its metadata selects.

#![no_main]

use libfuzzer_sys::arbitrary::Unstructured;
use libfuzzer_sys::fuzz_target;
use surrealdb_catalog::aggregation::AggregationStat;
use surrealdb_catalog::{
	Metadata, Record, RecordFormat, RecordType, StoredRecord, V1_EDGE_VARIANT, encode_record,
};
use surrealdb_expr::val::stored::ValueRef;
use surrealdb_expr::val::{
	Array, Number, RecordId, RecordIdKey, RecordIdKeyRange, Strand, TableName, Value,
	check_value_depth,
};
use surrealdb_types::{Decimal, f16};

mod stored_value;

/// The format the target writes records in: documents of generation 0, which a reader holding
/// no table layouts reads.
const DOCUMENT: RecordFormat = RecordFormat::Document {
	generation: None,
};

use stored_value::{Gen, hex, identical, read_loosely, walk};

fuzz_target!(|data: &[u8]| {
	let Some((&mode, rest)) = data.split_first() else {
		return;
	};
	match mode & 1 {
		0 => stored_bytes(rest),
		_ => generated(&mut Unstructured::new(rest)),
	}
});

/// Whether a record with this metadata is written in the v1 envelope: no
/// metadata, or an edge on the current adjacency layout with no statistics.
/// Any other metadata is written in the `revision` encoding.
fn written_v1(metadata: &Option<Metadata>) -> bool {
	match metadata {
		None => true,
		Some(Metadata {
			record_type,
			aggregation_stats,
		}) => {
			aggregation_stats.is_empty()
				&& matches!(
					record_type,
					RecordType::Edge {
						variant: V1_EDGE_VARIANT
					}
				)
		}
	}
}

/// Arbitrary bytes as the stored bytes of the record `t:1`.
fn stored_bytes(bytes: &[u8]) {
	let rid = RecordId {
		table: TableName::new("t"),
		key: RecordIdKey::Number(1),
	};
	let as_written = Record::decode_admitted_as_written(bytes, None, || rid.clone());
	let stored = match StoredRecord::parse(bytes) {
		Ok(Some(stored)) => stored,
		// A `revision`-encoded record, which an update writes in whichever
		// encoding its metadata selects.
		Ok(None) => {
			if let Ok(record) = &as_written {
				rewritten(record, &rid);
			}
			return;
		}
		Err(_) => {
			assert!(as_written.is_err(), "a record the parser refuses decodes as written");
			return;
		}
	};
	let _ = (stored.generation(), stored.is_edge(), stored.edge_variant(), stored.is_keyed());
	if stored.is_packed() {
		// A packed row is read only through its generation's descriptor, which this target holds
		// none of: its body is no stored value, and a read without layouts refuses it.
		assert!(stored.document().is_none(), "a packed row has a document body");
		assert!(!stored.is_keyed(), "a packed row is not a keyed document");
		assert!(stored.id_left_out(), "a packed row leaves its id out");
		assert!(as_written.is_err(), "a packed record decodes without its table's layouts");
		return;
	}
	let metadata = stored.metadata();
	assert_eq!(stored.is_edge(), metadata.is_some(), "a row is an edge iff its metadata says so");
	assert_eq!(stored.edge_variant(), metadata.as_ref().map(|_| V1_EDGE_VARIANT));
	assert!(
		written_v1(&metadata),
		"a v1 row's metadata is one the v1 envelope holds: {metadata:?}"
	);
	let decoded = stored.decode_document();
	if let Ok(r) = ValueRef::new(stored.document().expect("a document")) {
		match &decoded {
			Ok(record) => walk(r, &record.data, false),
			Err(_) => read_loosely(r, 0),
		}
	}
	let Ok(record) = decoded else {
		assert!(as_written.is_err(), "a record that does not decode decodes as written");
		return;
	};
	assert!(same_metadata(&metadata, &record.metadata), "{metadata:?}\n{:?}", record.metadata);
	let as_written = as_written.expect("a record that decodes decodes as written");
	let mut expected = record;
	if stored.is_keyed()
		&& let Value::Object(obj) = &mut expected.data
	{
		obj.0.insert(Strand::new("id"), Value::RecordId(rid.clone()));
	}
	assert!(
		same_record(&as_written, &expected),
		"as written {as_written:?}\nexpected {expected:?}"
	);
	rewritten(&as_written, &rid);
}

/// `record`, read under `rid`, written again as an update writes it: under its
/// key, its `id` left to the key when it is the key. It is written in the
/// encoding its metadata selects, reads back exactly as it was, and is written
/// the same way again; or it is refused for nesting deeper than a record may.
fn rewritten(record: &Record, rid: &RecordId) {
	let bytes = match encode_record(DOCUMENT, record.metadata.as_ref(), &record.data, Some(rid)) {
		Ok(bytes) => bytes,
		Err(e) => {
			assert!(
				check_value_depth(&record.data).is_err(),
				"{record:?} is read but not written: {e}"
			);
			return;
		}
	};
	let v1 = written_v1(&record.metadata);
	assert_eq!(
		bytes.first() == Some(&StoredRecord::MARKER),
		v1,
		"{record:?} is written as {}, in the wrong encoding",
		hex(&bytes)
	);
	let back = Record::decode_as_written(&bytes, None, || rid.clone()).unwrap_or_else(|e| {
		panic!("{record:?} is written as {}, which does not decode: {e}", hex(&bytes))
	});
	assert!(
		same_record(&back, record),
		"written {record:?}\nas      {}\nread as {back:?}",
		hex(&bytes)
	);
	let again =
		encode_record(DOCUMENT, back.metadata.as_ref(), &back.data, Some(rid)).expect("re-encodes");
	assert_eq!(again, bytes, "{record:?} is written two ways");
	// Only a v1 row's data is a stored value to read in place.
	if v1 {
		let stored = StoredRecord::parse(&bytes).expect("parses").expect("v1 bytes");
		assert_eq!(stored.stamp(), StoredRecord::NO_GENERATION);
		let data = stored.decode_document().expect("decodes").data;
		walk(
			ValueRef::new(stored.document().expect("a document")).expect("data has a tag"),
			&data,
			true,
		);
	}
}

/// A generated record, written under a generated key or none.
fn generated(u: &mut Unstructured<'_>) {
	let mut g = Gen::new(u);
	let rid = RecordId {
		table: TableName::new(g.string()),
		key: g.key(1),
	};
	let metadata = metadata(&mut g);
	let data = data(&mut g, &rid);
	let key = match g.below(3) {
		0 => None,
		_ => Some(&rid),
	};
	let bytes = match encode_record(DOCUMENT, metadata.as_ref(), &data, key) {
		Ok(bytes) => bytes,
		Err(e) => {
			assert!(check_value_depth(&data).is_err(), "{data:?} is not written: {e}");
			return;
		}
	};
	let v1 = written_v1(&metadata);
	assert_eq!(
		bytes.first() == Some(&StoredRecord::MARKER),
		v1,
		"written {metadata:?} as {}, in the wrong encoding",
		hex(&bytes)
	);
	let mut asked = false;
	let as_written = Record::decode_as_written(&bytes, None, || {
		asked = true;
		rid.clone()
	})
	.expect("a written record decodes as written");
	assert!(
		same_metadata(&as_written.metadata, &metadata),
		"written {metadata:?}\nread as {:?}",
		as_written.metadata
	);
	assert!(
		identical(&as_written.data, &data),
		"written {data:?}\nunder   {key:?}\nas      {}\nread as {:?}",
		hex(&bytes),
		as_written.data
	);
	let again = encode_record(DOCUMENT, as_written.metadata.as_ref(), &as_written.data, key)
		.expect("re-encodes");
	assert_eq!(again, bytes, "a record read back is written two ways");
	if !v1 {
		// A `revision` row keeps its whole record, `id` included.
		assert!(StoredRecord::parse(&bytes).expect("parses").is_none());
		assert!(!asked, "a revision row never asks for the key");
		return;
	}
	let stored = StoredRecord::parse(&bytes).expect("parses").expect("v1 bytes");
	assert_eq!(stored.stamp(), StoredRecord::NO_GENERATION);
	assert_eq!(stored.is_edge(), metadata.is_some());
	assert_eq!(stored.edge_variant(), metadata.as_ref().map(|_| V1_EDGE_VARIANT));
	let read = stored.metadata();
	assert!(
		same_metadata(&read, &metadata),
		"written {metadata:?}\nas      {}\nread as {read:?}",
		hex(&bytes)
	);
	let record = stored.decode_document().expect("a written record decodes");
	assert!(
		same_metadata(&record.metadata, &metadata),
		"written {metadata:?}\nstored as {:?}",
		record.metadata
	);
	assert_eq!(asked, stored.is_keyed(), "the key is asked for exactly when it was left out");
	// The data as stored lacks the `id` the key stands for.
	let mut kept = data.clone();
	if stored.is_keyed() {
		match &mut kept {
			Value::Object(obj) => {
				obj.0.remove("id");
			}
			_ => panic!("a keyed record's data is not an object"),
		}
	}
	assert!(identical(&record.data, &kept), "stored {:?}\nkept   {kept:?}", record.data);
	walk(
		ValueRef::new(stored.document().expect("a document")).expect("data has a tag"),
		&record.data,
		true,
	);
}

/// Metadata skewed toward what the v1 envelope holds: none about half the time,
/// an edge on the current layout with no statistics a quarter, and a few of the
/// shapes written in `revision` that sit next to it (an edge on layout 1, table
/// metadata with no statistics). The rest is any metadata at all.
fn metadata(g: &mut Gen<'_, '_>) -> Option<Metadata> {
	let bare = |record_type| {
		Some(Metadata {
			record_type,
			aggregation_stats: Vec::new(),
		})
	};
	match g.below(16) {
		0..=7 => None,
		8..=11 => bare(RecordType::Edge {
			variant: V1_EDGE_VARIANT,
		}),
		12 => bare(RecordType::Edge {
			variant: 1,
		}),
		13 => bare(RecordType::Table),
		_ => Some(Metadata {
			record_type: match g.below(2) {
				0 => RecordType::Table,
				_ => RecordType::Edge {
					variant: match g.below(3) {
						0 => V1_EDGE_VARIANT,
						_ => g.bits() as u16,
					},
				},
			},
			aggregation_stats: (0..g.below(4)).map(|_| stat(g)).collect(),
		}),
	}
}

fn stat(g: &mut Gen<'_, '_>) -> AggregationStat {
	let arg = match g.below(4) {
		0 => i64::MAX as usize,
		_ => g.below(64) as usize,
	};
	match g.below(11) {
		0 => AggregationStat::Count {
			count: g.int(),
		},
		1 => AggregationStat::CountValue {
			arg,
			count: g.int(),
		},
		2 => AggregationStat::NumberMax {
			arg,
			max: number(g),
		},
		3 => AggregationStat::NumberMin {
			arg,
			min: number(g),
		},
		4 => AggregationStat::Sum {
			arg,
			sum: number(g),
		},
		5 => AggregationStat::Mean {
			arg,
			sum: number(g),
			count: g.int(),
		},
		6 => AggregationStat::StdDev {
			arg,
			shift: number(g),
			sum: number(g),
			sum_of_squares: number(g),
			count: g.int(),
		},
		7 => AggregationStat::Variance {
			arg,
			shift: number(g),
			sum: number(g),
			sum_of_squares: number(g),
			count: g.int(),
		},
		8 => AggregationStat::TimeMax {
			arg,
			max: g.datetime(),
		},
		9 => AggregationStat::TimeMin {
			arg,
			min: g.datetime(),
		},
		_ => AggregationStat::Accumulate {
			arg,
			values: (0..g.below(4)).map(|_| g.value(1)).collect(),
		},
	}
}

fn number(g: &mut Gen<'_, '_>) -> Number {
	match g.below(12) {
		0 => Number::Int(g.int()),
		1 => Number::I8(g.bits() as i8),
		2 => Number::U8(g.bits() as u8),
		3 => Number::I16(g.bits() as i16),
		4 => Number::U16(g.bits() as u16),
		5 => Number::I32(g.bits() as i32),
		6 => Number::U32(g.bits() as u32),
		7 => Number::U64(g.bits()),
		8 => Number::F16(f16::from_bits(g.bits() as u16)),
		9 => Number::F32(f32::from_bits(g.bits() as u32)),
		10 => Number::Float(g.float()),
		_ => Number::Decimal(g.decimal()),
	}
}

/// A record's data: usually an object whose `id` is the key, a record id `==`
/// holds equal to the key without being it, any other value, or absent;
/// sometimes not an object at all.
fn data(g: &mut Gen<'_, '_>, rid: &RecordId) -> Value {
	match g.below(4) {
		3 => g.any(),
		_ => {
			let mut obj = g.object(0);
			let id = match g.below(4) {
				0 => None,
				1 => Some(Value::RecordId(rid.clone())),
				2 => Some(Value::RecordId(RecordId {
					table: rid.table.clone(),
					key: twin_key(&rid.key),
				})),
				_ => Some(g.value(1)),
			};
			match id {
				Some(id) => {
					obj.0.insert(Strand::new("id"), id);
				}
				None => {
					obj.0.remove("id");
				}
			}
			Value::Object(obj)
		}
	}
}

/// `key` with each number in it replaced by one `==` holds equal to it: an
/// integer by its float, a float by its integer, a zero by the other zero, and
/// a decimal by itself at a finer scale.
fn twin_key(key: &RecordIdKey) -> RecordIdKey {
	match key {
		RecordIdKey::Array(a) => RecordIdKey::Array(Array(a.0.iter().map(twin).collect())),
		RecordIdKey::Object(o) => {
			RecordIdKey::Object(o.0.iter().map(|(k, v)| (k.clone(), twin(v))).collect())
		}
		RecordIdKey::Range(r) => RecordIdKey::Range(Box::new(RecordIdKeyRange {
			start: r.start.as_ref().map(twin_key),
			end: r.end.as_ref().map(twin_key),
		})),
		key => key.clone(),
	}
}

fn twin(v: &Value) -> Value {
	match v {
		Value::Number(Number::Int(i)) if (*i as f64) as i64 == *i => {
			Value::Number(Number::Float(*i as f64))
		}
		Value::Number(Number::Float(f)) if *f == 0.0 => Value::Number(Number::Float(-f)),
		Value::Number(Number::Float(f)) if f.fract() == 0.0 && f.abs() < 1e15 => {
			Value::Number(Number::Int(*f as i64))
		}
		Value::Number(Number::Decimal(d)) => Value::Number(Number::Decimal(
			Decimal::try_from_i128_with_scale(d.mantissa() * 10, d.scale() + 1).unwrap_or(*d),
		)),
		Value::Array(a) => Value::Array(Array(a.0.iter().map(twin).collect())),
		Value::Object(o) => Value::Object(o.0.iter().map(|(k, v)| (k.clone(), twin(v))).collect()),
		v => v.clone(),
	}
}

fn same_record(a: &Record, b: &Record) -> bool {
	same_metadata(&a.metadata, &b.metadata) && identical(&a.data, &b.data)
}

fn same_metadata(a: &Option<Metadata>, b: &Option<Metadata>) -> bool {
	match (a, b) {
		(None, None) => true,
		(Some(a), Some(b)) => {
			a.record_type == b.record_type
				&& a.aggregation_stats.len() == b.aggregation_stats.len()
				&& a.aggregation_stats
					.iter()
					.zip(&b.aggregation_stats)
					.all(|(a, b)| same_stat(a, b))
		}
		_ => false,
	}
}

/// Stats compare field by field, their numbers and values strictly.
fn same_stat(a: &AggregationStat, b: &AggregationStat) -> bool {
	use AggregationStat as S;
	let n = |a: &Number, b: &Number| identical(&Value::Number(*a), &Value::Number(*b));
	match (a, b) {
		(
			S::Count {
				count: a,
			},
			S::Count {
				count: b,
			},
		) => a == b,
		(
			S::CountValue {
				arg: a,
				count: x,
			},
			S::CountValue {
				arg: b,
				count: y,
			},
		) => a == b && x == y,
		(
			S::NumberMax {
				arg: a,
				max: x,
			},
			S::NumberMax {
				arg: b,
				max: y,
			},
		)
		| (
			S::NumberMin {
				arg: a,
				min: x,
			},
			S::NumberMin {
				arg: b,
				min: y,
			},
		)
		| (
			S::Sum {
				arg: a,
				sum: x,
			},
			S::Sum {
				arg: b,
				sum: y,
			},
		) => a == b && n(x, y),
		(
			S::Mean {
				arg: a,
				sum: x,
				count: c,
			},
			S::Mean {
				arg: b,
				sum: y,
				count: d,
			},
		) => a == b && n(x, y) && c == d,
		(
			S::StdDev {
				arg: a,
				shift: s,
				sum: x,
				sum_of_squares: q,
				count: c,
			},
			S::StdDev {
				arg: b,
				shift: t,
				sum: y,
				sum_of_squares: r,
				count: d,
			},
		)
		| (
			S::Variance {
				arg: a,
				shift: s,
				sum: x,
				sum_of_squares: q,
				count: c,
			},
			S::Variance {
				arg: b,
				shift: t,
				sum: y,
				sum_of_squares: r,
				count: d,
			},
		) => a == b && n(s, t) && n(x, y) && n(q, r) && c == d,
		(
			S::TimeMax {
				arg: a,
				max: x,
			},
			S::TimeMax {
				arg: b,
				max: y,
			},
		)
		| (
			S::TimeMin {
				arg: a,
				min: x,
			},
			S::TimeMin {
				arg: b,
				min: y,
			},
		) => a == b && x.0 == y.0,
		(
			S::Accumulate {
				arg: a,
				values: x,
			},
			S::Accumulate {
				arg: b,
				values: y,
			},
		) => a == b && x.len() == y.len() && x.iter().zip(y).all(|(x, y)| identical(x, y)),
		_ => false,
	}
}
