//! Shaped blocks in requests held to CBOR: a block decodes, in the wire layer, to the array of
//! rows a plain CBOR array carries, within the bounds the request decoder enforces.

use std::collections::BTreeMap;
use std::sync::Arc;

use ciborium::Value as Cbor;
use surrealdb_types::shaped::{
	ColumnDef, Kind, PublicShaped, Shape, ShapeDef, ShapeScope, ValueShaper, block_contents,
	write_block,
};
use surrealdb_types::{
	Array, Bytes, Datetime, Duration, Number, Object, RecordId, RecordIdKey, Uuid, Value,
};

use super::cbor::{decode, decode_request};
use super::encoded::encode_dynamic;
use super::shaped_tests::canonical;

const LIMIT: usize = 64;

fn text(s: &str) -> Cbor {
	Cbor::Text(s.to_owned())
}

fn cbor_bytes(item: &Cbor) -> Vec<u8> {
	let mut bytes = Vec::new();
	ciborium::into_writer(item, &mut bytes).expect("encodes");
	bytes
}

fn obj(entries: Vec<(&str, Value)>) -> Value {
	Value::Object(Object::from(
		entries.into_iter().map(|(k, v)| (k.to_owned(), v)).collect::<BTreeMap<_, _>>(),
	))
}

fn int(n: i64) -> Value {
	Value::Number(Number::Int(n))
}

fn rid(table: &str, key: impl Into<RecordIdKey>) -> Value {
	Value::RecordId(RecordId::new(table, key))
}

/// Rows of every column kind, absent, `NULL` and `NONE` columns, a kind change and rows that are
/// not objects.
fn all_kinds() -> Vec<Value> {
	let at = Datetime::try_from((1_700_000_000i64, 123_456_789u32)).expect("a datetime");
	let early = Datetime::try_from((-62_135_596_801i64, 5u32)).expect("a datetime");
	vec![
		obj(vec![
			("id", rid("person", 1i64)),
			("name", Value::String("Tobie".into())),
			("age", int(42)),
			("ratio", Value::Number(Number::Float(0.25))),
			("nan", Value::Number(Number::Float(f64::NAN))),
			("dec", Value::Number(Number::Decimal(rust_decimal::Decimal::new(150, 2)))),
			("flag", Value::Bool(true)),
			("at", Value::Datetime(at)),
			("for", Value::Duration(Duration::new(90, 7))),
			("u", Value::Uuid(Uuid::from(uuid::Uuid::from_u128(7)))),
			("blob", Value::Bytes(Bytes::from(vec![0u8, 1, 255]))),
			("tags", Value::Array(vec![Value::String("a".into()), int(2)].into())),
			("nested", obj(vec![("x", int(1)), ("deep", obj(vec![("y", Value::Null)]))])),
			("gone", Value::None),
			("nil", Value::Null),
		]),
		obj(vec![
			("id", rid("person", "tobie")),
			("name", Value::Null),
			("age", Value::String("old".into())),
			("at", Value::Datetime(early)),
			("extra", int(-5)),
		]),
		obj(vec![
			("id", rid("other", Uuid::from(uuid::Uuid::from_u128(9)))),
			("link", rid("x", RecordIdKey::Array(Array::from(vec![int(1), int(2)])))),
		]),
		obj(vec![]),
		int(5),
		Value::String("a scalar row".into()),
		Value::None,
		Value::Array(vec![int(1)].into()),
		obj(vec![("id", rid("person", -3i64)), ("name", Value::String("é".into()))]),
	]
}

/// `rows` as the contents of one block (the byte string under tag 57).
fn block_of(rows: &[Value]) -> Vec<u8> {
	let mut shaper = ValueShaper::new();
	let mut scope = ShapeScope::new();
	let mut bytes = Vec::new();
	let mut runs: Vec<(Arc<Shape>, u32, std::ops::Range<usize>)> = Vec::new();
	for row in rows {
		let start = bytes.len();
		let shape =
			shaper.write(PublicShaped::new(row, encode_dynamic), &mut bytes).expect("writes");
		match runs.last_mut() {
			Some((s, n, range)) if Arc::ptr_eq(s, &shape) => {
				*n += 1;
				range.end = bytes.len();
			}
			_ => runs.push((shape, 1, start..bytes.len())),
		}
	}
	let mut block = Vec::new();
	write_block(
		&mut block,
		&mut scope,
		runs.iter().map(|(s, n, range)| (s, *n, &bytes[range.clone()])),
	)
	.expect("frames");
	block_contents(&block).expect("a block").to_vec()
}

/// A block of `shape`, each run being a row count and the bytes of that many rows.
fn raw_block(def: ShapeDef, runs: &[(u32, Vec<u8>)]) -> Vec<u8> {
	let shape = Arc::new(Shape::new(def).expect("a shape"));
	let mut scope = ShapeScope::new();
	let mut block = Vec::new();
	write_block(&mut block, &mut scope, runs.iter().map(|(n, rows)| (&shape, *n, rows.as_slice())))
		.expect("frames");
	block_contents(&block).expect("a block").to_vec()
}

fn tag57(contents: Vec<u8>) -> Cbor {
	Cbor::Tag(57, Box::new(Cbor::Bytes(contents)))
}

fn column(name: &str, kind: Kind, table: &str) -> ColumnDef {
	ColumnDef {
		name: name.to_owned(),
		kind,
		element: None,
		flags: 0,
		table: table.to_owned(),
		open: false,
		children: Vec::new(),
	}
}

fn request(params: Vec<Cbor>) -> Cbor {
	Cbor::Map(vec![
		(text("id"), 1.into()),
		(text("method"), text("insert")),
		(text("params"), Cbor::Array(params)),
	])
}

fn wrapped(item: Cbor, depth: usize) -> Cbor {
	(0..depth).fold(item, |inner, _| Cbor::Array(vec![inner]))
}

/// The refusal of `item` as a request.
fn refused(item: &Cbor) -> String {
	decode_request(&cbor_bytes(item), LIMIT).expect_err("is refused").to_string()
}

fn rows_as_cbor(rows: &[Value]) -> Cbor {
	let bytes = super::cbor::encode(Value::Array(rows.to_vec().into())).expect("encodes");
	ciborium::de::from_reader(bytes.as_slice()).expect("CBOR")
}

#[test]
fn a_block_decodes_as_the_array_its_rows_are() {
	let rows = all_kinds();
	let block = tag57(block_of(&rows));
	let plain = rows_as_cbor(&rows);
	for (with_block, with_array) in [
		(request(vec![text("t"), block.clone()]), request(vec![text("t"), plain.clone()])),
		(
			request(vec![text("RETURN $rows"), Cbor::Map(vec![(text("rows"), block.clone())])]),
			request(vec![text("RETURN $rows"), Cbor::Map(vec![(text("rows"), plain.clone())])]),
		),
		(
			request(vec![Cbor::Map(vec![(text("a"), Cbor::Array(vec![block.clone(), block]))])]),
			request(vec![Cbor::Map(vec![(text("a"), Cbor::Array(vec![plain.clone(), plain]))])]),
		),
	] {
		let expected = decode(&cbor_bytes(&with_array), LIMIT).expect("decodes");
		let actual = decode_request(&cbor_bytes(&with_block), LIMIT).expect("decodes");
		assert_eq!(canonical(actual), canonical(expected));
	}
}

#[test]
fn each_block_has_a_scope_of_its_own() {
	// Both blocks announce their shape as id 0.
	let a = vec![obj(vec![("n", int(1))])];
	let b = vec![obj(vec![("m", Value::String("x".into()))])];
	let item = request(vec![Cbor::Array(vec![tag57(block_of(&a)), tag57(block_of(&b))])]);
	let value = decode_request(&cbor_bytes(&item), LIMIT).expect("decodes");
	let expected = decode(
		&cbor_bytes(&request(vec![Cbor::Array(vec![rows_as_cbor(&a), rows_as_cbor(&b)])])),
		LIMIT,
	)
	.expect("decodes");
	assert_eq!(canonical(value), canonical(expected));
}

#[test]
fn decode_does_not_accept_a_block() {
	let item = request(vec![tag57(block_of(&all_kinds()))]);
	let err = decode(&cbor_bytes(&item), LIMIT).expect_err("refused").to_string();
	assert!(err.contains("tag 57"), "{err}");
}

#[test]
fn a_block_does_not_nest_in_a_block() {
	// A VALUE-root row whose slice is a block.
	let inner = tag57(block_of(&[int(1)]));
	let slice = cbor_bytes(&inner);
	let mut row = (slice.len() as u32).to_le_bytes().to_vec();
	row.extend_from_slice(&slice);
	let outer = tag57(raw_block(ShapeDef::value(), &[(1, row)]));
	let err = refused(&request(vec![outer]));
	assert!(err.contains("CBOR tag 57"), "{err}");

	// And a remainder holding one.
	let slice = cbor_bytes(&Cbor::Map(vec![(text("k"), inner)]));
	let mut row = (slice.len() as u32).to_le_bytes().to_vec();
	row.extend_from_slice(&slice);
	let outer = tag57(raw_block(ShapeDef::object(true, &[]), &[(1, row)]));
	let err = refused(&request(vec![outer]));
	assert!(err.contains("CBOR tag 57"), "{err}");
}

#[test]
fn a_block_tag_over_anything_but_bytes_is_refused() {
	for payload in [text("x"), Cbor::Array(vec![]), Cbor::Null, 1.into()] {
		let err = refused(&request(vec![Cbor::Tag(57, Box::new(payload))]));
		assert!(err.contains("not a byte string"), "{err}");
	}
}

/// A block of one row whose shape nests objects three deep, and the plain row it stands for.
fn deep_row() -> Value {
	obj(vec![("a", obj(vec![("b", obj(vec![("c", int(1))]))]))])
}

#[test]
fn a_block_nests_no_deeper_than_the_request_limit() {
	let deep = block_of(&[deep_row()]);
	// A scalar row whose slice nests `k` arrays.
	let slice_rows = |k: usize| {
		let slice = cbor_bytes(&wrapped(1.into(), k));
		let mut row = (slice.len() as u32).to_le_bytes().to_vec();
		row.extend_from_slice(&slice);
		raw_block(ShapeDef::value(), &[(1, row)])
	};
	let mut refused_some = false;
	let mut accepted_some = false;
	for depth in 0..LIMIT {
		let mut blocks = vec![deep.clone()];
		blocks.extend((0..LIMIT).step_by(7).map(slice_rows));
		for contents in blocks {
			let item = wrapped(tag57(contents), depth);
			let bytes = cbor_bytes(&item);
			match decode_request(&bytes, LIMIT) {
				Ok(value) => {
					accepted_some = true;
					// Whatever a block decodes to, a plain request carries within the limit.
					let plain = super::cbor::encode(value).expect("encodes");
					decode(&plain, LIMIT).expect("the plain form decodes within the limit");
				}
				Err(_) => refused_some = true,
			}
		}
	}
	assert!(accepted_some && refused_some);
	let small = 8;
	let err = decode_request(&cbor_bytes(&wrapped(tag57(deep), 6)), small)
		.expect_err("too deep")
		.to_string();
	assert!(err.contains("too deep"), "{err}");
}

fn nested_siblings(count: usize) -> ShapeDef {
	let columns: Vec<ColumnDef> =
		(0..count).map(|i| column(&format!("c{i:04}"), Kind::Object, "")).collect();
	ShapeDef::object(false, &columns)
}

fn refused_as_over_budget(item: &Cbor) {
	let err = refused(item);
	assert!(err.contains("more than its request's budget"), "{err}");
}

#[test]
fn a_block_cannot_decode_to_more_than_its_request_could_carry() {
	// (a) Thousands of object columns with no leaves, over many rows of no bytes.
	let rows = vec![(65_536u32, Vec::new()); 8];
	refused_as_over_budget(&request(vec![tag57(raw_block(nested_siblings(4096), &rows))]));

	// (a') One required object column with a very long name.
	let long = "n".repeat(65_535);
	let def = ShapeDef::object(false, &[column(&long, Kind::Object, "")]);
	refused_as_over_budget(&request(vec![tag57(raw_block(def, &[(65_536, Vec::new())]))]));

	// (a'') A record column with a long table, over rows that each name a short key.
	let table = "t".repeat(65_535);
	let def = ShapeDef::object(false, &[column("r", Kind::Record, &table)]);
	let mut row = 2u32.to_le_bytes().to_vec();
	row.extend_from_slice(&[1, b'k']);
	let rows: Vec<u8> = row.repeat(2_000);
	refused_as_over_budget(&request(vec![tag57(raw_block(def, &[(2_000, rows)]))]));

	// (b) No columns at all, over many full batches.
	let def = ShapeDef::object(false, &[]);
	let rows = vec![(65_536u32, Vec::new()); 40];
	refused_as_over_budget(&request(vec![tag57(raw_block(def, &rows))]));

	// (c) Many small blocks, each fine alone, share one budget.
	let one = raw_block(ShapeDef::object(false, &[]), &[(1_000, Vec::new())]);
	decode_request(&cbor_bytes(&request(vec![tag57(one.clone())])), 1024)
		.expect("a block of a thousand empty rows is within its budget");
	let many = Cbor::Array((0..4_000).map(|_| tag57(one.clone())).collect());
	let err = decode_request(&cbor_bytes(&request(vec![many])), 1024)
		.expect_err("shared budget")
		.to_string();
	assert!(err.contains("more than its request's budget"), "{err}");
}

#[test]
fn a_record_column_holds_only_its_own_table() {
	let def = ShapeDef::object(false, &[column("r", Kind::Record, "t")]);
	let record = |table: &str| {
		let mut slice = vec![3u8];
		slice.extend(cbor_bytes(&Cbor::Tag(8, Box::new(Cbor::Array(vec![text(table), 1.into()])))));
		let mut row = (slice.len() as u32).to_le_bytes().to_vec();
		row.extend_from_slice(&slice);
		row
	};
	let ok = tag57(raw_block(def.clone(), &[(1, record("t"))]));
	let value = decode_request(&cbor_bytes(&request(vec![ok])), LIMIT).expect("decodes");
	let expected = obj(vec![("r", rid("t", 1i64))]);
	assert_eq!(
		canonical(value),
		canonical(obj(vec![
			("id", int(1)),
			("method", Value::String("insert".into())),
			("params", Value::Array(vec![Value::Array(vec![expected].into())].into())),
		]))
	);
	let other = tag57(raw_block(def, &[(1, record("u"))]));
	let err = refused(&request(vec![other]));
	assert!(err.contains("another table"), "{err}");
}

#[test]
fn a_remainder_with_a_repeated_key_is_refused() {
	// {"a": 1, "a": 2}
	let slice = [0xa2, 0x61, b'a', 0x01, 0x61, b'a', 0x02];
	let mut row = (slice.len() as u32).to_le_bytes().to_vec();
	row.extend_from_slice(&slice);
	let block = tag57(raw_block(ShapeDef::object(true, &[]), &[(1, row)]));
	let err = refused(&request(vec![block]));
	assert!(err.contains("duplicate key a"), "{err}");
}

#[test]
fn a_repeated_key_is_refused_around_a_block() {
	let block = tag57(block_of(&[obj(vec![("n", int(1))])]));
	// Next to the block, in the request itself.
	let item = Cbor::Map(vec![
		(text("id"), 1.into()),
		(text("method"), text("insert")),
		(text("params"), Cbor::Array(vec![block.clone()])),
		(text("id"), 2.into()),
	]);
	assert!(refused(&item).contains("duplicate key id"));
	// In the map that holds the block.
	let item = request(vec![Cbor::Map(vec![(text("rows"), block.clone()), (text("rows"), block)])]);
	assert!(refused(&item).contains("duplicate key rows"));
}

#[test]
fn a_damaged_request_with_a_block_never_panics() {
	let item = request(vec![text("t"), tag57(block_of(&all_kinds())), Cbor::Map(vec![])]);
	let bytes = cbor_bytes(&item);
	decode_request(&bytes, LIMIT).expect("the undamaged request decodes");
	for at in 0..bytes.len() {
		for bit in 0..8 {
			let mut damaged = bytes.clone();
			damaged[at] ^= 1 << bit;
			let _ = decode_request(&damaged, LIMIT);
		}
		let _ = decode_request(&bytes[..at], LIMIT);
	}
}

/// Prints the bytes of `N` rows of a table-like object as a plain CBOR array and as one block,
/// and what `decode_request` takes per row for each:
/// `cargo test --release -p surrealdb-core --lib rpc::format::request_blocks_tests::measure --
/// --ignored --nocapture`.
#[test]
#[ignore = "a measurement, not a check"]
fn measure_decode_of_a_plain_array_and_of_a_block() {
	const N: usize = 100_000;
	let at = Datetime::try_from((1_700_000_000i64, 0u32)).expect("a datetime");
	let rows: Vec<Value> = (0..N as i64)
		.map(|i| {
			obj(vec![
				("n", int(i)),
				("score", Value::Number(Number::Float(i as f64 * 0.5))),
				("name", Value::String(format!("user number {i}"))),
				("at", Value::Datetime(at)),
				("active", Value::Bool(i % 2 == 0)),
				("owner", rid("person", i % 1000)),
			])
		})
		.collect();
	let plain = cbor_bytes(&request(vec![text("t"), rows_as_cbor(&rows)]));
	let block = cbor_bytes(&request(vec![text("t"), tag57(block_of(&rows))]));
	let limit = 1024;
	let time = |bytes: &[u8]| {
		decode_request(bytes, limit).expect("decodes");
		let start = web_time::Instant::now();
		for _ in 0..5 {
			std::hint::black_box(decode_request(bytes, limit).expect("decodes"));
		}
		start.elapsed().as_nanos() as f64 / (5 * N) as f64
	};
	println!("plain array: {} bytes, {:.0} ns/row", plain.len(), time(&plain));
	println!("one block:   {} bytes, {:.0} ns/row", block.len(), time(&block));
}
