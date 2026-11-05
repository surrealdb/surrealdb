use std::sync::Arc;

use super::*;
use crate::{
	Array, Bytes, Datetime, Duration, Number, Object, RecordId, RecordIdKey, Table, Uuid, Value,
};

thread_local! {
	/// The values the test codec has written: a dynamic slice names one by index.
	static HELD: std::cell::RefCell<Vec<Value>> = const { std::cell::RefCell::new(Vec::new()) };
}

/// The test codec: a dynamic item is a CBOR unsigned integer in the four-byte form, an index into
/// [`HELD`]. Remainders are CBOR maps of text keys to such items, as the wire's are of CBOR items.
fn dynamic(value: &Value, out: &mut Vec<u8>) -> Result<(), ShapedError> {
	let at = HELD.with(|held| {
		let mut held = held.borrow_mut();
		held.push(value.clone());
		held.len() - 1
	});
	out.push(0x1a);
	out.extend_from_slice(&(at as u32).to_be_bytes());
	Ok(())
}

fn item(bytes: &mut &[u8]) -> Result<Value, String> {
	let &[0x1a, a, b, c, d, ref rest @ ..] = *bytes else {
		return Err("not a held item".into());
	};
	*bytes = rest;
	let at = u32::from_be_bytes([a, b, c, d]) as usize;
	HELD.with(|held| held.borrow().get(at).cloned()).ok_or_else(|| "no such item".into())
}

/// A CBOR head at the front of `r`: its major type and argument.
fn head(r: &mut &[u8]) -> Result<(u8, usize), String> {
	let (&first, rest) = r.split_first().ok_or("truncated")?;
	let take = |n: usize| -> Result<(usize, &[u8]), String> {
		let b = rest.get(..n).ok_or("truncated")?;
		Ok((b.iter().fold(0usize, |a, b| (a << 8) | usize::from(*b)), &rest[n..]))
	};
	let (n, rest) = match first & 0x1f {
		n @ 0..=23 => (usize::from(n), rest),
		24 => take(1)?,
		25 => take(2)?,
		26 => take(4)?,
		_ => return Err("an unsupported head".into()),
	};
	*r = rest;
	Ok((first >> 5, n))
}

fn undynamic(bytes: &[u8]) -> Result<Value, String> {
	let mut r = bytes;
	let value = match r.first() {
		Some(&first) if first >> 5 == 5 => {
			let (_, n) = head(&mut r)?;
			let mut map = std::collections::BTreeMap::new();
			for _ in 0..n {
				let (major, len) = head(&mut r)?;
				if major != 3 || r.len() < len {
					return Err("not a text key".into());
				}
				let key = std::str::from_utf8(&r[..len]).map_err(|e| e.to_string())?;
				r = &r[len..];
				map.insert(key.to_owned(), item(&mut r)?);
			}
			Value::Object(Object::from(map))
		}
		_ => item(&mut r)?,
	};
	if !r.is_empty() {
		return Err("bytes past an item".into());
	}
	Ok(value)
}

fn obj(entries: Vec<(&str, Value)>) -> Value {
	Value::Object(entries.into_iter().map(|(k, v)| (k.to_owned(), v)).collect())
}

fn rid(table: &str, key: RecordIdKey) -> Value {
	Value::RecordId(RecordId {
		table: Table::new(table),
		key,
	})
}

fn int(n: i64) -> Value {
	Value::Number(Number::Int(n))
}

/// Encodes `rows` through one shaper into one block per `split` rows, and reads them back.
fn round_trip(rows: &[Value], split: usize) -> (Vec<Value>, Vec<Vec<u8>>) {
	let mut shaper = ValueShaper::new();
	let mut scope = ShapeScope::new();
	let mut reader = BlockReader::new();
	let mut decoded = Vec::new();
	let mut blocks = Vec::new();
	for chunk in rows.chunks(split.max(1)) {
		// Rows as a frame holds them: bytes back to back, each with its shape.
		let mut bytes = Vec::new();
		let mut runs: Vec<(Arc<Shape>, u32, std::ops::Range<usize>)> = Vec::new();
		for row in chunk {
			let start = bytes.len();
			let shape = shaper.write(PublicShaped::new(row, dynamic), &mut bytes).expect("writes");
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
		let contents = block_contents(&block).expect("a block");
		decoded.extend(reader.read_block(contents, &mut undynamic).expect("reads"));
		blocks.push(block);
	}
	(decoded, blocks)
}

fn sample_rows() -> Vec<Value> {
	let dt = Datetime::try_from((1_700_000_000i64, 123_456_789u32)).expect("a datetime");
	let early = Datetime::try_from((-62_135_596_801i64, 5u32)).expect("a datetime");
	vec![
		obj(vec![
			("id", rid("person", RecordIdKey::Number(1))),
			("name", Value::String("Tobie".into())),
			("age", int(42)),
			("ratio", Value::Number(Number::Float(0.25))),
			("dec", Value::Number(Number::Decimal("1.50".parse().expect("decimal")))),
			("flag", Value::Bool(true)),
			("at", Value::Datetime(dt)),
			("for", Value::Duration(Duration(std::time::Duration::new(90, 7)))),
			("u", Value::Uuid(Uuid(::uuid::Uuid::from_u128(7)))),
			("blob", Value::Bytes(Bytes(::bytes::Bytes::from_static(b"\x00\x01\xff")))),
			("tags", Value::Array(Array(vec![Value::String("a".into()), int(2)]))),
			("nested", obj(vec![("x", int(1)), ("deep", obj(vec![("y", Value::Null)]))])),
			("gone", Value::None),
			("nil", Value::Null),
		]),
		// Some columns missing, some NULL, a new key, a kind change.
		obj(vec![
			("id", rid("person", RecordIdKey::String("tobie".into()))),
			("name", Value::Null),
			("age", Value::String("old".into())),
			("at", Value::Datetime(early)),
			("extra", int(-5)),
		]),
		// A record of another table, and keys of every kind.
		obj(vec![
			("id", rid("other", RecordIdKey::Uuid(Uuid(::uuid::Uuid::from_u128(9))))),
			("link", rid("x", RecordIdKey::Array(Array(vec![int(1), int(2)])))),
		]),
		obj(vec![]),
		int(5),
		Value::String("a scalar row".into()),
		Value::None,
		Value::Array(Array(vec![int(1)])),
		obj(vec![
			("id", rid("person", RecordIdKey::Number(-3))),
			("name", Value::String("é".into())),
		]),
	]
}

#[test]
fn rows_decode_to_what_was_written() {
	let rows = sample_rows();
	let expected = rows.clone();
	for split in [1, 2, 3, rows.len()] {
		let (decoded, _) = round_trip(&rows, split);
		assert_eq!(decoded, expected, "split {split}");
	}
}

#[test]
fn a_shape_is_announced_once_per_scope() {
	let rows: Vec<Value> = (0..10)
		.map(|i| obj(vec![("id", rid("t", RecordIdKey::Number(i))), ("n", int(i))]))
		.collect();
	let (decoded, blocks) = round_trip(&rows, 3);
	assert_eq!(decoded, rows);
	let shapes = |block: &[u8]| {
		let contents = block_contents(block).expect("a block");
		let mut at = 1;
		let mut n = 0;
		while at < contents.len() {
			let len = u32::from_le_bytes(contents[at + 1..at + 5].try_into().expect("4")) as usize;
			n += usize::from(contents[at] == MESSAGE_SHAPE);
			at += 5 + len;
		}
		n
	};
	assert_eq!(shapes(&blocks[0]), 1);
	for block in &blocks[1..] {
		assert_eq!(shapes(block), 0, "later blocks reuse the shape");
	}
}

#[test]
fn a_shape_survives_its_message() {
	let mut shaper = ValueShaper::new();
	let mut out = Vec::new();
	let shape = shaper.write(PublicShaped::new(&sample_rows()[0], dynamic), &mut out).expect("w");
	let decoded = Shape::decode(shape.payload()).expect("decodes");
	assert_eq!(decoded, *shape);
	assert_eq!(decoded.def().tree(), shape.def().tree());
}

#[test]
fn heterogeneous_rows_settle_on_the_open_shape() {
	let rows: Vec<Value> = (0..100).map(|i| obj(vec![(&*format!("k{i}"), int(i))])).collect();
	let mut shaper = ValueShaper::new();
	let mut shapes = Vec::new();
	let mut out = Vec::new();
	for row in &rows {
		let shape = shaper.write(PublicShaped::new(row, dynamic), &mut out).expect("w");
		if !shapes.iter().any(|s| Arc::ptr_eq(s, &shape)) {
			shapes.push(shape);
		}
	}
	assert!(shapes.len() <= MAX_JOINS as usize + 1, "{} shapes", shapes.len());
	let last = shapes.last().expect("a shape");
	assert!(last.columns().is_empty() && last.def().root_rest != NO_SLOT, "the open shape");
	let (decoded, _) = round_trip(&rows, 7);
	assert_eq!(decoded, rows);
}

#[test]
fn a_seed_names_its_columns_from_the_first_row() {
	let seed = Arc::new(
		Shape::new(ShapeDef::object(
			false,
			&[
				ColumnDef {
					name: "id".into(),
					kind: Kind::Record,
					element: None,
					flags: 0,
					table: "t".into(),
					open: false,
					children: vec![],
				},
				ColumnDef {
					name: "secret".into(),
					kind: Kind::String,
					element: None,
					flags: MAY_BE_ABSENT,
					table: String::new(),
					open: false,
					children: vec![],
				},
			],
		))
		.expect("a shape"),
	);
	let mut shaper = ValueShaper::with_seed("t".into(), seed);
	let mut out = Vec::new();
	let row = obj(vec![("id", rid("t", RecordIdKey::Number(1)))]);
	let first = shaper.write(PublicShaped::new(&row, dynamic), &mut out).expect("w");
	assert!(first.columns().iter().any(|c| c.name == "secret"));
	let row =
		obj(vec![("id", rid("t", RecordIdKey::Number(2))), ("secret", Value::String("s".into()))]);
	let second = shaper.write(PublicShaped::new(&row, dynamic), &mut out).expect("w");
	assert!(Arc::ptr_eq(&first, &second), "no re-announcement");
}

/// A key the row does not hold (as a field the caller may not see is not held) and a key whose
/// value is `NONE` are different bytes, and `NONE` reads back as `NONE`.
#[test]
fn absent_and_none_are_different_bytes() {
	let none =
		obj(vec![("id", rid("t", RecordIdKey::Number(1))), ("x", Value::None), ("y", int(1))]);
	let absent = obj(vec![("id", rid("t", RecordIdKey::Number(1))), ("y", int(1))]);
	let warm = obj(vec![("id", rid("t", RecordIdKey::Number(1))), ("x", int(2)), ("y", int(1))]);
	let mut shaper = ValueShaper::new();
	shaper.write(PublicShaped::new(&warm, dynamic), &mut Vec::new()).expect("w");
	let (mut a, mut b) = (Vec::new(), Vec::new());
	let shape = shaper.write(PublicShaped::new(&none, dynamic), &mut a).expect("w");
	shaper.write(PublicShaped::new(&absent, dynamic), &mut b).expect("w");
	assert_ne!(a, b);
	let mut undyn = undynamic;
	let (read, _) = decode_row(&shape, &a, &mut undyn).expect("reads");
	assert_eq!(read, none);
	let (read, _) = decode_row(&shape, &b, &mut undyn).expect("reads");
	assert_eq!(read, absent);
}

/// Every bit flip and every truncation of a block is read or refused, never a panic.
#[test]
fn a_damaged_block_never_panics() {
	let rows = sample_rows();
	let (_, blocks) = round_trip(&rows, rows.len());
	let block = &blocks[0];
	let contents = block_contents(block).expect("a block").to_vec();
	for at in 0..contents.len() {
		for bit in 0..8 {
			let mut damaged = contents.clone();
			damaged[at] ^= 1 << bit;
			let _ = BlockReader::new().read_block(&damaged, &mut undynamic);
		}
	}
	for len in 0..contents.len() {
		let result = BlockReader::new().read_block(&contents[..len], &mut undynamic);
		// A cut that lands between two messages leaves a whole (shorter) block.
		if let Ok(values) = result {
			assert!(values.len() <= rows.len());
		}
	}
	for len in 0..block.len() {
		let _ = block_contents(&block[..len]);
	}
}

/// Shapes past the limits are refused when read.
#[test]
fn a_shape_past_the_limits_is_refused() {
	let mut def = ShapeDef::object(false, &[]);
	def.fixed_size = MAX_FIXED + 1;
	assert!(Shape::new(def).is_err());
	let mut nested = ColumnDef {
		name: "a".into(),
		kind: Kind::Int,
		element: None,
		flags: 0,
		table: String::new(),
		open: false,
		children: vec![],
	};
	for _ in 0..MAX_DEPTH {
		nested = ColumnDef {
			name: "a".into(),
			kind: Kind::Object,
			element: None,
			flags: 0,
			table: String::new(),
			open: false,
			children: vec![nested],
		};
	}
	assert!(Shape::new(ShapeDef::object(false, &[nested])).is_err());
	let mut columns = ShapeDef::object(
		false,
		&[ColumnDef {
			name: "b".into(),
			kind: Kind::Int,
			element: None,
			flags: 0,
			table: String::new(),
			open: false,
			children: vec![],
		}],
	);
	columns.columns[0].slot = 1;
	assert!(Shape::new(columns).is_err(), "a slot past the fixed region");
}

/// Columns a seed declares as arrays of one element kind hold their arrays natively; an array that
/// does not fit is written in a joined shape, as a dynamic value. Both read back as written, and
/// every bit flip of the block is read or refused.
#[test]
fn native_arrays_round_trip() {
	let column = |name: &str, kind: Kind, element: Option<Kind>| ColumnDef {
		name: name.into(),
		kind,
		element,
		flags: MAY_BE_ABSENT | MAY_BE_NULL,
		table: String::new(),
		open: false,
		children: vec![],
	};
	let seed = Arc::new(
		Shape::new(ShapeDef::object(
			false,
			&[
				column("id", Kind::Record, None),
				column("ints", Kind::Array, Some(Kind::Int)),
				column("tags", Kind::Array, Some(Kind::String)),
				column("when", Kind::Array, Some(Kind::Datetime)),
			],
		))
		.expect("a shape"),
	);
	let dt = Datetime::try_from((1_700_000_000i64, 5u32)).expect("a datetime");
	let strings =
		|v: &[&str]| Value::Array(Array(v.iter().map(|s| Value::String((*s).into())).collect()));
	let rows = vec![
		obj(vec![
			("id", rid("t", RecordIdKey::Number(1))),
			("ints", Value::Array(Array(vec![int(1), int(-2), int(i64::MAX)]))),
			("tags", strings(&["a", "", "é"])),
			("when", Value::Array(Array(vec![Value::Datetime(dt)]))),
		]),
		obj(vec![("id", rid("t", RecordIdKey::Number(2))), ("tags", strings(&[]))]),
		obj(vec![
			("id", rid("t", RecordIdKey::Number(3))),
			("tags", Value::Array(Array(vec![int(1)]))),
		]),
	];
	let mut shaper = ValueShaper::with_seed("t".into(), seed);
	let mut scope = ShapeScope::new();
	let mut bytes = Vec::new();
	let mut runs: Vec<(Arc<Shape>, u32, std::ops::Range<usize>)> = Vec::new();
	for row in &rows {
		let start = bytes.len();
		let shape = shaper.write(PublicShaped::new(row, dynamic), &mut bytes).expect("writes");
		runs.push((shape, 1, start..bytes.len()));
	}
	assert_eq!(runs[0].0.columns()[2].kind, Kind::Array, "the seed's array is native");
	let mut block = Vec::new();
	write_block(&mut block, &mut scope, runs.iter().map(|(s, n, r)| (s, *n, &bytes[r.clone()])))
		.expect("frames");
	let contents = block_contents(&block).expect("a block").to_vec();
	let read = BlockReader::new().read_block(&contents, &mut undynamic).expect("reads");
	assert_eq!(read, rows);
	for at in 0..contents.len() {
		for bit in 0..8 {
			let mut damaged = contents.clone();
			damaged[at] ^= 1 << bit;
			let _ = BlockReader::new().read_block(&damaged, &mut undynamic);
		}
	}
}

/// A key too long for a SHAPE message is no column: its object opens and holds it in its
/// remainder, so the row reads back whole; a shape that names such a column is refused.
#[test]
fn a_key_longer_than_a_shape_holds_is_in_the_remainder() {
	let long = "k".repeat(70_000);
	let rows = vec![
		obj(vec![("id", rid("t", RecordIdKey::Number(1))), (long.as_str(), int(1)), ("x", int(2))]),
		obj(vec![("nested", obj(vec![(long.as_str(), Value::Bool(true))]))]),
	];
	// A nested object met first, in a row of its own, opens its node.
	let (decoded, _) = round_trip(&rows[1..], 1);
	assert_eq!(decoded, rows[1..]);
	let (decoded, _) = round_trip(&rows, 2);
	assert_eq!(decoded, rows);
	let column = ColumnDef {
		name: long,
		kind: Kind::Int,
		element: None,
		flags: 0,
		table: String::new(),
		open: false,
		children: vec![],
	};
	assert!(Shape::new(ShapeDef::object(false, &[column])).is_err());
}

fn column(name: &str, kind: Kind, flags: u8, table: &str) -> ColumnDef {
	ColumnDef {
		name: name.into(),
		kind,
		element: None,
		flags,
		table: table.into(),
		open: false,
		children: vec![],
	}
}

/// A shape whose `id` holds the last var slot, though it sorts before the columns after it, takes
/// value-path rows: the var slots are opened in order.
#[test]
fn a_root_id_in_the_last_var_slot_takes_value_rows() {
	let mut def = ShapeDef::object(
		false,
		&[column("id", Kind::Record, 0, "t"), column("z", Kind::String, MAY_BE_ABSENT, "")],
	);
	for c in &mut def.columns {
		c.slot = if c.name == "id" {
			1
		} else {
			0
		};
	}
	let shape = Arc::new(Shape::new(def).expect("a shape"));
	let mut shaper = ValueShaper::new();
	shaper.set_current(&shape);
	let row = obj(vec![("id", rid("t", RecordIdKey::Number(1))), ("z", Value::String("s".into()))]);
	let mut bytes = Vec::new();
	let used = shaper.write(PublicShaped::new(&row, dynamic), &mut bytes).expect("writes");
	assert!(Arc::ptr_eq(&used, &shape));
	let (decoded, _) = decode_row(&shape, &bytes, &mut undynamic).expect("reads");
	assert_eq!(decoded, row);
}

/// A seed of more columns than a derived shape holds is used whole by a row that fits it.
#[test]
fn a_wide_seed_is_kept_by_a_row_that_fits() {
	let mut columns = Vec::new();
	for at in 0..(MAX_DERIVED_COLUMNS + 50) {
		columns.push(column(&format!("c{at:04}"), Kind::Int, MAY_BE_ABSENT, ""));
	}
	columns.push(column("id", Kind::Record, 0, "t"));
	let seed = Arc::new(Shape::new(ShapeDef::object(false, &columns)).expect("a shape"));
	let mut shaper = ValueShaper::with_seed("t".into(), Arc::clone(&seed));
	let row = obj(vec![("id", rid("t", RecordIdKey::Number(1))), ("c0003", int(3))]);
	let used = shaper.write(PublicShaped::new(&row, dynamic), &mut Vec::new()).expect("writes");
	assert!(Arc::ptr_eq(&used, &seed));
}

/// Rows that take no bytes are cut into batches the reader accepts, and a batch that declares
/// more of them than the format allows is refused.
#[test]
fn rows_of_no_bytes_are_bounded_per_batch() {
	let rows = vec![obj(vec![]); MAX_EMPTY_ROWS as usize + 10];
	let (decoded, _) = round_trip(&rows, rows.len());
	assert_eq!(decoded, rows);

	let shape = Arc::new(Shape::new(ShapeDef::object(false, &[])).expect("a shape"));
	let mut block = Vec::new();
	write_block(&mut block, &mut ShapeScope::new(), [(&shape, MAX_EMPTY_ROWS, &[][..])])
		.expect("frames");
	// The batch is the block's last message, its row count the last four bytes.
	let end = block.len();
	block[end - 4..].copy_from_slice(&u32::MAX.to_le_bytes());
	let contents = block_contents(&block).expect("a block");
	assert!(BlockReader::new().read_block(contents, &mut undynamic).is_err());
}

#[test]
fn a_shape_knows_how_deep_its_object_columns_nest() {
	let object = |name: &str, children: Vec<ColumnDef>| ColumnDef {
		kind: Kind::Object,
		children,
		..column(name, Kind::Object, 0, "")
	};
	let flat = ShapeDef::object(false, &[column("a", Kind::Int, 0, "")]);
	assert_eq!(Shape::new(flat).expect("a shape").depth(), 0);
	let two = ShapeDef::object(
		false,
		&[object("a", vec![object("b", vec![column("c", Kind::Int, 0, "")])])],
	);
	assert_eq!(Shape::new(two).expect("a shape").depth(), 2);
}

#[test]
fn a_budget_bounds_what_a_block_materialises() {
	let rows = sample_rows();
	let (_, blocks) = round_trip(&rows, rows.len());
	let contents = block_contents(&blocks[0]).expect("a block");
	let mut dynamic = |slice: &[u8], _: usize| undynamic(slice);
	let mut ample = Budget::new(1 << 20);
	BlockReader::new().read_block_within(contents, &mut dynamic, &mut ample).expect("reads");
	let mut scant = Budget::new(rows.len() as u64);
	let err = BlockReader::new()
		.read_block_within(contents, &mut dynamic, &mut scant)
		.expect_err("over budget");
	assert!(err.0.contains("budget"), "{err}");
	let mut shallow = Budget::new(1 << 20);
	shallow.max_shape_depth = 0;
	let err = BlockReader::new()
		.read_block_within(contents, &mut dynamic, &mut shallow)
		.expect_err("too deep");
	assert!(err.0.contains("too deep"), "{err}");
}
