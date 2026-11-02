//! The whole decode of a packed row, pinned outcome for outcome, and the check and in-place reads
//! held to it.
//!
//! [`check_golden.txt`] holds what [`StoredRecord::decode_packed`] answers for a deterministic
//! corpus: generated rows, every single-bit flip, every truncation and two appended bytes of each
//! short one and of a fixed sample of the offsets of each long one, and hand-built corruptions of
//! each rule the decode enforces. The fixture holds the answers of the decode as the reference
//! build gives them, which every instantiation of the walk must reproduce, error for error and in
//! the same order. It is never regenerated from the instantiated walk, whose answers it exists to
//! hold.
//!
//! A row's line is the 64-bit FNV-1a digest of its mutants' outcomes in generation order. An
//! outcome is the debug and display text of the refusal, or, for a row that decodes, the debug text
//! of its data. The table before the digests counts each distinct outcome of the corpus; a digest
//! that changes is diagnosed by running the corpus and diffing that table.
//!
//! [`StoredRecord::checked_packed`] is the same walk instantiated to build nothing. Over the same
//! corpus it accepts exactly the rows the decode accepts and refuses the others with the decode's
//! error, and every read of a checked row answers what the decode holds at that path.
//!
//! `generate` prints the fixture's text from the walk it runs, for diagnosing a digest that differs
//! from the fixture:
//!
//! ```text
//! cargo test -p surrealdb-catalog --lib packed::check_tests::generate -- --ignored --nocapture
//! ```

mod corpus;

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::sync::Arc;
use std::sync::atomic::AtomicU32;

use self::corpus::{SEEDS, gen_children, gen_object};
use super::tests::{
	Pres, Rng, Spec, at, datetime, descriptor, duration, int, layout, leaf, named, node, object,
	packed_of, string,
};
use super::{
	CheckedPackedRow, DATETIME_ABOVE, DATETIME_BELOW, DURATION_ESCAPE, FieldRef, Packing, PathRead,
	pack,
};
use crate::Generation;
use crate::layout::{
	ColumnRole, LayoutDescriptor, Location, PackedDescriptor, PhysicalKind, Presence,
};
use crate::record::{RowFlags, StoredRecord};
use crate::val::stored::{self, varint};
use crate::val::{self, Array, Number, Object, RecordId, RecordIdKey, TableName, Value};

const GOLDEN: &str = include_str!("check_golden.txt");

/// Rows shorter than this many bytes are mutated at every byte.
const MUTATED_IN_FULL: usize = 160;

/// The number of offsets at each end of a longer row, and spread across it, that are mutated.
const MUTATED_EDGE: usize = 64;

/// The 64-bit FNV-1a hash, which is the same on every toolchain.
struct Fnv(u64);

impl Fnv {
	fn new() -> Self {
		Self(0xcbf2_9ce4_8422_2325)
	}

	fn write(&mut self, bytes: &[u8]) {
		for &byte in bytes {
			self.0 ^= u64::from(byte);
			self.0 = self.0.wrapping_mul(0x0100_0000_01b3);
		}
	}
}

/// The record id the corpus decodes its rows with.
fn corpus_id() -> RecordId {
	RecordId {
		table: TableName::new("t"),
		key: RecordIdKey::Number(1),
	}
}

/// What the decode answered for `bytes`: its refusal as text, and the data of a row it accepted.
struct Outcome {
	text: String,
	built: Option<String>,
}

/// What [`StoredRecord::decode_packed`] answers for `bytes` read against `packed`.
fn outcome(packed: &PackedDescriptor, bytes: &[u8]) -> Outcome {
	match StoredRecord::parse(bytes) {
		Err(error) => Outcome {
			text: format!("envelope: {error:?}"),
			built: None,
		},
		Ok(None) => Outcome {
			text: "not in the v1 envelope".to_owned(),
			built: None,
		},
		Ok(Some(record)) => match record.decode_packed(packed, Some(corpus_id())) {
			Ok(decoded) => Outcome {
				text: "ok".to_owned(),
				built: Some(format!("{:?}", decoded.data)),
			},
			Err(error) => Outcome {
				text: format!("err: {error:?}: {error}"),
				built: None,
			},
		},
	}
}

/// The offsets of a row of `length` bytes that are mutated: every offset of a row shorter than
/// [`MUTATED_IN_FULL`], and of a longer one its first and last [`MUTATED_EDGE`] offsets and
/// [`MUTATED_EDGE`] offsets spread evenly across it, so the cost of a mutant walk stays linear in
/// the corpus.
fn mutated_offsets(length: usize) -> Vec<usize> {
	if length < MUTATED_IN_FULL {
		return (0..length).collect();
	}
	let mut offsets: Vec<usize> = (0..MUTATED_EDGE)
		.chain((0..MUTATED_EDGE).map(|n| n * length / MUTATED_EDGE))
		.chain(length - MUTATED_EDGE..length)
		.collect();
	offsets.sort_unstable();
	offsets.dedup();
	offsets
}

/// Every mutant of `bytes`, in the order the digests are taken over: each single-bit flip of each
/// mutated offset, each truncation to a mutated offset, and the row with a zero byte and with a
/// `0xFF` byte appended.
fn for_each_mutant(bytes: &[u8], mut visit: impl FnMut(&[u8])) {
	let offsets = mutated_offsets(bytes.len());
	let mut mutant = bytes.to_vec();
	for &at in &offsets {
		for bit in 0..8 {
			mutant[at] ^= 1 << bit;
			visit(&mutant);
			mutant[at] ^= 1 << bit;
		}
	}
	for &length in &offsets {
		visit(&bytes[..length]);
	}
	for tail in [0x00, 0xFF] {
		let mut longer = bytes.to_vec();
		longer.push(tail);
		visit(&longer);
	}
}

/// A packed row of the corpus.
struct CorpusRow {
	seed: u64,
	layout_index: usize,
	row_index: usize,
	layout: Arc<LayoutDescriptor>,
	bytes: Vec<u8>,
}

/// Every row the corpus holds, in generation order: for each seed, 24 descriptors, and for each
/// descriptor six records that fit it.
fn for_each_row(mut visit: impl FnMut(&CorpusRow)) {
	for &seed in SEEDS {
		let mut rng = Rng::new(seed);
		for layout_index in 0..24 {
			let open = rng.one_in(2);
			let children: Vec<(String, Spec)> = gen_children(&mut rng, 0);
			let layout = layout(descriptor(open, &children));
			let packed = packed_of(&layout);
			for row_index in 0..6 {
				let data = gen_object(&mut rng, &children, open, true);
				let generation = Generation::new(1 + rng.below(100) as u16).expect("not zero");
				let flags = RowFlags::edge(rng.one_in(4));
				let Packing::Packed(bytes) =
					pack(packed, generation, flags, &data).expect("a storable record packs")
				else {
					continue;
				};
				visit(&CorpusRow {
					seed,
					layout_index,
					row_index,
					layout: Arc::clone(&layout),
					bytes,
				});
			}
		}
	}
}

// Hand-built corruptions

/// Where the body of a row of generation 1 starts.
const BODY: usize = 3;

/// A descriptor that exercises the corruptions: an open root, every slot shape, a nested open node
/// and the two escaping kinds.
///
/// Columns: 0 root remainder, 1 `a` I32, 2 `b` Bool, 3 `c` Datetime, 4 `d` Datetime, 5 `e`
/// Duration, 6 `node`, 7 `node` remainder, 8 `node.x` I64, 9 `node.y` String, 10 `s` String, 11
/// `v` Value. All optional.
fn corrupt_layout() -> Arc<LayoutDescriptor> {
	let o = Pres::Optional;
	layout(descriptor(
		true,
		&named(vec![
			("a", leaf(PhysicalKind::I32, o)),
			("b", leaf(PhysicalKind::Bool, o)),
			("c", leaf(PhysicalKind::Datetime, o)),
			("d", leaf(PhysicalKind::Datetime, o)),
			("e", leaf(PhysicalKind::Duration, o)),
			(
				"node",
				node(
					o,
					true,
					vec![("x", leaf(PhysicalKind::I64, o)), ("y", leaf(PhysicalKind::String, o))],
				),
			),
			("s", leaf(PhysicalKind::String, o)),
			("v", leaf(PhysicalKind::Value, o)),
		]),
	))
}

const A: usize = 1;
const B: usize = 2;
const C: usize = 3;
const NODE_X: usize = 8;
const S: usize = 10;
const V: usize = 11;

fn packed_row(layout: &LayoutDescriptor, data: &Object) -> Vec<u8> {
	let Packing::Packed(bytes) = pack(packed_of(layout), Generation::FIRST, RowFlags::NONE, data)
		.expect("a storable record")
	else {
		panic!("the record does not fit its descriptor");
	};
	bytes
}

/// The index in a row's bytes of the fixed slot of `column`.
fn slot_at(layout: &LayoutDescriptor, column: usize) -> usize {
	let location = packed_of(layout).columns[column].location;
	let Location::Fixed(offset) = location else {
		panic!("column {column} is at {location:?}");
	};
	BODY + offset as usize
}

fn presence_bit(layout: &LayoutDescriptor, column: usize) -> u32 {
	match packed_of(layout).columns[column].presence {
		Presence::Optional {
			bit,
		}
		| Presence::Nullable {
			bit,
		}
		| Presence::OptionalNullable {
			bit,
		} => bit,
		Presence::Required => panic!("column {column} has no presence bit"),
	}
}

/// Flips presence bit `bit` of a row.
fn flip_presence(bytes: &mut [u8], bit: u32) {
	bytes[BODY + bit as usize / 8] ^= 1 << (bit % 8);
}

/// A row of generation 1 in parts.
struct Parts {
	header: u8,
	fixed: Vec<u8>,
	slices: Vec<Vec<u8>>,
	trailer: Vec<u8>,
}

fn split(layout: &LayoutDescriptor, bytes: &[u8]) -> Parts {
	let packed = packed_of(layout);
	assert_eq!(bytes[2], 1, "generation 1");
	let width = match bytes[1] & StoredRecord::LAYOUT {
		StoredRecord::PACKED_U8 => 1,
		StoredRecord::PACKED_U16 => 2,
		_ => 4,
	};
	let body = &bytes[BODY..];
	let fixed = packed.fixed_size as usize;
	let count = packed.var_count as usize;
	let var = fixed + count * width;
	let mut slices = Vec::new();
	let mut start = 0;
	for index in 0..count {
		let at = fixed + index * width;
		let mut raw = [0u8; 8];
		raw[..width].copy_from_slice(&body[at..at + width]);
		let end = u64::from_le_bytes(raw) as usize;
		slices.push(body[var + start..var + end].to_vec());
		start = end;
	}
	Parts {
		header: bytes[1],
		fixed: body[..fixed].to_vec(),
		slices,
		trailer: body[var + start..].to_vec(),
	}
}

/// The row of `parts` with end offsets `width` bytes wide, with ESCAPES set when it has a trailer.
fn join_with_width(parts: &Parts, width: usize) -> Vec<u8> {
	let layout = match width {
		1 => StoredRecord::PACKED_U8,
		2 => StoredRecord::PACKED_U16,
		_ => StoredRecord::PACKED_U32,
	};
	let mut header = (parts.header & !StoredRecord::LAYOUT & !StoredRecord::ESCAPES) | layout;
	if !parts.trailer.is_empty() {
		header |= StoredRecord::ESCAPES;
	}
	let mut out = vec![StoredRecord::MARKER, header, 1];
	out.extend_from_slice(&parts.fixed);
	let mut end = 0u64;
	for slice in &parts.slices {
		end += slice.len() as u64;
		out.extend_from_slice(&end.to_le_bytes()[..width]);
	}
	for slice in &parts.slices {
		out.extend_from_slice(slice);
	}
	out.extend_from_slice(&parts.trailer);
	out
}

/// The row of `parts` at the narrowest end width.
fn join(parts: &Parts) -> Vec<u8> {
	let length: usize = parts.slices.iter().map(Vec::len).sum();
	let width = match length {
		0..=255 => 1,
		256..=65_535 => 2,
		_ => 4,
	};
	join_with_width(parts, width)
}

/// `bytes` with variable slice `var` replaced by `slice`.
fn with_raw_slice(layout: &LayoutDescriptor, bytes: &[u8], var: usize, slice: &[u8]) -> Vec<u8> {
	let mut parts = split(layout, bytes);
	parts.slices[var] = slice.to_vec();
	join(&parts)
}

/// `bytes` with variable slice `var` replaced by the stored encoding of `value`.
fn with_slice(layout: &LayoutDescriptor, bytes: &[u8], var: usize, value: &Value) -> Vec<u8> {
	with_raw_slice(layout, bytes, var, &stored::encode(value).expect("encodes"))
}

/// A trailer entry: the column's LEB128 index, then a 12-byte payload of seconds and nanoseconds.
fn entry(column: usize, seconds: i64, nanoseconds: u32) -> Vec<u8> {
	let mut buf = [0u8; varint::MAX_LEN];
	let mut out = varint::write_u64(column as u64, &mut buf).to_vec();
	out.extend_from_slice(&seconds.to_le_bytes());
	out.extend_from_slice(&nanoseconds.to_le_bytes());
	out
}

const FAR_PAST: i64 = -20_000_000_000;
const FAR_FUTURE: i64 = 20_000_000_000;

fn far(seconds: i64) -> Value {
	datetime(at(seconds, 5))
}

fn obj(entries: Vec<(&str, Value)>) -> Value {
	Value::Object(object(entries))
}

/// One hand-built corruption, with the descriptor it is read against.
struct Targeted {
	name: &'static str,
	layout: Arc<LayoutDescriptor>,
	bytes: Vec<u8>,
	/// Whether the decode accepts the row. Every other case is a refusal.
	accepted: bool,
}

fn targeted() -> Vec<Targeted> {
	let mut cases = Vec::new();
	let mut push =
		|name: &'static str, layout: &Arc<LayoutDescriptor>, bytes: Vec<u8>, accepted| {
			cases.push(Targeted {
				name,
				layout: Arc::clone(layout),
				bytes,
				accepted,
			});
		};
	let l = corrupt_layout();
	let packed = packed_of(&l);

	// Padding bits past the presence count and past the bool count.
	let mut bytes = packed_row(&l, &object(vec![]));
	bytes[BODY + 1] |= 0x80;
	push("padding_presence", &l, bytes, false);
	let mut bytes = packed_row(&l, &object(vec![("b", Value::Bool(true))]));
	let bools = BODY + (packed.presence_bits as usize).div_ceil(8);
	bytes[bools] |= 0x80;
	push("padding_bool", &l, bytes, false);

	// An end-offset width wider than the narrowest.
	let row = packed_row(&l, &object(vec![("s", string("hi"))]));
	push("width_not_narrowest", &l, join_with_width(&split(&l, &row), 2), false);

	// The trailer and ESCAPES.
	let escaped = packed_row(&l, &object(vec![("c", far(FAR_FUTURE))]));
	let mut bytes = escaped.clone();
	bytes[1] &= !StoredRecord::ESCAPES;
	push("trailer_without_escapes", &l, bytes, false);
	let mut bytes = packed_row(&l, &object(vec![]));
	bytes[1] |= StoredRecord::ESCAPES;
	push("escapes_without_trailer", &l, bytes, false);
	let both = packed_row(&l, &object(vec![("c", far(FAR_FUTURE)), ("d", far(FAR_PAST))]));
	let mut parts = split(&l, &both);
	let (first, second) = parts.trailer.split_at(13);
	parts.trailer = [second, first].concat();
	push("trailer_out_of_order", &l, join(&parts), false);
	let mut parts = split(&l, &both);
	parts.trailer.truncate(13);
	push("escape_slot_without_entry", &l, join(&parts), false);
	let mut bytes = packed_row(&l, &object(vec![("c", far(FAR_FUTURE))]));
	let at_c = slot_at(&l, C);
	bytes[at_c..at_c + 8].copy_from_slice(&DATETIME_BELOW.to_le_bytes());
	push("escape_slot_swapped", &l, bytes, false);
	let mut parts = split(&l, &escaped);
	parts.trailer = entry(C, FAR_PAST, 0);
	push("escape_payload_not_the_slots", &l, join(&parts), false);
	let mut parts = split(&l, &escaped);
	parts.trailer = [entry(C, FAR_FUTURE, 5), entry(C, FAR_FUTURE, 5)].concat();
	push("trailer_entry_repeated", &l, join(&parts), false);
	let mut bytes = escaped;
	flip_presence(&mut bytes, presence_bit(&l, C));
	push("trailer_entry_for_absent_column", &l, bytes, false);
	let mut bytes = packed_row(&l, &object(vec![("e", duration(std::time::Duration::MAX))]));
	let at_c = slot_at(&l, C);
	bytes[at_c..at_c + 8].copy_from_slice(&DATETIME_ABOVE.to_le_bytes());
	push("escape_slot_of_an_absent_column", &l, bytes, false);
	let mut bytes =
		packed_row(&l, &object(vec![("e", duration(std::time::Duration::from_secs(5)))]));
	let at_e = slot_at(&l, 5);
	bytes[at_e..at_e + 8].copy_from_slice(&DURATION_ESCAPE.to_le_bytes());
	push("duration_escape_with_no_entry", &l, bytes, false);

	// Remainders.
	let row = packed_row(&l, &object(vec![("extra", int(1))]));
	let remainder = |value: Value| with_slice(&l, &row, 0, &value);
	push("remainder_not_an_object", &l, remainder(Value::Array(Array(vec![int(1)]))), false);
	push("remainder_empty", &l, remainder(Value::Object(Object::default())), false);
	push("remainder_with_root_id", &l, remainder(obj(vec![("id", int(1))])), false);
	push("remainder_with_none", &l, remainder(obj(vec![("k", Value::None)])), false);
	push("remainder_with_bad_bytes", &l, with_raw_slice(&l, &row, 0, &[0xEE, 0x01]), false);
	let present_a =
		packed_row(&l, &object(vec![("a", Value::Number(Number::I32(5))), ("extra", int(1))]));
	push(
		"key_in_column_and_remainder",
		&l,
		with_slice(&l, &present_a, 0, &obj(vec![("a", int(1))])),
		false,
	);
	push(
		"key_in_remainder_and_absent_column",
		&l,
		with_slice(&l, &row, 0, &obj(vec![("a", int(1))])),
		true,
	);
	let nested = packed_row(&l, &object(vec![("node", obj(vec![("extra", int(1))]))]));
	push(
		"nested_remainder_with_id",
		&l,
		with_slice(&l, &nested, 1, &obj(vec![("id", int(1))])),
		true,
	);
	push(
		"nested_remainder_with_none",
		&l,
		with_slice(&l, &nested, 1, &obj(vec![("k", Value::None)])),
		false,
	);
	push(
		"remainder_bit_clear_with_slice",
		&l,
		{
			let mut bytes = row.clone();
			flip_presence(&mut bytes, presence_bit(&l, 0));
			bytes
		},
		false,
	);
	let nested_collision =
		packed_row(&l, &object(vec![("node", obj(vec![("x", int(5)), ("extra", int(1))]))]));
	push(
		"nested_key_in_column_and_remainder",
		&l,
		with_slice(&l, &nested_collision, 1, &obj(vec![("x", int(1))])),
		false,
	);
	// A collision and a later bad slot: the slot's refusal is the answer.
	let both = packed_row(
		&l,
		&object(vec![("a", Value::Number(Number::I32(5))), ("extra", int(1)), ("s", string("hi"))]),
	);
	let mut parts = split(&l, &both);
	parts.slices[0] = stored::encode(&obj(vec![("a", int(1))])).expect("encodes");
	parts.slices[3] = vec![0xFF, 0xFE];
	push("collision_then_bad_utf8_slot", &l, join(&parts), false);

	// Unused slots.
	let mut bytes = packed_row(&l, &object(vec![]));
	bytes[slot_at(&l, A) + 2] = 1;
	push("unused_fixed_slot", &l, bytes, false);
	let mut bytes = packed_row(&l, &object(vec![("b", Value::Bool(false))]));
	flip_presence(&mut bytes, presence_bit(&l, B));
	let bits = BODY + (packed.presence_bits as usize).div_ceil(8);
	bytes[bits] |= 1;
	push("unused_bool_bit", &l, bytes, false);
	for (name, column, data) in [
		("unused_string_slice", S, object(vec![("s", string("hi"))])),
		("unused_value_slice", V, object(vec![("v", int(3))])),
	] {
		let mut bytes = packed_row(&l, &data);
		flip_presence(&mut bytes, presence_bit(&l, column));
		push(name, &l, bytes, false);
	}

	// Presence under an absent node.
	let mut bytes = packed_row(&l, &object(vec![("node", obj(vec![("x", int(5))]))]));
	flip_presence(&mut bytes, presence_bit(&l, 6));
	push("column_under_absent_node", &l, bytes.clone(), false);
	flip_presence(&mut bytes, presence_bit(&l, NODE_X));
	push("slot_under_absent_node", &l, bytes, false);
	let mut bytes = packed_row(&l, &object(vec![("node", obj(vec![("y", string("in"))]))]));
	flip_presence(&mut bytes, presence_bit(&l, 6));
	push("slice_under_absent_node", &l, bytes, false);

	// A NULL node with a child's bit set, and both bits of an optional nullable set.
	let nullable = layout(descriptor(
		false,
		&named(vec![(
			"n",
			node(Pres::Nullable, false, vec![("x", leaf(PhysicalKind::I64, Pres::Optional))]),
		)]),
	));
	let row = packed_row(&nullable, &object(vec![("n", Value::Null)]));
	let mut bytes = row.clone();
	flip_presence(&mut bytes, 1);
	push("null_node_with_child_bits", &nullable, bytes, false);
	push("null_node", &nullable, row, true);
	let both = layout(descriptor(false, &named(vec![("a", leaf(PhysicalKind::I64, Pres::Both))])));
	let mut bytes = packed_row(&both, &object(vec![("a", int(1))]));
	flip_presence(&mut bytes, 1);
	push("both_presence_bits", &both, bytes, false);

	// Kinds: UTF-8, literal codes, a decimal's flags, a record key's table code.
	let row = packed_row(&l, &object(vec![("s", string("hi"))]));
	push("invalid_utf8", &l, with_raw_slice(&l, &row, 3, &[0xFF, 0xFE]), false);
	let literals = layout(descriptor(
		false,
		&[(
			"l".to_owned(),
			Spec::Leaf {
				kind: PhysicalKind::LiteralCode(1),
				presence: Pres::Required,
				literals: vec![string("red"), string("green")],
				tables: Vec::new(),
			},
		)],
	));
	let mut bytes = packed_row(&literals, &object(vec![("l", string("red"))]));
	let at = slot_at(&literals, 0);
	bytes[at] = 7;
	push("bad_literal_code", &literals, bytes, false);
	let decimal = layout(descriptor(
		false,
		&named(vec![("n", leaf(PhysicalKind::Decimal16, Pres::Required))]),
	));
	let mut bytes = packed_row(
		&decimal,
		&object(vec![("n", Value::Number(crate::val::Number::Decimal(1.into())))]),
	);
	let at = slot_at(&decimal, 0);
	bytes[at + 2] = 0xFF;
	push("decimal_flag_bits", &decimal, bytes, false);
	// A bigdecimal slot holds exactly one payload per value: the sign byte, the scale as a
	// canonical varint and a magnitude with no zero high byte, and no negative zero.
	let big = layout(descriptor(
		false,
		&named(vec![("n", leaf(PhysicalKind::BigDecimal, Pres::Required))]),
	));
	let one: surrealdb_types::BigDecimal = "1.5".parse().expect("a bigdecimal");
	let row = packed_row(
		&big,
		&object(vec![("n", Value::Number(crate::val::Number::BigDecimal(Box::new(one))))]),
	);
	push("bigdecimal_canonical", &big, with_raw_slice(&big, &row, 0, &[0, 1, 15]), true);
	push("bigdecimal_sign", &big, with_raw_slice(&big, &row, 0, &[2, 1, 15]), false);
	push("bigdecimal_negative_zero", &big, with_raw_slice(&big, &row, 0, &[1, 0]), false);
	push("bigdecimal_zero_high_byte", &big, with_raw_slice(&big, &row, 0, &[0, 1, 15, 0]), false);
	push(
		"bigdecimal_overlong_scale",
		&big,
		with_raw_slice(&big, &row, 0, &[0, 0x81, 0, 15]),
		false,
	);
	push("bigdecimal_no_scale", &big, with_raw_slice(&big, &row, 0, &[0]), false);
	let keys = layout(descriptor(
		false,
		&[(
			"k".to_owned(),
			Spec::Leaf {
				kind: PhysicalKind::RecordKey,
				presence: Pres::Required,
				literals: Vec::new(),
				tables: vec![TableName::new("a"), TableName::new("b")],
			},
		)],
	));
	let key = Value::RecordId(RecordId {
		table: TableName::new("b"),
		key: RecordIdKey::Number(7),
	});
	let row = packed_row(&keys, &object(vec![("k", key)]));
	push("record_key_table_code", &keys, with_raw_slice(&keys, &row, 0, &[9, 1]), false);
	push("record_key_empty", &keys, with_raw_slice(&keys, &row, 0, &[0]), false);
	push("record_key_unknown_tag", &keys, with_raw_slice(&keys, &row, 0, &[0, 0x7F]), false);

	// A `Value` column: NONE, and NULL where the presence bits hold it.
	push(
		"value_none",
		&l,
		with_raw_slice(
			&l,
			&packed_row(&l, &object(vec![("v", int(3))])),
			4,
			&stored::encode(&Value::None).expect("encodes"),
		),
		false,
	);
	let nullable_value =
		layout(descriptor(false, &named(vec![("v", leaf(PhysicalKind::Value, Pres::Nullable))])));
	let row = packed_row(&nullable_value, &object(vec![("v", int(3))]));
	push(
		"value_null_where_bits_hold_it",
		&nullable_value,
		with_slice(&nullable_value, &row, 0, &Value::Null),
		false,
	);
	cases
}

// The fixture

/// The corpus as the fixture holds it: the table of outcomes with their counts, the targeted
/// corruptions' outcomes, and each row's digest.
fn corpus_text() -> String {
	let mut counts: BTreeMap<String, usize> = BTreeMap::new();
	let mut digests = String::new();
	let mut rows = 0usize;
	let mut mutants = 0usize;
	for_each_row(|row| {
		let packed = packed_of(&row.layout);
		let mut fnv = Fnv::new();
		let mut tried = 0usize;
		let mut take = |bytes: &[u8]| {
			let outcome = outcome(packed, bytes);
			fnv.write(outcome.text.as_bytes());
			fnv.write(&[0xFF]);
			if let Some(built) = &outcome.built {
				fnv.write(built.as_bytes());
			}
			fnv.write(&[0xFE]);
			*counts.entry(outcome.text).or_default() += 1;
			tried += 1;
		};
		take(&row.bytes);
		for_each_mutant(&row.bytes, &mut take);
		rows += 1;
		mutants += tried;
		let _ = writeln!(
			digests,
			"{} {} {} {} {} {:016x}",
			row.seed,
			row.layout_index,
			row.row_index,
			row.bytes.len(),
			tried,
			fnv.0
		);
	});
	let mut text = format!("rows {rows} outcomes {mutants}\n");
	text.push_str("# outcome counts\n");
	for (outcome, count) in &counts {
		let _ = writeln!(text, "{count}\t{outcome}");
	}
	text.push_str("# targeted\n");
	for case in targeted() {
		let outcome = outcome(packed_of(&case.layout), &case.bytes);
		assert_eq!(
			outcome.text == "ok",
			case.accepted,
			"{}: the decode answered {}",
			case.name,
			outcome.text
		);
		let _ = writeln!(text, "{}\t{}", case.name, outcome.text);
	}
	text.push_str("# digests\n");
	text.push_str(&digests);
	text
}

/// Prints the corpus text between markers, for diffing against the fixture.
#[test]
#[ignore = "prints the corpus text for diffing against the fixture, which it never writes"]
fn generate() {
	println!("-----BEGIN CHECK GOLDEN-----");
	print!("{}", corpus_text());
	println!("-----END CHECK GOLDEN-----");
}

/// The decode answers, for every row and mutant of the corpus and every hand-built corruption,
/// what the fixture holds.
#[test]
fn the_whole_decode_answers_what_the_fixture_holds() {
	let now = corpus_text();
	if now != GOLDEN {
		for (index, (a, b)) in now.lines().zip(GOLDEN.lines()).enumerate() {
			assert_eq!(a, b, "line {index} of the corpus differs from the fixture");
		}
		assert_eq!(now.lines().count(), GOLDEN.lines().count(), "the fixture's length");
	}
}

// The check and the reads of a checked row

fn hex(bytes: &[u8]) -> String {
	bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// The value a field read holds, walked, and `None` for an absent field. An object node is read
/// through [`CheckedPackedRow::decode_node`].
fn field_value(
	checked: &CheckedPackedRow<'_>,
	column: Option<usize>,
	path_len: usize,
	field: FieldRef<'_>,
) -> Option<Value> {
	Some(match field {
		FieldRef::Absent => return None,
		FieldRef::Null => Value::Null,
		FieldRef::Object => {
			let node =
				checked.decode_node(column.expect("an object node is a column")).expect("a node");
			Value::Object(node)
		}
		FieldRef::Bool(v) => Value::Bool(v),
		FieldRef::I8(v) => Value::Number(Number::I8(v)),
		FieldRef::I16(v) => Value::Number(Number::I16(v)),
		FieldRef::I32(v) => Value::Number(Number::I32(v)),
		FieldRef::Int(v) => int(v),
		FieldRef::U8(v) => Value::Number(Number::U8(v)),
		FieldRef::U16(v) => Value::Number(Number::U16(v)),
		FieldRef::U32(v) => Value::Number(Number::U32(v)),
		FieldRef::U64(v) => Value::Number(Number::U64(v)),
		FieldRef::F16(v) => Value::Number(Number::F16(v)),
		FieldRef::F32(v) => Value::Number(Number::F32(v)),
		FieldRef::Float(v) => Value::Number(Number::Float(v)),
		FieldRef::Decimal(v) => Value::Number(Number::Decimal(v)),
		FieldRef::Uuid(v) => Value::Uuid(val::Uuid(v)),
		FieldRef::Datetime(v) => Value::Datetime(val::Datetime(v)),
		FieldRef::Duration(v) => Value::Duration(val::Duration(v)),
		FieldRef::Point(x, y) => Value::Geometry(val::Geometry::Point((x, y).into())),
		FieldRef::String(v) => string(v),
		FieldRef::Bytes(v) => Value::Bytes(val::Bytes::from(v.to_vec())),
		FieldRef::Literal(v) => v.clone(),
		FieldRef::RecordId(table, key) => Value::RecordId(RecordId {
			table: table.clone(),
			key: stored::decode_key_nested(key, path_len + 1).expect("a checked key"),
		}),
		FieldRef::Stored(v) => v.decode().expect("a checked value"),
	})
}

/// The value at `path` in `data`, `None` where a key is missing or a step is not an object.
fn value_at<'v>(data: &'v Object, path: &[String]) -> Option<&'v Value> {
	match walk_of(data, path) {
		Walk::Found(value) => Some(value),
		Walk::Missing | Walk::NotAnObject => None,
	}
}

/// What a walk of a path through a decoded record reaches.
#[derive(Debug)]
enum Walk<'v> {
	Found(&'v Value),
	Missing,
	NotAnObject,
}

/// The walk of `path` through `data`, as the record's document is walked: a missing key ends it as
/// missing, and a step past anything but an object ends it as a step through no object.
fn walk_of<'v>(data: &'v Object, path: &[String]) -> Walk<'v> {
	let Some((last, walk)) = path.split_last() else {
		return Walk::Missing;
	};
	let mut node = data;
	for key in walk {
		match node.get(key.as_str()) {
			Some(Value::Object(inner)) => node = inner,
			Some(_) => return Walk::NotAnObject,
			None => return Walk::Missing,
		}
	}
	match node.get(last.as_str()) {
		Some(value) => Walk::Found(value),
		None => Walk::Missing,
	}
}

/// The read of `path` from a checked row, walked as [`walk_of`] walks the decoded record: a value
/// [`PathRead::Held`] holds is stepped into for the rest of the path.
fn read_walk(checked: &CheckedPackedRow<'_>, path: &[String]) -> ReadWalk {
	let read = checked.read_path(path.iter().map(String::as_str)).expect("a read of a checked row");
	let value = match read {
		PathRead::Missing => return ReadWalk::Missing,
		PathRead::NotAnObject => return ReadWalk::NotAnObject,
		PathRead::Field {
			field,
			column,
		} => field_value(checked, column, path.len(), field),
		PathRead::Held {
			value,
			depth,
		} => {
			let mut value = value;
			for key in &path[depth..] {
				let Some(object) = value.as_object().expect("a checked value") else {
					return ReadWalk::NotAnObject;
				};
				match object.validated().get(key).expect("a checked value") {
					Some(next) => value = next,
					None => return ReadWalk::Missing,
				}
			}
			Some(value.decode().expect("a checked value"))
		}
	};
	match value {
		Some(value) => ReadWalk::Found(value),
		None => panic!("a path read answers a field that is absent"),
	}
}

/// What [`read_walk`] reaches: [`Walk`] with the value walked out of the row.
#[derive(Debug)]
enum ReadWalk {
	Found(Value),
	Missing,
	NotAnObject,
}

/// Whether a path read walks as the decoded record walks.
fn same_walk(read: &ReadWalk, expected: &Walk<'_>) -> bool {
	match (read, expected) {
		(ReadWalk::Found(a), Walk::Found(b)) => val::same_value(a, b),
		(ReadWalk::Missing, Walk::Missing) | (ReadWalk::NotAnObject, Walk::NotAnObject) => true,
		_ => false,
	}
}

fn same(read: Option<&Value>, expected: Option<&Value>) -> bool {
	match (read, expected) {
		(None, None) => true,
		(Some(a), Some(b)) => val::same_value(a, b),
		_ => false,
	}
}

/// Every read of a checked row answers what the whole decode holds.
fn check_reads(
	packed: &PackedDescriptor,
	data: &Object,
	checked: &CheckedPackedRow<'_>,
	row: &[u8],
) {
	for (index, column) in packed.columns.iter().enumerate() {
		if column.role != ColumnRole::Field {
			continue;
		}
		let expected = value_at(data, &column.path);
		// The column's path, and a step past it, walk as the decoded record walks.
		let mut below = column.path.clone();
		below.push("k".to_owned());
		for path in [&column.path, &below] {
			let read = read_walk(checked, path);
			let walked = walk_of(data, path);
			assert!(
				same_walk(&read, &walked),
				"read_path({path:?}) of {}\nread     {read:?}\nexpected {walked:?}\ndecoded  {data:?}",
				hex(row)
			);
		}
		if column.kind == PhysicalKind::Object {
			let read = checked
				.read_path(column.path.iter().map(String::as_str))
				.expect("a read of a checked row");
			assert_eq!(
				matches!(
					read,
					PathRead::Field {
						field: FieldRef::Object,
						column: Some(at),
					} if at == index
				),
				matches!(checked.read_column(index), Ok(FieldRef::Object)),
				"an object node reads as an object, with its column, exactly when the column holds one"
			);
		}
		if let [name] = column.path.as_slice() {
			let hint = AtomicU32::new(u32::MAX);
			for read in [checked.read_root(name), checked.read_column(index)] {
				let read = read.expect("a read of a checked row");
				let got = field_value(checked, Some(index), 1, read);
				assert!(same(got.as_ref(), expected), "read_root({name:?}) of {}", hex(row));
			}
			// A hint resolves the name, is replaced when it names another column, and is kept
			// when it names this one.
			for start in [
				u32::MAX,
				0,
				u32::try_from(index).unwrap_or(0),
				u32::try_from(index + 1).unwrap_or(0),
			] {
				hint.store(start, std::sync::atomic::Ordering::Relaxed);
				assert_eq!(checked.root_column(name, &hint), Some(index));
				assert_eq!(hint.load(std::sync::atomic::Ordering::Relaxed), index as u32);
			}
		}
	}
	// Names the descriptor has no column for read what the root's remainder holds, if anything.
	for name in ["a", "extra", "o", "u", "w", "ä", "zz", "", "node"] {
		let expected = data.get(name);
		let has_column = packed
			.columns
			.iter()
			.any(|column| column.role == ColumnRole::Field && column.path == [name]);
		if has_column {
			continue;
		}
		for read in [checked.read_root(name), checked.read_root_remainder(name)] {
			let read = read.expect("a read of a checked row");
			let got = field_value(checked, None, 1, read);
			assert!(same(got.as_ref(), expected), "read_root({name:?}) of {}", hex(row));
		}
	}
}

/// Runs the check and the whole decode on `bytes`: they must agree, and, when they accept it, every
/// read of the checked row must answer what the decode holds. Answers whether the row was
/// accepted.
fn check_row(packed: &PackedDescriptor, bytes: &[u8]) -> bool {
	let Ok(Some(record)) = StoredRecord::parse(bytes) else {
		return false;
	};
	match (record.decode_packed(packed, Some(corpus_id())), record.checked_packed(packed)) {
		(Ok(decoded), Ok(checked)) => {
			let Value::Object(data) = decoded.data else {
				panic!("a packed row decodes to an object");
			};
			check_reads(packed, &data, &checked, bytes);
			true
		}
		(Err(decode), Err(check)) => {
			assert_eq!(check, decode, "the check's error is not the decode's: {}", hex(bytes));
			false
		}
		(decode, check) => panic!(
			"the check and the decode disagree on {}\ndecode {:?}\ncheck  {:?}",
			hex(bytes),
			decode.map(|_| ()),
			check.map(|_| ())
		),
	}
}

/// The check accepts exactly the rows the decode accepts and refuses the others with its error,
/// and a checked row reads field by field to what the decode holds, over every row of the corpus
/// and every mutant of it.
#[test]
fn the_check_is_the_decode_and_checked_reads_are_its_values() {
	let mut accepted_mutants = 0usize;
	let mut rows = 0usize;
	for_each_row(|row| {
		let packed = packed_of(&row.layout);
		assert!(check_row(packed, &row.bytes), "a generated row checks");
		rows += 1;
		for_each_mutant(&row.bytes, |mutant| {
			if check_row(packed, mutant) {
				accepted_mutants += 1;
			}
		});
	});
	assert!(rows > 1_000, "{rows} rows");
	assert!(accepted_mutants > 1_000, "{accepted_mutants} mutants accepted");
}

/// The hand-built corruptions are refused by the check with the decode's error, and the rows the
/// decode accepts are accepted.
#[test]
fn the_check_refuses_the_hand_built_corruptions_as_the_decode_does() {
	for case in targeted() {
		let packed = packed_of(&case.layout);
		let accepted = check_row(packed, &case.bytes);
		assert_eq!(accepted, case.accepted, "{}", case.name);
	}
}

/// A path below an object node whose column the row leaves absent, and that an open ancestor's
/// remainder holds under the node's key, reads through that remainder, as the decode holds it.
#[test]
fn a_path_below_an_absent_node_reads_the_remainder_that_holds_it() {
	let l = corrupt_layout();
	let packed = packed_of(&l);
	let row = packed_row(&l, &object(vec![("extra", int(1))]));
	let held = obj(vec![(
		"node",
		obj(vec![("deep", obj(vec![("k", int(2))])), ("flat", int(1)), ("nil", Value::Null)]),
	)]);
	let bytes = with_slice(&l, &row, 0, &held);
	let record = StoredRecord::parse(&bytes).expect("parses").expect("v1");
	let decoded = record.decode_packed(packed, Some(corpus_id())).expect("the decode accepts");
	let Value::Object(data) = decoded.data else {
		panic!("a packed row decodes to an object");
	};
	let checked = record.checked_packed(packed).expect("the check accepts");
	let paths: [&[&str]; 9] = [
		&["node", "flat"],
		&["node", "deep"],
		&["node", "deep", "k"],
		&["node", "deep", "k", "below"],
		&["node", "deep", "missing"],
		&["node", "nil"],
		&["node", "nil", "below"],
		&["node", "flat", "below"],
		&["node", "missing"],
	];
	for path in paths {
		let path: Vec<String> = path.iter().map(|segment| (*segment).to_owned()).collect();
		let read = read_walk(&checked, &path);
		let expected = walk_of(&data, &path);
		assert!(
			same_walk(&read, &expected),
			"read_path({path:?}) of {}\nread     {read:?}\nexpected {expected:?}\ndecoded  {data:?}",
			hex(&bytes)
		);
	}
	// `node` is held by the root's remainder, which the rest of the path steps into, and `node.x`
	// names a column the row leaves absent that the held value does not hold: missing.
	let read = checked.read_path(["node", "x"].into_iter()).expect("a read of a checked row");
	assert!(
		matches!(
			read,
			PathRead::Held {
				depth: 1,
				..
			}
		),
		"{read:?}"
	);
	let path = ["node".to_owned(), "x".to_owned()];
	assert!(matches!(read_walk(&checked, &path), ReadWalk::Missing));
}
