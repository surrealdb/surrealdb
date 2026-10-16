//! The records the check tests run over: a seeded generator of descriptors and records that
//! covers every physical kind and presence, nested object nodes, open nodes and their remainders,
//! the datetime and duration escapes and every end-offset width. It generates the same records as
//! the generator of `packed/tests.rs`, whose items are private to that module.

use chrono::{DateTime, Utc};
use surrealdb_types::Decimal;

use super::super::kind::widen_f32;
use super::super::tests::{
	Pres, Rng, Spec, at, bytes, datetime, decimal, duration, float, int, leap_second, point,
	record, string, uuid, wide_literals,
};
use super::super::{DATETIME_ABOVE, DATETIME_BELOW, DURATION_ESCAPE};
use crate::layout::PhysicalKind;
use crate::val::{self, Array, Object, RecordId, RecordIdKey, Strand, TableName, Value};

const ALL_PRES: [Pres; 4] = [Pres::Required, Pres::Optional, Pres::Nullable, Pres::Both];

fn presence_of(spec: &Spec) -> Pres {
	match spec {
		Spec::Leaf {
			presence,
			..
		}
		| Spec::Node {
			presence,
			..
		} => *presence,
	}
}

fn absent(presence: Pres) -> bool {
	matches!(presence, Pres::Optional | Pres::Both)
}

fn null(presence: Pres) -> bool {
	matches!(presence, Pres::Nullable | Pres::Both)
}

/// The seeds the corpus runs.
pub(super) const SEEDS: &[u64] =
	&[1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233, 377, 610, 987, 0xDEAD_BEEF];

/// Field names, in byte order. `id` is not a field of the root.
const NAMES: &[&str] = &["a", "b", "c", "dd", "e", "f", "id", "k", "m", "q", "s", "x", "z", "é"];

/// Keys no generated field has, for remainders.
const EXTRA: &[&str] = &["extra", "o", "u", "w", "ä"];

/// Every physical kind but `Object`, with a placeholder width where the kind takes one.
pub(super) const LEAF_KINDS: &[PhysicalKind] = &[
	PhysicalKind::Bool,
	PhysicalKind::I8,
	PhysicalKind::I16,
	PhysicalKind::I32,
	PhysicalKind::I64,
	PhysicalKind::U8,
	PhysicalKind::U16,
	PhysicalKind::U32,
	PhysicalKind::U64,
	PhysicalKind::F32,
	PhysicalKind::F64,
	PhysicalKind::Decimal16,
	PhysicalKind::Fixed(0),
	PhysicalKind::Uuid,
	PhysicalKind::Datetime,
	PhysicalKind::DatetimeWide,
	PhysicalKind::Duration,
	PhysicalKind::Point,
	PhysicalKind::LiteralCode(0),
	PhysicalKind::String,
	PhysicalKind::Bytes,
	PhysicalKind::RecordKey,
	PhysicalKind::DecimalVar,
	PhysicalKind::Value,
];

/// Scalars a literal union may hold, all distinct.
fn literal_pool() -> Vec<Value> {
	vec![
		string("red"),
		string("green"),
		int(1),
		float(1.0),
		int(-2),
		Value::Bool(true),
		Value::Bool(false),
		duration(std::time::Duration::from_secs(1)),
		decimal(Decimal::new(150, 2)),
		decimal(Decimal::new(15, 1)),
	]
}

const TABLES: &[&str] = &["a", "b", "person"];

fn gen_leaf(rng: &mut Rng, presence: Pres) -> Spec {
	let mut kind = LEAF_KINDS[rng.index(LEAF_KINDS.len())];
	let mut literals = Vec::new();
	let mut tables = Vec::new();
	match &mut kind {
		PhysicalKind::Fixed(width) => {
			*width = match rng.below(4) {
				0 => 1,
				1 => 16,
				2 => 4096,
				_ => 1 + rng.below(40) as u32,
			}
		}
		PhysicalKind::LiteralCode(width) => {
			if rng.one_in(3) {
				*width = 2;
				literals = wide_literals();
			} else {
				*width = 1 + rng.below(2) as u32;
				let mut pool = literal_pool();
				let n = 1 + rng.index(pool.len());
				while pool.len() > n {
					pool.remove(rng.index(pool.len()));
				}
				literals = pool;
			}
		}
		PhysicalKind::RecordKey => {
			for table in TABLES {
				if tables.is_empty() || rng.one_in(2) {
					tables.push(TableName::new(*table));
				}
			}
		}
		_ => {}
	}
	Spec::Leaf {
		kind,
		presence,
		literals,
		tables,
	}
}

pub(super) fn gen_children(rng: &mut Rng, depth: usize) -> Vec<(String, Spec)> {
	let most = if depth == 0 {
		10
	} else {
		5
	};
	let mut names: Vec<&str> =
		NAMES.iter().copied().filter(|name| depth > 0 || *name != "id").collect();
	let count = rng.index(most + 1).min(names.len());
	let mut chosen = Vec::new();
	for _ in 0..count {
		chosen.push(names.remove(rng.index(names.len())));
	}
	chosen.sort_unstable();
	chosen
		.into_iter()
		.map(|name| {
			let presence = ALL_PRES[rng.index(4)];
			let spec = if depth < 3 && rng.one_in(5) {
				Spec::Node {
					presence,
					open: rng.one_in(2),
					children: gen_children(rng, depth + 1),
				}
			} else {
				gen_leaf(rng, presence)
			};
			(name.to_owned(), spec)
		})
		.collect()
}

fn gen_int_in(rng: &mut Rng, lo: i64, hi: i64) -> i64 {
	match rng.below(5) {
		0 => lo,
		1 => hi,
		2 => 0,
		_ => {
			let span = (hi as i128 - lo as i128 + 1) as u128;
			(lo as i128 + (u128::from(rng.next()) % span) as i128) as i64
		}
	}
}

fn gen_decimal(rng: &mut Rng) -> Decimal {
	let hi = match rng.below(3) {
		0 => 0,
		1 => u32::MAX,
		_ => rng.next() as u32,
	};
	Decimal::from_parts(
		rng.next() as u32,
		rng.next() as u32,
		hi,
		rng.one_in(2),
		rng.below(29) as u32,
	)
}

fn gen_string(rng: &mut Rng) -> String {
	const CHARS: &[char] = &['a', 'z', ' ', 'é', '名', '🦀', '\0', '"'];
	let length = match rng.below(60) {
		0 => 300,
		1 => 70_000,
		_ => rng.index(12),
	};
	if length > 12 {
		return "x".repeat(length);
	}
	(0..length).map(|_| CHARS[rng.index(CHARS.len())]).collect()
}

fn gen_bytes(rng: &mut Rng, length: usize) -> Vec<u8> {
	(0..length).map(|_| rng.next() as u8).collect()
}

fn gen_datetime(rng: &mut Rng, wide: bool) -> DateTime<Utc> {
	match rng.below(10) {
		0 => DateTime::from_timestamp_nanos(DATETIME_BELOW),
		1 => DateTime::from_timestamp_nanos(DATETIME_ABOVE),
		2 => {
			at(-10_000_000_000 - rng.below(200_000_000_000) as i64, rng.below(1_000_000_000) as u32)
		}
		3 => {
			at(10_000_000_000 + rng.below(200_000_000_000) as i64, rng.below(1_000_000_000) as u32)
		}
		4 => DateTime::<Utc>::MIN_UTC,
		5 => DateTime::<Utc>::MAX_UTC,
		6 if wide => leap_second(),
		_ => DateTime::from_timestamp_nanos(rng.next() as i64),
	}
}

fn gen_duration(rng: &mut Rng) -> std::time::Duration {
	match rng.below(6) {
		0 => std::time::Duration::from_nanos(DURATION_ESCAPE),
		1 => std::time::Duration::MAX,
		2 => std::time::Duration::from_secs(rng.next()),
		_ => std::time::Duration::from_nanos(rng.next() >> rng.below(64)),
	}
}

fn gen_key(rng: &mut Rng) -> RecordIdKey {
	match rng.below(4) {
		0 => RecordIdKey::Number(rng.next() as i64),
		1 => RecordIdKey::String(Strand::new(gen_string(rng))),
		2 => RecordIdKey::Uuid(val::Uuid(uuid::Uuid::from_u64_pair(rng.next(), rng.next()))),
		_ => RecordIdKey::Array(Array(vec![int(rng.below(10) as i64), string("k")])),
	}
}

/// Any value but `NONE`, nested at most `depth` more levels.
fn gen_any(rng: &mut Rng, depth: usize) -> Value {
	match rng.below(if depth == 0 {
		12
	} else {
		14
	}) {
		0 => Value::Null,
		1 => Value::Bool(rng.one_in(2)),
		2 => int(rng.next() as i64),
		3 => float(f64::from_bits(rng.next())),
		4 => decimal(gen_decimal(rng)),
		5 => string(&gen_string(rng)),
		6 => {
			let length = rng.index(6);
			bytes(&gen_bytes(rng, length))
		}
		7 => duration(gen_duration(rng)),
		8 => datetime(gen_datetime(rng, true)),
		9 => uuid(u128::from(rng.next()) << 64 | u128::from(rng.next())),
		10 => record(TABLES[rng.index(TABLES.len())], gen_key(rng)),
		11 => point(f64::from_bits(rng.next()), f64::from_bits(rng.next())),
		12 => Value::Array(Array((0..rng.index(4)).map(|_| gen_any(rng, depth - 1)).collect())),
		_ => {
			let mut inner = Object::default();
			for _ in 0..rng.index(4) {
				let key = NAMES[rng.index(NAMES.len())];
				inner.0.insert(Strand::new(key), gen_any(rng, depth - 1));
			}
			Value::Object(inner)
		}
	}
}

/// A present value of a leaf. One in fifty does not fit the column.
fn gen_value(rng: &mut Rng, kind: PhysicalKind, literals: &[Value], tables: &[TableName]) -> Value {
	if rng.one_in(50) {
		return match kind {
			PhysicalKind::Bool => int(1),
			PhysicalKind::I8 => int(1_000),
			PhysicalKind::U8 | PhysicalKind::U16 | PhysicalKind::U32 | PhysicalKind::U64 => int(-1),
			PhysicalKind::F32 => float(0.1),
			PhysicalKind::Datetime => datetime(leap_second()),
			PhysicalKind::LiteralCode(_) => string("not a literal"),
			PhysicalKind::RecordKey => record("elsewhere", RecordIdKey::Number(1)),
			PhysicalKind::Value => Value::None,
			_ => Value::Bool(true),
		};
	}
	match kind {
		PhysicalKind::Bool => Value::Bool(rng.one_in(2)),
		PhysicalKind::I8 => int(gen_int_in(rng, i8::MIN.into(), i8::MAX.into())),
		PhysicalKind::I16 => int(gen_int_in(rng, i16::MIN.into(), i16::MAX.into())),
		PhysicalKind::I32 => int(gen_int_in(rng, i32::MIN.into(), i32::MAX.into())),
		PhysicalKind::I64 => int(gen_int_in(rng, i64::MIN, i64::MAX)),
		PhysicalKind::U8 => int(gen_int_in(rng, 0, u8::MAX.into())),
		PhysicalKind::U16 => int(gen_int_in(rng, 0, u16::MAX.into())),
		PhysicalKind::U32 => int(gen_int_in(rng, 0, u32::MAX.into())),
		PhysicalKind::U64 => int(gen_int_in(rng, 0, i64::MAX)),
		PhysicalKind::F32 => float(widen_f32(rng.next() as u32)),
		PhysicalKind::F64 => float(f64::from_bits(rng.next())),
		PhysicalKind::Decimal16 | PhysicalKind::DecimalVar => decimal(gen_decimal(rng)),
		PhysicalKind::Fixed(width) => bytes(&gen_bytes(rng, width as usize)),
		PhysicalKind::Uuid => uuid(u128::from(rng.next()) << 64 | u128::from(rng.next())),
		PhysicalKind::Datetime => datetime(gen_datetime(rng, false)),
		PhysicalKind::DatetimeWide => datetime(gen_datetime(rng, true)),
		PhysicalKind::Duration => duration(gen_duration(rng)),
		PhysicalKind::Point => point(f64::from_bits(rng.next()), f64::from_bits(rng.next())),
		PhysicalKind::LiteralCode(_) => literals[rng.index(literals.len())].clone(),
		PhysicalKind::String => string(&gen_string(rng)),
		PhysicalKind::Bytes => {
			let length = rng.index(20);
			bytes(&gen_bytes(rng, length))
		}
		PhysicalKind::RecordKey => {
			let table = &tables[rng.index(tables.len())];
			Value::RecordId(RecordId {
				table: table.clone(),
				key: gen_key(rng),
			})
		}
		PhysicalKind::Value => match rng.below(4) {
			0 => Value::Null,
			_ => gen_any(rng, 2),
		},
		PhysicalKind::Object => unreachable!("an object node is not a leaf"),
	}
}

/// A record for a node over `children`: each key absent, `NULL` or present as its presence
/// allows, one in forty in a state it does not, and, in an open node, keys that have no column.
pub(super) fn gen_object(
	rng: &mut Rng,
	children: &[(String, Spec)],
	open: bool,
	root: bool,
) -> Object {
	let mut object = Object::default();
	for (name, spec) in children {
		let presence = presence_of(spec);
		let mut states = vec![2u8];
		if absent(presence) {
			states.push(0);
		}
		if null(presence) {
			states.push(1);
		}
		let state = if rng.one_in(40) {
			rng.below(3) as u8
		} else {
			states[rng.index(states.len())]
		};
		let value = match state {
			0 => continue,
			1 => Value::Null,
			_ => match spec {
				Spec::Leaf {
					kind,
					literals,
					tables,
					..
				} => gen_value(rng, *kind, literals, tables),
				Spec::Node {
					open,
					children,
					..
				} => Value::Object(gen_object(rng, children, *open, false)),
			},
		};
		object.0.insert(Strand::new(name), value);
	}
	if open && rng.one_in(2) {
		for _ in 0..1 + rng.index(3) {
			let key = EXTRA[rng.index(EXTRA.len())];
			object.0.insert(Strand::new(key), gen_any(rng, 2));
		}
	}
	if root && rng.one_in(3) {
		object.0.insert(Strand::new("id"), record("t", RecordIdKey::Number(1)));
	}
	object
}
