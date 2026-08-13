//! Property test: generate arbitrary `Value`s, format them with `ToSql`, and check that
//! parsing the formatted text yields the original value back.
//!
//! Values whose SQON rendering is ambiguous or unsupported are skipped:
//! - `Geometry` and `Regex` have no SQON syntax; a bare `Table` is not a value.
//! - `NaN` floats never compare equal.
//! - Files render with escape sequences for characters outside the file-string character set, which
//!   the file-string grammar rejects.
//! - Tables named like keywords (`true:1`) render without escaping and re-parse as the keyword.
//! - A record id as the first element of a set re-parses as an object key (`{person:tobie}`), and
//!   record ids or nested ranges directly inside range bounds re-parse ambiguously (`person:1..5`
//!   is a record id key range, not a range of record ids).

use std::ops::Bound;

use arbitrary::{Arbitrary, Unstructured};
use surrealdb_types::{Number, RecordIdKey, ToSql, Value};

fn keyword_like(s: &str) -> bool {
	s.eq_ignore_ascii_case("null")
		|| s.eq_ignore_ascii_case("none")
		|| s.eq_ignore_ascii_case("true")
		|| s.eq_ignore_ascii_case("false")
		|| s == "NaN"
		|| s == "Infinity"
}

fn file_str_supported(s: &str, extra: &[char]) -> bool {
	s.chars()
		.all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.') || extra.contains(&c))
}

fn key_supported(key: &RecordIdKey) -> bool {
	match key {
		RecordIdKey::Number(_) | RecordIdKey::Uuid(_) | RecordIdKey::String(_) => true,
		RecordIdKey::Array(a) => a.iter().all(supported),
		RecordIdKey::Object(o) => o.values().all(supported),
		RecordIdKey::Range(r) => {
			let bound_supported = |b: &Bound<RecordIdKey>| match b {
				Bound::Unbounded => true,
				Bound::Included(k) | Bound::Excluded(k) => {
					!matches!(k, RecordIdKey::Range(_)) && key_supported(k)
				}
			};
			bound_supported(&r.start) && bound_supported(&r.end)
		}
	}
}

fn supported(value: &Value) -> bool {
	match value {
		Value::None
		| Value::Null
		| Value::Bool(_)
		| Value::String(_)
		| Value::Bytes(_)
		| Value::Duration(_)
		| Value::Datetime(_)
		| Value::Uuid(_) => true,
		Value::Number(Number::Float(f)) => !f.is_nan(),
		Value::Number(_) => true,
		Value::Geometry(_) | Value::Regex(_) | Value::Table(_) => false,
		Value::File(f) => file_str_supported(&f.bucket, &[]) && file_str_supported(&f.key, &['/']),
		Value::Array(a) => a.iter().all(supported),
		Value::Object(o) => o.values().all(supported),
		Value::Set(s) => {
			!matches!(s.iter().next(), Some(Value::RecordId(_))) && s.iter().all(supported)
		}
		Value::RecordId(r) => !keyword_like(&r.table) && key_supported(&r.key),
		Value::Range(r) => {
			let bound_supported = |b: &Bound<Value>| match b {
				Bound::Unbounded => true,
				Bound::Included(v) | Bound::Excluded(v) => {
					!matches!(v, Value::Range(_) | Value::RecordId(_)) && supported(v)
				}
			};
			bound_supported(&r.start) && bound_supported(&r.end)
		}
	}
}

/// The parser's recursion limit is 128; skip values nested deeper than a safe margin.
fn depth(value: &Value) -> usize {
	fn key_depth(key: &RecordIdKey) -> usize {
		match key {
			RecordIdKey::Array(a) => 1 + a.iter().map(depth).max().unwrap_or(0),
			RecordIdKey::Object(o) => 1 + o.values().map(depth).max().unwrap_or(0),
			RecordIdKey::Range(r) => {
				let bound = |b: &Bound<RecordIdKey>| match b {
					Bound::Unbounded => 0,
					Bound::Included(k) | Bound::Excluded(k) => key_depth(k),
				};
				1 + bound(&r.start).max(bound(&r.end))
			}
			_ => 0,
		}
	}

	match value {
		Value::Array(a) => 1 + a.iter().map(depth).max().unwrap_or(0),
		Value::Object(o) => 1 + o.values().map(depth).max().unwrap_or(0),
		Value::Set(s) => 1 + s.iter().map(depth).max().unwrap_or(0),
		Value::RecordId(r) => 1 + key_depth(&r.key),
		Value::Range(r) => {
			let bound = |b: &Bound<Value>| match b {
				Bound::Unbounded => 0,
				Bound::Included(v) | Bound::Excluded(v) => depth(v),
			};
			1 + bound(&r.start).max(bound(&r.end))
		}
		_ => 0,
	}
}

/// Deterministic pseudo-random byte source (splitmix64).
struct SplitMix64(u64);

impl SplitMix64 {
	fn next(&mut self) -> u64 {
		self.0 = self.0.wrapping_add(0x9E3779B97F4A7C15);
		let mut z = self.0;
		z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
		z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
		z ^ (z >> 31)
	}

	fn fill(&mut self, buf: &mut [u8]) {
		for chunk in buf.chunks_mut(8) {
			let bytes = self.next().to_le_bytes();
			chunk.copy_from_slice(&bytes[..chunk.len()]);
		}
	}
}

#[test]
fn roundtrip_arbitrary_values() {
	let mut rng = SplitMix64(0x5EED_5EED_5EED_5EED);
	let mut buf = vec![0u8; 1 << 14];

	let mut tested = 0usize;
	for _ in 0..8192 {
		rng.fill(&mut buf);
		let mut u = Unstructured::new(&buf);
		let Ok(value) = Value::arbitrary(&mut u) else {
			continue;
		};
		if !supported(&value) || depth(&value) > 64 {
			continue;
		}

		let sql = value.to_sql();
		match surrealdb_sqon::from_str::<Value>(&sql) {
			Ok(parsed) => assert_eq!(
				parsed, value,
				"roundtrip mismatch: `{sql}` parsed to {parsed:?}, expected {value:?}"
			),
			Err(e) => panic!("failed to parse `{sql}` (formatted from {value:?}): {e}"),
		}
		tested += 1;
	}

	// Guard against the generator or the filter degenerating into skipping everything.
	assert!(tested > 500, "only {tested} generated values were tested");
}
