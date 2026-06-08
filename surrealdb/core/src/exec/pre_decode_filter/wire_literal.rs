//! Plan-time pre-encoded literals for byte-level predicate evaluation.
//!
//! Under rev-2 every [`Value`] wire is canonical (`u16 rev || tag || payload`):
//! two values are equal iff their wire bytes are equal. We exploit this by
//! pre-encoding the literal side of every predicate **once at plan time** so
//! the hot loop only does `memcmp` (eq), `memchr::memmem::find` (contains),
//! or a `HashMap` probe (set membership).
//!
//! Cross-type and cross-Number-variant operators (e.g. `Int(1) == Float(1.0)`)
//! cannot be decided by byte-eq; callers must fall back to full decode +
//! [`crate::fnc::operate`] for those. [`LiteralWire::Other`] and
//! [`LiteralSet::fallback`] carry the original [`Value`] so the fallback path
//! always has a decoded literal to compare against.

use std::collections::HashSet;
use std::ops::Range;
use std::sync::Arc;

use revision::SerializeRevisioned;

use crate::val::{Number, Value};

/// Inner [`Number`] discriminant on the rev-1 wire. Mirrors the
/// declaration order of [`crate::val::Number`] and is what
/// `<Number as SerializeRevisioned>` emits as a single varint byte for
/// values `< 251` (which the three variants we have today always satisfy).
///
/// Used by [`LiteralWire::Number`] to pre-extract the literal's
/// sub-variant at plan time so the wire comparator can short-circuit
/// same-sub-variant `Int` / `Decimal` byte mismatches as definitively
/// unequal instead of bailing to a full Value decode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum NumberSubVariant {
	Int = 0,
	Float = 1,
	Decimal = 2,
}

impl NumberSubVariant {
	#[inline]
	pub(crate) fn from_number(n: &Number) -> Self {
		match n {
			Number::Int(_) => NumberSubVariant::Int,
			Number::Float(_) => NumberSubVariant::Float,
			Number::Decimal(_) => NumberSubVariant::Decimal,
		}
	}

	/// Decode a sub-variant from a raw wire byte, returning [`None`] for
	/// any value not in `{0, 1, 2}`. Hardens against truncated /
	/// corrupted Number payloads — a non-matching byte means we can't
	/// claim same-sub-variant safely.
	#[inline]
	pub(crate) fn from_wire_byte(b: u8) -> Option<Self> {
		match b {
			0 => Some(NumberSubVariant::Int),
			1 => Some(NumberSubVariant::Float),
			2 => Some(NumberSubVariant::Decimal),
			_ => None,
		}
	}

	/// `true` if same-sub-variant byte equality decides value equality.
	/// `Int` and `Decimal` qualify (canonical-per-value byte encoding);
	/// `Float` doesn't (`+0.0 / -0.0` byte-differ but compare equal under
	/// `f64::PartialEq`, NaN bit patterns may compare either way).
	#[inline]
	pub(crate) fn byte_eq_decides_value_eq(self) -> bool {
		matches!(self, NumberSubVariant::Int | NumberSubVariant::Decimal)
	}
}

/// One predicate literal, optionally pre-encoded into rev-2 wire bytes.
#[derive(Debug, Clone)]
pub(crate) enum LiteralWire {
	/// `Value::String(Strand)` literal.
	///
	/// * `full_wire` — the full rev-2 `Value` wire (rev prefix + tag + Strand payload) for byte-eq
	///   against a navigated leaf's wire bytes.
	/// * `utf8_range` — sub-range into `full_wire` covering the raw UTF-8 needle bytes (no length
	///   prefix) for `memchr::memmem::find` substring checks. Reuses the same allocation rather
	///   than carrying a second `Vec<u8>` copy of the needle.
	Strand {
		full_wire: Vec<u8>,
		utf8_range: Range<usize>,
	},
	/// `Value::Number(_)` literal, pre-encoded as the full rev-2 `Value` wire.
	///
	/// Number byte-eq is variant-sensitive: `Int(1)` and `Float(1.0)` have
	/// distinct wire bytes despite being [`Value::equal`]-equal, so wire-eq
	/// only short-circuits when the runtime value's variant matches the
	/// literal's. The fallback path catches cross-variant cases.
	///
	/// `sub_variant_id` is the inner [`crate::val::Number`] discriminant
	/// (`0 = Int`, `1 = Float`, `2 = Decimal`), pre-extracted at plan time
	/// so the per-row wire comparator can peek the runtime value's
	/// sub-variant in one byte read and decide:
	///
	/// - **Different sub-variants** → bail to fallback decode (`Int(1)` ↔ `Float(1.0)`
	///   cross-variant equality needs `Number::PartialEq`).
	/// - **Same `Int` / `Decimal` sub-variant, byte match** → equal.
	/// - **Same `Int` / `Decimal` sub-variant, byte mismatch** → unequal (canonical-per-value byte
	///   representation).
	/// - **Same `Float` sub-variant** → bail to fallback regardless of byte equality: `+0.0` vs
	///   `-0.0` byte-differ but compare equal under `f64::PartialEq`, and `NaN` bit patterns may
	///   compare equal or unequal depending on the `Value::equal` arm. Cheap to defer the few
	///   Float-eq predicates and keep wire-eq correct.
	///
	/// `int_value` is the literal's `i64` body, pre-decoded at plan time
	/// for the wire-fast same-sub-variant **range** comparator
	/// (`<` / `<=` / `>` / `>=` against the runtime value). `Some` only
	/// for `Number::Int` literals; `None` for `Float` (NaN / ±0 makes
	/// total ordering unsafe via byte / `f64::PartialOrd` compare) and
	/// `Decimal` (the wire form isn't lexicographically ordered by its
	/// binary encoding — decode would be required, killing the
	/// wire-fast win). The wire comparator bails on `None` so the
	/// fallback path handles those sub-variants.
	Number {
		full_wire: Vec<u8>,
		sub_variant_id: NumberSubVariant,
		int_value: Option<i64>,
	},
	/// "Canonical byte-eq" literal: the full rev-2 `Value` wire **is** the
	/// equality criterion. Covers every variant where:
	///
	/// 1. Same-variant equality reduces to byte equality of the wire (the encoding has exactly one
	///    representation per `Value` instance — no normalisation pass at decode time).
	/// 2. No cross-variant equality arms exist in `Value::equal`.
	///
	/// `Inline` ([`Value::None`], [`Value::Null`]) and `Fixed(1)`
	/// ([`Value::Bool`]) qualify trivially: their full wire is 2–3 bytes
	/// and a different variant's wire has a different tag so byte-eq is
	/// definitively `false`. `Varlen` variants that qualify today —
	/// [`Value::Datetime`], [`Value::Uuid`], [`Value::Duration`] — share
	/// the same property: their inner representations are stored in a
	/// single canonical encoding (nanos, 16-byte UUID, secs+nanos
	/// respectively) and `PartialEq` is a thin wrapper over those bytes.
	///
	/// `Strand` (asymmetric with `Regex`) and `Number` (cross-sub-variant)
	/// are NOT covered here; they keep their dedicated variants.
	Canonical {
		full_wire: Vec<u8>,
	},
	/// `Value::Regex(_)` literal. The pre-compiled `regex::Regex` is kept
	/// alive in an `Arc` so cloned `PredNode`s share it. Eval matches the
	/// field's `Strand` value against the regex via a zero-copy `&str`
	/// view over the wire-borrowed UTF-8 bytes (see
	/// [`crate::exec::pre_decode_filter::wire_cmp::strand_payload_str_unchecked`]).
	Regex {
		regex: Arc<crate::val::Regex>,
	},
	/// Everything else — the wire-fast path doesn't apply. Callers retain
	/// the original [`Value`] separately (on `PredNode::Leaf::literal`,
	/// `FusedFlatClause::literal`, `ArrayElementContains::needle`) for the
	/// full-decode fallback, so no payload is carried here.
	Other,
}

impl LiteralWire {
	/// Pre-encode `lit` once. Falls into [`LiteralWire::Other`] for any
	/// variant the wire-fast path doesn't cover today; the rest go through
	/// the existing decode + `evidence_from_binary_cmp` fallback.
	pub(crate) fn from_value(lit: &Value) -> Self {
		match lit {
			Value::String(s) => {
				let mut full = Vec::new();
				lit.serialize_revisioned(&mut full)
					.expect("serialize Value::String never errors into Vec");
				// Locate the inner UTF-8 bytes inside `full_wire` so the
				// substring path doesn't need a second allocation. The
				// Strand needle's UTF-8 is the trailing `s.len()` bytes
				// of the full wire (`rev || u16 tag || u32_le len || varint
				// strand_len || utf8`). Computing this from
				// `full_wire.len() - utf8.len()` is robust against future
				// envelope-prefix changes.
				let utf8_len = s.as_str().len();
				let start = full.len() - utf8_len;
				let end = full.len();
				debug_assert_eq!(
					&full[start..end],
					s.as_str().as_bytes(),
					"Strand wire layout assumption violated",
				);
				LiteralWire::Strand {
					full_wire: full,
					utf8_range: start..end,
				}
			}
			Value::Number(n) => {
				let mut full = Vec::new();
				lit.serialize_revisioned(&mut full)
					.expect("serialize Value::Number never errors into Vec");
				let int_value = match n {
					Number::Int(i) => Some(*i),
					Number::Float(_) | Number::Decimal(_) => None,
				};
				LiteralWire::Number {
					full_wire: full,
					sub_variant_id: NumberSubVariant::from_number(n),
					int_value,
				}
			}
			Value::None
			| Value::Null
			| Value::Bool(_)
			| Value::Datetime(_)
			| Value::Uuid(_)
			| Value::Duration(_) => {
				let mut full = Vec::new();
				lit.serialize_revisioned(&mut full)
					.expect("serialize byte-eq-canonical Value never errors into Vec");
				LiteralWire::Canonical {
					full_wire: full,
				}
			}
			Value::Regex(r) => LiteralWire::Regex {
				regex: Arc::new(r.clone()),
			},
			_ => LiteralWire::Other,
		}
	}

	/// Borrow the inner UTF-8 needle bytes of a `Strand` literal — the
	/// substring search path consumes these as the `memchr::memmem` needle.
	/// Returns `None` for any other variant.
	pub(crate) fn strand_utf8(&self) -> Option<&[u8]> {
		match self {
			LiteralWire::Strand {
				full_wire,
				utf8_range,
			} => Some(&full_wire[utf8_range.clone()]),
			_ => None,
		}
	}
}

/// Partitioned literal set for `IN` / `NOT IN` and array-overlap evaluators.
///
/// Strand and Number elements have their full rev-2 Value wire bytes as keys
/// of dedicated `HashSet<Vec<u8>>` partitions — probing the right partition
/// from the navigated value's tag is one hash lookup with no decode. Compound
/// or non-byte-eq-safe elements (Object, Array, RecordId, ...) fall through
/// to `fallback`, which decoders use directly.
///
/// **Memory note:** elements are kept in both the caller-owned `HashSet<Value>`
/// (preserved on `PredNode::LeafSetMembership::set` for the fallback path)
/// and the partitioned form here. For `IN (...)` lists with thousands of
/// literals this roughly doubles the plan-time footprint of the set; that
/// trade buys per-row probes with zero allocation, which is the right call
/// for the hot scan path but is worth knowing if a future change wants to
/// drop the redundancy.
#[derive(Debug, Default, Clone)]
pub(crate) struct LiteralSet {
	pub(crate) strands: HashSet<Vec<u8>>,
	pub(crate) numbers: HashSet<Vec<u8>>,
	pub(crate) fallback: HashSet<Value>,
	/// True iff any element in `fallback` could equal-match a Strand-tagged
	/// value asymmetrically via `Value::equal` (today: `Value::Regex`). When
	/// set, `wire_cmp::wire_value_in_set` must hand a Strand-tag miss off to
	/// the decode-fallback path rather than answering `Some(false)`.
	strand_asymmetric: bool,
	/// Bitmask of [`NumberSubVariant`]s present in [`Self::numbers`]
	/// (bit 0 = `Int`, bit 1 = `Float`, bit 2 = `Decimal`).
	///
	/// `Number::PartialEq` is cross-variant — `Int(1) == Float(1.0) ==
	/// Decimal(1)` — and `Number::Hash` agrees, so `HashSet<Value>`
	/// collapses cross-variant duplicates to a single stored sub-variant
	/// (whichever was inserted first). Knowing exactly which
	/// sub-variants are in the set lets `wire_cmp::wire_value_in_set`
	/// decide a Number-tag miss precisely:
	///
	/// - **Runtime sub-variant matches a stored one AND is byte-canonical (`Int` / `Decimal`)** →
	///   byte miss is definitive value miss (`Some(false)`).
	/// - **Runtime sub-variant is `Float`** → defer to fallback (NaN / ±0 edge cases).
	/// - **Runtime sub-variant is *not* in the set's sub-variants AND the set holds *some*
	///   Number** → cross-variant value equality may still hold (`Number::PartialEq`), defer to
	///   fallback.
	number_sub_variants: u8,
}

impl LiteralSet {
	pub(crate) fn from_set(set: &HashSet<Value>) -> Self {
		let mut out = LiteralSet::default();
		for v in set {
			match v {
				Value::String(_) => {
					let mut full = Vec::new();
					v.serialize_revisioned(&mut full).expect("serialize Strand into Vec");
					out.strands.insert(full);
				}
				Value::Number(n) => {
					out.number_sub_variants |= 1u8 << (NumberSubVariant::from_number(n) as u8);
					let mut full = Vec::new();
					v.serialize_revisioned(&mut full).expect("serialize Number into Vec");
					out.numbers.insert(full);
				}
				Value::Regex(_) => {
					// Defense-in-depth only: the planner's
					// `literal_hashset_element_safe` (see
					// `pre_decode_filter::compile`) excludes `Value::Regex`
					// from the input to `try_build_inside_literal_hashset`,
					// so in production a `LeafSetMembership` never reaches
					// `LiteralSet::from_set` with a Regex element. This arm
					// (and the `strand_asymmetric` flag it sets) is reachable
					// today only from direct test construction. Do not
					// "simplify" it away without first relaxing the planner
					// filter — otherwise the runtime loses its safety net
					// against the Strand ↔ Regex asymmetric equality in
					// `Value::equal`.
					out.strand_asymmetric = true;
					out.fallback.insert(v.clone());
				}
				_ => {
					out.fallback.insert(v.clone());
				}
			}
		}
		out
	}

	/// True iff the [`Self::fallback`] partition holds any element that could
	/// equal-match a Strand-tagged value via `Value::equal`'s asymmetric arms.
	/// Today the only such element is `Value::Regex(...)`; the flag is set by
	/// [`Self::from_set`] when populating the partitions.
	pub(crate) fn has_strand_asymmetric_match(&self) -> bool {
		self.strand_asymmetric
	}

	/// Bitmask of [`NumberSubVariant`]s present in [`Self::numbers`]
	/// (bit `n` set iff a `Number` of sub-variant id `n` was inserted).
	#[inline]
	pub(crate) fn number_sub_variants_mask(&self) -> u8 {
		self.number_sub_variants
	}

	/// True iff [`Self::numbers`] holds more than one Number sub-variant
	/// (`Int` / `Float` / `Decimal`). When set, the cross-variant equality
	/// arms of `Number::PartialEq` matter and a wire-fast partition miss
	/// alone isn't decisive — callers must defer to the decode-fallback
	/// path. The runtime `wire_value_in_set` does a finer-grained check
	/// against the actual field sub-variant via
	/// [`Self::number_sub_variants_mask`]; this coarse predicate is kept
	/// for tests that only need the boolean.
	#[cfg(test)]
	#[inline]
	pub(crate) fn has_number_cross_variant(&self) -> bool {
		self.number_sub_variants.count_ones() > 1
	}

	/// Total element count across all partitions; matches `set.len()` for
	/// the original `HashSet<Value>` it was built from. Test-only helper —
	/// production callers reach into the individual partitions directly.
	#[cfg(test)]
	pub(crate) fn len(&self) -> usize {
		self.strands.len() + self.numbers.len() + self.fallback.len()
	}
}

#[cfg(test)]
mod tests {
	use std::collections::HashSet;

	use rust_decimal::Decimal;

	use super::*;
	use crate::val::Number;

	#[test]
	fn from_value_strand_round_trips_and_extracts_utf8() {
		let lit = Value::String("hello world".into());
		let built = LiteralWire::from_value(&lit);
		let LiteralWire::Strand {
			ref full_wire,
			ref utf8_range,
		} = built
		else {
			panic!("expected Strand variant");
		};
		// utf8_range slices into the full wire — no second allocation.
		assert_eq!(&full_wire[utf8_range.clone()], b"hello world");
		assert_eq!(built.strand_utf8(), Some(b"hello world".as_slice()));
		// `full_wire` must round-trip back to the original Value.
		let mut r: &[u8] = &full_wire[..];
		let decoded =
			<Value as revision::DeserializeRevisioned>::deserialize_revisioned(&mut r).unwrap();
		assert_eq!(decoded, lit);
	}

	#[test]
	fn from_value_strand_with_multi_byte_utf8_indexes_range_correctly() {
		// "🚀x" is 4+1 = 5 bytes of UTF-8. Ensure the utf8_range covers
		// exactly the trailing bytes (no off-by-one from the varint
		// length prefix's width).
		let lit = Value::String("🚀x".into());
		let built = LiteralWire::from_value(&lit);
		let LiteralWire::Strand {
			ref full_wire,
			ref utf8_range,
		} = built
		else {
			panic!("expected Strand variant");
		};
		assert_eq!(&full_wire[utf8_range.clone()], "🚀x".as_bytes());
	}

	#[test]
	fn from_value_number_round_trips() {
		let lit = Value::Number(Number::Int(42));
		let LiteralWire::Number {
			full_wire,
			sub_variant_id,
			int_value,
		} = LiteralWire::from_value(&lit)
		else {
			panic!("expected Number variant");
		};
		assert_eq!(sub_variant_id, NumberSubVariant::Int);
		assert_eq!(int_value, Some(42));
		let mut r: &[u8] = &full_wire;
		let decoded =
			<Value as revision::DeserializeRevisioned>::deserialize_revisioned(&mut r).unwrap();
		assert_eq!(decoded, lit);
	}

	#[test]
	fn from_value_number_float_has_no_int_value() {
		let lit = Value::Number(Number::Float(1.5));
		let LiteralWire::Number {
			sub_variant_id,
			int_value,
			..
		} = LiteralWire::from_value(&lit)
		else {
			panic!("expected Number variant");
		};
		assert_eq!(sub_variant_id, NumberSubVariant::Float);
		assert_eq!(int_value, None, "Float literals should not carry an int_value");
	}

	#[test]
	fn from_value_number_decimal_has_no_int_value() {
		use rust_decimal::Decimal;
		let lit = Value::Number(Number::Decimal(Decimal::from(42)));
		let LiteralWire::Number {
			sub_variant_id,
			int_value,
			..
		} = LiteralWire::from_value(&lit)
		else {
			panic!("expected Number variant");
		};
		assert_eq!(sub_variant_id, NumberSubVariant::Decimal);
		assert_eq!(int_value, None, "Decimal literals should not carry an int_value");
	}

	#[test]
	fn from_value_canonical_variants_round_trip() {
		use std::time::Duration as StdDuration;

		use chrono::{TimeZone, Utc};

		use crate::val::{Datetime, Duration as SurrealDuration, Uuid as SurrealUuid};

		let inputs: Vec<Value> = vec![
			Value::None,
			Value::Null,
			Value::Bool(true),
			Value::Bool(false),
			Value::Datetime(Datetime(Utc.with_ymd_and_hms(2024, 1, 2, 3, 4, 5).unwrap())),
			Value::Uuid(SurrealUuid(uuid::Uuid::from_u128(0xdead_beef))),
			Value::Duration(SurrealDuration(StdDuration::from_secs(42))),
		];
		for lit in inputs {
			match LiteralWire::from_value(&lit) {
				LiteralWire::Canonical {
					full_wire,
				} => {
					let mut r: &[u8] = &full_wire;
					let decoded =
						<Value as revision::DeserializeRevisioned>::deserialize_revisioned(&mut r)
							.unwrap();
					assert_eq!(decoded, lit, "round-trip mismatch for {lit:?}");
				}
				other => panic!("expected Canonical for {lit:?}, got {other:?}"),
			}
		}
	}

	#[test]
	fn from_value_compound_falls_back_to_other() {
		// Compound and unsupported leaf shapes still land on
		// `LiteralWire::Other` for the decode fallback.
		use std::collections::BTreeMap;

		use surrealdb_strand::Strand;
		let obj = Value::Object(crate::val::Object::from(BTreeMap::from([(
			Strand::from("k"),
			Value::Number(Number::Int(1)),
		)])));
		assert!(matches!(LiteralWire::from_value(&obj), LiteralWire::Other));
	}

	#[test]
	fn literal_set_partitions_correctly() {
		let mut set = HashSet::new();
		set.insert(Value::String("a".into()));
		set.insert(Value::String("b".into()));
		set.insert(Value::Number(Number::Int(1)));
		// Use a Decimal value (7) that doesn't collapse with Int(1) under
		// `Number::PartialEq` — picking `Decimal::from(1)` would dedupe.
		set.insert(Value::Number(Number::Decimal(Decimal::from(7))));
		set.insert(Value::Bool(true));
		let ls = LiteralSet::from_set(&set);
		assert_eq!(ls.len(), 5);
		assert_eq!(ls.strands.len(), 2);
		assert_eq!(ls.numbers.len(), 2);
		assert_eq!(ls.fallback.len(), 1);
		assert!(!ls.has_strand_asymmetric_match());
		// Int(1) and Decimal(7) are distinct sub-variants → cross-variant.
		assert!(ls.has_number_cross_variant());
	}

	#[test]
	fn literal_set_single_number_subvariant_is_not_cross_variant() {
		let mut set = HashSet::new();
		set.insert(Value::Number(Number::Int(1)));
		set.insert(Value::Number(Number::Int(2)));
		let ls = LiteralSet::from_set(&set);
		assert_eq!(ls.numbers.len(), 2);
		assert!(!ls.has_number_cross_variant());
	}

	#[test]
	fn literal_set_marks_number_cross_variant_with_int_and_float() {
		let mut set = HashSet::new();
		set.insert(Value::Number(Number::Int(1)));
		set.insert(Value::Number(Number::Float(2.5)));
		let ls = LiteralSet::from_set(&set);
		assert_eq!(ls.numbers.len(), 2);
		assert!(ls.has_number_cross_variant());
	}

	#[test]
	fn literal_set_compound_only_goes_to_fallback() {
		let mut set = HashSet::new();
		set.insert(Value::Bool(true));
		set.insert(Value::Bool(false));
		let ls = LiteralSet::from_set(&set);
		assert_eq!(ls.strands.len(), 0);
		assert_eq!(ls.numbers.len(), 0);
		assert_eq!(ls.fallback.len(), 2);
		assert!(!ls.has_strand_asymmetric_match());
	}

	#[test]
	fn literal_set_marks_strand_asymmetric_when_regex_present() {
		use std::str::FromStr;

		use crate::val::Regex;
		let mut set = HashSet::new();
		set.insert(Value::String("a".into()));
		set.insert(Value::Regex(Regex::from_str("/^x/").unwrap()));
		let ls = LiteralSet::from_set(&set);
		assert!(ls.has_strand_asymmetric_match());
		assert_eq!(ls.fallback.len(), 1);
	}
}
