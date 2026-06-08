//! Typed byte-level comparison for navigated [`Value`] wire bytes.
//!
//! Used by the pre-decode filter's leaf and set-membership paths to evaluate
//! the common cases (`Equal`/`NotEqual`/`ExactEqual`, `Contain`/`NotContain`
//! on Strand, set membership on Strand/Number) without decoding the leaf
//! [`Value`]. Returns [`None`] for any operator/type combination the wire
//! path doesn't cover so callers fall back to the existing decode +
//! [`crate::fnc::operate`] path.
//!
//! Strand wire form is canonical (`u16 rev=1 || varint len || pre-validated
//! UTF-8`), so byte-equality is value-equality. Number wire form is canonical
//! per variant — `Int(1)` and `Float(1.0)` have distinct wires despite being
//! `Value::equal`-equal, so cross-variant Number eq is left to the fallback
//! path.
//!
//! **Operator commutativity:** `Equal` / `NotEqual` / `ExactEqual` all
//! commute, so the planner's `reversed` flag never changes their truth value
//! and the wire branches simply ignore it. `Contain` / `NotContain` are
//! **not** commutative — under `reversed = true` the literal becomes the
//! haystack rather than the needle, which is not byte-comparable without
//! decoding the literal anew, so the wire path bails on that case.

use revision::optimised::tag::{SizeClass, Tag, read_tag};

use super::Evidence;
use super::wire_literal::{LiteralSet, LiteralWire, NumberSubVariant};
use crate::expr::operator::BinaryOperator;

/// rev-2 [`Value`] variant ids. Must match the declaration order in
/// `surrealdb/core/src/val/mod.rs`; guarded by an assert in the test module.
pub(super) mod variant_id {
	pub(crate) const NUMBER: u8 = 3;
	pub(crate) const STRING: u8 = 4;
	/// [`Value::Regex`] — a `Value::String(s)` can compare equal to a
	/// `Value::Regex(r)` (and vice versa) when the regex matches the string,
	/// see `Value::equal`. The wire-fast eq / set paths must therefore bail
	/// to the full-decode fallback whenever a Strand-tagged comparand meets
	/// a Regex peer, since byte-eq cannot decide that case.
	pub(crate) const REGEX: u8 = 16;
}

/// `(rev, tag)` peeked from a rev-2 [`Value`] wire. `bytes` is positioned at
/// the start of the payload (past the rev prefix and tag byte).
struct ValuePeek<'r> {
	tag: Tag,
	size_class: SizeClass,
	payload: &'r [u8],
}

/// Peek the rev prefix and optimised tag of a rev-2 [`Value`] wire without
/// advancing the original slice. Returns the consumed length and the parsed
/// header so callers can locate the payload.
fn peek_value(bytes: &[u8]) -> Option<ValuePeek<'_>> {
	let mut r: &[u8] = bytes;
	let rev = <u16 as revision::DeserializeRevisioned>::deserialize_revisioned(&mut r).ok()?;
	if rev != 2 {
		return None;
	}
	let tag = read_tag(&mut r).ok()?;
	let size_class = tag.size_class().ok()?;
	Some(ValuePeek {
		tag,
		size_class,
		payload: r,
	})
}

/// Peek the inner [`crate::val::Number`] sub-variant from a rev-2
/// [`Value::Number(_)`]'s already-located payload.
///
/// `peek.payload` for a `Varlen` `Value::Number(_)` is positioned at the
/// `u32_le payload_length` prefix; the inner `Number` rev-1 wire follows.
/// Under the default (non-`fixed-width-encoding`) varint codec the next
/// two values are both 1-byte varints (`u16` Number rev = `0x01`,
/// `u32` discriminant ∈ `{0, 1, 2}`), so `peek.payload[5]` is the
/// sub-variant id.
///
/// Returns [`None`] for truncated payloads or a non-canonical
/// sub-variant byte (so the caller bails to fallback rather than
/// claiming a same-sub-variant match against garbage).
///
/// **Wire-layout invariant.** The fixed offset assumes
/// `<u16 as DeserializeRevisioned>` emits a single byte for `rev = 1`
/// and `<u32 as DeserializeRevisioned>` emits a single byte for
/// discriminants `< 251`. Both hold under the default codec and the
/// three Number variants present today. A `debug_assert!` cross-checks
/// the offset against a proper deserialise in dev builds; if the
/// underlying varint shape ever changes the test suite will surface it
/// loudly.
#[inline]
fn peek_number_sub_variant(payload: &[u8]) -> Option<NumberSubVariant> {
	// 4 bytes u32_le payload_length + 1 byte Number rev + 1 byte discriminant.
	if payload.len() < 6 {
		return None;
	}
	let sv = NumberSubVariant::from_wire_byte(payload[5])?;
	#[cfg(debug_assertions)]
	{
		// Cross-check against a proper varint deserialise of the inner
		// Number wire — catches any future varint-encoding drift.
		let proper = (|| -> Option<u32> {
			let mut r: &[u8] = &payload[4..];
			let _rev =
				<u16 as revision::DeserializeRevisioned>::deserialize_revisioned(&mut r).ok()?;
			<u32 as revision::DeserializeRevisioned>::deserialize_revisioned(&mut r).ok()
		})();
		debug_assert_eq!(
			proper,
			Some(sv as u32),
			"peek_number_sub_variant: fast-path byte at offset 5 disagrees with varint decode",
		);
	}
	Some(sv)
}

/// Compare a navigated [`Value`] wire against a pre-encoded [`LiteralWire`]
/// for the given operator. Returns [`None`] to mean "wire path doesn't cover
/// this combination, decode and fall back."
pub(crate) fn evaluate_leaf_on_wire(
	value_bytes: &[u8],
	op: &BinaryOperator,
	lit: &LiteralWire,
	reversed: bool,
) -> Option<Evidence> {
	match op {
		// Commutative — `reversed` is irrelevant. See the operator note in
		// the module docstring.
		BinaryOperator::Equal | BinaryOperator::ExactEqual => {
			let truth = wire_eq(value_bytes, lit)?;
			Some(evidence_bool(truth))
		}
		BinaryOperator::NotEqual => {
			let truth = wire_eq(value_bytes, lit)?;
			Some(evidence_bool(!truth))
		}
		BinaryOperator::Contain | BinaryOperator::NotContain => {
			// Non-commutative. `field CONTAIN literal` makes the literal
			// the needle and the field's value the haystack — which is
			// what the wire-fast Strand-vs-Strand path below handles.
			// `reversed = true` would mean `literal CONTAIN field`, which
			// flips the roles and requires a haystack on the literal side;
			// we don't have one pre-encoded, so bail to the decode path.
			//
			// Array `CONTAIN` element is compiled to `ArrayElementContains`
			// (see `compile.rs`), not `PredNode::Leaf`, so we never see it
			// here in the `Contain` arm.
			if reversed {
				return None;
			}
			let utf8 = lit.strand_utf8()?;
			let peek = peek_value(value_bytes)?;
			if peek.tag.variant_id() != variant_id::STRING
				|| !matches!(peek.size_class, SizeClass::Varlen)
			{
				return None;
			}
			// Past the rev+tag+u32_le-length envelope, the payload is the
			// Strand wire: `<varint len> || utf8`. Read the length, then
			// run `memmem` on the inline utf8 slice.
			let payload = varlen_payload_after_u32_len(peek.payload)?;
			let mut r: &[u8] = payload;
			let len =
				<usize as revision::DeserializeRevisioned>::deserialize_revisioned(&mut r).ok()?;
			if r.len() < len {
				return None;
			}
			let haystack = &r[..len];
			let hit = memchr::memmem::find(haystack, utf8).is_some();
			let truth = match op {
				BinaryOperator::Contain => hit,
				BinaryOperator::NotContain => !hit,
				_ => unreachable!(),
			};
			Some(evidence_bool(truth))
		}
		BinaryOperator::LessThan
		| BinaryOperator::LessThanEqual
		| BinaryOperator::MoreThan
		| BinaryOperator::MoreThanEqual => {
			// Range comparisons are wire-fast only for same-sub-variant
			// `Int` Number predicates today. The literal's `i64` body is
			// pre-decoded in `LiteralWire::Number::int_value`; the
			// runtime value's `i64` is varint-deserialised once per row.
			// `Float` bails (NaN / `±0.0` total-order issues) and
			// `Decimal` bails (wire encoding isn't lexicographically
			// ordered; decode would be required, killing the win).
			// Anything else (Strand / Object / cross-tag / cross-
			// sub-variant Number) bails to the existing fallback path,
			// which `Number::PartialOrd` resolves correctly.
			let order = wire_cmp_int_number(value_bytes, lit)?;
			// The planner's `reversed` flag swaps the literal/field
			// roles (e.g. `5 > field` carries `op = MoreThan, reversed
			// = true` instead of being normalised to `field < 5`).
			// `wire_cmp_int_number` returns the field-vs-literal
			// ordering; if reversed, flip to literal-vs-field by
			// reversing the `Ordering`.
			let order = if reversed {
				order.reverse()
			} else {
				order
			};
			let truth = match op {
				BinaryOperator::LessThan => order == std::cmp::Ordering::Less,
				BinaryOperator::LessThanEqual => order != std::cmp::Ordering::Greater,
				BinaryOperator::MoreThan => order == std::cmp::Ordering::Greater,
				BinaryOperator::MoreThanEqual => order != std::cmp::Ordering::Less,
				_ => unreachable!(),
			};
			Some(evidence_bool(truth))
		}
		_ => None,
	}
}

/// Compare a navigated [`Value`] wire to a `Number::Int` literal as
/// `i64` and return the [`Ordering`](std::cmp::Ordering) of
/// `field <=> literal`.
///
/// Returns [`None`] (bail to fallback decode) for any case the wire
/// path doesn't cover:
///
/// - Literal isn't [`LiteralWire::Number`] with a `Some(int_value)` → range only fast-paths `Int`
///   literals.
/// - Runtime value's outer tag isn't `NUMBER` → cross-type comparisons need `Value`'s ordering
///   semantics.
/// - Runtime value's Number sub-variant isn't `Int` → cross-sub-variant ordering needs
///   `Number::PartialOrd`.
/// - Runtime value's body is truncated or won't decode.
fn wire_cmp_int_number(value_bytes: &[u8], lit: &LiteralWire) -> Option<std::cmp::Ordering> {
	let LiteralWire::Number {
		sub_variant_id,
		int_value,
		..
	} = lit
	else {
		return None;
	};
	if *sub_variant_id != NumberSubVariant::Int {
		return None;
	}
	let lit_i64 = (*int_value)?;
	let peek = peek_value(value_bytes)?;
	if peek.tag.variant_id() != variant_id::NUMBER {
		return None;
	}
	let field_sv = peek_number_sub_variant(peek.payload)?;
	if field_sv != NumberSubVariant::Int {
		return None;
	}
	// Skip past the `u32_le` payload length, `u16` Number rev, and
	// `u32` discriminant to reach the `i64` body bytes. The Number rev
	// (`0x01`) and the `Int` discriminant (`0x00`) are each a single
	// varint byte under the default codec, so the body starts at
	// `peek.payload[6]`. Use `i64::deserialize_revisioned` to honour
	// the ZigZag-varint encoding rather than assuming a fixed width.
	if peek.payload.len() < 6 {
		return None;
	}
	let mut body: &[u8] = &peek.payload[6..];
	let field_i64 =
		<i64 as revision::DeserializeRevisioned>::deserialize_revisioned(&mut body).ok()?;
	Some(field_i64.cmp(&lit_i64))
}

/// Probe `value_bytes` against a [`LiteralSet`]. Returns `Some(true)` /
/// `Some(false)` only when the value's wire tag dispatches to a wire-fast
/// partition (Strand → `set.strands`, Number → `set.numbers`); otherwise
/// returns [`None`] so the caller falls back to full decode + `set.contains`.
///
/// **Strand asymmetric-equality safety:** when the set has any element that
/// could equal-match a Strand-tagged value via `Value::equal`'s asymmetric
/// arms (today: `Value::Regex`), the Strand partition probe can only soundly
/// answer `Some(true)` — a miss must fall back so the regex elements get
/// their chance. See [`LiteralSet::has_strand_asymmetric_match`].
///
/// **Number cross-variant safety:** `Number::PartialEq` treats
/// `Int(1) == Float(1.0) == Decimal(1)` and `Number::Hash` agrees, so a
/// `HashSet<Value>` containing those entries collapses to a single stored
/// sub-variant per equivalence class. The wire-fast path inspects the
/// runtime value's Number sub-variant against the set's stored
/// sub-variants (via [`LiteralSet::number_sub_variants_mask`]):
///
/// - Runtime sub-variant is the **only** one stored AND is byte-canonical (`Int` / `Decimal`) →
///   byte miss is definitive value miss.
/// - Runtime sub-variant is `Float` → defer to fallback (`+0.0` ↔ `-0.0` and NaN edge cases).
/// - Runtime sub-variant is **not** in the set's mask but the mask is non-empty → cross-variant
///   equality may still hold under `Number::PartialEq`, defer to fallback.
pub(crate) fn wire_value_in_set(value_bytes: &[u8], set: &LiteralSet) -> Option<bool> {
	let peek = peek_value(value_bytes)?;
	match peek.tag.variant_id() {
		variant_id::STRING => {
			if set.strands.contains(value_bytes) {
				return Some(true);
			}
			// On a miss, hand off to the decode path if the set has any
			// element that could equal-match a Strand asymmetrically.
			if set.has_strand_asymmetric_match() {
				return None;
			}
			Some(false)
		}
		variant_id::NUMBER => {
			// Empty Number partition: no stored entry can match. Skip the
			// sub-variant peek and mask computation. Mirrors the implicit
			// short-circuit in the Strand arm (where `strand_asymmetric =
			// false` collapses to `Some(false)` after a hash miss on an
			// empty `strands`).
			if set.numbers.is_empty() {
				return Some(false);
			}
			if set.numbers.contains(value_bytes) {
				return Some(true);
			}
			// Byte miss on the numbers partition. Decide between
			// `Some(false)` and a fallback by inspecting the runtime
			// sub-variant against the set's stored sub-variants.
			let mask = set.number_sub_variants_mask();
			let field_sv = peek_number_sub_variant(peek.payload)?;
			let field_bit = 1u8 << (field_sv as u8);
			// Cross-variant ambiguity: the field's sub-variant is absent
			// from the set, but the set holds *some* other sub-variant
			// that could be cross-variant `==`. Defer to fallback.
			if (mask & field_bit) == 0 {
				return None;
			}
			// Field sub-variant matches a stored sub-variant. Float byte
			// miss is still inconclusive (NaN / ±0); Int / Decimal are
			// canonical-per-value so a hash miss is a value miss.
			if !field_sv.byte_eq_decides_value_eq() {
				return None;
			}
			// Also defer if the set mixes sub-variants — even if the
			// field's sub-variant is byte-canonical, a stored
			// *other*-sub-variant entry could still be cross-variant
			// `==` to the field.
			if mask & !field_bit != 0 {
				return None;
			}
			Some(false)
		}
		_ => None,
	}
}

/// Byte-equality between a navigated [`Value`] wire and an array element
/// candidate (for `ArrayElementContains`).
///
/// Returns [`None`] when the wire path doesn't cover the pair shape (caller
/// falls back to full element decode + `Value::eq`).
pub(crate) fn eq_wire(value_bytes: &[u8], lit: &LiteralWire) -> Option<bool> {
	wire_eq(value_bytes, lit)
}

fn wire_eq(value_bytes: &[u8], lit: &LiteralWire) -> Option<bool> {
	match lit {
		LiteralWire::Strand {
			full_wire,
			utf8_range: _,
		} => {
			let peek = peek_value(value_bytes)?;
			let vid = peek.tag.variant_id();
			if vid == variant_id::STRING {
				return Some(value_bytes == full_wire.as_slice());
			}
			// `Value::equal` treats `String <-> Regex` asymmetrically: a
			// `Value::Regex(r)` compares equal to a `Value::String(s)` iff
			// `r.is_match(s)`. Byte-eq cannot decide that, so fall back to
			// the decode path for Regex peers.
			if vid == variant_id::REGEX {
				return None;
			}
			// Strand has no other cross-variant equality under
			// `Value::equal`; everything else is definitively unequal.
			Some(false)
		}
		LiteralWire::Number {
			full_wire,
			sub_variant_id,
			int_value: _,
		} => {
			let peek = peek_value(value_bytes)?;
			if peek.tag.variant_id() != variant_id::NUMBER {
				// Number has no cross-variant equality under `Value::equal`
				// against non-Number variants — answering false here is
				// safe regardless of the peer.
				return Some(false);
			}
			// Peek the runtime value's Number sub-variant. If it differs
			// from the literal's, the two could still be value-equal under
			// `Number::PartialEq` (`Int(1) == Float(1.0) == Decimal(1)`),
			// so bail to fallback.
			let field_sv = peek_number_sub_variant(peek.payload)?;
			if field_sv != *sub_variant_id {
				return None;
			}
			// Same sub-variant. `Float` byte equality is unreliable
			// (NaN bit-equal but ≠ under `f64::PartialEq`; `+0.0` ↔ `-0.0`
			// byte-differ but `==` under `f64::PartialEq`). Cheap to defer
			// the few Float-eq predicates to the full decode and keep the
			// wire-fast path correct.
			if !field_sv.byte_eq_decides_value_eq() {
				return None;
			}
			// Int / Decimal: byte form is canonical per value.
			Some(value_bytes == full_wire.as_slice())
		}
		LiteralWire::Canonical {
			full_wire,
		} => {
			// "Canonical byte-eq" literal: see [`LiteralWire::Canonical`]
			// for the closure invariant. The full rev-2 wire is the
			// equality criterion; same-variant matches byte-eq, different
			// variants byte-mismatch (different tag → different wire).
			Some(value_bytes == full_wire.as_slice())
		}
		LiteralWire::Regex {
			regex,
		} => {
			// `Value::equal(Strand(s), Regex(r))` is asymmetric — true iff
			// `r.is_match(s)`. Peek the field's wire: if it's a Strand,
			// borrow its UTF-8 payload as `&str` (via the unchecked-utf8
			// invariant — see [`strand_payload_str_unchecked`]) and run
			// the pre-compiled regex against it.
			//
			// Other variant tags fall back to the decode path: Regex-vs-
			// Regex equality compares the underlying pattern strings,
			// which we don't reconstruct here; Number / Bool / etc. need
			// the full `Value::equal` arms.
			let peek = peek_value(value_bytes)?;
			if peek.tag.variant_id() != variant_id::STRING {
				return None;
			}
			let s = strand_payload_str_unchecked(value_bytes)?;
			Some(regex.inner().is_match(s))
		}
		LiteralWire::Other => None,
	}
}

/// Read the `u32_le` length prefix of a rev-2 `Varlen` Value payload and
/// return the inner body slice it points to.
///
/// After [`peek_value`] consumes rev + tag, `peek.payload` for a `Varlen`
/// variant points at the `u32_le payload_length` prefix; this helper reads
/// the length and returns the next `length` bytes — i.e. the variant body.
/// Generic over any `Varlen` variant (Strand, Number, Object, ...), not just
/// Strand.
fn varlen_payload_after_u32_len(payload: &[u8]) -> Option<&[u8]> {
	if payload.len() < 4 {
		return None;
	}
	let len = u32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]]) as usize;
	let rest = &payload[4..];
	if rest.len() < len {
		return None;
	}
	Some(&rest[..len])
}

/// Borrow the UTF-8 bytes inside a rev-2 `Value::String(Strand)` wire as
/// `&str` without re-validating UTF-8.
///
/// `value_bytes` must point at the start of a `Value::String(Strand)` wire
/// (rev prefix + variant tag + `u32_le` envelope length + Strand body where
/// the Strand body is `<varint len> || utf8`). On success returns the
/// borrowed `&str` view of the inner UTF-8 bytes (no length prefix);
/// returns `None` on wire-shape mismatch (wrong tag, truncated lengths).
///
/// # Safety invariant
///
/// The Strand wire's UTF-8 payload is **pre-validated at serialise time**:
/// every path that produces a `Strand` (parser, decoder, `Strand::from`)
/// goes through `str::from_utf8` or similar before writing the wire bytes,
/// and the wire layout has no normalisation pass at decode time. So the
/// bytes returned by [`varlen_payload_after_u32_len`] for a `String`
/// variant are guaranteed to be valid UTF-8 by the surrealdb invariant.
///
/// `from_utf8_unchecked` skips the per-byte validation that
/// `from_utf8(...).unwrap()` would perform — a non-trivial saving for
/// long Strand values in the scan hot path.
///
/// In debug builds the `debug_assert!` surfaces any wire-validation
/// regression immediately; in release builds the check is elided.
pub(crate) fn strand_payload_str_unchecked(value_bytes: &[u8]) -> Option<&str> {
	let peek = peek_value(value_bytes)?;
	if peek.tag.variant_id() != variant_id::STRING || !matches!(peek.size_class, SizeClass::Varlen)
	{
		return None;
	}
	let strand_wire = varlen_payload_after_u32_len(peek.payload)?;
	let mut r: &[u8] = strand_wire;
	let len = <usize as revision::DeserializeRevisioned>::deserialize_revisioned(&mut r).ok()?;
	if r.len() < len {
		return None;
	}
	let utf8 = &r[..len];
	debug_assert!(
		std::str::from_utf8(utf8).is_ok(),
		"strand_payload_str_unchecked: rev-2 invariant violated — \
		 Strand wire bytes are not valid UTF-8",
	);
	// SAFETY: see the "Safety invariant" section in the docstring.
	// Strand wire bytes are pre-validated UTF-8 by every writer; the
	// rev-2 envelope has no decode-time normalisation. The debug assert
	// catches any regression in writer-side validation. Lifetime of the
	// returned `&str` is tied to `value_bytes`, which the caller owns.
	Some(unsafe { std::str::from_utf8_unchecked(utf8) })
}

#[inline]
fn evidence_bool(b: bool) -> Evidence {
	if b {
		Evidence::ProvablyTrue
	} else {
		Evidence::ProvablyFalse
	}
}

#[cfg(test)]
mod tests {
	use revision::SerializeRevisioned;
	use surrealdb_strand::Strand;

	use super::*;
	use crate::val::{Number, Value};

	fn encoded(v: &Value) -> Vec<u8> {
		let mut out = Vec::new();
		v.serialize_revisioned(&mut out).unwrap();
		out
	}

	#[test]
	fn variant_ids_track_value_declaration_order() {
		use std::str::FromStr;

		use crate::val::Regex;
		let s = encoded(&Value::String(Strand::from("x")));
		let n = encoded(&Value::Number(Number::Int(1)));
		let r = encoded(&Value::Regex(Regex::from_str("/x/").unwrap()));
		assert_eq!(Tag(s[1]).variant_id(), variant_id::STRING);
		assert_eq!(Tag(n[1]).variant_id(), variant_id::NUMBER);
		assert_eq!(Tag(r[1]).variant_id(), variant_id::REGEX);
	}

	/// Static-shape guard for [`peek_number_sub_variant`]. The fast path reads
	/// the Number sub-variant discriminant at the hardcoded offset
	/// `payload[5]` (= 4 bytes `u32_le` payload length + 1 byte `u16` Number
	/// rev + 0 byte offset into the `u32` discriminant). That offset is
	/// only correct under the default varint codec, where `u16 = 1` and
	/// `u32 < 251` each serialise to a single byte. If the `revision` crate
	/// is ever built with `fixed-width-encoding` (directly or transitively),
	/// those widths change to 2 and 4 bytes respectively and the hardcoded
	/// offset reads garbage. This test fails loudly under that scenario.
	#[test]
	fn number_sub_variant_offset_invariant_holds() {
		// Inner Number wire = `<u16 rev=1> || <u32 discriminant> || <body>`.
		let mut u16_buf = Vec::new();
		<u16 as revision::SerializeRevisioned>::serialize_revisioned(&1u16, &mut u16_buf).unwrap();
		assert_eq!(
			u16_buf.len(),
			1,
			"u16=1 must serialise to a single varint byte; `fixed-width-encoding` would \
			 break peek_number_sub_variant's hardcoded payload[5] offset",
		);
		for disc in [0u32, 1, 2, 250] {
			let mut u32_buf = Vec::new();
			<u32 as revision::SerializeRevisioned>::serialize_revisioned(&disc, &mut u32_buf)
				.unwrap();
			assert_eq!(
				u32_buf.len(),
				1,
				"u32={disc} must serialise to a single varint byte; \
				 `fixed-width-encoding` would break peek_number_sub_variant",
			);
		}
	}

	#[test]
	fn wire_eq_strand_matches() {
		let lit = LiteralWire::from_value(&Value::String("abc".into()));
		let v = encoded(&Value::String("abc".into()));
		assert_eq!(wire_eq(&v, &lit), Some(true));
	}

	#[test]
	fn wire_eq_strand_mismatch_same_variant() {
		let lit = LiteralWire::from_value(&Value::String("abc".into()));
		let v = encoded(&Value::String("abd".into()));
		assert_eq!(wire_eq(&v, &lit), Some(false));
	}

	#[test]
	fn wire_eq_strand_vs_non_strand_non_regex_returns_false() {
		let lit = LiteralWire::from_value(&Value::String("abc".into()));
		let v = encoded(&Value::Number(Number::Int(1)));
		assert_eq!(wire_eq(&v, &lit), Some(false));
	}

	/// Regression: `Value::String(s) == Value::Regex(r)` is `true` when the
	/// regex matches the string (see `Value::equal`). The wire path cannot
	/// decide this without invoking the regex engine, so it must bail to the
	/// decode fallback. Confirms a Strand literal against a Regex-tagged
	/// field returns `None`, not `Some(false)`.
	#[test]
	fn wire_eq_strand_vs_regex_bails_to_none() {
		use std::str::FromStr;

		use crate::val::Regex;
		let lit = LiteralWire::from_value(&Value::String("abcdef".into()));
		let v = encoded(&Value::Regex(Regex::from_str("^abc").unwrap()));
		assert_eq!(wire_eq(&v, &lit), None);
	}

	#[test]
	fn evaluate_leaf_eq_strand_vs_regex_bails_to_none() {
		use std::str::FromStr;

		use crate::val::Regex;
		let lit = LiteralWire::from_value(&Value::String("abcdef".into()));
		let v = encoded(&Value::Regex(Regex::from_str("^abc").unwrap()));
		assert_eq!(evaluate_leaf_on_wire(&v, &BinaryOperator::Equal, &lit, false), None);
	}

	#[test]
	fn wire_eq_number_same_variant_matches() {
		let lit = LiteralWire::from_value(&Value::Number(Number::Int(42)));
		let v = encoded(&Value::Number(Number::Int(42)));
		assert_eq!(wire_eq(&v, &lit), Some(true));
	}

	#[test]
	fn wire_eq_number_cross_variant_bails_to_none() {
		// Int(1) and Float(1.0) compare equal under Value::eq but encode
		// differently — wire-eq must return None so the caller falls back.
		let lit = LiteralWire::from_value(&Value::Number(Number::Int(1)));
		let v = encoded(&Value::Number(Number::Float(1.0)));
		assert_eq!(wire_eq(&v, &lit), None);
	}

	#[test]
	fn evaluate_leaf_on_wire_eq_strand_provably_true() {
		let lit = LiteralWire::from_value(&Value::String("abc".into()));
		let v = encoded(&Value::String("abc".into()));
		assert_eq!(
			evaluate_leaf_on_wire(&v, &BinaryOperator::Equal, &lit, false),
			Some(Evidence::ProvablyTrue)
		);
	}

	#[test]
	fn evaluate_leaf_on_wire_ne_strand_provably_false() {
		let lit = LiteralWire::from_value(&Value::String("abc".into()));
		let v = encoded(&Value::String("abc".into()));
		assert_eq!(
			evaluate_leaf_on_wire(&v, &BinaryOperator::NotEqual, &lit, false),
			Some(Evidence::ProvablyFalse)
		);
	}

	#[test]
	fn evaluate_leaf_on_wire_contain_strand_hit() {
		let lit = LiteralWire::from_value(&Value::String("ell".into()));
		let v = encoded(&Value::String("hello world".into()));
		assert_eq!(
			evaluate_leaf_on_wire(&v, &BinaryOperator::Contain, &lit, false),
			Some(Evidence::ProvablyTrue)
		);
	}

	#[test]
	fn evaluate_leaf_on_wire_contain_strand_miss() {
		let lit = LiteralWire::from_value(&Value::String("zzz".into()));
		let v = encoded(&Value::String("hello world".into()));
		assert_eq!(
			evaluate_leaf_on_wire(&v, &BinaryOperator::Contain, &lit, false),
			Some(Evidence::ProvablyFalse)
		);
	}

	#[test]
	fn evaluate_leaf_on_wire_unsupported_op_returns_none() {
		// Pick an operator the wire path doesn't cover for the chosen
		// type pair. `Outside` (the rev-1 inside-fence inversion) has no
		// wire-fast Number arm — `evaluate_leaf_on_wire` returns
		// `None` from the catch-all branch so the caller falls back to
		// the decode path. (`<` / `<=` / `>` / `>=` against an `Int`
		// literal are now covered — see
		// `wire_cmp_int_number_*` tests below.)
		let lit = LiteralWire::from_value(&Value::Number(Number::Int(1)));
		let v = encoded(&Value::Number(Number::Int(1)));
		assert_eq!(evaluate_leaf_on_wire(&v, &BinaryOperator::Outside, &lit, false), None);
	}

	#[test]
	fn wire_value_in_set_strand_partition() {
		use std::collections::HashSet;
		let mut s = HashSet::new();
		s.insert(Value::String("a".into()));
		s.insert(Value::String("b".into()));
		let ls = LiteralSet::from_set(&s);
		let in_a = encoded(&Value::String("a".into()));
		let out = encoded(&Value::String("z".into()));
		assert_eq!(wire_value_in_set(&in_a, &ls), Some(true));
		assert_eq!(wire_value_in_set(&out, &ls), Some(false));
	}

	#[test]
	fn wire_value_in_set_number_partition() {
		use std::collections::HashSet;
		let mut s = HashSet::new();
		s.insert(Value::Number(Number::Int(1)));
		s.insert(Value::Number(Number::Int(2)));
		let ls = LiteralSet::from_set(&s);
		let in_v = encoded(&Value::Number(Number::Int(2)));
		let out = encoded(&Value::Number(Number::Int(9)));
		assert_eq!(wire_value_in_set(&in_v, &ls), Some(true));
		// All entries are `Int`, so a miss is definitive.
		assert_eq!(wire_value_in_set(&out, &ls), Some(false));
	}

	/// Regression: `Number::PartialEq` collapses `Int(1) == Float(1.0) ==
	/// Decimal(1)`. A `HashSet<Value>` built from `{Float(1.0)}` would
	/// answer `set.contains(&Int(1))` as `true` via the cross-variant
	/// arms in `Number::PartialEq`. The wire partition only stores the
	/// `Float(1.0)` bytes, so a runtime `Int(1)` byte-misses — but the
	/// semantic answer is still `true`. The wire path must therefore
	/// return `None` on a miss whenever the partition mixes sub-variants,
	/// deferring to the decode fallback for the cross-variant resolution.
	#[test]
	fn wire_value_in_set_number_miss_with_cross_variant_returns_none() {
		use std::collections::HashSet;
		let mut s = HashSet::new();
		s.insert(Value::Number(Number::Int(1)));
		s.insert(Value::Number(Number::Float(2.5)));
		let ls = LiteralSet::from_set(&s);
		assert!(ls.has_number_cross_variant());
		// `Float(1.0)` is not stored bytewise but is value-equal to the
		// stored `Int(1)`; wire path can't decide, must defer.
		let v = encoded(&Value::Number(Number::Float(1.0)));
		assert_eq!(wire_value_in_set(&v, &ls), None);
	}

	/// Direct hit on the Number partition stays definitive even when the
	/// set mixes sub-variants.
	#[test]
	fn wire_value_in_set_number_hit_with_cross_variant_is_definitive() {
		use std::collections::HashSet;
		let mut s = HashSet::new();
		s.insert(Value::Number(Number::Int(1)));
		s.insert(Value::Number(Number::Float(2.5)));
		let ls = LiteralSet::from_set(&s);
		// Exact byte match against the stored `Int(1)` entry.
		let v = encoded(&Value::Number(Number::Int(1)));
		assert_eq!(wire_value_in_set(&v, &ls), Some(true));
	}

	#[test]
	fn wire_value_in_set_unsupported_variant_returns_none() {
		use std::collections::HashSet;
		let mut s = HashSet::new();
		s.insert(Value::String("a".into()));
		let ls = LiteralSet::from_set(&s);
		let v = encoded(&Value::Bool(true));
		assert_eq!(wire_value_in_set(&v, &ls), None);
	}

	/// Regression: a literal set mixing Strand and Regex must hand the
	/// Strand-tag miss off to the decode fallback so the regex element
	/// gets its chance at matching the value. Confirms the wire path no
	/// longer returns `Some(false)` in that case.
	#[test]
	fn wire_value_in_set_strand_miss_with_regex_element_returns_none() {
		use std::collections::HashSet;
		use std::str::FromStr;

		use crate::val::Regex;
		let mut s = HashSet::new();
		s.insert(Value::String("known".into()));
		s.insert(Value::Regex(Regex::from_str("^abc").unwrap()));
		let ls = LiteralSet::from_set(&s);
		assert!(ls.has_strand_asymmetric_match());
		// "abcdef" is not in the Strand partition but matches the regex —
		// the wire path can't decide, so it must defer to the decode
		// fallback.
		let v = encoded(&Value::String("abcdef".into()));
		assert_eq!(wire_value_in_set(&v, &ls), None);
	}

	/// Direct hit on the Strand partition stays definitive even when the
	/// set has a Regex peer.
	#[test]
	fn wire_value_in_set_strand_hit_with_regex_element_is_definitive() {
		use std::collections::HashSet;
		use std::str::FromStr;

		use crate::val::Regex;
		let mut s = HashSet::new();
		s.insert(Value::String("known".into()));
		s.insert(Value::Regex(Regex::from_str("^abc").unwrap()));
		let ls = LiteralSet::from_set(&s);
		let v = encoded(&Value::String("known".into()));
		assert_eq!(wire_value_in_set(&v, &ls), Some(true));
	}

	#[test]
	fn eq_wire_strand_array_element() {
		// Shape used by ArrayElementContains: per-element wire bytes vs.
		// pre-encoded needle.
		let lit = LiteralWire::from_value(&Value::String("foo".into()));
		let v = encoded(&Value::String("foo".into()));
		assert_eq!(eq_wire(&v, &lit), Some(true));
	}

	// ------------------------------------------------------------------
	// Canonical byte-eq: None / Null / Bool / Datetime / Uuid / Duration
	// ------------------------------------------------------------------

	#[test]
	fn wire_eq_canonical_none_matches_none() {
		let lit = LiteralWire::from_value(&Value::None);
		assert!(matches!(lit, LiteralWire::Canonical { .. }));
		let v = encoded(&Value::None);
		assert_eq!(wire_eq(&v, &lit), Some(true));
	}

	#[test]
	fn wire_eq_canonical_null_matches_null() {
		let lit = LiteralWire::from_value(&Value::Null);
		let v = encoded(&Value::Null);
		assert_eq!(wire_eq(&v, &lit), Some(true));
	}

	#[test]
	fn wire_eq_canonical_bool_true_matches_bool_true() {
		let lit = LiteralWire::from_value(&Value::Bool(true));
		let v = encoded(&Value::Bool(true));
		assert_eq!(wire_eq(&v, &lit), Some(true));
	}

	#[test]
	fn wire_eq_canonical_bool_true_does_not_match_bool_false() {
		let lit = LiteralWire::from_value(&Value::Bool(true));
		let v = encoded(&Value::Bool(false));
		assert_eq!(wire_eq(&v, &lit), Some(false));
	}

	#[test]
	fn wire_eq_canonical_null_does_not_match_none() {
		// `Value::None` and `Value::Null` are distinct variants under
		// `Value::equal`; their tags differ on the wire so byte-eq returns
		// `Some(false)`, matching the semantic answer.
		let lit = LiteralWire::from_value(&Value::Null);
		let v = encoded(&Value::None);
		assert_eq!(wire_eq(&v, &lit), Some(false));
	}

	#[test]
	fn wire_eq_canonical_null_does_not_match_string() {
		let lit = LiteralWire::from_value(&Value::Null);
		let v = encoded(&Value::String("anything".into()));
		assert_eq!(wire_eq(&v, &lit), Some(false));
	}

	#[test]
	fn wire_eq_canonical_bool_does_not_match_number() {
		let lit = LiteralWire::from_value(&Value::Bool(true));
		let v = encoded(&Value::Number(Number::Int(1)));
		assert_eq!(wire_eq(&v, &lit), Some(false));
	}

	#[test]
	fn wire_eq_canonical_datetime_matches_same_instant() {
		use chrono::{TimeZone, Utc};

		use crate::val::Datetime;
		let dt = Datetime(Utc.with_ymd_and_hms(2024, 5, 21, 0, 0, 0).unwrap());
		let lit = LiteralWire::from_value(&Value::Datetime(dt));
		let v = encoded(&Value::Datetime(dt));
		assert_eq!(wire_eq(&v, &lit), Some(true));
	}

	#[test]
	fn wire_eq_canonical_datetime_mismatches_different_instant() {
		use chrono::{TimeZone, Utc};

		use crate::val::Datetime;
		let a = Datetime(Utc.with_ymd_and_hms(2024, 5, 21, 0, 0, 0).unwrap());
		let b = Datetime(Utc.with_ymd_and_hms(2024, 5, 21, 0, 0, 1).unwrap());
		let lit = LiteralWire::from_value(&Value::Datetime(a));
		let v = encoded(&Value::Datetime(b));
		assert_eq!(wire_eq(&v, &lit), Some(false));
	}

	#[test]
	fn wire_eq_canonical_uuid_matches_and_mismatches() {
		use crate::val::Uuid as SurrealUuid;
		let a = SurrealUuid(uuid::Uuid::from_u128(0xa1));
		let b = SurrealUuid(uuid::Uuid::from_u128(0xb2));
		let lit = LiteralWire::from_value(&Value::Uuid(a));
		assert_eq!(wire_eq(&encoded(&Value::Uuid(a)), &lit), Some(true));
		assert_eq!(wire_eq(&encoded(&Value::Uuid(b)), &lit), Some(false));
	}

	#[test]
	fn wire_eq_canonical_duration_matches_and_mismatches() {
		use std::time::Duration as StdDuration;

		use crate::val::Duration as SurrealDuration;
		let a = SurrealDuration(StdDuration::from_secs(5));
		let b = SurrealDuration(StdDuration::from_secs(6));
		let lit = LiteralWire::from_value(&Value::Duration(a));
		assert_eq!(wire_eq(&encoded(&Value::Duration(a)), &lit), Some(true));
		assert_eq!(wire_eq(&encoded(&Value::Duration(b)), &lit), Some(false));
	}

	#[test]
	fn wire_eq_canonical_datetime_does_not_match_duration() {
		// Cross-variant: different tags → byte-mismatch → `Some(false)`.
		use std::time::Duration as StdDuration;

		use chrono::Utc;

		use crate::val::{Datetime, Duration as SurrealDuration};
		let lit = LiteralWire::from_value(&Value::Datetime(Datetime(Utc::now())));
		let v = encoded(&Value::Duration(SurrealDuration(StdDuration::from_secs(0))));
		assert_eq!(wire_eq(&v, &lit), Some(false));
	}

	#[test]
	fn evaluate_leaf_on_wire_eq_bool_provably_true() {
		let lit = LiteralWire::from_value(&Value::Bool(true));
		let v = encoded(&Value::Bool(true));
		assert_eq!(
			evaluate_leaf_on_wire(&v, &BinaryOperator::Equal, &lit, false),
			Some(Evidence::ProvablyTrue)
		);
	}

	#[test]
	fn evaluate_leaf_on_wire_ne_null_provably_true_on_string() {
		let lit = LiteralWire::from_value(&Value::Null);
		let v = encoded(&Value::String("x".into()));
		assert_eq!(
			evaluate_leaf_on_wire(&v, &BinaryOperator::NotEqual, &lit, false),
			Some(Evidence::ProvablyTrue)
		);
	}

	// ------------------------------------------------------------------
	// strand_payload_str_unchecked + LiteralWire::Regex
	// ------------------------------------------------------------------

	#[test]
	fn strand_payload_str_unchecked_borrows_inner_utf8() {
		let v = encoded(&Value::String(Strand::from("hello 世界")));
		let s = strand_payload_str_unchecked(&v).expect("string payload");
		assert_eq!(s, "hello 世界");
	}

	#[test]
	fn strand_payload_str_unchecked_returns_none_on_non_string() {
		let v = encoded(&Value::Number(Number::Int(1)));
		assert_eq!(strand_payload_str_unchecked(&v), None);
	}

	#[test]
	fn wire_eq_regex_literal_matches_strand_value() {
		use std::str::FromStr;

		use crate::val::Regex;
		let lit = LiteralWire::from_value(&Value::Regex(Regex::from_str("^abc").unwrap()));
		let v = encoded(&Value::String(Strand::from("abcdef")));
		assert_eq!(wire_eq(&v, &lit), Some(true));
	}

	#[test]
	fn wire_eq_regex_literal_does_not_match_strand_value() {
		use std::str::FromStr;

		use crate::val::Regex;
		let lit = LiteralWire::from_value(&Value::Regex(Regex::from_str("^xyz").unwrap()));
		let v = encoded(&Value::String(Strand::from("abcdef")));
		assert_eq!(wire_eq(&v, &lit), Some(false));
	}

	#[test]
	fn wire_eq_regex_literal_against_non_strand_bails_to_decode() {
		use std::str::FromStr;

		use crate::val::Regex;
		let lit = LiteralWire::from_value(&Value::Regex(Regex::from_str("/x/").unwrap()));
		let v = encoded(&Value::Number(Number::Int(1)));
		// Number isn't matchable by a regex via `Value::equal`; the wire
		// path defers to the decode fallback rather than answer false here.
		assert_eq!(wire_eq(&v, &lit), None);
	}

	#[test]
	fn evaluate_leaf_on_wire_eq_regex_provably_true_on_match() {
		use std::str::FromStr;

		use crate::val::Regex;
		let lit = LiteralWire::from_value(&Value::Regex(Regex::from_str("^abc").unwrap()));
		let v = encoded(&Value::String(Strand::from("abcdef")));
		assert_eq!(
			evaluate_leaf_on_wire(&v, &BinaryOperator::Equal, &lit, false),
			Some(Evidence::ProvablyTrue)
		);
	}

	#[test]
	fn evaluate_leaf_on_wire_ne_regex_provably_false_on_match() {
		use std::str::FromStr;

		use crate::val::Regex;
		let lit = LiteralWire::from_value(&Value::Regex(Regex::from_str("^abc").unwrap()));
		let v = encoded(&Value::String(Strand::from("abcdef")));
		assert_eq!(
			evaluate_leaf_on_wire(&v, &BinaryOperator::NotEqual, &lit, false),
			Some(Evidence::ProvablyFalse)
		);
	}

	// ---------------------------------------------------------------------
	// Number sub-variant fast path — eliminates the previous
	// `Value::__deserialize_after_header` fallback for same-sub-variant
	// `Int`/`Decimal` mismatches in the scan/predicate hot loop.
	// ---------------------------------------------------------------------

	#[test]
	fn wire_eq_number_int_byte_mismatch_is_provably_false() {
		// Previously: same outer Number variant + byte mismatch returned
		// `None` (defer to decode). Now: same Int sub-variant + byte
		// mismatch is decisively `Some(false)` — Int's wire form is
		// canonical-per-value.
		let lit = LiteralWire::from_value(&Value::Number(Number::Int(5)));
		let v = encoded(&Value::Number(Number::Int(7)));
		assert_eq!(wire_eq(&v, &lit), Some(false));
	}

	#[test]
	fn wire_eq_number_decimal_byte_mismatch_is_provably_false() {
		use rust_decimal::Decimal;
		let lit = LiteralWire::from_value(&Value::Number(Number::Decimal(Decimal::from(5))));
		let v = encoded(&Value::Number(Number::Decimal(Decimal::from(7))));
		assert_eq!(wire_eq(&v, &lit), Some(false));
	}

	#[test]
	fn wire_eq_number_float_byte_mismatch_bails_to_none() {
		// Float same-sub-variant byte mismatch bails because of `+0.0` ↔
		// `-0.0` (byte-different, `f64::PartialEq`-equal). Cheap to defer
		// the rare Float-eq predicates to the fallback decode.
		let lit = LiteralWire::from_value(&Value::Number(Number::Float(1.5)));
		let v = encoded(&Value::Number(Number::Float(2.5)));
		assert_eq!(wire_eq(&v, &lit), None);
	}

	#[test]
	fn wire_eq_number_float_byte_match_bails_to_none() {
		// Even a byte-exact Float match bails: NaN bit patterns can
		// byte-equal yet compare unequal (`f64::PartialEq` says NaN !=
		// NaN). Conservatively deferring all Float comparisons to the
		// fallback keeps the wire-fast path correct for both NaN and the
		// `±0.0` case. The cost is the rare Float-eq predicate paying a
		// decode; the per-row Int/Decimal hot loop is unaffected.
		let lit = LiteralWire::from_value(&Value::Number(Number::Float(1.5)));
		let v = encoded(&Value::Number(Number::Float(1.5)));
		assert_eq!(wire_eq(&v, &lit), None);
	}

	#[test]
	fn wire_eq_number_int_lit_vs_float_field_bails_to_none() {
		// Cross-sub-variant: `Int(1) == Float(1.0)` under
		// `Number::PartialEq`, so wire byte-eq can't decide. Defer.
		let lit = LiteralWire::from_value(&Value::Number(Number::Int(1)));
		let v = encoded(&Value::Number(Number::Float(1.0)));
		assert_eq!(wire_eq(&v, &lit), None);
	}

	#[test]
	fn wire_eq_number_decimal_lit_vs_int_field_bails_to_none() {
		use rust_decimal::Decimal;
		let lit = LiteralWire::from_value(&Value::Number(Number::Decimal(Decimal::from(5))));
		let v = encoded(&Value::Number(Number::Int(5)));
		assert_eq!(wire_eq(&v, &lit), None);
	}

	#[test]
	fn wire_value_in_set_all_int_field_int_miss_is_provably_false() {
		// Set is all `Int`, field is `Int(99)` not in the set. Byte miss
		// is decisive: same sub-variant, canonical encoding, no
		// cross-variant arms in play.
		use std::collections::HashSet;
		let mut s = HashSet::new();
		s.insert(Value::Number(Number::Int(1)));
		s.insert(Value::Number(Number::Int(2)));
		let ls = LiteralSet::from_set(&s);
		let v = encoded(&Value::Number(Number::Int(99)));
		assert_eq!(wire_value_in_set(&v, &ls), Some(false));
	}

	/// Regression for a subtle cross-variant bug in the previous coarse
	/// `has_number_cross_variant` guard: an all-`Int` set probed with a
	/// `Float`-tagged runtime value would (incorrectly) answer `Some(false)`
	/// because the set's flag was `false` (no mixed sub-variants).
	/// `Number::PartialEq` would still treat e.g. `Float(5.0) == Int(5)` as
	/// `true`, so the answer must be `None` (defer to fallback) whenever the
	/// runtime sub-variant isn't in the set's stored sub-variants and the
	/// set holds any Number at all.
	#[test]
	fn wire_value_in_set_all_int_field_float_bails_to_none() {
		use std::collections::HashSet;
		let mut s = HashSet::new();
		s.insert(Value::Number(Number::Int(5)));
		let ls = LiteralSet::from_set(&s);
		// Field is `Float(5.0)` — value-equal to the stored `Int(5)` under
		// `Number::PartialEq`. Byte-miss + cross-variant ambiguity → defer.
		let v = encoded(&Value::Number(Number::Float(5.0)));
		assert_eq!(wire_value_in_set(&v, &ls), None);
	}

	#[test]
	fn wire_value_in_set_all_float_field_float_miss_bails_to_none() {
		// Same sub-variant but `Float` byte miss can't be claimed definitive
		// (NaN, ±0). Defer regardless of cross-variant content.
		use std::collections::HashSet;
		let mut s = HashSet::new();
		s.insert(Value::Number(Number::Float(1.0)));
		s.insert(Value::Number(Number::Float(2.0)));
		let ls = LiteralSet::from_set(&s);
		let v = encoded(&Value::Number(Number::Float(3.0)));
		assert_eq!(wire_value_in_set(&v, &ls), None);
	}

	#[test]
	fn wire_value_in_set_all_int_field_int_with_mixed_set_bails_to_none() {
		// Mixed Int+Float set, field Int(99) not in the set. Even though
		// the field's sub-variant matches one of the set's sub-variants,
		// a stored Float entry could be cross-variant `==` to the field
		// under `Number::PartialEq` — defer.
		use std::collections::HashSet;
		let mut s = HashSet::new();
		s.insert(Value::Number(Number::Int(1)));
		s.insert(Value::Number(Number::Float(2.5)));
		let ls = LiteralSet::from_set(&s);
		let v = encoded(&Value::Number(Number::Int(99)));
		assert_eq!(wire_value_in_set(&v, &ls), None);
	}

	// ---------------------------------------------------------------------
	// Range comparisons (`<` / `<=` / `>` / `>=`) — wire-fast for `Int`
	// literals against same-sub-variant `Int` fields. Eliminates the
	// previous ~3.5% `Value::__deserialize_after_header` slice in the
	// `field >= X AND field <= Y` (range) scan profile.
	// ---------------------------------------------------------------------

	#[test]
	fn evaluate_leaf_on_wire_lt_int_provably_true() {
		let lit = LiteralWire::from_value(&Value::Number(Number::Int(10)));
		let v = encoded(&Value::Number(Number::Int(5)));
		assert_eq!(
			evaluate_leaf_on_wire(&v, &BinaryOperator::LessThan, &lit, false),
			Some(Evidence::ProvablyTrue)
		);
	}

	#[test]
	fn evaluate_leaf_on_wire_lt_int_provably_false_on_equal() {
		let lit = LiteralWire::from_value(&Value::Number(Number::Int(10)));
		let v = encoded(&Value::Number(Number::Int(10)));
		assert_eq!(
			evaluate_leaf_on_wire(&v, &BinaryOperator::LessThan, &lit, false),
			Some(Evidence::ProvablyFalse)
		);
	}

	#[test]
	fn evaluate_leaf_on_wire_lt_int_provably_false_when_greater() {
		let lit = LiteralWire::from_value(&Value::Number(Number::Int(10)));
		let v = encoded(&Value::Number(Number::Int(20)));
		assert_eq!(
			evaluate_leaf_on_wire(&v, &BinaryOperator::LessThan, &lit, false),
			Some(Evidence::ProvablyFalse)
		);
	}

	#[test]
	fn evaluate_leaf_on_wire_lte_int_provably_true_on_equal() {
		let lit = LiteralWire::from_value(&Value::Number(Number::Int(10)));
		let v = encoded(&Value::Number(Number::Int(10)));
		assert_eq!(
			evaluate_leaf_on_wire(&v, &BinaryOperator::LessThanEqual, &lit, false),
			Some(Evidence::ProvablyTrue)
		);
	}

	#[test]
	fn evaluate_leaf_on_wire_gt_int_provably_true() {
		let lit = LiteralWire::from_value(&Value::Number(Number::Int(5)));
		let v = encoded(&Value::Number(Number::Int(10)));
		assert_eq!(
			evaluate_leaf_on_wire(&v, &BinaryOperator::MoreThan, &lit, false),
			Some(Evidence::ProvablyTrue)
		);
	}

	#[test]
	fn evaluate_leaf_on_wire_gte_int_provably_true_on_equal() {
		let lit = LiteralWire::from_value(&Value::Number(Number::Int(10)));
		let v = encoded(&Value::Number(Number::Int(10)));
		assert_eq!(
			evaluate_leaf_on_wire(&v, &BinaryOperator::MoreThanEqual, &lit, false),
			Some(Evidence::ProvablyTrue)
		);
	}

	#[test]
	fn evaluate_leaf_on_wire_range_int_handles_negatives() {
		// ZigZag encoding for negatives — exercise the varint decode
		// path with `Int(-100)` against `Int(-50)`. `-100 < -50` is true.
		let lit = LiteralWire::from_value(&Value::Number(Number::Int(-50)));
		let v = encoded(&Value::Number(Number::Int(-100)));
		assert_eq!(
			evaluate_leaf_on_wire(&v, &BinaryOperator::LessThan, &lit, false),
			Some(Evidence::ProvablyTrue)
		);
	}

	#[test]
	fn evaluate_leaf_on_wire_range_int_handles_reversed_form() {
		// `5 > field` carries `op = MoreThan, reversed = true` (literal
		// on the LHS); semantically `field < 5`. For `field = Int(3)`
		// the answer is `ProvablyTrue`.
		let lit = LiteralWire::from_value(&Value::Number(Number::Int(5)));
		let v = encoded(&Value::Number(Number::Int(3)));
		assert_eq!(
			evaluate_leaf_on_wire(&v, &BinaryOperator::MoreThan, &lit, true),
			Some(Evidence::ProvablyTrue)
		);
	}

	#[test]
	fn evaluate_leaf_on_wire_range_int_lit_vs_float_field_bails_to_none() {
		// Cross-sub-variant — bail. `Number::PartialOrd` would still
		// produce a definitive answer (`Int(5) < Float(5.5)` is `true`),
		// but resolving it byte-side would require decoding the Float
		// and going through `Number`'s cross-variant compare arm. Cheap
		// to defer to the fallback decoder, which already handles it.
		let lit = LiteralWire::from_value(&Value::Number(Number::Int(5)));
		let v = encoded(&Value::Number(Number::Float(5.5)));
		assert_eq!(evaluate_leaf_on_wire(&v, &BinaryOperator::LessThan, &lit, false), None);
	}

	#[test]
	fn evaluate_leaf_on_wire_range_float_lit_bails_to_none() {
		// Float literals have no `int_value` — range comparator bails
		// (`f64::PartialOrd` has NaN / ±0.0 edges; the fallback decoder
		// applies `Number::PartialOrd` semantics).
		let lit = LiteralWire::from_value(&Value::Number(Number::Float(1.5)));
		let v = encoded(&Value::Number(Number::Float(2.5)));
		assert_eq!(evaluate_leaf_on_wire(&v, &BinaryOperator::LessThan, &lit, false), None);
	}

	#[test]
	fn evaluate_leaf_on_wire_range_decimal_lit_bails_to_none() {
		use rust_decimal::Decimal;
		let lit = LiteralWire::from_value(&Value::Number(Number::Decimal(Decimal::from(10))));
		let v = encoded(&Value::Number(Number::Decimal(Decimal::from(5))));
		assert_eq!(evaluate_leaf_on_wire(&v, &BinaryOperator::LessThan, &lit, false), None);
	}

	#[test]
	fn evaluate_leaf_on_wire_range_int_lit_vs_non_number_field_bails_to_none() {
		// `String < Int(N)` has its own semantics in `Value::compare`
		// (lexicographic vs cross-type). The wire path returns `None`
		// for any non-Number field tag against a Number literal.
		let lit = LiteralWire::from_value(&Value::Number(Number::Int(5)));
		let v = encoded(&Value::String("abc".into()));
		assert_eq!(evaluate_leaf_on_wire(&v, &BinaryOperator::LessThan, &lit, false), None);
	}

	#[test]
	fn evaluate_leaf_on_wire_range_non_number_lit_bails_to_none() {
		// String literal on the comparator side — the wire-fast range
		// path is Number-only, anything else falls through to the catch-
		// all `_ => None` arm in `evaluate_leaf_on_wire`.
		let lit = LiteralWire::from_value(&Value::String("abc".into()));
		let v = encoded(&Value::String("abd".into()));
		assert_eq!(evaluate_leaf_on_wire(&v, &BinaryOperator::LessThan, &lit, false), None);
	}
}
