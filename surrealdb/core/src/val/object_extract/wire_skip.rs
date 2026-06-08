//! O(1) rev-2 envelope skip for [`crate::val::Value`] wires, without the
//! `SkipRevisioned` trait.
//!
//! The legacy `<Value as SkipRevisioned>::skip_revisioned` path was forbidden
//! in the scan/predicate hot path because its name implies a recursive
//! skip-walk; under rev-2 it actually inlines to a tag + length-prefix read,
//! but the directive is to call the optimised envelope primitives directly
//! so the cost model is obvious at every call site.
//!
//! This helper reads the `u16` rev prefix, validates `rev == 2`, then reads
//! the optimised tag and advances the reader past the value's payload in O(1)
//! using:
//!
//! - **Inline** variants ([`Value::None`], [`Value::Null`]): the tag byte *is* the entire encoding
//!   — nothing to advance.
//! - **Fixed** variants ([`Value::Bool`]): per-variant static size from the table below.
//! - **Varlen** variants (everything else): `u32_le payload_length` followed by the payload —
//!   advance by the length.
//!
//! Cross-revision: rev-1 records hit the conversion path before reaching the
//! predicate evaluator, so `skip_value_wire` only encounters rev-2 in
//! production. We still return a clean error on non-2 so callers can fall
//! back to full decode rather than silently misinterpret bytes.
//!
//! **Revision lockstep:** [`VALUE_FIXED_SIZES`] is hand-derived from the
//! `#[revisioned(revision(2, optimised))]` macro on `Value`. When the
//! revision bumps (rev 3+) or any new variant adopts `#[revision(size =
//! "fixed(N)")]`, this module must be updated in lockstep — otherwise the
//! cursor would advance by zero bytes for the new Fixed variant and the
//! scan path would silently misread. The
//! [`tests::variant_layout_matches_value_declaration`] test catches the
//! variant-id / Fixed-size drift at test time, and the rev guard inside
//! [`skip_value_wire`] catches the revision bump at runtime by bailing
//! to fallback.

use revision::Error as RevisionError;
use revision::optimised::tag::{SizeClass, read_tag};

/// Variant ids for [`crate::val::Value`] at rev 2.
///
/// Must match the declaration order in `surrealdb/core/src/val/mod.rs`.
/// The optimised macro assigns ids by source order (None=0, Null=1,
/// Bool=2, Number=3, String=4, ...) and we mirror them here so the size
/// table and the round-trip test cover every alive variant exactly once.
#[cfg(test)]
mod variant_ids {
	pub const NONE: u8 = 0;
	pub const NULL: u8 = 1;
	pub const BOOL: u8 = 2;
	pub const NUMBER: u8 = 3;
	pub const STRING: u8 = 4;
	pub const REGEX: u8 = 16;
}

/// Fixed-size payload table for rev-2 [`crate::val::Value`] variants. Indexed
/// by `variant_id`; entries for non-Fixed variants are unused (and set to 0).
///
/// Mirrors the `__SIZE_TABLE` the optimised-enum macro generates internally.
/// `Bool(bool)` is currently the only Fixed variant in `Value` (1 byte).
const VALUE_FIXED_SIZES: [u8; 32] = {
	let mut t = [0u8; 32];
	t[2] = 1; // Bool
	t
};

/// Skip past one rev-2 [`crate::val::Value`] wire in O(1).
///
/// Reads the `u16 rev` prefix (varint-encoded; rev=2 fits in one byte), then
/// the 1-byte optimised tag, then advances the reader past the payload using
/// the size class:
///
/// - `Inline` → no payload, 0 extra bytes;
/// - `Fixed`  → constant size from [`VALUE_FIXED_SIZES`] indexed by variant id;
/// - `Varlen` → read `u32_le` length, advance that many bytes.
///
/// Returns [`Err`] if the rev prefix is not 2 (so callers can fall back to a
/// full `DeserializeRevisioned` path that handles legacy revisions), or if
/// any read goes past the buffer.
#[inline]
pub(crate) fn skip_value_wire(reader: &mut &[u8]) -> Result<(), RevisionError> {
	// `u16` is varint-encoded under the default (non-`fixed-width-encoding`)
	// build: rev=2 fits in a single byte. Use `DeserializeRevisioned` here so
	// the encoding stays in lockstep with the macro-generated reader, rather
	// than hardcoding `from_le_bytes`.
	let rev = <u16 as revision::DeserializeRevisioned>::deserialize_revisioned(reader)?;
	if rev != 2 {
		return Err(RevisionError::Deserialize(format!(
			"skip_value_wire: expected Value revision 2, got {rev}"
		)));
	}
	let tag = read_tag(reader)?;
	let sc = tag.size_class()?;
	match sc {
		SizeClass::Inline => Ok(()),
		SizeClass::Fixed => {
			let n = VALUE_FIXED_SIZES[tag.variant_id() as usize] as usize;
			if reader.len() < n {
				return Err(RevisionError::Deserialize(
					"skip_value_wire: fixed payload exceeds remaining bytes".into(),
				));
			}
			*reader = &reader[n..];
			Ok(())
		}
		SizeClass::Varlen => {
			if reader.len() < 4 {
				return Err(RevisionError::Deserialize(
					"skip_value_wire: truncated varlen length prefix".into(),
				));
			}
			let mut len_buf = [0u8; 4];
			len_buf.copy_from_slice(&reader[..4]);
			*reader = &reader[4..];
			let len = u32::from_le_bytes(len_buf) as usize;
			if reader.len() < len {
				return Err(RevisionError::Deserialize(
					"skip_value_wire: varlen payload exceeds remaining bytes".into(),
				));
			}
			*reader = &reader[len..];
			Ok(())
		}
	}
}

/// Parse the rev-2 optimised envelope `<u16 rev=2 || u32_le payload_length>`
/// and return the payload slice.
///
/// Used by single-field `#[revisioned(revision(2, optimised))]` types whose
/// payload **is** that field's body (no offset prologue, no field separator
/// — e.g. [`Object`] (`#[revision(indexed_map)]`) and [`Array`]
/// (`#[revision(indexed_seq)]`)) to bypass the macro-emitted
/// `walk_revisioned → into_walk_field_0 → walker()` chain.
///
/// The macro-emitted `into_walk_field_*` accessors call the field type's
/// `skip_*` impl to find the field's byte boundary. Even now that the
/// indexed-body skip is O(1) (revision 0.26.0+), the macro path still pays
/// the prologue parse twice — once in the skip to derive the field's bytes
/// for the `IndexedMapView`, then again in `IndexedMapWalker::from_payload`
/// when the caller asks for a walker. Bypassing both is one fewer prologue
/// validation per descent.
///
/// Because the only field IS the payload, the envelope length already
/// tells us where the body ends — no skip pass needed at all.
///
/// **Caller-asserted rev-2 invariant.** Skips the `u16 rev` re-read on
/// the assumption that the outer walker already validated it.
///
/// Concrete case: descending from a rev-2 [`Value::Object(_)`] — the
/// parent `Value` walker already read its rev prefix, and by
/// `#[revisioned]` macro construction a rev-2 `Value::Object(_)` always
/// wraps a rev-2 [`Object`]. Reading the inner rev would be redundant.
///
/// Saves one varint decode + one branch per descent level per row. Small
/// per call, but multiplied across descent depth × rows it adds up.
///
/// In `debug_assertions` builds, the prefix byte is checked against
/// `0x02` (the varint of rev=2) so a serialiser regression surfaces at
/// test time; release builds skip the check entirely.
///
/// Returns an error on a truncated envelope.
///
/// [`Object`]: crate::val::Object
/// [`Array`]: crate::val::Array
#[inline]
pub(crate) fn rev2_optimised_payload_unchecked(wire: &[u8]) -> Result<&[u8], RevisionError> {
	if wire.is_empty() {
		return Err(RevisionError::Deserialize(
			"rev2_optimised_payload_unchecked: empty wire".into(),
		));
	}
	debug_assert_eq!(
		wire[0], 2u8,
		"rev2_optimised_payload_unchecked: caller-asserted rev 2 invariant violated",
	);
	let r = &wire[1..];
	if r.len() < 4 {
		return Err(RevisionError::Deserialize(
			"rev2_optimised_payload_unchecked: truncated u32_le envelope length".into(),
		));
	}
	let payload_len = u32::from_le_bytes([r[0], r[1], r[2], r[3]]) as usize;
	let r = &r[4..];
	if r.len() < payload_len {
		return Err(RevisionError::Deserialize(
			"rev2_optimised_payload_unchecked: payload exceeds remaining bytes".into(),
		));
	}
	Ok(&r[..payload_len])
}

#[cfg(test)]
mod tests {
	use std::collections::BTreeMap;

	use revision::SerializeRevisioned;
	use surrealdb_strand::Strand;

	use super::*;
	use crate::val::{Array, Number, Object, Value};

	fn encoded(v: &Value) -> Vec<u8> {
		let mut out = Vec::new();
		v.serialize_revisioned(&mut out).expect("serialize");
		out
	}

	#[test]
	fn skips_inline_none() {
		let bytes = encoded(&Value::None);
		// rev (1 byte for rev=2) + tag (1 byte) = 2 bytes; inline has no payload.
		assert_eq!(bytes.len(), 2);
		let mut r: &[u8] = &bytes;
		skip_value_wire(&mut r).unwrap();
		assert!(r.is_empty(), "skip_value_wire must consume entire wire");
	}

	#[test]
	fn skips_inline_null() {
		let bytes = encoded(&Value::Null);
		assert_eq!(bytes.len(), 2);
		let mut r: &[u8] = &bytes;
		skip_value_wire(&mut r).unwrap();
		assert!(r.is_empty());
	}

	#[test]
	fn skips_fixed_bool() {
		for v in [Value::Bool(true), Value::Bool(false)] {
			let bytes = encoded(&v);
			// rev (1) + tag (1) + Bool payload (1) = 3 bytes.
			assert_eq!(bytes.len(), 3);
			let mut r: &[u8] = &bytes;
			skip_value_wire(&mut r).unwrap();
			assert!(r.is_empty());
		}
	}

	#[test]
	fn skips_varlen_string() {
		let bytes = encoded(&Value::String(Strand::from("hello world")));
		let mut r: &[u8] = &bytes;
		skip_value_wire(&mut r).unwrap();
		assert!(r.is_empty());
	}

	#[test]
	fn skips_varlen_number() {
		let bytes = encoded(&Value::Number(Number::Int(42)));
		let mut r: &[u8] = &bytes;
		skip_value_wire(&mut r).unwrap();
		assert!(r.is_empty());
	}

	#[test]
	fn skips_varlen_array() {
		let arr = Value::Array(Array::from(vec![
			Value::Number(Number::Int(1)),
			Value::String("x".into()),
		]));
		let bytes = encoded(&arr);
		let mut r: &[u8] = &bytes;
		skip_value_wire(&mut r).unwrap();
		assert!(r.is_empty());
	}

	#[test]
	fn skips_varlen_object() {
		let obj = Value::Object(Object::from(BTreeMap::from([
			(Strand::from("a"), Value::Number(Number::Int(1))),
			(Strand::from("b"), Value::String("y".into())),
		])));
		let bytes = encoded(&obj);
		let mut r: &[u8] = &bytes;
		skip_value_wire(&mut r).unwrap();
		assert!(r.is_empty());
	}

	#[test]
	fn skips_through_concatenated_values() {
		// Common shape during a legacy sub-threshold map scan: concatenated
		// values back-to-back. After each `skip_value_wire`, the reader must
		// land at the next value's first byte.
		let mut buf = Vec::new();
		buf.extend(encoded(&Value::None));
		buf.extend(encoded(&Value::Bool(true)));
		buf.extend(encoded(&Value::Number(Number::Int(7))));
		buf.extend(encoded(&Value::String("payload".into())));
		let mut r: &[u8] = &buf;
		skip_value_wire(&mut r).unwrap();
		skip_value_wire(&mut r).unwrap();
		skip_value_wire(&mut r).unwrap();
		skip_value_wire(&mut r).unwrap();
		assert!(r.is_empty());
	}

	#[test]
	fn errors_on_wrong_revision() {
		// Craft a fake "rev=1" prefix and ensure we bail rather than try to
		// interpret a rev-1 body as rev-2.
		let bytes = [0x01u8, 0x00];
		let mut r: &[u8] = &bytes;
		let err = skip_value_wire(&mut r).expect_err("must bail on rev != 2");
		match err {
			RevisionError::Deserialize(s) => assert!(s.contains("revision 2")),
			other => panic!("expected Deserialize error, got {other:?}"),
		}
	}

	#[test]
	fn errors_on_truncated_varlen() {
		// rev=2 tag + Strand varlen tag (variant_id=4 String, sc=Varlen) but
		// missing u32_le length prefix.
		let bytes = [0x02u8, (4 | (0b10 << 5))];
		let mut r: &[u8] = &bytes;
		assert!(skip_value_wire(&mut r).is_err());
	}

	#[test]
	fn errors_on_empty_buffer() {
		// The very first read (rev prefix) goes off the end. Document the
		// contract: callers can safely hand an empty reader to
		// `skip_value_wire` and observe a clean error rather than UB.
		let mut r: &[u8] = &[];
		assert!(skip_value_wire(&mut r).is_err());
	}

	#[test]
	fn errors_on_truncated_fixed_payload() {
		// Mirror of `errors_on_truncated_varlen` but for the `Fixed` size
		// class. `Bool` is the only Fixed variant (1 byte payload), so build
		// a rev=2 wire that announces `Bool` then truncates the payload.
		// Bool variant_id = 2, size_class = Fixed (0b01).
		let bytes = [0x02u8, (2u8 | (0b01u8 << 5))];
		let mut r: &[u8] = &bytes;
		let err = skip_value_wire(&mut r).expect_err("truncated Fixed payload must error");
		match err {
			RevisionError::Deserialize(s) => {
				assert!(s.contains("fixed payload"), "unexpected error message: {s}")
			}
			other => panic!("expected Deserialize error, got {other:?}"),
		}
	}

	#[test]
	fn errors_on_reserved_size_class() {
		// Size-class bits `0b11` are reserved; `Tag::size_class()` must
		// reject the tag rather than silently picking a class. Construct
		// a rev=2 wire whose tag has the high two bits set (size_class =
		// 0b11) — `read_tag` parses it, but `size_class()` returns Err
		// and `skip_value_wire` propagates that as an error.
		let bytes = [0x02u8, 0b0110_0000];
		let mut r: &[u8] = &bytes;
		assert!(skip_value_wire(&mut r).is_err());
	}

	/// Every Varlen `Value` variant must round-trip through `skip_value_wire`
	/// — a parametrised guard against a future variant-id table change in
	/// `wire_cmp` / `wire_literal` desyncing from the skip path. Each entry
	/// pairs a representative value with the expected variant_id.
	#[test]
	fn skips_all_varlen_variants() {
		use std::str::FromStr;
		use std::time::Duration as StdDuration;

		use chrono::{TimeZone, Utc};

		use crate::val::{
			Bytes, Datetime, Duration as SurrealDuration, File, RecordId, Regex, Set, TableName,
			Uuid as SurrealUuid,
		};

		let cases: Vec<(Value, u8)> = vec![
			(Value::Duration(SurrealDuration(StdDuration::from_secs(1))), 5),
			(Value::Datetime(Datetime(Utc.with_ymd_and_hms(2024, 5, 21, 0, 0, 0).unwrap())), 6),
			(Value::Uuid(SurrealUuid(uuid::Uuid::from_u128(0xa1))), 7),
			(Value::Set(Set::new()), 9),
			(Value::Bytes(Bytes::from(vec![1u8, 2, 3])), 12),
			(Value::Table(TableName::from("foo")), 13),
			(
				Value::RecordId(RecordId {
					table: TableName::from("foo"),
					key: crate::val::RecordIdKey::from(Strand::from("bar")),
				}),
				14,
			),
			(Value::File(File::new("bucket".to_string(), "key".to_string())), 15),
			(Value::Regex(Regex::from_str("/x/").unwrap()), 16),
		];

		for (v, expected_id) in cases {
			let bytes = encoded(&v);
			// rev byte + tag byte + Varlen payload. Confirm the tag's
			// variant_id matches before exercising the skip — otherwise a
			// silent reordering would produce a passing-but-wrong test.
			let tag = revision::optimised::tag::Tag(bytes[1]);
			assert_eq!(
				tag.variant_id(),
				expected_id,
				"variant_id drift for {v:?} (got {}, expected {expected_id})",
				tag.variant_id(),
			);
			assert_eq!(
				tag.size_class().unwrap(),
				revision::optimised::tag::SizeClass::Varlen,
				"size_class drift for {v:?}",
			);
			let mut r: &[u8] = &bytes;
			skip_value_wire(&mut r).expect("skip_value_wire must succeed");
			assert!(r.is_empty(), "skip_value_wire must consume entire wire for {v:?}");
		}
	}

	#[test]
	fn skips_geometry_variant() {
		// `Geometry` is exercised separately because its constructor is more
		// involved than the other Varlen variants and pulling `geo` types
		// into the table above would obscure the simpler cases.
		use crate::val::Geometry;
		let v = Value::Geometry(Geometry::Point(geo::Point::new(0.0, 0.0)));
		let bytes = encoded(&v);
		let tag = revision::optimised::tag::Tag(bytes[1]);
		assert_eq!(tag.variant_id(), 11, "Geometry variant_id drift");
		assert_eq!(tag.size_class().unwrap(), revision::optimised::tag::SizeClass::Varlen);
		let mut r: &[u8] = &bytes;
		skip_value_wire(&mut r).unwrap();
		assert!(r.is_empty());
	}

	#[test]
	fn skips_range_variant() {
		// `Range` (variant_id=17) shares the Varlen path; the bounds carry
		// nested values, so the envelope's u32_le length is what advances
		// us — verify the skip consumes the entire wire regardless of
		// payload depth.
		use std::ops::Bound;

		use crate::val::Range;
		let v = Value::Range(Box::new(Range {
			start: Bound::Included(Value::Number(Number::Int(1))),
			end: Bound::Excluded(Value::Number(Number::Int(10))),
		}));
		let bytes = encoded(&v);
		let tag = revision::optimised::tag::Tag(bytes[1]);
		assert_eq!(tag.variant_id(), 17, "Range variant_id drift");
		assert_eq!(tag.size_class().unwrap(), revision::optimised::tag::SizeClass::Varlen);
		let mut r: &[u8] = &bytes;
		skip_value_wire(&mut r).unwrap();
		assert!(r.is_empty());
	}

	#[test]
	fn variant_layout_matches_value_declaration() {
		use std::str::FromStr;

		use crate::val::Regex;

		// Guard against silent reordering: encode each variant whose
		// numeric id is load-bearing elsewhere (`wire_cmp`, `wire_literal`,
		// `VALUE_FIXED_SIZES`) and confirm the tag's variant_id matches.
		fn header(v: &Value) -> (u8, revision::optimised::tag::SizeClass) {
			let bytes = encoded(v);
			// rev=2 occupies bytes[0]; tag is bytes[1].
			let tag = revision::optimised::tag::Tag(bytes[1]);
			(tag.variant_id(), tag.size_class().unwrap())
		}

		assert_eq!(header(&Value::None).0, variant_ids::NONE);
		assert_eq!(header(&Value::Null).0, variant_ids::NULL);
		assert_eq!(header(&Value::Bool(false)).0, variant_ids::BOOL);
		assert_eq!(header(&Value::Number(Number::Int(0))).0, variant_ids::NUMBER);
		assert_eq!(header(&Value::String(Strand::from("x"))).0, variant_ids::STRING);
		assert_eq!(header(&Value::Regex(Regex::from_str("/x/").unwrap())).0, variant_ids::REGEX);

		// Every Fixed-sized variant must have a non-zero entry in
		// `VALUE_FIXED_SIZES`. If a future Value variant adopts
		// `#[revision(size = "fixed(N)")]` and we forget to update the
		// table, `skip_value_wire` would advance the cursor by zero bytes
		// and silently misread.
		let fixed_samples: &[(Value, u8)] = &[(Value::Bool(false), 1), (Value::Bool(true), 1)];
		for (v, expected_size) in fixed_samples {
			let (vid, sc) = header(v);
			assert_eq!(sc, revision::optimised::tag::SizeClass::Fixed);
			assert_eq!(VALUE_FIXED_SIZES[vid as usize], *expected_size);
		}
	}
}
