//! Numeric value type used throughout SurrealDB.
//!
//! This module defines Number, a discriminated union over Int (i64), Float (f64),
//! and Decimal (rust_decimal::Decimal), and implements arithmetic, comparison,
//! and conversions. For storage in index keys, Numbers are serialized with a
//! canonical, lexicographic encoding (via expr::decimal::DecimalLexEncoder)
//! so that byte-wise ordering matches numeric ordering and numerically-equal
//! values across variants normalize to identical bytes.
//!
//! Key points:
//! - Ordering: `Ord`, `Eq` and `Hash` are [`surrealdb_types::Number`]'s, which compares numbers of
//!   every kind by exact value, so `0.1f` sits above `0.1dec`. That is the order of their key
//!   encodings once a decimal is normalised (`Decimal::normalize`), so an index read and a scan
//!   admit the same numbers, and `Hash` agrees with `Eq`.
//! - Decimal scale in keys: the encoding keeps a decimal's trailing zeros, so a computed `2.50dec`
//!   and the literal `2.5dec` encode apart though one value. A B-tree index stamped at
//!   `BTREE_NORMALISED_DECIMAL_KEYS_FORMAT_VERSION` or later respells each decimal and bigdecimal
//!   without them before encoding, so its keys agree with the order above.
//! - Bigdecimals in keys: a `bigdecimal` is spelled from its coefficient's digits and scale in the
//!   same encoding, so it orders among every other kind, and `1.5bigdec` has the bytes of `1.5dec`.
//!   Its coefficient may be longer than any other kind's and its scale wider; both take forms of
//!   the encoding that no other kind reaches (see `DecimalLexEncoder`).
//! - Normalization in keys: 0 (Int), 0.0 (Float) and 0dec (Decimal) encode to the same byte
//!   sequence for keys, so UNIQUE indexes treat them as equal.
//! - Special float values: NaN, +∞ and −∞ are given fixed encodings that fit in the total ordering
//!   used by keys (see DecimalLexEncoder docs).
//! - Stream-friendly: the numeric encoding contains an in-band terminator and appends a 0x00 byte,
//!   allowing concatenation in composite keys without ambiguity during decoding.

#![deny(clippy::wildcard_enum_match_arm)]

use std::cmp::Ordering;
use std::f64::consts::PI;
use std::fmt::{self, Debug, Display, Formatter};
use std::hash;
use std::iter::{Product, Sum};
use std::ops::{self, Add, Div, Mul, Neg, Rem, Sub};
use std::str::FromStr;

use anyhow::{Result, bail, ensure};
use revision::{
	DeserializeRevisioned, Revisioned, SerializeRevisioned, SkipCheckRevisioned, SkipRevisioned,
	WalkRevisioned,
};
use rust_decimal::Decimal;
use rust_decimal::prelude::*;
use storekey::{BorrowDecode, Encode};
use surrealdb_collections::bounded;
use surrealdb_types::{BigDecimal, SqlFormat, ToSql};

use super::{IndexFormat, IndexIdFormat};
use crate::expr::Error;
use crate::expr::decimal::{DecimalLexEncoder, KeyDecimal, LexDecimal};
use crate::val::{TryAdd, TryDiv, TryFloatDiv, TryMul, TryNeg, TryPow, TryRem, TrySub};

#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Encode, BorrowDecode)]
pub enum NumberKind {
	Int = 0,
	Float = 1,
	Decimal = 2,
	I8 = 3,
	U8 = 4,
	I16 = 5,
	U16 = 6,
	I32 = 7,
	U32 = 8,
	U64 = 9,
	F16 = 10,
	F32 = 11,
	BigDecimal = 12,
}

impl NumberKind {
	#[must_use]
	pub const fn to_types(self) -> surrealdb_types::NumberKind {
		match self {
			Self::Int => surrealdb_types::NumberKind::Int,
			Self::Float => surrealdb_types::NumberKind::Float,
			Self::Decimal => surrealdb_types::NumberKind::Decimal,
			Self::I8 => surrealdb_types::NumberKind::I8,
			Self::U8 => surrealdb_types::NumberKind::U8,
			Self::I16 => surrealdb_types::NumberKind::I16,
			Self::U16 => surrealdb_types::NumberKind::U16,
			Self::I32 => surrealdb_types::NumberKind::I32,
			Self::U32 => surrealdb_types::NumberKind::U32,
			Self::U64 => surrealdb_types::NumberKind::U64,
			Self::F16 => surrealdb_types::NumberKind::F16,
			Self::F32 => surrealdb_types::NumberKind::F32,
			Self::BigDecimal => surrealdb_types::NumberKind::BigDecimal,
		}
	}

	#[must_use]
	pub const fn from_types(k: surrealdb_types::NumberKind) -> Self {
		match k {
			surrealdb_types::NumberKind::Int => Self::Int,
			surrealdb_types::NumberKind::Float => Self::Float,
			surrealdb_types::NumberKind::Decimal => Self::Decimal,
			surrealdb_types::NumberKind::I8 => Self::I8,
			surrealdb_types::NumberKind::U8 => Self::U8,
			surrealdb_types::NumberKind::I16 => Self::I16,
			surrealdb_types::NumberKind::U16 => Self::U16,
			surrealdb_types::NumberKind::I32 => Self::I32,
			surrealdb_types::NumberKind::U32 => Self::U32,
			surrealdb_types::NumberKind::U64 => Self::U64,
			surrealdb_types::NumberKind::F16 => Self::F16,
			surrealdb_types::NumberKind::F32 => Self::F32,
			surrealdb_types::NumberKind::BigDecimal => Self::BigDecimal,
		}
	}

	/// Result kind of a binary arithmetic operator over `self` and `other`.
	///
	/// Delegates to [`surrealdb_types::NumberKind::join`].
	#[must_use]
	pub const fn join(self, other: Self) -> Self {
		Self::from_types(self.to_types().join(other.to_types()))
	}
}

impl From<surrealdb_types::NumberKind> for NumberKind {
	fn from(k: surrealdb_types::NumberKind) -> Self {
		Self::from_types(k)
	}
}

impl From<NumberKind> for surrealdb_types::NumberKind {
	fn from(k: NumberKind) -> Self {
		k.to_types()
	}
}

#[derive(Clone, Debug)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum Number {
	Int(i64),
	Float(f64),
	Decimal(Decimal),
	I8(i8),
	U8(u8),
	I16(i16),
	U16(u16),
	I32(i32),
	U32(u32),
	U64(u64),
	F16(#[cfg_attr(feature = "arbitrary", arbitrary(with = arbitrary_f16))] half::f16),
	F32(f32),
	/// An arbitrary-precision decimal. Boxed so that `Number` keeps its 24-byte size; the
	/// enum is therefore not `Copy`.
	BigDecimal(Box<surrealdb_types::BigDecimal>),
}

const _: () = assert!(std::mem::size_of::<Number>() == 24);

#[cfg(feature = "arbitrary")]
fn arbitrary_f16(u: &mut arbitrary::Unstructured<'_>) -> arbitrary::Result<half::f16> {
	<u16 as arbitrary::Arbitrary>::arbitrary(u).map(half::f16::from_bits)
}

impl Revisioned for Number {
	#[inline]
	fn revision() -> u16 {
		3
	}
}

impl SerializeRevisioned for Number {
	fn serialize_revisioned<W: std::io::Write>(
		&self,
		writer: &mut W,
	) -> std::result::Result<(), revision::Error> {
		match self {
			Self::Int(v) => {
				1u16.serialize_revisioned(writer)?;
				0u32.serialize_revisioned(writer)?;
				v.serialize_revisioned(writer)
			}
			Self::Float(v) => {
				1u16.serialize_revisioned(writer)?;
				1u32.serialize_revisioned(writer)?;
				v.serialize_revisioned(writer)
			}
			Self::Decimal(v) => {
				1u16.serialize_revisioned(writer)?;
				2u32.serialize_revisioned(writer)?;
				v.serialize_revisioned(writer)
			}
			Self::I8(v) => {
				2u16.serialize_revisioned(writer)?;
				3u32.serialize_revisioned(writer)?;
				v.serialize_revisioned(writer)
			}
			Self::U8(v) => {
				2u16.serialize_revisioned(writer)?;
				4u32.serialize_revisioned(writer)?;
				v.serialize_revisioned(writer)
			}
			Self::I16(v) => {
				2u16.serialize_revisioned(writer)?;
				5u32.serialize_revisioned(writer)?;
				v.serialize_revisioned(writer)
			}
			Self::U16(v) => {
				2u16.serialize_revisioned(writer)?;
				6u32.serialize_revisioned(writer)?;
				v.serialize_revisioned(writer)
			}
			Self::I32(v) => {
				2u16.serialize_revisioned(writer)?;
				7u32.serialize_revisioned(writer)?;
				v.serialize_revisioned(writer)
			}
			Self::U32(v) => {
				2u16.serialize_revisioned(writer)?;
				8u32.serialize_revisioned(writer)?;
				v.serialize_revisioned(writer)
			}
			Self::U64(v) => {
				2u16.serialize_revisioned(writer)?;
				9u32.serialize_revisioned(writer)?;
				v.serialize_revisioned(writer)
			}
			Self::F16(v) => {
				2u16.serialize_revisioned(writer)?;
				10u32.serialize_revisioned(writer)?;
				v.to_bits().serialize_revisioned(writer)
			}
			Self::F32(v) => {
				2u16.serialize_revisioned(writer)?;
				11u32.serialize_revisioned(writer)?;
				v.serialize_revisioned(writer)
			}
			Self::BigDecimal(v) => {
				3u16.serialize_revisioned(writer)?;
				12u32.serialize_revisioned(writer)?;
				v.to_string().serialize_revisioned(writer)
			}
		}
	}
}

impl DeserializeRevisioned for Number {
	fn deserialize_revisioned<R: std::io::Read>(
		reader: &mut R,
	) -> std::result::Result<Self, revision::Error> {
		let rev = bounded::read_revision(reader, "Number", 3)?;
		let variant = u32::deserialize_revisioned(reader)?;
		match (rev, variant) {
			(1, 0) | (2, 0) => Ok(Self::Int(i64::deserialize_revisioned(reader)?)),
			(1, 1) | (2, 1) => Ok(Self::Float(f64::deserialize_revisioned(reader)?)),
			(1, 2) | (2, 2) => Ok(Self::Decimal(Decimal::deserialize_revisioned(reader)?)),
			(2, 3) => Ok(Self::I8(i8::deserialize_revisioned(reader)?)),
			(2, 4) => Ok(Self::U8(u8::deserialize_revisioned(reader)?)),
			(2, 5) => Ok(Self::I16(i16::deserialize_revisioned(reader)?)),
			(2, 6) => Ok(Self::U16(u16::deserialize_revisioned(reader)?)),
			(2, 7) => Ok(Self::I32(i32::deserialize_revisioned(reader)?)),
			(2, 8) => Ok(Self::U32(u32::deserialize_revisioned(reader)?)),
			(2, 9) => Ok(Self::U64(u64::deserialize_revisioned(reader)?)),
			(2, 10) => Ok(Self::F16(half::f16::from_bits(u16::deserialize_revisioned(reader)?))),
			(2, 11) => Ok(Self::F32(f32::deserialize_revisioned(reader)?)),
			(3, 12) => {
				let text = String::deserialize_revisioned(reader)?;
				let decimal = text
					.parse::<surrealdb_types::BigDecimal>()
					.map_err(|e| revision::Error::Deserialize(e.to_string()))?;
				Ok(Self::BigDecimal(Box::new(decimal)))
			}
			_ => Err(revision::Error::Deserialize(format!(
				"Invalid enum variant `{variant}` for type `Number` at revision `{rev}`"
			))),
		}
	}
}

impl SkipRevisioned for Number {
	fn skip_revisioned<R: std::io::Read>(
		reader: &mut R,
	) -> std::result::Result<(), revision::Error> {
		let rev = bounded::read_revision(reader, "Number", 3)?;
		let variant = u32::deserialize_revisioned(reader)?;
		match (rev, variant) {
			(1, 0) | (2, 0) => i64::skip_revisioned(reader),
			(1, 1) | (2, 1) => f64::skip_revisioned(reader),
			(1, 2) | (2, 2) => Decimal::skip_revisioned(reader),
			(2, 3) => i8::skip_revisioned(reader),
			(2, 4) => u8::skip_revisioned(reader),
			(2, 5) => i16::skip_revisioned(reader),
			(2, 6) => u16::skip_revisioned(reader),
			(2, 7) => i32::skip_revisioned(reader),
			(2, 8) => u32::skip_revisioned(reader),
			(2, 9) => u64::skip_revisioned(reader),
			(2, 10) => u16::skip_revisioned(reader),
			(2, 11) => f32::skip_revisioned(reader),
			(3, 12) => String::skip_revisioned(reader),
			_ => Err(revision::Error::Deserialize(format!(
				"Invalid enum variant `{variant}` for type `Number` at revision `{rev}`"
			))),
		}
	}

	fn skip_revisioned_slice(
		reader: &mut revision::SliceReader<'_>,
	) -> std::result::Result<(), revision::Error> {
		let rev = bounded::read_revision(reader, "Number", 3)?;
		let variant = u32::deserialize_revisioned(reader)?;
		match (rev, variant) {
			(1, 0) | (2, 0) => i64::skip_revisioned_slice(reader),
			(1, 1) | (2, 1) => f64::skip_revisioned_slice(reader),
			(1, 2) | (2, 2) => Decimal::skip_revisioned_slice(reader),
			(2, 3) => i8::skip_revisioned_slice(reader),
			(2, 4) => u8::skip_revisioned_slice(reader),
			(2, 5) => i16::skip_revisioned_slice(reader),
			(2, 6) => u16::skip_revisioned_slice(reader),
			(2, 7) => i32::skip_revisioned_slice(reader),
			(2, 8) => u32::skip_revisioned_slice(reader),
			(2, 9) => u64::skip_revisioned_slice(reader),
			(2, 10) => u16::skip_revisioned_slice(reader),
			(2, 11) => f32::skip_revisioned_slice(reader),
			(3, 12) => String::skip_revisioned_slice(reader),
			_ => Err(revision::Error::Deserialize(format!(
				"Invalid enum variant `{variant}` for type `Number` at revision `{rev}`"
			))),
		}
	}
}

impl SkipCheckRevisioned for Number {
	fn skip_check_revisioned<R: std::io::Read>(
		reader: &mut R,
	) -> std::result::Result<(), revision::Error> {
		Self::skip_revisioned(reader)
	}
}

impl WalkRevisioned for Number {
	type Walker<'r, R: revision::BorrowedReader + 'r> = revision::LeafWalker<'r, Number, R>;

	#[inline]
	fn walk_revisioned<'r, R: revision::BorrowedReader>(
		reader: &'r mut R,
	) -> std::result::Result<Self::Walker<'r, R>, revision::Error> {
		Ok(revision::LeafWalker::new(reader))
	}
}

impl Default for Number {
	fn default() -> Self {
		Self::Int(0)
	}
}

macro_rules! from_prim_ints {
	($($int: ty),*) => {
		$(
			impl From<$int> for Number {
				fn from(i: $int) -> Self {
					Self::Int(i as i64)
				}
			}
		)*
	};
}

from_prim_ints!(i8, i16, i32, i64, isize, u8, u16, u32);

impl From<u64> for Number {
	fn from(v: u64) -> Self {
		if v <= i64::MAX as u64 {
			Self::Int(v as i64)
		} else {
			Self::Decimal(v.into())
		}
	}
}

impl From<usize> for Number {
	fn from(v: usize) -> Self {
		if v <= i64::MAX as usize {
			Self::Int(v as i64)
		} else {
			Self::Decimal(v.into())
		}
	}
}

impl TryFrom<i128> for Number {
	type Error = Error;
	fn try_from(i: i128) -> Result<Self, Self::Error> {
		if let Ok(v) = i64::try_from(i) {
			Ok(Self::Int(v))
		} else if let Some(v) = Decimal::from_i128(i) {
			Ok(Self::Decimal(v))
		} else {
			Err(Error::TryFrom(i.to_string(), "Number"))
		}
	}
}

impl TryFrom<u128> for Number {
	type Error = Error;
	fn try_from(i: u128) -> Result<Self, Self::Error> {
		if let Ok(v) = i64::try_from(i) {
			Ok(Self::Int(v))
		} else if let Some(v) = Decimal::from_u128(i) {
			Ok(Self::Decimal(v))
		} else {
			Err(Error::TryFrom(i.to_string(), "Number"))
		}
	}
}

impl From<half::f16> for Number {
	fn from(f: half::f16) -> Self {
		Self::Float(f.to_f64())
	}
}

impl From<f32> for Number {
	fn from(f: f32) -> Self {
		Self::Float(f as f64)
	}
}

impl From<f64> for Number {
	fn from(f: f64) -> Self {
		Self::Float(f)
	}
}

impl From<Decimal> for Number {
	fn from(v: Decimal) -> Self {
		Self::Decimal(v)
	}
}

impl From<surrealdb_types::Number> for Number {
	fn from(v: surrealdb_types::Number) -> Self {
		match v {
			surrealdb_types::Number::Int(i) => Self::Int(i),
			surrealdb_types::Number::Float(f) => Self::Float(f),
			surrealdb_types::Number::Decimal(d) => Self::Decimal(d),
			surrealdb_types::Number::I8(i) => Self::I8(i),
			surrealdb_types::Number::U8(u) => Self::U8(u),
			surrealdb_types::Number::I16(i) => Self::I16(i),
			surrealdb_types::Number::U16(u) => Self::U16(u),
			surrealdb_types::Number::I32(i) => Self::I32(i),
			surrealdb_types::Number::U32(u) => Self::U32(u),
			surrealdb_types::Number::U64(u) => Self::U64(u),
			surrealdb_types::Number::F16(f) => Self::F16(f),
			surrealdb_types::Number::F32(f) => Self::F32(f),
			surrealdb_types::Number::BigDecimal(b) => Self::BigDecimal(b),
		}
	}
}

impl From<Number> for surrealdb_types::Number {
	fn from(v: Number) -> Self {
		match v {
			Number::Int(i) => Self::Int(i),
			Number::Float(f) => Self::Float(f),
			Number::Decimal(d) => Self::Decimal(d),
			Number::I8(i) => Self::I8(i),
			Number::U8(u) => Self::U8(u),
			Number::I16(i) => Self::I16(i),
			Number::U16(u) => Self::U16(u),
			Number::I32(i) => Self::I32(i),
			Number::U32(u) => Self::U32(u),
			Number::U64(u) => Self::U64(u),
			Number::F16(f) => Self::F16(f),
			Number::F32(f) => Self::F32(f),
			Number::BigDecimal(b) => Self::BigDecimal(b),
		}
	}
}

impl FromStr for Number {
	type Err = ();
	fn from_str(s: &str) -> Result<Self, Self::Err> {
		// Attempt to parse as i64
		match s.parse::<i64>() {
			// Store it as an i64
			Ok(v) => Ok(Self::Int(v)),
			// It wasn't parsed as a i64 so parse as a float
			_ => match s.parse::<f64>() {
				// Store it as a float
				Ok(v) => Ok(Self::Float(v)),
				// It wasn't parsed as a number
				_ => Err(()),
			},
		}
	}
}

macro_rules! try_into_prim {
	// TODO: switch to one argument per int once https://github.com/rust-lang/rust/issues/29599 is stable
	($($int: ty => $to_int: ident),*) => {
		$(
			impl TryFrom<Number> for $int {
				type Error = Error;
				fn try_from(value: Number) -> Result<Self, Self::Error> {
					match value {
						Number::Int(v) => match v.$to_int() {
							Some(v) => Ok(v),
							None => Err(Error::TryFrom(value.to_sql(), stringify!($int))),
						},
						Number::Float(v) => match v.$to_int() {
							Some(v) => Ok(v),
							None => Err(Error::TryFrom(value.to_sql(), stringify!($int))),
						},
						Number::Decimal(ref v) => match v.$to_int() {
							Some(v) => Ok(v),
							None => Err(Error::TryFrom(value.to_sql(), stringify!($int))),
						},
						Number::BigDecimal(ref v) => match Decimal::try_from(v.as_ref()).ok().and_then(|d| d.$to_int()) {
							Some(v) => Ok(v),
							None => Err(Error::TryFrom(value.to_sql(), stringify!($int))),
						},
						Number::I8(v) => match v.$to_int() {
							Some(v) => Ok(v),
							None => Err(Error::TryFrom(value.to_sql(), stringify!($int))),
						},
						Number::U8(v) => match v.$to_int() {
							Some(v) => Ok(v),
							None => Err(Error::TryFrom(value.to_sql(), stringify!($int))),
						},
						Number::I16(v) => match v.$to_int() {
							Some(v) => Ok(v),
							None => Err(Error::TryFrom(value.to_sql(), stringify!($int))),
						},
						Number::U16(v) => match v.$to_int() {
							Some(v) => Ok(v),
							None => Err(Error::TryFrom(value.to_sql(), stringify!($int))),
						},
						Number::I32(v) => match v.$to_int() {
							Some(v) => Ok(v),
							None => Err(Error::TryFrom(value.to_sql(), stringify!($int))),
						},
						Number::U32(v) => match v.$to_int() {
							Some(v) => Ok(v),
							None => Err(Error::TryFrom(value.to_sql(), stringify!($int))),
						},
						Number::U64(v) => match v.$to_int() {
							Some(v) => Ok(v),
							None => Err(Error::TryFrom(value.to_sql(), stringify!($int))),
						},
						Number::F16(v) => match v.to_f32().$to_int() {
							Some(v) => Ok(v),
							None => Err(Error::TryFrom(value.to_sql(), stringify!($int))),
						},
						Number::F32(v) => match v.$to_int() {
							Some(v) => Ok(v),
							None => Err(Error::TryFrom(value.to_sql(), stringify!($int))),
						},
					}
				}
			}
		)*
	};
}

try_into_prim!(
	i8 => to_i8, i16 => to_i16, i32 => to_i32, i64 => to_i64, i128 => to_i128,
	u8 => to_u8, u16 => to_u16, u32 => to_u32, u64 => to_u64, u128 => to_u128,
	f32 => to_f32, f64 => to_f64
);

impl TryFrom<Number> for half::f16 {
	type Error = Error;
	fn try_from(value: Number) -> Result<Self, Self::Error> {
		value.as_f16().ok_or_else(|| Error::TryFrom(value.to_sql(), "f16"))
	}
}

impl TryFrom<Number> for Decimal {
	type Error = Error;
	fn try_from(value: Number) -> Result<Self, Self::Error> {
		match value {
			Number::Int(v) => match Decimal::from_i64(v) {
				Some(v) => Ok(v),
				None => Err(Error::TryFrom(value.to_sql(), "Decimal")),
			},
			Number::Float(v) => match Decimal::try_from(v) {
				Ok(v) => Ok(v),
				_ => Err(Error::TryFrom(value.to_sql(), "Decimal")),
			},
			Number::Decimal(x) => Ok(x),
			Number::BigDecimal(ref v) => match Decimal::try_from(v.as_ref()) {
				Ok(d) => Ok(d),
				_ => Err(Error::TryFrom(value.to_sql(), "Decimal")),
			},
			Number::I8(v) => Ok(Decimal::from(v)),
			Number::U8(v) => Ok(Decimal::from(v)),
			Number::I16(v) => Ok(Decimal::from(v)),
			Number::U16(v) => Ok(Decimal::from(v)),
			Number::I32(v) => Ok(Decimal::from(v)),
			Number::U32(v) => Ok(Decimal::from(v)),
			Number::U64(v) => Ok(Decimal::from(v)),
			Number::F16(v) => match Decimal::try_from(v.to_f64()) {
				Ok(v) => Ok(v),
				_ => Err(Error::TryFrom(value.to_sql(), "Decimal")),
			},
			Number::F32(v) => match Decimal::try_from(v as f64) {
				Ok(v) => Ok(v),
				_ => Err(Error::TryFrom(value.to_sql(), "Decimal")),
			},
		}
	}
}

impl Display for Number {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		match self {
			Number::Int(v) => Display::fmt(v, f),
			Number::Float(v) => Display::fmt(v, f),
			Number::Decimal(v) => Display::fmt(v, f),
			Number::I8(v) => Display::fmt(v, f),
			Number::U8(v) => Display::fmt(v, f),
			Number::I16(v) => Display::fmt(v, f),
			Number::U16(v) => Display::fmt(v, f),
			Number::I32(v) => Display::fmt(v, f),
			Number::U32(v) => Display::fmt(v, f),
			Number::U64(v) => Display::fmt(v, f),
			Number::F16(v) => {
				if !v.is_finite() {
					return Display::fmt(v, f);
				}
				let s = surrealdb_types::Number::F16(*v).to_sql();
				f.write_str(s.strip_suffix("f16").unwrap_or(&s))
			}
			Number::F32(v) => Display::fmt(v, f),
			Number::BigDecimal(v) => write!(f, "{v}bigdec"),
		}
	}
}

impl ToSql for Number {
	fn fmt_sql(&self, f: &mut String, sql_fmt: SqlFormat) {
		surrealdb_types::Number::from(self.clone()).fmt_sql(f, sql_fmt);
	}
}

pub use surrealdb_types::Wide;

impl Number {
	// -----------------------------------
	// Constants
	// -----------------------------------

	pub const NAN: Number = Number::Float(f64::NAN);

	// -----------------------------------
	// Simple number detection
	// -----------------------------------

	pub fn is_int(&self) -> bool {
		matches!(self, Number::Int(_))
	}

	pub fn wide(&self) -> Wide<'_> {
		match *self {
			Number::BigDecimal(ref v) => Wide::BigDecimal(v.as_ref()),
			Number::Int(v) => Wide::Int(v),
			Number::Float(v) => Wide::Float(v),
			Number::Decimal(v) => Wide::Decimal(v),
			Number::I8(v) => Wide::Int(v as i64),
			Number::U8(v) => Wide::Int(v as i64),
			Number::I16(v) => Wide::Int(v as i64),
			Number::U16(v) => Wide::Int(v as i64),
			Number::I32(v) => Wide::Int(v as i64),
			Number::U32(v) => Wide::Int(v as i64),
			Number::U64(v) => match i64::try_from(v) {
				Ok(i) => Wide::Int(i),
				Err(_) => Wide::Decimal(Decimal::from(v)),
			},
			Number::F16(v) => Wide::Float(v.to_f64()),
			Number::F32(v) => Wide::Float(v as f64),
		}
	}

	pub fn widened(&self) -> Number {
		match self.wide() {
			Wide::Int(v) => Number::Int(v),
			Wide::Float(v) => Number::Float(v),
			Wide::Decimal(v) => Number::Decimal(v),
			Wide::BigDecimal(v) => Number::BigDecimal(Box::new(v.clone())),
		}
	}

	/// The value as a `bigdecimal` for a cast: every number that has one, floats included. A
	/// float converts through its shortest decimal text, and NaN or an infinity has none.
	pub(crate) fn as_bigdecimal_cast(&self) -> Option<BigDecimal> {
		self.to_big_for_op()
	}

	/// The value as a `bigdecimal` for a coercion. Every finite number has one: a float converts
	/// through its shortest decimal text, as a coercion to `decimal` converts it, so a column of
	/// either kind accepts the same numbers. NaN and the infinities have none.
	pub(crate) fn as_bigdecimal_lossless(&self) -> Option<BigDecimal> {
		self.to_big_for_op()
	}

	/// The value as a [`BigDecimal`], for arithmetic in which a `bigdecimal` operand makes the
	/// result a `bigdecimal`. Integers and `decimal` convert exactly. A float converts through
	/// its shortest decimal text, as a `decimal` operand converts a float, and is `None` when
	/// it is NaN or infinite.
	fn to_big_for_op(&self) -> Option<BigDecimal> {
		match *self {
			Number::Int(v) => Some(BigDecimal::from(v)),
			Number::Float(v) => BigDecimal::try_from_f64(v).ok(),
			Number::Decimal(v) => Some(BigDecimal::from(v)),
			Number::I8(v) => Some(BigDecimal::from(i64::from(v))),
			Number::U8(v) => Some(BigDecimal::from(u64::from(v))),
			Number::I16(v) => Some(BigDecimal::from(i64::from(v))),
			Number::U16(v) => Some(BigDecimal::from(u64::from(v))),
			Number::I32(v) => Some(BigDecimal::from(i64::from(v))),
			Number::U32(v) => Some(BigDecimal::from(u64::from(v))),
			Number::U64(v) => Some(BigDecimal::from(v)),
			Number::F16(v) => BigDecimal::try_from_f64(v.to_f64()).ok(),
			Number::F32(v) => BigDecimal::try_from_f64(f64::from(v)).ok(),
			Number::BigDecimal(ref v) => Some(v.as_ref().clone()),
		}
	}

	pub fn number_kind(&self) -> NumberKind {
		match *self {
			Number::BigDecimal(_) => NumberKind::BigDecimal,
			Number::Int(_) => NumberKind::Int,
			Number::Float(_) => NumberKind::Float,
			Number::Decimal(_) => NumberKind::Decimal,
			Number::I8(_) => NumberKind::I8,
			Number::U8(_) => NumberKind::U8,
			Number::I16(_) => NumberKind::I16,
			Number::U16(_) => NumberKind::U16,
			Number::I32(_) => NumberKind::I32,
			Number::U32(_) => NumberKind::U32,
			Number::U64(_) => NumberKind::U64,
			Number::F16(_) => NumberKind::F16,
			Number::F32(_) => NumberKind::F32,
		}
	}

	pub fn is_nan(&self) -> bool {
		match self {
			Number::Float(f) => f.is_nan(),
			Number::F16(f) => f.is_nan(),
			Number::F32(f) => f.is_nan(),
			Number::Int(_)
			| Number::Decimal(_)
			| Number::I8(_)
			| Number::U8(_)
			| Number::I16(_)
			| Number::U16(_)
			| Number::I32(_)
			| Number::U32(_)
			| Number::U64(_)
			| Number::BigDecimal(_) => false,
		}
	}

	/// Adds `other` to a running total that must stay exact.
	///
	/// Two integers whose sum does not fit an `i64` are summed as a `Decimal`
	/// rather than wrapping or failing. A float anywhere makes the total a
	/// float, whichever side holds the decimal that came of widening integers,
	/// so a total does not depend on the order its terms arrive in. Every other
	/// pairing is [`TryAdd`]. Used by aggregates that finish with a division
	/// (`math::mean`), where an intermediate total may leave `i64` range and
	/// come back.
	#[allow(
		clippy::needless_pass_by_value,
		reason = "Number is not Copy; the arithmetic methods take their operands by value like the operator impls"
	)]
	pub fn add_exact(self, other: Number) -> Result<Number> {
		match (self.wide(), other.wide()) {
			(Wide::Int(v), Wide::Int(w)) => Ok(match v.checked_add(w) {
				Some(sum) => Number::Int(sum),
				None => Number::Decimal(Decimal::from(v) + Decimal::from(w)),
			}),
			(Wide::Decimal(d), Wide::Float(f)) | (Wide::Float(f), Wide::Decimal(d)) => {
				Ok(Number::Float(d.to_f64().unwrap_or(f64::NAN) + f))
			}
			(Wide::Int(_), Wide::Float(_))
			| (Wide::Float(_), Wide::Int(_))
			| (Wide::Float(_), Wide::Float(_))
			| (Wide::Int(_), Wide::Decimal(_))
			| (Wide::Decimal(_), Wide::Int(_))
			| (Wide::Decimal(_), Wide::Decimal(_))
			| (Wide::BigDecimal(_), _)
			| (_, Wide::BigDecimal(_)) => self.widened().try_add(other.widened()),
		}
	}

	/// Subtracts `other` from a running total kept by [`Self::add_exact`], with
	/// the same widening: two integers whose difference does not fit an `i64`
	/// are subtracted as a `Decimal`, and a float makes the result a float.
	#[allow(
		clippy::needless_pass_by_value,
		reason = "Number is not Copy; the arithmetic methods take their operands by value like the operator impls"
	)]
	pub fn sub_exact(self, other: Number) -> Result<Number> {
		match (self.wide(), other.wide()) {
			(Wide::Int(v), Wide::Int(w)) => Ok(match v.checked_sub(w) {
				Some(difference) => Number::Int(difference),
				None => Number::Decimal(Decimal::from(v) - Decimal::from(w)),
			}),
			(Wide::Decimal(d), Wide::Float(f)) => {
				Ok(Number::Float(d.to_f64().unwrap_or(f64::NAN) - f))
			}
			(Wide::Float(f), Wide::Decimal(d)) => {
				Ok(Number::Float(f - d.to_f64().unwrap_or(f64::NAN)))
			}
			(Wide::Int(_), Wide::Float(_))
			| (Wide::Float(_), Wide::Int(_))
			| (Wide::Float(_), Wide::Float(_))
			| (Wide::Int(_), Wide::Decimal(_))
			| (Wide::Decimal(_), Wide::Int(_))
			| (Wide::Decimal(_), Wide::Decimal(_))
			| (Wide::BigDecimal(_), _)
			| (_, Wide::BigDecimal(_)) => self.widened().try_sub(other.widened()),
		}
	}

	pub fn is_float(&self) -> bool {
		matches!(self, Number::Float(_))
	}

	pub fn is_truthy(&self) -> bool {
		match self {
			Number::BigDecimal(v) => !v.is_zero(),
			Number::Int(v) => *v != 0,
			Number::Float(v) => *v != 0.0,
			Number::Decimal(v) => *v != Decimal::ZERO,
			Number::I8(v) => *v != 0,
			Number::U8(v) => *v != 0,
			Number::I16(v) => *v != 0,
			Number::U16(v) => *v != 0,
			Number::I32(v) => *v != 0,
			Number::U32(v) => *v != 0,
			Number::U64(v) => *v != 0,
			Number::F16(v) => *v != half::f16::ZERO && *v != half::f16::NEG_ZERO,
			Number::F32(v) => *v != 0.0,
		}
	}

	pub fn is_zero(&self) -> bool {
		match self {
			Number::BigDecimal(v) => v.is_zero(),
			Number::Int(v) => *v == 0,
			Number::Float(v) => *v == 0.0,
			Number::Decimal(v) => *v == Decimal::ZERO,
			Number::I8(v) => *v == 0,
			Number::U8(v) => *v == 0,
			Number::I16(v) => *v == 0,
			Number::U16(v) => *v == 0,
			Number::I32(v) => *v == 0,
			Number::U32(v) => *v == 0,
			Number::U64(v) => *v == 0,
			Number::F16(v) => *v == half::f16::ZERO || *v == half::f16::NEG_ZERO,
			Number::F32(v) => *v == 0.0,
		}
	}

	// -----------------------------------
	// Simple conversion of number
	// -----------------------------------

	pub fn as_usize(&self) -> usize {
		match *self {
			Number::BigDecimal(ref v) => {
				big_trunc_i64(v).and_then(|i| usize::try_from(i).ok()).unwrap_or_default()
			}
			Number::Int(v) => v as usize,
			Number::Float(v) => v as usize,
			Number::Decimal(v) => v.try_into().unwrap_or_default(),
			Number::I8(v) => v as usize,
			Number::U8(v) => v as usize,
			Number::I16(v) => v as usize,
			Number::U16(v) => v as usize,
			Number::I32(v) => v as usize,
			Number::U32(v) => v as usize,
			Number::U64(v) => v as usize,
			Number::F16(v) => v.to_f64() as usize,
			Number::F32(v) => v as usize,
		}
	}

	pub fn as_int(&self) -> i64 {
		match *self {
			Number::BigDecimal(ref v) => big_trunc_i64(v).unwrap_or_default(),
			Number::Int(v) => v,
			Number::Float(v) => v as i64,
			Number::Decimal(v) => v.try_into().unwrap_or_default(),
			Number::I8(v) => v as i64,
			Number::U8(v) => v as i64,
			Number::I16(v) => v as i64,
			Number::U16(v) => v as i64,
			Number::I32(v) => v as i64,
			Number::U32(v) => v as i64,
			Number::U64(v) => v as i64,
			Number::F16(v) => v.to_f64() as i64,
			Number::F32(v) => v as i64,
		}
	}

	pub fn as_float(&self) -> f64 {
		match *self {
			Number::BigDecimal(ref v) => v.to_f64(),
			Number::Int(v) => v as f64,
			Number::Float(v) => v,
			Number::Decimal(v) => v.try_into().unwrap_or_default(),
			Number::I8(v) => v as f64,
			Number::U8(v) => v as f64,
			Number::I16(v) => v as f64,
			Number::U16(v) => v as f64,
			Number::I32(v) => v as f64,
			Number::U32(v) => v as f64,
			Number::U64(v) => v as f64,
			Number::F16(v) => v.to_f64(),
			Number::F32(v) => v as f64,
		}
	}

	pub fn as_decimal(&self) -> Decimal {
		match *self {
			Number::BigDecimal(ref v) => Decimal::try_from(v.as_ref()).unwrap_or_default(),
			Number::Int(v) => Decimal::from(v),
			Number::Float(v) => Decimal::try_from(v).unwrap_or_default(),
			Number::Decimal(v) => v,
			Number::I8(v) => Decimal::from(v),
			Number::U8(v) => Decimal::from(v),
			Number::I16(v) => Decimal::from(v),
			Number::U16(v) => Decimal::from(v),
			Number::I32(v) => Decimal::from(v),
			Number::U32(v) => Decimal::from(v),
			Number::U64(v) => Decimal::from(v),
			Number::F16(v) => Decimal::try_from(v.to_f64()).unwrap_or_default(),
			Number::F32(v) => Decimal::try_from(v as f64).unwrap_or_default(),
		}
	}

	// -----------------------------------
	// Complex conversion of number
	// -----------------------------------

	/// Convert to an `i64` only when the value round-trips exactly.
	///
	/// Returns `None` for any value that does not represent an exact integer
	/// within `i64` range — including NaN, infinities, fractional values, and
	/// out-of-range magnitudes. Lossy `as` casts (which saturate or truncate
	/// silently) are deliberately avoided so callers can reject the input
	/// rather than write a wrong value.
	pub fn as_int_lossless(&self) -> Option<i64> {
		match *self {
			Number::BigDecimal(ref v) => v.to_i64(),
			Number::Int(v) => Some(v),
			Number::Float(v) => {
				// `i64::MAX as f64` rounds up to 2^63 (the next representable
				// f64), which is *not* in i64 range. `-(i64::MIN as f64)` is
				// exactly 2^63 and is the right exclusive upper bound.
				if !v.is_finite()
					|| v.fract() != 0.0
					|| v < i64::MIN as f64
					|| v >= -(i64::MIN as f64)
				{
					return None;
				}
				Some(v as i64)
			}
			Number::Decimal(v) => {
				if v.fract().is_zero() {
					v.try_into().ok()
				} else {
					None
				}
			}
			Number::I8(v) => Some(v as i64),
			Number::U8(v) => Some(v as i64),
			Number::I16(v) => Some(v as i64),
			Number::U16(v) => Some(v as i64),
			Number::I32(v) => Some(v as i64),
			Number::U32(v) => Some(v as i64),
			Number::U64(v) => i64::try_from(v).ok(),
			Number::F16(v) => {
				let f = v.to_f32();
				if !f.is_finite() || f.fract() != 0.0 {
					return None;
				}
				Some(f as i64)
			}
			Number::F32(v) => {
				if !v.is_finite() || v.fract() != 0.0 {
					return None;
				}
				let f = v as f64;
				if f < i64::MIN as f64 || f >= -(i64::MIN as f64) {
					return None;
				}
				Some(v as i64)
			}
		}
	}

	/// Convert to an `f64` only when the value can be represented as an `f64`.
	pub fn as_float_lossless(&self) -> Option<f64> {
		match *self {
			Number::BigDecimal(ref v) => Some(v.to_f64()).filter(|f| f.is_finite()),
			Number::Int(v) => Some(v as f64),
			Number::Float(v) => Some(v),
			Number::Decimal(v) => f64::try_from(v).ok(),
			Number::I8(v) => Some(v as f64),
			Number::U8(v) => Some(v as f64),
			Number::I16(v) => Some(v as f64),
			Number::U16(v) => Some(v as f64),
			Number::I32(v) => Some(v as f64),
			Number::U32(v) => Some(v as f64),
			Number::U64(v) => Some(v as f64),
			Number::F16(v) => Some(v.to_f64()),
			Number::F32(v) => Some(v as f64),
		}
	}

	/// Convert to a `u64` only when the value represents an exact integer in `0..=u64::MAX`.
	pub fn as_u64_lossless(&self) -> Option<u64> {
		const TWO_POW_64: f64 = f64::from_bits(0x43f0_0000_0000_0000);
		match *self {
			Number::BigDecimal(ref v) => v.to_u64(),
			Number::Int(v) => u64::try_from(v).ok(),
			Number::Float(v) => {
				if !v.is_finite() || v < 0.0 || v.fract() != 0.0 || v >= TWO_POW_64 {
					return None;
				}
				let val = v as u64;
				((val as f64) == v).then_some(val)
			}
			Number::Decimal(v) => {
				use rust_decimal::prelude::ToPrimitive;
				if v.fract().is_zero() {
					v.to_u64()
				} else {
					None
				}
			}
			Number::I8(v) => u64::try_from(v).ok(),
			Number::U8(v) => Some(v as u64),
			Number::I16(v) => u64::try_from(v).ok(),
			Number::U16(v) => Some(v as u64),
			Number::I32(v) => u64::try_from(v).ok(),
			Number::U32(v) => Some(v as u64),
			Number::U64(v) => Some(v),
			Number::F16(v) => {
				let f = v.to_f64();
				(f.is_finite() && f >= 0.0 && f.fract() == 0.0).then_some(f as u64)
			}
			Number::F32(v) => {
				let f = v as f64;
				if !f.is_finite() || f < 0.0 || f.fract() != 0.0 || f >= TWO_POW_64 {
					return None;
				}
				let val = f as u64;
				((val as f64) == f).then_some(val)
			}
		}
	}

	/// Convert to an `f32`, refusing finite inputs that overflow to infinity.
	pub fn as_f32(&self) -> Option<f32> {
		let f = self.as_float_lossless()?;
		let res = f as f32;
		if f.is_finite() && !res.is_finite() {
			None
		} else {
			Some(res)
		}
	}

	/// Convert to an `f16`, refusing finite inputs that overflow to infinity.
	pub fn as_f16(&self) -> Option<half::f16> {
		let f = self.as_float_lossless()?;
		let res = half::f16::from_f64(f);
		if f.is_finite() && !res.is_finite() {
			None
		} else {
			Some(res)
		}
	}

	/// Convert to a `Decimal` only when the value can be represented as a `Decimal`.
	pub fn as_decimal_lossless(&self) -> Option<Decimal> {
		match *self {
			Number::BigDecimal(ref v) => Decimal::try_from(v.as_ref()).ok(),
			Number::Int(v) => Some(Decimal::from(v)),
			Number::Float(v) => Decimal::try_from(v).ok(),
			Number::Decimal(v) => Some(v),
			Number::I8(v) => Some(Decimal::from(v)),
			Number::U8(v) => Some(Decimal::from(v)),
			Number::I16(v) => Some(Decimal::from(v)),
			Number::U16(v) => Some(Decimal::from(v)),
			Number::I32(v) => Some(Decimal::from(v)),
			Number::U32(v) => Some(Decimal::from(v)),
			Number::U64(v) => Some(Decimal::from(v)),
			Number::F16(v) => Decimal::try_from(v.to_f64()).ok(),
			Number::F32(v) => Decimal::try_from(v as f64).ok(),
		}
	}

	/// Convert to a `usize` suitable for array indexing.
	///
	/// Returns `None` for any value that does not represent an exact
	/// non-negative integer within `usize` range — including negatives,
	/// NaN, infinities, fractional values, and out-of-range magnitudes.
	pub fn as_array_index(&self) -> Option<usize> {
		match *self {
			Number::BigDecimal(ref v) => v.to_u64().and_then(|u| usize::try_from(u).ok()),
			Number::Int(v) => usize::try_from(v).ok(),
			Number::Float(v) => {
				if !v.is_finite() || v < 0.0 || v.fract() != 0.0 {
					return None;
				}
				// `f64 as usize` saturates above `usize::MAX`; round-trip back
				// through f64 to reject any value that couldn't be represented
				// exactly.
				let idx = v as usize;
				(idx as f64 == v).then_some(idx)
			}
			Number::Decimal(v) => {
				if v.fract().is_zero() {
					v.to_usize()
				} else {
					None
				}
			}
			Number::I8(v) => usize::try_from(v).ok(),
			Number::U8(v) => Some(v as usize),
			Number::I16(v) => usize::try_from(v).ok(),
			Number::U16(v) => Some(v as usize),
			Number::I32(v) => usize::try_from(v).ok(),
			Number::U32(v) => Some(v as usize),
			Number::U64(v) => usize::try_from(v).ok(),
			Number::F16(v) => {
				let f = v.to_f32();
				if !f.is_finite() || f < 0.0 || f.fract() != 0.0 {
					return None;
				}
				Some(f as usize)
			}
			Number::F32(v) => {
				if !v.is_finite() || v < 0.0 || v.fract() != 0.0 {
					return None;
				}
				let idx = v as usize;
				(idx as f32 == v).then_some(idx)
			}
		}
	}

	pub fn to_int(&self) -> i64 {
		match *self {
			Number::BigDecimal(ref v) => big_trunc_i64(v).unwrap_or_default(),
			Number::Int(v) => v,
			Number::Float(v) => v as i64,
			Number::Decimal(v) => v.to_i64().unwrap_or_default(),
			Number::I8(v) => v as i64,
			Number::U8(v) => v as i64,
			Number::I16(v) => v as i64,
			Number::U16(v) => v as i64,
			Number::I32(v) => v as i64,
			Number::U32(v) => v as i64,
			Number::U64(v) => v as i64,
			Number::F16(v) => v.to_f64() as i64,
			Number::F32(v) => v as i64,
		}
	}

	/// The number as an `f64`. Inlined so that per-component loops in other
	/// crates (the vector functions' norms and distances) compile it into the
	/// loop whatever link-time optimisation decides.
	#[inline]
	pub fn to_float(&self) -> f64 {
		match *self {
			Number::BigDecimal(ref v) => v.to_f64(),
			Number::Int(v) => v as f64,
			Number::Float(v) => v,
			Number::Decimal(v) => v.try_into().unwrap_or_default(),
			Number::I8(v) => v as f64,
			Number::U8(v) => v as f64,
			Number::I16(v) => v as f64,
			Number::U16(v) => v as f64,
			Number::I32(v) => v as f64,
			Number::U32(v) => v as f64,
			Number::U64(v) => v as f64,
			Number::F16(v) => v.to_f64(),
			Number::F32(v) => v as f64,
		}
	}

	pub fn to_decimal(&self) -> Decimal {
		match *self {
			Number::BigDecimal(ref v) => Decimal::try_from(v.as_ref()).unwrap_or_default(),
			Number::Int(v) => Decimal::from(v),
			Number::Float(v) => Decimal::from_f64(v).unwrap_or_default(),
			Number::Decimal(v) => v,
			Number::I8(v) => Decimal::from(v),
			Number::U8(v) => Decimal::from(v),
			Number::I16(v) => Decimal::from(v),
			Number::U16(v) => Decimal::from(v),
			Number::I32(v) => Decimal::from(v),
			Number::U32(v) => Decimal::from(v),
			Number::U64(v) => Decimal::from(v),
			Number::F16(v) => Decimal::from_f32(v.to_f32()).unwrap_or_default(),
			Number::F32(v) => Decimal::from_f32(v).unwrap_or_default(),
		}
	}

	/// Converts this Number to a lexicographically ordered byte buffer.
	///
	/// This serializes the Number using DecimalLexEncoder so that byte-wise
	/// comparison preserves numeric ordering. This is essential for database
	/// indexes where key bytes must sort the same way as their numeric values.
	///
	/// Ordering guarantees:
	/// - If `a < b` numerically, then `a.as_decimal_buf() < b.as_decimal_buf()` lexicographically.
	///
	/// Encoding format:
	/// - A leading class/marker byte indicates zero, finite negative, finite positive, negative
	///   infinity, positive infinity, or NaN.
	/// - Two bytes encode a biased scale for finite values.
	/// - Packed base-10 digits follow (2 digits per byte), with an in-band terminator ensured by
	///   the packing scheme; the encoder also appends a trailing 0x00 terminator byte for
	///   stream-friendly decoding.
	///
	/// Notes:
	/// - There is no extra "type marker" for Int/Float/Decimal variants; all variants are converted
	///   to a [`LexDecimal`] first, which is what orders them against each other.
	/// - Special float values (NaN/±∞) are mapped to fixed encodings at the extremes to preserve a
	///   total order.
	///
	/// A `bigdecimal` is spelled in the same encoding, from its coefficient's digits as written
	/// and its scale, so it orders among every other kind and has the bytes of a `decimal` with
	/// the same coefficient and scale: `1.50bigdec` is spelled as `1.50dec` and `1bigdec` as
	/// `1`. A coefficient past 39 digits, or a scale past the two-byte range, takes the
	/// encoding's wide forms, which no other kind reaches.
	pub fn as_decimal_buf(&self) -> Vec<u8> {
		match self.key_form() {
			KeyForm::Lex(dec) => DecimalLexEncoder::encode(dec),
			KeyForm::Big(v) => bigdecimal_buf(v),
		}
	}

	/// The value in the form the key encoding works on: a [`LexDecimal`] for every kind but a
	/// `bigdecimal` (see [`LexDecimal`] for how each converts), and the `bigdecimal` itself,
	/// whose coefficient a [`LexDecimal`] need not hold and which is spelled from its digits.
	fn key_form(&self) -> KeyForm<'_> {
		KeyForm::Lex(match self {
			Self::Int(v) => LexDecimal::from_i64(*v),
			Self::Float(v) => LexDecimal::from_f64(*v),
			Self::Decimal(v) => LexDecimal::from_decimal(*v),
			Self::I8(v) => LexDecimal::from_i64(*v as i64),
			Self::U8(v) => LexDecimal::from_i64(*v as i64),
			Self::I16(v) => LexDecimal::from_i64(*v as i64),
			Self::U16(v) => LexDecimal::from_i64(*v as i64),
			Self::I32(v) => LexDecimal::from_i64(*v as i64),
			Self::U32(v) => LexDecimal::from_i64(*v as i64),
			Self::U64(v) => match i64::try_from(*v) {
				Ok(i) => LexDecimal::from_i64(i),
				Err(_) => LexDecimal::from_decimal(Decimal::from(*v)),
			},
			Self::F16(v) => LexDecimal::from_f64(f64::from(*v)),
			Self::F32(v) => LexDecimal::from_f64(*v as f64),
			Self::BigDecimal(v) => return KeyForm::Big(v),
		})
	}

	/// Reconstructs a Number from a lexicographically ordered byte buffer.
	///
	/// This deserializes a buffer produced by `as_decimal_buf()` using
	/// DecimalLexEncoder, recovering the numeric value. All Number variants are
	/// normalized through the same encoding, so the original variant (Int/Float/
	/// Decimal) is not preserved; only the value (and special cases like NaN/±∞)
	/// matters for ordering and equality in keys.
	///
	/// The decoder recognizes:
	/// - Zero, finite negatives, finite positives (via marker and biased scale)
	/// - Negative/positive infinity, NaN (fixed encodings)
	/// - An explicit in-band terminator added by the encoder, which ensures the mantissa decoder
	///   stops before any following data in the stream.
	///
	/// A finite value decodes as a `Decimal` when it is zero or `10^-6 <= |value| < 2^96`,
	/// rounded to the 28 fractional digits and 96-bit mantissa a `Decimal` holds (see
	/// [`LexDecimal::to_decimal_rounded`]), and as the nearest `f64` otherwise. Which one
	/// depends on the value's magnitude alone, and the decoded value is within rounding of the
	/// encoded one at every magnitude. NaN and the infinities decode as the `f64` they stand for.
	///
	/// A value in one of the wide forms, which only a `bigdecimal` is written in (a coefficient
	/// past a `u128`, or a scale past the two-byte range), decodes exactly as a `bigdecimal`.
	///
	/// Returns the reconstructed Number or an error if the buffer is empty or
	/// cannot be decoded.
	pub fn from_decimal_buf(b: &[u8]) -> Result<Self> {
		Ok(match DecimalLexEncoder::decode_key(b)? {
			KeyDecimal::Lex(dec) => match dec.to_decimal_rounded() {
				Some(dec) => Number::Decimal(dec),
				None => Number::Float(dec.to_f64()),
			},
			KeyDecimal::Wide {
				negative,
				digits,
				exponent,
			} => Number::BigDecimal(Box::new(BigDecimal::from_digits(negative, &digits, exponent)?)),
		})
	}

	/// Reconstructs a Number of the given kind from a buffer produced by
	/// `as_decimal_buf()`.
	///
	/// This is the inverse of the buffer's encoding for the kind it was written
	/// with: an `Int` decodes to the same `i64`, a `Decimal` to a `Decimal` of
	/// the same value, and a `Float` to the same `f64` bit for bit (the sign of a
	/// zero and the payload of a NaN are not stored). Decoding then encoding
	/// therefore reproduces the buffer, which a record id read back out of a key
	/// relies on to address its own record.
	///
	/// A float is decoded from the decimal digits by [`LexDecimal::to_f64`]. A `bigdecimal`
	/// decodes from its digits exactly, with the scale its trailing zeros give it, in any form
	/// of the encoding; every other kind is read only from the forms it is written in.
	pub fn from_decimal_buf_kind(b: &[u8], kind: NumberKind) -> Result<Self> {
		if kind == NumberKind::BigDecimal {
			let decimal = match DecimalLexEncoder::decode_key(b)? {
				KeyDecimal::Lex(LexDecimal::Finite {
					negative,
					coefficient,
					exponent,
				}) => BigDecimal::from_digits(
					negative,
					coefficient.to_string().as_bytes(),
					i64::from(exponent),
				)?,
				KeyDecimal::Lex(
					dec @ (LexDecimal::NaN
					| LexDecimal::Infinite {
						..
					}),
				) => {
					bail!("Invalid bigdecimal value: {dec}")
				}
				KeyDecimal::Wide {
					negative,
					digits,
					exponent,
				} => BigDecimal::from_digits(negative, &digits, exponent)?,
			};
			return Ok(Number::BigDecimal(Box::new(decimal)));
		}
		let dec = DecimalLexEncoder::decode(b)?;
		match kind {
			NumberKind::BigDecimal => {
				bail!("a bigdecimal is read from its key above")
			}
			NumberKind::Int => {
				ensure!(dec.is_finite(), format!("Invalid integer value: {dec}"));
				Ok(Number::Int(dec.to_i64_positional()?))
			}
			NumberKind::Float => Ok(Number::Float(dec.to_f64())),
			NumberKind::Decimal => {
				ensure!(dec.is_finite(), format!("Invalid decimal value: {dec}"));
				Ok(Number::Decimal(dec.to_decimal_exact()?))
			}
			NumberKind::I8 => {
				ensure!(dec.is_finite(), format!("Invalid integer value: {dec}"));
				let val = dec.to_i64_positional()?;
				Ok(Number::I8(i8::try_from(val)?))
			}
			NumberKind::U8 => {
				ensure!(dec.is_finite(), format!("Invalid integer value: {dec}"));
				let val = dec.to_i64_positional()?;
				Ok(Number::U8(u8::try_from(val)?))
			}
			NumberKind::I16 => {
				ensure!(dec.is_finite(), format!("Invalid integer value: {dec}"));
				let val = dec.to_i64_positional()?;
				Ok(Number::I16(i16::try_from(val)?))
			}
			NumberKind::U16 => {
				ensure!(dec.is_finite(), format!("Invalid integer value: {dec}"));
				let val = dec.to_i64_positional()?;
				Ok(Number::U16(u16::try_from(val)?))
			}
			NumberKind::I32 => {
				ensure!(dec.is_finite(), format!("Invalid integer value: {dec}"));
				let val = dec.to_i64_positional()?;
				Ok(Number::I32(i32::try_from(val)?))
			}
			NumberKind::U32 => {
				ensure!(dec.is_finite(), format!("Invalid integer value: {dec}"));
				let val = dec.to_i64_positional()?;
				Ok(Number::U32(u32::try_from(val)?))
			}
			NumberKind::U64 => {
				ensure!(dec.is_finite(), format!("Invalid integer value: {dec}"));
				let d = dec.to_decimal_exact()?;
				Ok(Number::U64(u64::try_from(d)?))
			}
			NumberKind::F16 => Ok(Number::F16(half::f16::from_f32(dec.to_f64() as f32))),
			NumberKind::F32 => Ok(Number::F32(dec.to_f64() as f32)),
		}
	}

	// -----------------------------------
	//
	// -----------------------------------

	pub fn abs(&self) -> Self {
		match *self {
			Number::BigDecimal(ref v) => Number::BigDecimal(Box::new(v.abs())),
			Number::Int(v) => Number::Int(v.abs()),
			Number::Float(v) => Number::Float(v.abs()),
			Number::Decimal(v) => Number::Decimal(v.abs()),
			Number::I8(v) => Number::I8(v.abs()),
			Number::U8(v) => Number::U8(v),
			Number::I16(v) => Number::I16(v.abs()),
			Number::U16(v) => Number::U16(v),
			Number::I32(v) => Number::I32(v.abs()),
			Number::U32(v) => Number::U32(v),
			Number::U64(v) => Number::U64(v),
			Number::F16(v) => Number::F16(half::f16::from_bits(v.to_bits() & 0x7fff)),
			Number::F32(v) => Number::F32(v.abs()),
		}
	}

	pub fn checked_abs(&self) -> Option<Self> {
		match *self {
			Number::BigDecimal(ref v) => Some(Number::BigDecimal(Box::new(v.abs()))),
			Number::Int(v) => v.checked_abs().map(Number::Int),
			Number::Float(v) => Some(Number::Float(v.abs())),
			Number::Decimal(v) => Some(Number::Decimal(v.abs())),
			Number::I8(v) => v.checked_abs().map(Number::I8),
			Number::U8(v) => Some(Number::U8(v)),
			Number::I16(v) => v.checked_abs().map(Number::I16),
			Number::U16(v) => Some(Number::U16(v)),
			Number::I32(v) => v.checked_abs().map(Number::I32),
			Number::U32(v) => Some(Number::U32(v)),
			Number::U64(v) => Some(Number::U64(v)),
			Number::F16(v) => Some(Number::F16(half::f16::from_bits(v.to_bits() & 0x7fff))),
			Number::F32(v) => Some(Number::F32(v.abs())),
		}
	}

	fn map_float_fn(self, f: impl FnOnce(f64) -> f64) -> Self {
		match self {
			Number::F16(v) => Number::F16(half::f16::from_f64(f(v.to_f64()))),
			Number::F32(v) => Number::F32(f(v as f64) as f32),
			Number::Int(_)
			| Number::Float(_)
			| Number::Decimal(_)
			| Number::I8(_)
			| Number::U8(_)
			| Number::I16(_)
			| Number::U16(_)
			| Number::I32(_)
			| Number::U32(_)
			| Number::U64(_)
			| Number::BigDecimal(_) => Number::Float(f(self.to_float())),
		}
	}

	pub(crate) fn as_i128(&self) -> Option<i128> {
		match *self {
			Number::Int(v) => Some(v as i128),
			Number::I8(v) => Some(v as i128),
			Number::U8(v) => Some(v as i128),
			Number::I16(v) => Some(v as i128),
			Number::U16(v) => Some(v as i128),
			Number::I32(v) => Some(v as i128),
			Number::U32(v) => Some(v as i128),
			Number::U64(v) => Some(v as i128),
			Number::Float(_)
			| Number::Decimal(_)
			| Number::F16(_)
			| Number::F32(_)
			| Number::BigDecimal(_) => None,
		}
	}

	pub(crate) fn from_i128_in_kind(v: i128, kind: NumberKind) -> Option<Self> {
		match kind {
			NumberKind::I8 => i8::try_from(v).ok().map(Number::I8),
			NumberKind::U8 => u8::try_from(v).ok().map(Number::U8),
			NumberKind::I16 => i16::try_from(v).ok().map(Number::I16),
			NumberKind::U16 => u16::try_from(v).ok().map(Number::U16),
			NumberKind::I32 => i32::try_from(v).ok().map(Number::I32),
			NumberKind::U32 => u32::try_from(v).ok().map(Number::U32),
			NumberKind::Int => i64::try_from(v).ok().map(Number::Int),
			NumberKind::U64 => u64::try_from(v).ok().map(Number::U64),
			NumberKind::Decimal => Decimal::from_i128(v).map(Number::Decimal),
			NumberKind::BigDecimal => Some(Number::BigDecimal(Box::new(BigDecimal::from(v)))),
			NumberKind::F16 => Some(Number::F16(half::f16::from_f64(v as f64))),
			NumberKind::F32 => Some(Number::F32(v as f32)),
			NumberKind::Float => Some(Number::Float(v as f64)),
		}
	}

	fn from_i128_wrapping_in_kind(v: i128, kind: NumberKind) -> Self {
		match kind {
			NumberKind::I8 => Number::I8(v as i8),
			NumberKind::U8 => Number::U8(v as u8),
			NumberKind::I16 => Number::I16(v as i16),
			NumberKind::U16 => Number::U16(v as u16),
			NumberKind::I32 => Number::I32(v as i32),
			NumberKind::U32 => Number::U32(v as u32),
			NumberKind::Int => Number::Int(v as i64),
			NumberKind::U64 => Number::U64(v as u64),
			NumberKind::Decimal => Number::Decimal(Decimal::from_i128(v).unwrap_or_default()),
			NumberKind::BigDecimal => Number::BigDecimal(Box::new(BigDecimal::from(v))),
			NumberKind::F16 => Number::F16(half::f16::from_f64(v as f64)),
			NumberKind::F32 => Number::F32(v as f32),
			NumberKind::Float => Number::Float(v as f64),
		}
	}

	fn from_f64_in_kind(v: f64, kind: NumberKind) -> Self {
		match kind {
			NumberKind::F16 => Number::F16(half::f16::from_f64(v)),
			NumberKind::F32 => Number::F32(v as f32),
			NumberKind::BigDecimal => BigDecimal::try_from_f64(v)
				.map_or(Number::Float(v), |b| Number::BigDecimal(Box::new(b))),
			NumberKind::Float
			| NumberKind::Int
			| NumberKind::Decimal
			| NumberKind::I8
			| NumberKind::U8
			| NumberKind::I16
			| NumberKind::U16
			| NumberKind::I32
			| NumberKind::U32
			| NumberKind::U64 => Number::Float(v),
		}
	}

	fn to_decimal_for_op(&self) -> Option<Decimal> {
		self.as_decimal_lossless()
	}

	fn op_err_string(&self) -> String {
		match self {
			Number::Decimal(d) => d.to_string(),
			Number::Int(_)
			| Number::Float(_)
			| Number::I8(_)
			| Number::U8(_)
			| Number::I16(_)
			| Number::U16(_)
			| Number::I32(_)
			| Number::U32(_)
			| Number::U64(_)
			| Number::F16(_)
			| Number::F32(_)
			| Number::BigDecimal(_) => self.to_sql(),
		}
	}

	pub fn acos(self) -> Self {
		self.map_float_fn(f64::acos)
	}

	pub fn asin(self) -> Self {
		self.map_float_fn(f64::asin)
	}

	pub fn atan(self) -> Self {
		self.map_float_fn(f64::atan)
	}

	pub fn acot(self) -> Self {
		self.map_float_fn(|x| PI / 2.0 - x.atan())
	}

	pub fn ceil(self) -> Self {
		match self {
			Number::Int(v) => Number::Int(v),
			Number::Float(v) => Number::Float(v.ceil()),
			Number::Decimal(v) => Number::Decimal(v.ceil()),
			Number::I8(v) => Number::I8(v),
			Number::U8(v) => Number::U8(v),
			Number::I16(v) => Number::I16(v),
			Number::U16(v) => Number::U16(v),
			Number::I32(v) => Number::I32(v),
			Number::U32(v) => Number::U32(v),
			Number::U64(v) => Number::U64(v),
			Number::F16(v) => Number::F16(half::f16::from_f64(v.to_f64().ceil())),
			Number::F32(v) => Number::F32((v as f64).ceil() as f32),
			Number::BigDecimal(v) => rounded_big(v.ceil(), &v),
		}
	}

	/// `self` held within `min..=max`, which the caller orders, NaN excluded. A decimal held
	/// between bounds that do not convert to an ordered pair of decimals is clamped as a float.
	/// Never panics: bounds that the conversion to floats leaves out of order hold `self` at the
	/// bound it passes, and a NaN `self` stays NaN.
	#[allow(
		clippy::needless_pass_by_value,
		reason = "Number is not Copy; the arithmetic methods take their operands by value like the operator impls"
	)]
	pub fn clamp(self, min: Self, max: Self) -> Self {
		fn clamp_f64(n: f64, low: f64, high: f64) -> f64 {
			if n < low {
				low
			} else if n > high {
				high
			} else {
				n
			}
		}
		let exact = |bound: &Self| match bound {
			Number::Float(f) => Decimal::from_f64(*f),
			Number::F16(f) => Decimal::from_f64(f64::from(*f)),
			Number::F32(f) => Decimal::from_f64(*f as f64),
			Number::BigDecimal(b) => Decimal::try_from(b.as_ref()).ok(),
			Number::Int(_)
			| Number::Decimal(_)
			| Number::I8(_)
			| Number::U8(_)
			| Number::I16(_)
			| Number::U16(_)
			| Number::I32(_)
			| Number::U32(_)
			| Number::U64(_) => Some(bound.to_decimal()),
		};
		if let (Some(n), Some(low), Some(high)) = (self.as_i128(), min.as_i128(), max.as_i128())
			&& low <= high
			&& let Some(res) = Self::from_i128_in_kind(n.clamp(low, high), self.number_kind())
		{
			return res;
		}
		if let Number::BigDecimal(n) = &self
			&& let (Some(low), Some(high)) = (min.to_big_for_op(), max.to_big_for_op())
			&& low <= high
		{
			return Number::BigDecimal(Box::new(n.as_ref().clone().clamp(low, high)));
		}
		match &self {
			Number::Decimal(n) => match (exact(&min), exact(&max)) {
				(Some(low), Some(high)) if low <= high => Number::Decimal((*n).clamp(low, high)),
				_ => Number::Float(clamp_f64(
					Number::Decimal(*n).to_float(),
					min.to_float(),
					max.to_float(),
				)),
			},
			Number::F16(n) => Number::F16(half::f16::from_f64(clamp_f64(
				f64::from(*n),
				min.to_float(),
				max.to_float(),
			))),
			Number::F32(n) => {
				Number::F32(clamp_f64(*n as f64, min.to_float(), max.to_float()) as f32)
			}
			Number::Int(_)
			| Number::Float(_)
			| Number::I8(_)
			| Number::U8(_)
			| Number::I16(_)
			| Number::U16(_)
			| Number::I32(_)
			| Number::U32(_)
			| Number::U64(_)
			| Number::BigDecimal(_) => {
				Number::Float(clamp_f64(self.to_float(), min.to_float(), max.to_float()))
			}
		}
	}

	pub fn cos(self) -> Self {
		self.map_float_fn(f64::cos)
	}

	pub fn cot(self) -> Self {
		self.map_float_fn(|x| 1.0 / x.tan())
	}

	pub fn deg2rad(self) -> Self {
		self.map_float_fn(f64::to_radians)
	}

	pub fn floor(self) -> Self {
		match self {
			Number::Int(v) => Number::Int(v),
			Number::Float(v) => Number::Float(v.floor()),
			Number::Decimal(v) => Number::Decimal(v.floor()),
			Number::I8(v) => Number::I8(v),
			Number::U8(v) => Number::U8(v),
			Number::I16(v) => Number::I16(v),
			Number::U16(v) => Number::U16(v),
			Number::I32(v) => Number::I32(v),
			Number::U32(v) => Number::U32(v),
			Number::U64(v) => Number::U64(v),
			Number::F16(v) => Number::F16(half::f16::from_f64(v.to_f64().floor())),
			Number::F32(v) => Number::F32((v as f64).floor() as f32),
			Number::BigDecimal(v) => rounded_big(v.floor(), &v),
		}
	}

	fn lerp_f64(from: f64, to: f64, factor: f64) -> f64 {
		from + factor * (to - from)
	}

	fn lerp_decimal(from: Decimal, to: Decimal, factor: Decimal) -> Option<Decimal> {
		to.checked_sub(from)?.checked_mul(factor)?.checked_add(from)
	}

	#[allow(
		clippy::needless_pass_by_value,
		reason = "Number is not Copy; the arithmetic methods take their operands by value like the operator impls"
	)]
	pub fn lerp(self, from: Self, to: Self) -> Self {
		if let Number::Decimal(val) = self {
			return exact_decimal(&from)
				.zip(exact_decimal(&to))
				.and_then(|(from, to)| Self::lerp_decimal(from, to, val))
				.map(Self::Decimal)
				.unwrap_or_else(|| {
					Self::lerp_f64(from.to_float(), to.to_float(), Number::Decimal(val).to_float())
						.into()
				});
		}
		if let Number::BigDecimal(val) = &self {
			return match (from.to_big_for_op(), to.to_big_for_op()) {
				(Some(f), Some(t)) => t
					.checked_sub(&f)
					.and_then(|d| d.checked_mul(val.as_ref()))
					.and_then(|p| p.checked_add(&f))
					.map(|r| Number::BigDecimal(Box::new(r)))
					.unwrap_or_else(|| {
						Number::Float(Self::lerp_f64(f.to_f64(), t.to_f64(), val.to_f64()))
					}),
				_ => Number::Float(Self::lerp_f64(from.to_float(), to.to_float(), val.to_f64())),
			};
		}
		let res = Self::lerp_f64(from.to_float(), to.to_float(), self.to_float());
		match self.number_kind().join(from.number_kind()).join(to.number_kind()) {
			NumberKind::F16 => Number::F16(half::f16::from_f64(res)),
			NumberKind::F32 => Number::F32(res as f32),
			NumberKind::Int
			| NumberKind::Float
			| NumberKind::Decimal
			| NumberKind::I8
			| NumberKind::U8
			| NumberKind::I16
			| NumberKind::U16
			| NumberKind::I32
			| NumberKind::U32
			| NumberKind::U64
			| NumberKind::BigDecimal => Number::Float(res),
		}
	}

	fn repeat_f64(t: f64, m: f64) -> f64 {
		(t - (t / m).floor() * m).clamp(0.0, m)
	}

	fn repeat_decimal(t: Decimal, m: Decimal) -> Option<Decimal> {
		let q = t.checked_div(m)?.floor();
		let prod = q.checked_mul(m)?;
		Some(t.checked_sub(prod)?.clamp(Decimal::ZERO, m))
	}

	#[allow(
		clippy::needless_pass_by_value,
		reason = "Number is not Copy; the arithmetic methods take their operands by value like the operator impls"
	)]
	pub fn lerp_angle(self, from: Self, to: Self) -> Self {
		if let Number::Decimal(val) = self {
			return exact_decimal(&from)
				.zip(exact_decimal(&to))
				.and_then(|(from, to)| {
					let diff = to.checked_sub(from)?;
					let mut dt = Self::repeat_decimal(diff, Decimal::from(360))?;
					if dt > Decimal::from(180) {
						dt = Decimal::from(360).checked_sub(dt)?;
					}
					let target = from.checked_add(dt)?;
					Self::lerp_decimal(from, target, val)
				})
				.map(Self::Decimal)
				.unwrap_or_else(|| {
					let val = Number::Decimal(val).to_float();
					let from = from.to_float();
					let to = to.to_float();
					let mut dt = Self::repeat_f64(to - from, 360.0);
					if dt > 180.0 {
						dt = 360.0 - dt;
					}
					Self::lerp_f64(from, from + dt, val).into()
				});
		}
		let v_f = self.to_float();
		let from_f = from.to_float();
		let to_f = to.to_float();
		let mut dt = Self::repeat_f64(to_f - from_f, 360.0);
		if dt > 180.0 {
			dt = 360.0 - dt;
		}
		let res = Self::lerp_f64(from_f, from_f + dt, v_f);
		match self.number_kind().join(from.number_kind()).join(to.number_kind()) {
			NumberKind::F16 => Number::F16(half::f16::from_f64(res)),
			NumberKind::F32 => Number::F32(res as f32),
			NumberKind::Int
			| NumberKind::Float
			| NumberKind::Decimal
			| NumberKind::I8
			| NumberKind::U8
			| NumberKind::I16
			| NumberKind::U16
			| NumberKind::I32
			| NumberKind::U32
			| NumberKind::U64
			| NumberKind::BigDecimal => Number::Float(res),
		}
	}

	pub fn ln(self) -> Self {
		self.map_float_fn(f64::ln)
	}

	#[allow(
		clippy::needless_pass_by_value,
		reason = "Number is not Copy; the arithmetic methods take their operands by value like the operator impls"
	)]
	pub fn log(self, base: Self) -> Self {
		let res = self.to_float().log(base.to_float());
		match self.number_kind().join(base.number_kind()) {
			NumberKind::F16 => Number::F16(half::f16::from_f64(res)),
			NumberKind::F32 => Number::F32(res as f32),
			NumberKind::Int
			| NumberKind::Float
			| NumberKind::Decimal
			| NumberKind::I8
			| NumberKind::U8
			| NumberKind::I16
			| NumberKind::U16
			| NumberKind::I32
			| NumberKind::U32
			| NumberKind::U64
			| NumberKind::BigDecimal => Number::Float(res),
		}
	}

	pub fn log2(self) -> Self {
		self.map_float_fn(f64::log2)
	}

	pub fn log10(self) -> Self {
		self.map_float_fn(f64::log10)
	}

	pub fn rad2deg(self) -> Self {
		self.map_float_fn(f64::to_degrees)
	}

	pub fn round(self) -> Self {
		match self {
			Number::Int(v) => Number::Int(v),
			Number::Float(v) => Number::Float(v.round()),
			Number::Decimal(v) => Number::Decimal(v.round()),
			Number::I8(v) => Number::I8(v),
			Number::U8(v) => Number::U8(v),
			Number::I16(v) => Number::I16(v),
			Number::U16(v) => Number::U16(v),
			Number::I32(v) => Number::I32(v),
			Number::U32(v) => Number::U32(v),
			Number::U64(v) => Number::U64(v),
			Number::F16(v) => Number::F16(half::f16::from_f64(v.to_f64().round())),
			Number::F32(v) => Number::F32((v as f64).round() as f32),
			Number::BigDecimal(v) => rounded_big(v.round(0), &v),
		}
	}

	pub fn fixed(self, precision: usize) -> Number {
		match self {
			Number::Int(v) => Number::Int(v),
			// Truncate via the formatter so subnormals and very large
			// magnitudes (which would overflow a `10^precision` multiplier)
			// stay representable; non-finite f64s round-trip too.
			Number::Float(v) => Number::Float(
				format!("{v:.precision$}")
					.parse::<f64>()
					.expect("formatted f64 always parses back as f64"),
			),
			Number::Decimal(v) => Number::Decimal(v.round_dp(precision as u32)),
			Number::BigDecimal(v) => {
				let places = i64::try_from(precision).unwrap_or(i64::MAX);
				if v.scale() <= places {
					Number::BigDecimal(v)
				} else {
					rounded_big(v.round(places), &v)
				}
			}
			Number::I8(v) => Number::I8(v),
			Number::U8(v) => Number::U8(v),
			Number::I16(v) => Number::I16(v),
			Number::U16(v) => Number::U16(v),
			Number::I32(v) => Number::I32(v),
			Number::U32(v) => Number::U32(v),
			Number::U64(v) => Number::U64(v),
			Number::F16(v) => {
				let f = v.to_f64();
				let parsed = format!("{f:.precision$}")
					.parse::<f64>()
					.expect("formatted f64 always parses back as f64");
				Number::F16(half::f16::from_f64(parsed))
			}
			Number::F32(v) => {
				let f = v as f64;
				let parsed = format!("{f:.precision$}")
					.parse::<f64>()
					.expect("formatted f64 always parses back as f64");
				Number::F32(parsed as f32)
			}
		}
	}

	pub fn sign(self) -> Self {
		match self {
			Number::Int(n) => Number::Int(n.signum()),
			// Either zero signs to zero, as an integer or a decimal zero does.
			Number::Float(n) => Number::Float(if n == 0.0 {
				0.0
			} else {
				n.signum()
			}),
			Number::Decimal(n) => Number::Decimal(n.signum()),
			Number::I8(n) => Number::I8(n.signum()),
			Number::U8(n) => Number::U8(if n == 0 {
				0
			} else {
				1
			}),
			Number::I16(n) => Number::I16(n.signum()),
			Number::U16(n) => Number::U16(if n == 0 {
				0
			} else {
				1
			}),
			Number::I32(n) => Number::I32(n.signum()),
			Number::U32(n) => Number::U32(if n == 0 {
				0
			} else {
				1
			}),
			Number::U64(n) => Number::U64(if n == 0 {
				0
			} else {
				1
			}),
			Number::F16(n) => {
				let f = n.to_f32();
				Number::F16(if f == 0.0 {
					half::f16::ZERO
				} else {
					half::f16::from_f32(f.signum())
				})
			}
			Number::F32(n) => Number::F32(if n == 0.0 {
				0.0f32
			} else {
				n.signum()
			}),
			Number::BigDecimal(v) => {
				Number::BigDecimal(Box::new(BigDecimal::from(i64::from(v.signum()))))
			}
		}
	}

	pub fn sin(self) -> Self {
		self.map_float_fn(f64::sin)
	}

	pub fn tan(self) -> Self {
		self.map_float_fn(f64::tan)
	}

	pub fn sqrt(self) -> Self {
		match self {
			Number::Int(v) => Number::Float((v as f64).sqrt()),
			Number::Float(v) => Number::Float(v.sqrt()),
			Number::Decimal(v) => Number::Decimal(v.sqrt().unwrap_or_default()),
			Number::I8(v) => Number::Float((v as f64).sqrt()),
			Number::U8(v) => Number::Float((v as f64).sqrt()),
			Number::I16(v) => Number::Float((v as f64).sqrt()),
			Number::U16(v) => Number::Float((v as f64).sqrt()),
			Number::I32(v) => Number::Float((v as f64).sqrt()),
			Number::U32(v) => Number::Float((v as f64).sqrt()),
			Number::U64(v) => Number::Float((v as f64).sqrt()),
			Number::F16(v) => Number::F16(half::f16::from_f64(v.to_f64().sqrt())),
			Number::F32(v) => Number::F32((v as f64).sqrt() as f32),
			Number::BigDecimal(v) => {
				match Decimal::try_from(v.as_ref()).ok().and_then(|d| d.sqrt()) {
					Some(d) => Number::Decimal(d),
					None => Number::Float(v.to_f64().sqrt()),
				}
			}
		}
	}
}

/// The exact `Decimal` of a number, or `None` where it has none. Used by `lerp` and `clamp`.
fn exact_decimal(n: &Number) -> Option<Decimal> {
	match n {
		Number::Float(f) => Decimal::from_f64(*f),
		Number::F16(v) => Decimal::from_f64(f64::from(*v)),
		Number::F32(v) => Decimal::from_f64(f64::from(*v)),
		Number::BigDecimal(v) => Decimal::try_from(v.as_ref()).ok(),
		Number::Int(_)
		| Number::Decimal(_)
		| Number::I8(_)
		| Number::U8(_)
		| Number::I16(_)
		| Number::U16(_)
		| Number::I32(_)
		| Number::U32(_)
		| Number::U64(_) => n.as_decimal_lossless(),
	}
}

/// The integer part of a `bigdecimal`, truncated toward zero, when it fits an `i64`.
fn big_trunc_i64(v: &BigDecimal) -> Option<i64> {
	v.trunc(0).ok()?.to_i64()
}

/// The rounded `bigdecimal`, or the float of `original` if the rounding cannot be formed.
fn rounded_big<E>(result: std::result::Result<BigDecimal, E>, original: &BigDecimal) -> Number {
	match result {
		Ok(v) => Number::BigDecimal(Box::new(v)),
		Err(_) => Number::Float(original.to_f64()),
	}
}

impl Eq for Number {}

/// Compares two floats as a total order: `-0.0` equals `0.0`, and every other pair
/// (NaN included) follows [`f64::total_cmp`].
///
/// Merging the two zeros loses no transitivity, since they are adjacent in `total_cmp`.
///
/// Geometry coordinate streams use this order, where NaNs of different sign and payload stay
/// distinct. [`Number`] orders itself differently: every NaN is one value above `+inf`.
pub(crate) fn cmp_f64(a: f64, b: f64) -> Ordering {
	if a == 0.0 && b == 0.0 {
		Ordering::Equal
	} else {
		a.total_cmp(&b)
	}
}

/// Numbers order, compare and hash as [`surrealdb_types::Number`] does, so a
/// value reads the same in the engine and once returned. Every NaN, whatever its
/// sign and payload, is one value above every other number, `+inf` included;
/// numbers of different kinds are equal exactly when they are the same number,
/// and equal numbers hash alike.
///
/// That is also the order of their index keys, so an indexed range and a scan
/// admit the same numbers. A key spells a float by [`LexDecimal::from_f64`],
/// exactly except when the expansion overflows a `u128` coefficient, and then
/// to at least 38 significant digits, within `10^-37` of the float relatively.
/// A float rounded so has at least 33 fractional bits, `a · 2^-k` with `a` odd
/// and below `2^53`, and differs from any decimal `m · 10^-s` (`s <= 28`) by at
/// least `1 / (a · 5^s) > 1 / (2^53 · 5^28)`, about `3 · 10^-36` relatively, so
/// the rounding never reaches or crosses a decimal.
impl Ord for Number {
	fn cmp(&self, other: &Self) -> Ordering {
		self.wide().total_cmp(&other.wide())
	}
}

impl hash::Hash for Number {
	fn hash<H: hash::Hasher>(&self, state: &mut H) {
		self.wide().hash_number(state);
	}
}

impl PartialEq for Number {
	fn eq(&self, other: &Self) -> bool {
		self.wide().total_eq(&other.wide())
	}
}

impl PartialOrd for Number {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		Some(self.cmp(other))
	}
}

/// `lhs` and `rhs` as `BigDecimal`s, combined by `op`. `None` when an operand has no `BigDecimal`
/// (a NaN or infinite float) or when `op` refuses the result.
fn big_checked(
	lhs: &Number,
	rhs: &Number,
	op: fn(&BigDecimal, &BigDecimal) -> Option<BigDecimal>,
) -> Option<BigDecimal> {
	let v = lhs.to_big_for_op()?;
	let w = rhs.to_big_for_op()?;
	op(&v, &w)
}

/// Exact quotient rounded as [`BigDecimal::div_default`]. `None` for a zero divisor.
fn big_div(a: &BigDecimal, b: &BigDecimal) -> Option<BigDecimal> {
	a.div_default(b).ok()
}

/// The remainder with the sign of the dividend, as `Decimal::checked_rem` gives it: `a - b * q`
/// with `q = a / b` truncated toward zero. `None` for a zero divisor.
fn big_rem(a: &BigDecimal, b: &BigDecimal) -> Option<BigDecimal> {
	if b.is_zero() {
		return None;
	}
	// Rounding the quotient to the nearest integer leaves a remainder of at most |b| / 2 in
	// magnitude. Its sign can be the opposite of the dividend's, and adding or subtracting |b|
	// once moves it to the dividend's side, which is the truncated remainder.
	let nearest = a.div(b, 0).ok()?;
	let remainder = a.checked_sub(&nearest.checked_mul(b)?)?;
	if remainder.is_zero() || remainder.signum() == a.signum() {
		return Some(remainder);
	}
	let step = if a.signum() < 0 {
		-b.abs()
	} else {
		b.abs()
	};
	remainder.checked_add(&step)
}

/// `base` raised to `power` for a `bigdecimal` result.
///
/// An integer exponent is exact: a non-negative one by repeated multiplication, a negative one as
/// the reciprocal rounded as [`BigDecimal::div_default`] rounds. A power that is not an integer is
/// taken as a float. `None` when the exact result cannot be formed within the digit and scale
/// bounds, when the base is NaN or infinite, or when a zero base has a negative exponent.
fn big_pow(base: &Number, power: &Number) -> Option<Number> {
	let as_float = || Number::Float(base.to_float().powf(power.to_float()));
	let Some(exponent) = power.to_big_for_op() else {
		return Some(as_float());
	};
	let exponent = exponent.normalized();
	if exponent.scale() != 0 {
		return Some(as_float());
	}
	let Some(p) = exponent.to_i64() else {
		return Some(as_float());
	};
	let v = base.to_big_for_op()?;
	let one = BigDecimal::from(1i64);
	let magnitude = p.unsigned_abs();
	let result = if v.is_zero() {
		match p.cmp(&0) {
			Ordering::Less => return None,
			Ordering::Equal => BigDecimal::from(1i64),
			Ordering::Greater => BigDecimal::from(0i64),
		}
	} else if v.normalized() == one || v.normalized() == -one.clone() {
		let negative = v.signum() < 0 && magnitude % 2 == 1;
		BigDecimal::from(if negative {
			-1i64
		} else {
			1i64
		})
	} else {
		// The coefficient of `v^|p|` has at most |p| times the digits of `v`'s, trailing zeros
		// included: `1e145455` is one significant digit but 145456 positions.
		let coefficient = v.coefficient_digits().len() as u128;
		let digits = coefficient.checked_mul(u128::from(magnitude))?;
		if digits > u128::try_from(surrealdb_types::bigdecimal::MAX_DIGITS).ok()? {
			return None;
		}
		let power_of_base = big_powi(&v, magnitude)?;
		if p >= 0 {
			power_of_base
		} else {
			one.div_default(&power_of_base).ok()?
		}
	};
	Some(Number::BigDecimal(Box::new(result)))
}

/// `base^exponent` by repeated squaring, every product checked for the scale bound.
fn big_powi(base: &BigDecimal, mut exponent: u64) -> Option<BigDecimal> {
	let mut result = BigDecimal::from(1i64);
	let mut square = base.clone();
	loop {
		if exponent & 1 == 1 {
			result = result.checked_mul(&square)?;
		}
		exponent >>= 1;
		if exponent == 0 {
			return Some(result);
		}
		square = square.checked_mul(&square)?;
	}
}

macro_rules! impl_simple_try_op {
	($trt:ident, $fn:ident, $unchecked:ident, $checked:ident, $big:expr) => {
		impl $trt for Number {
			type Output = Self;
			fn $fn(self, other: Self) -> Result<Self> {
				let target = self.number_kind().join(other.number_kind());
				match target {
					NumberKind::I8
					| NumberKind::U8
					| NumberKind::I16
					| NumberKind::U16
					| NumberKind::I32
					| NumberKind::U32
					| NumberKind::Int
					| NumberKind::U64 => {
						let v = self.as_i128().unwrap_or_default();
						let w = other.as_i128().unwrap_or_default();
						let res = v.$checked(w).and_then(|r| Self::from_i128_in_kind(r, target));
						res.ok_or_else(|| {
							Error::$trt(self.op_err_string(), other.op_err_string()).into()
						})
					}
					NumberKind::F16 | NumberKind::F32 | NumberKind::Float => {
						let res = self.as_float().$unchecked(other.as_float());
						Ok(Self::from_f64_in_kind(res, target))
					}
					NumberKind::Decimal => {
						let res = self
							.to_decimal_for_op()
							.zip(other.to_decimal_for_op())
							.and_then(|(v, w)| v.$checked(w))
							.ok_or_else(|| {
								Error::$trt(self.op_err_string(), other.op_err_string())
							})?;
						Ok(Number::Decimal(res))
					}
					NumberKind::BigDecimal => {
						let res = big_checked(&self, &other, $big).ok_or_else(|| {
							Error::$trt(self.op_err_string(), other.op_err_string())
						})?;
						Ok(Number::BigDecimal(Box::new(res)))
					}
				}
			}
		}
	};
}

impl_simple_try_op!(TryAdd, try_add, add, checked_add, BigDecimal::checked_add);
impl_simple_try_op!(TrySub, try_sub, sub, checked_sub, BigDecimal::checked_sub);
impl_simple_try_op!(TryMul, try_mul, mul, checked_mul, BigDecimal::checked_mul);
impl_simple_try_op!(TryDiv, try_div, div, checked_div, big_div);
impl_simple_try_op!(TryRem, try_rem, rem, checked_rem, big_rem);

impl TryPow for Number {
	type Output = Self;
	fn try_pow(self, power: Self) -> Result<Self> {
		let target = self.number_kind().join(power.number_kind());
		match target {
			NumberKind::I8
			| NumberKind::U8
			| NumberKind::I16
			| NumberKind::U16
			| NumberKind::I32
			| NumberKind::U32
			| NumberKind::Int
			| NumberKind::U64 => {
				let v = self.as_i128().unwrap_or_default();
				let p = power.as_i128().unwrap_or_default();
				let res: i128 = match v {
					0 => match p.cmp(&0) {
						// 0^(-x)
						Ordering::Less => {
							bail!(Error::TryPow(self.op_err_string(), power.op_err_string()))
						}
						// 0^0
						Ordering::Equal => 1,
						// 0^x
						Ordering::Greater => 0,
					},
					// 1^p
					1 => 1,
					-1 => {
						if p % 2 == 0 {
							// (-1)^even
							1
						} else {
							// (-1)^odd
							-1
						}
					}
					// try_into may cause an error, which would be wrong for the above cases.
					_ => p.try_into().ok().and_then(|exp| v.checked_pow(exp)).ok_or_else(|| {
						Error::TryPow(self.op_err_string(), power.op_err_string())
					})?,
				};
				Ok(Self::from_i128_in_kind(res, target)
					.ok_or_else(|| Error::TryPow(self.op_err_string(), power.op_err_string()))?)
			}
			NumberKind::F16 | NumberKind::F32 | NumberKind::Float => {
				let res = self.as_float().powf(power.as_float());
				Ok(Self::from_f64_in_kind(res, target))
			}
			NumberKind::BigDecimal => Ok(big_pow(&self, &power)
				.ok_or_else(|| Error::TryPow(self.op_err_string(), power.op_err_string()))?),
			NumberKind::Decimal => match (self.wide(), power.wide()) {
				(Wide::Decimal(v), Wide::Int(p)) => {
					if v.is_zero() && p < 0 {
						bail!(Error::TryPow(self.op_err_string(), power.op_err_string()));
					}
					Ok(Self::Decimal(v.checked_powi(p).ok_or_else(|| {
						Error::TryPow(self.op_err_string(), power.op_err_string())
					})?))
				}
				(Wide::Decimal(v), Wide::Float(p)) => {
					if v.is_zero() && p < 0.0 {
						bail!(Error::TryPow(self.op_err_string(), power.op_err_string()));
					}
					Ok(Self::Decimal(v.checked_powf(p).ok_or_else(|| {
						Error::TryPow(self.op_err_string(), power.op_err_string())
					})?))
				}
				(Wide::Decimal(v), Wide::Decimal(p)) => {
					if v.is_zero() && p.is_sign_negative() && !p.is_zero() {
						bail!(Error::TryPow(self.op_err_string(), power.op_err_string()));
					}
					Ok(Self::Decimal(v.checked_powd(p).ok_or_else(|| {
						Error::TryPow(self.op_err_string(), power.op_err_string())
					})?))
				}
				(Wide::Int(_), Wide::Decimal(_))
				| (Wide::Float(_), Wide::Decimal(_))
				| (Wide::Int(_), Wide::Int(_)) => {
					if let (Some(v), Some(p_dec), Some(p)) =
						(self.to_decimal_for_op(), power.to_decimal_for_op(), power.as_i128())
						&& self.as_i128().is_some()
					{
						if v.is_zero() && p < 0 {
							bail!(Error::TryPow(self.op_err_string(), power.op_err_string()));
						}
						let res = match i64::try_from(p) {
							Ok(pi) => v.checked_powi(pi),
							Err(_) => v.checked_powd(p_dec),
						}
						.ok_or_else(|| {
							Error::TryPow(self.op_err_string(), power.op_err_string())
						})?;
						Ok(Self::Decimal(res))
					} else {
						Ok(self.as_float().powf(power.as_float()).into())
					}
				}
				(Wide::Int(_), Wide::Float(_))
				| (Wide::Float(_), Wide::Int(_))
				| (Wide::Float(_), Wide::Float(_))
				| (Wide::BigDecimal(_), _)
				| (_, Wide::BigDecimal(_)) => Ok(self.as_float().powf(power.as_float()).into()),
			},
		}
	}
}

impl TryNeg for Number {
	type Output = Self;

	fn try_neg(self) -> Result<Self::Output> {
		Ok(match self {
			Self::Int(n) => {
				Number::Int(n.checked_neg().ok_or_else(|| Error::TryNeg(self.op_err_string()))?)
			}
			Self::Float(n) => Number::Float(-n),
			Self::Decimal(n) => Number::Decimal(-n),
			Self::BigDecimal(n) => Number::BigDecimal(Box::new(-*n)),
			Self::I8(n) => {
				Number::I8(n.checked_neg().ok_or_else(|| Error::TryNeg(self.op_err_string()))?)
			}
			Self::U8(n) => {
				if n == 0 {
					Number::U8(0)
				} else {
					bail!(Error::TryNeg(self.op_err_string()))
				}
			}
			Self::I16(n) => {
				Number::I16(n.checked_neg().ok_or_else(|| Error::TryNeg(self.op_err_string()))?)
			}
			Self::U16(n) => {
				if n == 0 {
					Number::U16(0)
				} else {
					bail!(Error::TryNeg(self.op_err_string()))
				}
			}
			Self::I32(n) => {
				Number::I32(n.checked_neg().ok_or_else(|| Error::TryNeg(self.op_err_string()))?)
			}
			Self::U32(n) => {
				if n == 0 {
					Number::U32(0)
				} else {
					bail!(Error::TryNeg(self.op_err_string()))
				}
			}
			Self::U64(n) => {
				if n == 0 {
					Number::U64(0)
				} else {
					bail!(Error::TryNeg(self.op_err_string()))
				}
			}
			Self::F16(n) => Number::F16(-n),
			Self::F32(n) => Number::F32(-n),
		})
	}
}

impl TryFloatDiv for Number {
	type Output = Self;
	fn try_float_div(self, other: Self) -> Result<Self> {
		let target = self.number_kind().join(other.number_kind());
		match target {
			NumberKind::I8
			| NumberKind::U8
			| NumberKind::I16
			| NumberKind::U16
			| NumberKind::I32
			| NumberKind::U32
			| NumberKind::Int
			| NumberKind::U64 => {
				let v = self.as_i128().unwrap_or_default();
				let w = other.as_i128().unwrap_or_default();
				match v.checked_rem(w) {
					Some(0) => {
						let res = v.checked_div(w).and_then(|q| Self::from_i128_in_kind(q, target));
						Ok(res.ok_or_else(|| {
							Error::TryDiv(self.op_err_string(), other.op_err_string())
						})?)
					}
					Some(_) => Ok(Number::Float((v as f64).div(w as f64))),
					None => bail!(Error::TryDiv(self.op_err_string(), other.op_err_string())),
				}
			}
			NumberKind::F16
			| NumberKind::F32
			| NumberKind::Float
			| NumberKind::Decimal
			| NumberKind::BigDecimal => self.try_div(other),
		}
	}
}

// `+`, `-`, `*` and `/` never panic. Over two integers `+`, `-` and `*` wrap on overflow in every
// build, and a quotient an integer cannot hold (a division by zero, `MIN / -1`) is the float of the
// division; a decimal result that overflows or divides by zero is the float of the operation. The
// checked forms that raise instead are `TryAdd`, `TrySub`, `TryMul` and `TryDiv`.

/// `exact` of `lhs` and `rhs` as a decimal, or `float` of the two as floats where either operand
/// or the result cannot be represented as a decimal.
fn decimal_or_float(
	lhs: &Number,
	rhs: &Number,
	exact: fn(Decimal, Decimal) -> Option<Decimal>,
	float: fn(f64, f64) -> f64,
) -> Number {
	match lhs.to_decimal_for_op().zip(rhs.to_decimal_for_op()).and_then(|(v, w)| exact(v, w)) {
		Some(result) => Number::Decimal(result),
		None => Number::Float(float(lhs.to_float(), rhs.to_float())),
	}
}

/// The `bigdecimal` counterpart of [`decimal_or_float`].
fn big_or_float(
	lhs: &Number,
	rhs: &Number,
	exact: fn(&BigDecimal, &BigDecimal) -> Option<BigDecimal>,
	float: fn(f64, f64) -> f64,
) -> Number {
	match big_checked(lhs, rhs, exact) {
		Some(result) => Number::BigDecimal(Box::new(result)),
		None => Number::Float(float(lhs.to_float(), rhs.to_float())),
	}
}

impl ops::Add for Number {
	type Output = Self;
	fn add(self, other: Self) -> Self {
		let target = self.number_kind().join(other.number_kind());
		match target {
			NumberKind::I8
			| NumberKind::U8
			| NumberKind::I16
			| NumberKind::U16
			| NumberKind::I32
			| NumberKind::U32
			| NumberKind::Int
			| NumberKind::U64 => {
				let v = self.as_i128().unwrap_or_default();
				let w = other.as_i128().unwrap_or_default();
				Self::from_i128_wrapping_in_kind(v.wrapping_add(w), target)
			}
			NumberKind::F16 | NumberKind::F32 | NumberKind::Float => {
				Self::from_f64_in_kind(self.as_float() + other.as_float(), target)
			}
			NumberKind::Decimal => {
				decimal_or_float(&self, &other, Decimal::checked_add, |a, b| a + b)
			}
			NumberKind::BigDecimal => {
				big_or_float(&self, &other, BigDecimal::checked_add, |a, b| a + b)
			}
		}
	}
}

impl<'b> ops::Add<&'b Number> for &Number {
	type Output = Number;
	fn add(self, other: &'b Number) -> Number {
		self.clone().add(other.clone())
	}
}

impl ops::Sub for Number {
	type Output = Self;
	fn sub(self, other: Self) -> Self {
		let target = self.number_kind().join(other.number_kind());
		match target {
			NumberKind::I8
			| NumberKind::U8
			| NumberKind::I16
			| NumberKind::U16
			| NumberKind::I32
			| NumberKind::U32
			| NumberKind::Int
			| NumberKind::U64 => {
				let v = self.as_i128().unwrap_or_default();
				let w = other.as_i128().unwrap_or_default();
				Self::from_i128_wrapping_in_kind(v.wrapping_sub(w), target)
			}
			NumberKind::F16 | NumberKind::F32 | NumberKind::Float => {
				Self::from_f64_in_kind(self.as_float() - other.as_float(), target)
			}
			NumberKind::Decimal => {
				decimal_or_float(&self, &other, Decimal::checked_sub, |a, b| a - b)
			}
			NumberKind::BigDecimal => {
				big_or_float(&self, &other, BigDecimal::checked_sub, |a, b| a - b)
			}
		}
	}
}

impl<'b> ops::Sub<&'b Number> for &Number {
	type Output = Number;
	fn sub(self, other: &'b Number) -> Number {
		self.clone().sub(other.clone())
	}
}

impl ops::Mul for Number {
	type Output = Self;
	fn mul(self, other: Self) -> Self {
		let target = self.number_kind().join(other.number_kind());
		match target {
			NumberKind::I8
			| NumberKind::U8
			| NumberKind::I16
			| NumberKind::U16
			| NumberKind::I32
			| NumberKind::U32
			| NumberKind::Int
			| NumberKind::U64 => {
				let v = self.as_i128().unwrap_or_default();
				let w = other.as_i128().unwrap_or_default();
				Self::from_i128_wrapping_in_kind(v.wrapping_mul(w), target)
			}
			NumberKind::F16 | NumberKind::F32 | NumberKind::Float => {
				Self::from_f64_in_kind(self.as_float() * other.as_float(), target)
			}
			NumberKind::Decimal => {
				decimal_or_float(&self, &other, Decimal::checked_mul, |a, b| a * b)
			}
			NumberKind::BigDecimal => {
				big_or_float(&self, &other, BigDecimal::checked_mul, |a, b| a * b)
			}
		}
	}
}

impl<'b> ops::Mul<&'b Number> for &Number {
	type Output = Number;
	fn mul(self, other: &'b Number) -> Number {
		self.clone().mul(other.clone())
	}
}

impl ops::Div for Number {
	type Output = Self;
	fn div(self, other: Self) -> Self {
		let target = self.number_kind().join(other.number_kind());
		match target {
			NumberKind::I8
			| NumberKind::U8
			| NumberKind::I16
			| NumberKind::U16
			| NumberKind::I32
			| NumberKind::U32
			| NumberKind::Int
			| NumberKind::U64 => {
				let v = self.as_i128().unwrap_or_default();
				let w = other.as_i128().unwrap_or_default();
				match v.checked_div(w).and_then(|q| Self::from_i128_in_kind(q, target)) {
					Some(quotient) => quotient,
					None => Number::Float(v as f64 / w as f64),
				}
			}
			NumberKind::F16 | NumberKind::F32 | NumberKind::Float => {
				Self::from_f64_in_kind(self.as_float() / other.as_float(), target)
			}
			NumberKind::Decimal => {
				decimal_or_float(&self, &other, Decimal::checked_div, |a, b| a / b)
			}
			NumberKind::BigDecimal => big_or_float(&self, &other, big_div, |a, b| a / b),
		}
	}
}

impl<'b> ops::Div<&'b Number> for &Number {
	type Output = Number;
	fn div(self, other: &'b Number) -> Number {
		self.clone().div(other.clone())
	}
}

impl Neg for Number {
	type Output = Self;

	/// The negation; the integer `MIN` wraps to itself, as `-` wraps in every build.
	fn neg(self) -> Self::Output {
		match self {
			Self::Int(n) => Number::Int(n.wrapping_neg()),
			Self::Float(n) => Number::Float(-n),
			Self::Decimal(n) => Number::Decimal(-n),
			Self::BigDecimal(n) => Number::BigDecimal(Box::new(-*n)),
			Self::I8(n) => Number::I8(n.wrapping_neg()),
			Self::U8(n) => Number::U8(n.wrapping_neg()),
			Self::I16(n) => Number::I16(n.wrapping_neg()),
			Self::U16(n) => Number::U16(n.wrapping_neg()),
			Self::I32(n) => Number::I32(n.wrapping_neg()),
			Self::U32(n) => Number::U32(n.wrapping_neg()),
			Self::U64(n) => Number::U64(n.wrapping_neg()),
			Self::F16(n) => Number::F16(-n),
			Self::F32(n) => Number::F32(-n),
		}
	}
}

// ------------------------------

impl Sum<Self> for Number {
	fn sum<I>(mut iter: I) -> Number
	where
		I: Iterator<Item = Self>,
	{
		let Some(first) = iter.next() else {
			return Number::Int(0);
		};
		let seed = match first {
			Self::Float(f) => Self::Float(f + 0.0),
			Self::F32(f) => Self::F32(f + 0.0),
			Self::F16(f) => Self::F16(f + half::f16::ZERO),
			Self::Int(_)
			| Self::Decimal(_)
			| Self::I8(_)
			| Self::U8(_)
			| Self::I16(_)
			| Self::U16(_)
			| Self::I32(_)
			| Self::U32(_)
			| Self::U64(_)
			| Self::BigDecimal(_) => first,
		};
		iter.fold(seed, |a, b| a + b)
	}
}

impl<'a> Sum<&'a Self> for Number {
	fn sum<I>(mut iter: I) -> Number
	where
		I: Iterator<Item = &'a Self>,
	{
		let Some(first) = iter.next() else {
			return Number::Int(0);
		};
		let seed = match first {
			Self::Float(f) => Self::Float(f + 0.0),
			Self::F32(f) => Self::F32(f + 0.0),
			Self::F16(f) => Self::F16(*f + half::f16::ZERO),
			Self::Int(_)
			| Self::Decimal(_)
			| Self::I8(_)
			| Self::U8(_)
			| Self::I16(_)
			| Self::U16(_)
			| Self::I32(_)
			| Self::U32(_)
			| Self::U64(_)
			| Self::BigDecimal(_) => first.clone(),
		};
		iter.fold(seed, |a, b| &a + b)
	}
}

fn mul_product(a: Number, b: Number) -> Number {
	match (a, b) {
		(Number::Int(v), Number::Int(w)) => match v.checked_mul(w) {
			Some(p) => Number::Int(p),
			None => decimal_or_float(
				&Number::Decimal(Decimal::from(v)),
				&Number::Decimal(Decimal::from(w)),
				Decimal::checked_mul,
				|x, y| x * y,
			),
		},
		(a, b) => a * b,
	}
}

impl Product<Self> for Number {
	fn product<I>(mut iter: I) -> Number
	where
		I: Iterator<Item = Self>,
	{
		let Some(first) = iter.next() else {
			return Number::Int(1);
		};
		iter.fold(first, mul_product)
	}
}

impl<'a> Product<&'a Self> for Number {
	fn product<I>(mut iter: I) -> Number
	where
		I: Iterator<Item = &'a Self>,
	{
		let Some(first) = iter.next() else {
			return Number::Int(1);
		};
		iter.fold(first.clone(), |a, b| mul_product(a, b.clone()))
	}
}

pub struct Sorted<T>(pub T);

pub trait Sort {
	fn sorted(&mut self) -> Sorted<&Self>
	where
		Self: Sized;
}

impl Sort for Vec<Number> {
	fn sorted(&mut self) -> Sorted<&Vec<Number>> {
		self.sort();
		Sorted(self)
	}
}

impl ToFloat for Number {
	#[inline]
	fn to_float(&self) -> f64 {
		Number::to_float(self)
	}
}

impl Encode<()> for Number {
	fn encode<W: std::io::Write>(
		&self,
		w: &mut storekey::Writer<W>,
	) -> std::result::Result<(), storekey::EncodeError> {
		w.write_slice(&self.as_decimal_buf())?;
		let kind = self.number_kind();
		Encode::<()>::encode(&kind, w)?;
		Ok(())
	}
}

impl<'de> BorrowDecode<'de, ()> for Number {
	fn borrow_decode(
		r: &mut storekey::BorrowReader<'de>,
	) -> std::result::Result<Self, storekey::DecodeError> {
		let slice = r.read_cow()?;
		let kind: NumberKind = BorrowDecode::<'de, ()>::borrow_decode(r)?;
		Number::from_decimal_buf_kind(slice.as_ref(), kind)
			.map_err(|_| storekey::DecodeError::InvalidFormat)
	}
}

impl Encode<IndexFormat> for Number {
	fn encode<W: std::io::Write>(
		&self,
		w: &mut storekey::Writer<W>,
	) -> std::result::Result<(), storekey::EncodeError> {
		w.write_slice(&self.as_decimal_buf())
	}
}

impl<'de> BorrowDecode<'de, IndexFormat> for Number {
	fn borrow_decode(
		r: &mut storekey::BorrowReader<'de>,
	) -> std::result::Result<Self, storekey::DecodeError> {
		let slice = r.read_cow()?;
		Number::from_decimal_buf(slice.as_ref()).map_err(|_| storekey::DecodeError::InvalidFormat)
	}
}

/// A number inside an index key's record id is spelled as an indexed number is.
impl Encode<IndexIdFormat> for Number {
	fn encode<W: std::io::Write>(
		&self,
		w: &mut storekey::Writer<W>,
	) -> std::result::Result<(), storekey::EncodeError> {
		Encode::<IndexFormat>::encode(self, w)
	}
}

impl<'de> BorrowDecode<'de, IndexIdFormat> for Number {
	fn borrow_decode(
		r: &mut storekey::BorrowReader<'de>,
	) -> std::result::Result<Self, storekey::DecodeError> {
		BorrowDecode::<'de, IndexFormat>::borrow_decode(r)
	}
}

/// A number in the form the key encoding spells it from.
enum KeyForm<'a> {
	Lex(LexDecimal),
	Big(&'a BigDecimal),
}

/// The key encoding of the `bigdecimal` `v`: its coefficient's digits as written, trailing
/// zeros included, with the scale of its leading digit. See [`Number::as_decimal_buf`].
fn bigdecimal_buf(v: &BigDecimal) -> Vec<u8> {
	if v.is_zero() {
		return DecimalLexEncoder::encode(LexDecimal::ZERO);
	}
	let digits: Vec<u8> = v.coefficient_digits().bytes().map(|b| b - b'0').collect();
	// The leading digit's power of ten. Both terms are far inside i64: a coefficient's digits
	// and a scale are bounded by what memory holds.
	let scale = (digits.len() as i64 - 1).saturating_sub(v.scale());
	let mut out = Vec::with_capacity(digits.len() / 2 + 13);
	DecimalLexEncoder::encode_finite(v.signum() < 0, &digits, scale, &mut out);
	out
}

/// Conversion to `f64`. The implementations are marked `#[inline]` so that the
/// per-component loops of the vector functions, in other crates, compile them
/// into the loop whatever link-time optimisation decides.
pub trait ToFloat {
	fn to_float(&self) -> f64;
}

impl ToFloat for f64 {
	#[inline]
	fn to_float(&self) -> f64 {
		*self
	}
}

impl ToFloat for f32 {
	#[inline]
	fn to_float(&self) -> f64 {
		*self as f64
	}
}

impl ToFloat for half::f16 {
	#[inline]
	fn to_float(&self) -> f64 {
		f64::from(*self)
	}
}

impl ToFloat for i64 {
	#[inline]
	fn to_float(&self) -> f64 {
		*self as f64
	}
}

impl ToFloat for i32 {
	#[inline]
	fn to_float(&self) -> f64 {
		*self as f64
	}
}

impl ToFloat for i16 {
	#[inline]
	fn to_float(&self) -> f64 {
		*self as f64
	}
}

impl ToFloat for i8 {
	#[inline]
	fn to_float(&self) -> f64 {
		*self as f64
	}
}

impl ToFloat for u64 {
	#[inline]
	fn to_float(&self) -> f64 {
		*self as f64
	}
}

impl ToFloat for u32 {
	#[inline]
	fn to_float(&self) -> f64 {
		*self as f64
	}
}

impl ToFloat for u16 {
	#[inline]
	fn to_float(&self) -> f64 {
		*self as f64
	}
}

impl ToFloat for u8 {
	#[inline]
	fn to_float(&self) -> f64 {
		*self as f64
	}
}

#[cfg(test)]
mod tests {
	use std::cmp::Ordering;

	use ahash::HashSet;
	use common::decimal::DecimalExt;
	use rand::Rng;
	use rand::seq::SliceRandom;
	use rust_decimal::Decimal;
	use rust_decimal::prelude::ToPrimitive;

	use super::*;

	/// Whether two numbers agree to `digits` significant digits, to within one
	/// unit of the last. Both are truncated to the same unit, so the comparison
	/// is exact integer arithmetic whatever the magnitudes. Zero agrees only with
	/// zero.
	fn agree(a: LexDecimal, b: LexDecimal, digits: u32) -> bool {
		let (
			LexDecimal::Finite {
				negative: na,
				coefficient: ca,
				exponent: ea,
			},
			LexDecimal::Finite {
				negative: nb,
				coefficient: cb,
				exponent: eb,
			},
		) = (a, b)
		else {
			return false;
		};
		if ca == 0 || cb == 0 {
			return ca == cb;
		}
		if na != nb {
			return false;
		}
		let lead = |c: u128, e: i32| e + c.ilog10() as i32 + 1;
		// The exponent of one unit in the last of `digits` digits, measured from the
		// larger of the two leading digits.
		let unit = lead(ca, ea).max(lead(cb, eb)) - digits as i32;
		let scaled = |c: u128, e: i32| {
			let shift = e - unit;
			if shift >= 0 {
				c * 10u128.pow(shift as u32)
			} else {
				10u128.checked_pow((-shift) as u32).map_or(0, |p| c / p)
			}
		};
		scaled(ca, ea).abs_diff(scaled(cb, eb)) <= 1
	}

	/// Whether a decoded number agrees with the value a key was written from.
	fn agrees(original: LexDecimal, decoded: &Number, digits: u32) -> bool {
		let KeyForm::Lex(decoded) = decoded.key_form() else {
			panic!("a decimal key decodes to no bigdecimal");
		};
		agree(original, decoded, digits)
	}

	#[test]
	fn test_decimal_ext_from_str_normalized() {
		let decimal = Decimal::from_str_normalized("0.0").unwrap();
		assert_eq!(decimal.to_string(), "0");
		assert_eq!(decimal.to_i64(), Some(0));
		assert_eq!(decimal.to_f64(), Some(0.0));

		let decimal = Decimal::from_str_normalized("123.456").unwrap();
		assert_eq!(decimal.to_string(), "123.456");
		assert_eq!(decimal.to_i64(), Some(123));
		assert_eq!(decimal.to_f64(), Some(123.456));

		let decimal =
			Decimal::from_str_normalized("13.5719384719384719385639856394139476937756394756")
				.unwrap();
		assert_eq!(decimal.to_string(), "13.571938471938471938563985639");
		assert_eq!(decimal.to_i64(), Some(13));
		assert_eq!(decimal.to_f64(), Some(13.571_938_471_938_472));
	}

	#[test]
	fn test_try_float_div() {
		let (sum_one, count_one) = (Number::Int(5), Number::Int(2));
		assert_eq!(sum_one.try_float_div(count_one).unwrap(), Number::Float(2.5));
		// i64::MIN

		let (sum_two, count_two) = (Number::Int(10), Number::Int(5));
		assert_eq!(sum_two.try_float_div(count_two).unwrap(), Number::Int(2));

		let (sum_three, count_three) = (Number::Float(6.3), Number::Int(3));
		assert_eq!(sum_three.try_float_div(count_three).unwrap(), Number::Float(2.1));

		// Above 2^53, exactness must use integer remainder rather than f64 rounding.
		assert_eq!(
			Number::U64(u64::MAX).try_float_div(Number::U64(3)).unwrap(),
			Number::U64(u64::MAX / 3)
		);
		assert_eq!(
			Number::U64(u64::MAX - 1).try_float_div(Number::U64(3)).unwrap(),
			Number::Float(((u64::MAX - 1) as f64) / 3.0)
		);
	}

	#[test]
	fn test_sum_and_product_preserve_u64_kind() {
		let nums = [Number::U64(2), Number::U64(3)];
		assert_eq!(nums.clone().into_iter().sum::<Number>(), Number::U64(5));
		assert_eq!(nums.iter().sum::<Number>(), Number::U64(5));
		assert_eq!(nums.clone().into_iter().product::<Number>(), Number::U64(6));
		assert_eq!(nums.iter().product::<Number>(), Number::U64(6));
	}

	#[test]
	fn ord_test() {
		let a = Number::Float(-f64::INFINITY);
		let b = Number::Float(-1f64);
		let c = Number::Float(1f64);
		let d = Number::Decimal(
			Decimal::from_str_normalized("1.0000000000000000000000000002").unwrap(),
		);
		let e = Number::Decimal(Decimal::from_str_normalized("1.1").unwrap());
		let f = Number::Float(1.1f64);
		let g = Number::Float(1.5f64);
		let h = Number::Decimal(Decimal::from_str_normalized("1.5").unwrap());
		let i = Number::Float(f64::INFINITY);
		let j = Number::Float(f64::NAN);
		let k = Number::Float(-f64::NAN);
		let original = vec![a, b, c, d, e, f, g, h, i, j, k];
		let mut copy = original.clone();
		let mut rng = rand::rng();
		copy.shuffle(&mut rng);
		copy.sort();
		assert_eq!(original, copy);
	}

	#[test]
	fn every_nan_is_one_value_above_every_number() {
		let nans = [f64::NAN, -f64::NAN, f64::from_bits(0x7ff0_0000_0000_0001)];
		let numbers = [
			Number::Float(f64::INFINITY),
			Number::Float(-f64::INFINITY),
			Number::Float(0.0),
			Number::Int(i64::MAX),
			Number::Int(i64::MIN),
			Number::Decimal(Decimal::MAX),
			Number::Decimal(Decimal::MIN),
		];
		for x in nans {
			let x = Number::Float(x);
			for y in nans {
				assert_eq!(x, Number::Float(y));
				assert_eq!(x.cmp(&Number::Float(y)), Ordering::Equal);
			}
			for n in &numbers {
				assert_eq!(x.cmp(n), Ordering::Greater, "{x:?} against {n:?}");
				assert_eq!(n.cmp(&x), Ordering::Less, "{n:?} against {x:?}");
				assert_ne!(&x, n);
			}
			assert_eq!(x.as_decimal_buf(), Number::Float(f64::NAN).as_decimal_buf());
		}
	}

	#[test]
	fn ord_fuzz() {
		fn random_number() -> Number {
			let mut rng = rand::rng();
			match rng.random_range(0..3) {
				0 => Number::Int(rng.random()),
				1 => Number::Float(f64::from_bits(rng.random())),
				_ => Number::Decimal(Number::Float(f64::from_bits(rng.random())).as_decimal()),
			}
		}

		fn random_permutation(number: Number) -> Number {
			let mut rng = rand::rng();
			let value = match rng.random_range(0..4) {
				0 => number + Number::from(rng.random::<f64>()),
				1 if !matches!(number, Number::Int(i64::MIN)) => number * Number::from(-1),
				2 => Number::Float(number.as_float().next_down()),
				_ => number,
			};
			match rng.random_range(0..3) {
				0 => Number::Int(value.as_int()),
				1 => Number::Float(value.as_float()),
				_ => Number::Decimal(value.as_decimal()),
			}
		}

		fn assert_partial_ord(x: &Number, y: &Number) {
			// PartialOrd requirements
			assert_eq!(x == y, x.partial_cmp(y) == Some(Ordering::Equal), "{x:?} {y:?}");

			// Ord consistent with PartialOrd
			assert_eq!(x.partial_cmp(y), Some(x.cmp(y)), "{x:?} {y:?}");
		}

		fn assert_consistent(a: &Number, b: &Number, c: &Number) {
			assert_partial_ord(a, b);
			assert_partial_ord(b, c);
			assert_partial_ord(c, a);

			// Transitive property (without the fix, these can fail)
			if a == b && b == c {
				assert_eq!(a, c, "{a:?} {b:?} {c:?}");
			}
			if a != b && b == c {
				assert_ne!(a, c, "{a:?} {b:?} {c:?}");
			}
			if a < b && b < c {
				assert!(a < c, "{a:?} {b:?} {c:?}");
			}
			if a > b && b > c {
				assert!(a > c, "{a:?} {b:?} {c:?}");
			}

			// Duality
			assert_eq!(a == b, b == a, "{a:?} {b:?}");
			assert_eq!(a < b, b > a, "{a:?} {b:?}");
		}

		for _ in 0..100000 {
			let base = random_number();
			let a = random_permutation(base);
			let b = random_permutation(a.clone());
			let c = random_permutation(b.clone());
			assert_consistent(&a, &b, &c);
		}
	}

	/// A float equals a decimal only when the decimal spells the float's exact
	/// value, so `0.1f`, whose key spelling is
	/// `0.100000000000000005551115123125782702118`, is above `0.1dec`.
	#[test]
	fn a_float_compares_with_a_decimal_by_its_exact_expansion() {
		let dec = |s: &str| Number::Decimal(Decimal::from_str_normalized(s).unwrap());
		for s in ["0.1", "0.3", "0.07", "1.1", "19.99", "0.11111"] {
			let f = Number::Float(s.parse().unwrap());
			assert_ne!(f, dec(s), "{s}");
		}
		assert!(Number::Float(0.1) > dec("0.1"));
		assert!(Number::Float(0.3) < dec("0.3"));
		assert_eq!(Number::Float(2f64.powi(-20)), dec("0.00000095367431640625"));
		for s in ["0", "0.5", "1", "1.5", "-2.25", "1024"] {
			let f = Number::Float(s.parse().unwrap());
			assert_eq!(f, dec(s), "{s}");
		}
		assert_eq!(Number::Float(-0.0), dec("0"));
		assert!(Number::Float(1.0) < dec("1.0000000000000000000000000001"));
		assert!(Number::Float(f64::INFINITY) > dec("79228162514264337593543950335"));
		assert!(Number::Float(f64::NEG_INFINITY) < dec("-79228162514264337593543950335"));
		// A decimal's trailing zeros, and the bounds of an `i64`, change nothing.
		let mut padded = Decimal::from_str_normalized("2.5").unwrap();
		padded.rescale(3);
		assert_eq!(Number::Float(2.5), Number::Decimal(padded));
		assert_eq!(Number::Float(2f64.powi(63)), dec("9223372036854775808"));
		assert_eq!(Number::Float(-(2f64.powi(63))), Number::Int(i64::MIN));
		// A subnormal float lies between zero and the smallest decimals.
		let subnormal = f64::from_bits(1);
		assert!(Number::Float(subnormal) < Number::Decimal(Decimal::new(1, 28)));
		assert!(Number::Float(subnormal) > dec("0"));
		assert!(Number::Float(-subnormal) > Number::Decimal(Decimal::new(-1, 28)));
	}

	/// A float and a decimal order as their index keys whatever the float's
	/// expansion, including a float whose key rounds it: no decimal lies within
	/// that rounding of it.
	#[test]
	fn a_float_and_a_decimal_order_as_their_keys_whatever_the_expansion() {
		fn key(n: Number) -> Vec<u8> {
			match n {
				Number::Decimal(d) => Number::Decimal(d.normalize()).as_decimal_buf(),
				n @ (Number::Int(_)
				| Number::Float(_)
				| Number::I8(_)
				| Number::U8(_)
				| Number::I16(_)
				| Number::U16(_)
				| Number::I32(_)
				| Number::U32(_)
				| Number::U64(_)
				| Number::F16(_)
				| Number::F32(_)
				| Number::BigDecimal(_)) => n.as_decimal_buf(),
			}
		}
		let mut rng = rand::rng();
		let mut floats =
			vec![0.1, 0.3, 1.0 / 3.0, 2f64.powi(-33), 2f64.powi(-60), f64::from_bits(1)];
		let mut decimals =
			vec![Decimal::new(1, 1), Decimal::new(1, 28), Decimal::MAX, Decimal::MIN];
		for _ in 0..2_000 {
			let scale = rng.random_range(0..=28);
			let mantissa = rng.random::<i64>() as i128 >> rng.random_range(0..40);
			let d = Decimal::from_i128_with_scale(mantissa, scale);
			decimals.push(d);
			if let Some(f) = d.to_f64() {
				// The float nearest the decimal, and its neighbours.
				floats.extend([
					f,
					f64::from_bits(f.to_bits() + 1),
					f64::from_bits(f.to_bits() - 1),
				]);
			}
			let f = f64::from_bits(rng.random());
			if f.is_finite() {
				floats.push(f);
			}
		}
		// A float with a binary fraction finer than `2^-33` is one a key can round.
		let fine = floats.iter().filter(|f| (**f * 2f64.powi(33)).fract() != 0.0).count();
		assert!(fine > 1_000, "the sample holds floats whose keys round: {fine}");
		for &f in &floats {
			let (f, kf) = (Number::Float(f), key(Number::Float(f)));
			for &d in &decimals {
				let d = Number::Decimal(d);
				assert_eq!(f.cmp(&d), kf.cmp(&key(d.clone())), "{f:?} {d:?}");
			}
		}
	}

	/// `Ord` orders every pair of numbers as their key bytes do, and numbers that
	/// compare equal hash alike, whatever their kinds.
	#[test]
	fn ord_and_hash_agree_with_the_key_encoding() {
		fn hash(n: &impl std::hash::Hash) -> u64 {
			use std::hash::{BuildHasher, BuildHasherDefault};
			BuildHasherDefault::<std::collections::hash_map::DefaultHasher>::default().hash_one(n)
		}
		fn kinds_of(n: &Number) -> [Number; 3] {
			[Number::Int(n.as_int()), Number::Float(n.as_float()), Number::Decimal(n.to_decimal())]
		}
		/// `a` and `b` order as the key bytes of their normalised forms, and hash
		/// alike when equal.
		fn agree((a, ka): &(Number, Vec<u8>), (b, kb): &(Number, Vec<u8>)) {
			assert_eq!(a.cmp(b), ka.cmp(kb), "{a:?} {b:?}");
			if a == b {
				assert_eq!(hash(a), hash(b), "{a:?} {b:?}");
			}
		}
		let mut rng = rand::rng();
		let numbers = vec![
			Number::Float(f64::NAN),
			Number::Float(f64::INFINITY),
			Number::Float(f64::NEG_INFINITY),
			Number::Float(-0.0),
			Number::Float(0.0),
			Number::Float(0.1),
			Number::Float(-1.0),
			Number::Float(f64::MIN_POSITIVE),
			Number::Float(f64::MAX),
			Number::Int(i64::MAX),
			Number::Int(i64::MIN),
			Number::Float(2f64.powi(53)),
			Number::Int(9_007_199_254_740_993),
			Number::Float(2f64.powi(63)),
			Number::Float(-(2f64.powi(63))),
			Number::Float(f64::from_bits(1)),
			Number::Float(-f64::from_bits(1)),
		];
		// Every kind of every special value, checked against every other.
		// Each kind, and a decimal with trailing zeros that compares equal to it.
		let keyed = |ns: &[Number]| -> Vec<(Number, Vec<u8>)> {
			ns.iter()
				.flat_map(kinds_of)
				.flat_map(|n| match n {
					Number::Decimal(d) => {
						let mut padded = d;
						padded.rescale(d.scale() + 2);
						vec![n, Number::Decimal(padded)]
					}
					n @ (Number::Int(_)
					| Number::Float(_)
					| Number::I8(_)
					| Number::U8(_)
					| Number::I16(_)
					| Number::U16(_)
					| Number::I32(_)
					| Number::U32(_)
					| Number::U64(_)
					| Number::F16(_)
					| Number::F32(_)
					| Number::BigDecimal(_)) => vec![n],
				})
				.map(|n| {
					let key = match &n {
						Number::Decimal(d) => Number::Decimal(d.normalize()).as_decimal_buf(),
						n @ (Number::Int(_)
						| Number::Float(_)
						| Number::I8(_)
						| Number::U8(_)
						| Number::I16(_)
						| Number::U16(_)
						| Number::I32(_)
						| Number::U32(_)
						| Number::U64(_)
						| Number::F16(_)
						| Number::F32(_)
						| Number::BigDecimal(_)) => n.as_decimal_buf(),
					};
					(n, key)
				})
				.collect()
		};
		let specials = keyed(&numbers);
		for a in &specials {
			for b in &specials {
				agree(a, b);
			}
		}
		// Random values next to their neighbours and to decimals near them.
		let mut randoms = Vec::new();
		for _ in 0..300 {
			let f = match rng.random_range(0..3) {
				0 => f64::from_bits(rng.random()),
				1 => rng.random_range(-1e6..1e6),
				_ => rng.random_range(-1.0..1.0),
			};
			if f.is_finite() {
				randoms.push(Number::Float(f));
				randoms.push(Number::Float(f.next_up()));
				randoms.push(Number::Decimal(Number::Float(f).as_decimal()));
			}
		}
		let mut keyed = keyed(&randoms);
		keyed.extend(specials);
		keyed.shuffle(&mut rng);
		for pair in keyed.chunks(2).filter(|c| c.len() == 2).chain(keyed.windows(2)) {
			agree(&pair[0], &pair[1]);
		}
		keyed.sort_by(|a, b| a.0.cmp(&b.0));
		for pair in keyed.windows(2) {
			agree(&pair[0], &pair[1]);
		}
	}

	#[test]
	fn serialised_ord_test() {
		let ordering = [
			Number::from(f64::NEG_INFINITY),
			Number::from(f64::MIN),
			Number::Int(i64::MIN),
			Number::from(-1000),
			Number::from(-100),
			Number::from(-10),
			Number::from(-1.5),
			Number::from(-1),
			Number::from(0),
			Number::from(1),
			Number::from(1.5),
			Number::from(2),
			Number::from(10),
			Number::from(100),
			Number::from(1000),
			Number::from(i64::MAX),
			Number::from(f64::MAX),
			Number::from(f64::INFINITY),
			Number::from(f64::NAN),
		];
		for window in ordering.windows(2) {
			let n1 = &window[0];
			let n2 = &window[1];
			assert!(n1 < n2, "{n1:?} < {n2:?} (before serialization)");
			let b1 = n1.as_decimal_buf();
			let b2 = n2.as_decimal_buf();
			assert!(b1 < b2, "{n1:?} < {n2:?} (after serialization) - {b1:?} < {b2:?}");
			let r1 = Number::from_decimal_buf(&b1).unwrap();
			let r2 = Number::from_decimal_buf(&b2).unwrap();
			assert!(r1.eq(n1), "{r1:?} = {n1:?} (after deserialization)");
			assert!(r2.eq(n2), "{r2:?} = {n2:?} (after deserialization)");
		}
	}

	#[test]
	fn serialised_test() {
		let check = |numbers: &[Number]| {
			let mut buffers = HashSet::default();
			for n1 in numbers {
				let b = n1.as_decimal_buf();
				let n2 = Number::from_decimal_buf(&b).unwrap();
				buffers.insert(b);
				assert!(n1.eq(&n2), "{n1:?} = {n2:?} (after deserialization)");
			}
			assert_eq!(buffers.len(), 1, "{numbers:?}");
		};
		check(&[Number::Int(0), Number::Float(0.0), Number::Decimal(Decimal::ZERO)]);
		check(&[Number::Int(1), Number::Float(1.0), Number::Decimal(Decimal::ONE)]);
		check(&[Number::Int(-1), Number::Float(-1.0), Number::Decimal(Decimal::NEGATIVE_ONE)]);
		check(&[Number::Float(1.5), Number::Decimal(Decimal::from_str_normalized("1.5").unwrap())]);
	}

	/// A number as an index key stores it, and as a read of that key decodes it.
	fn through_index_key(n: &Number) -> Number {
		let key = storekey::encode_vec_format::<IndexFormat, _>(n).unwrap();
		storekey::decode_borrow_format::<IndexFormat, Number>(&key)
			.unwrap_or_else(|e| panic!("the index key of {n:?} does not decode: {e}"))
	}

	fn decimal(text: &str) -> Decimal {
		text.parse().unwrap()
	}

	/// The decoded number is a `Decimal` of exactly this mantissa and scale.
	#[track_caller]
	fn assert_decimal(decoded: Number, mantissa: i128, scale: u32) {
		match decoded {
			Number::Decimal(d) => assert_eq!((d.mantissa(), d.scale()), (mantissa, scale), "{d}"),
			other @ (Number::Int(_)
			| Number::Float(_)
			| Number::I8(_)
			| Number::U8(_)
			| Number::I16(_)
			| Number::U16(_)
			| Number::I32(_)
			| Number::U32(_)
			| Number::U64(_)
			| Number::F16(_)
			| Number::F32(_)
			| Number::BigDecimal(_)) => {
				panic!("decoded as {other:?}, not the decimal {mantissa}e-{scale}")
			}
		}
	}

	#[test]
	fn an_index_key_decodes_a_float_below_a_millionth_to_the_float_it_was_written_from() {
		for f in [
			1e-20,
			5.6527459748369866e-8,
			1e-7,
			9.99e-7,
			1e-28,
			1e-29,
			1e-300,
			f64::MIN_POSITIVE,
			5e-324,
		] {
			for f in [f, -f] {
				match through_index_key(&Number::Float(f)) {
					Number::Float(g) => {
						assert_eq!(g.to_bits(), f.to_bits(), "{f:e} decoded as {g:e}")
					}
					other @ (Number::Int(_)
					| Number::Decimal(_)
					| Number::I8(_)
					| Number::U8(_)
					| Number::I16(_)
					| Number::U16(_)
					| Number::I32(_)
					| Number::U32(_)
					| Number::U64(_)
					| Number::F16(_)
					| Number::F32(_)
					| Number::BigDecimal(_)) => {
						panic!("{f:e} decoded as {other:?}, not a float")
					}
				}
			}
		}
	}

	#[test]
	fn an_index_key_decodes_a_float_to_the_decimal_it_has_always_decoded_to() {
		// 0.1 is 0.1000000000000000055511151231257827…, of which a Decimal holds 28 places.
		assert_decimal(through_index_key(&Number::Float(0.1)), 1000000000000000055511151231, 28);
		assert_decimal(through_index_key(&Number::Float(-0.1)), -1000000000000000055511151231, 28);
		assert_decimal(through_index_key(&Number::Float(1.5)), 15, 1);
		assert_decimal(through_index_key(&Number::Float(0.0)), 0, 0);
		assert_decimal(through_index_key(&Number::Float(1e28)), 9999999999999999583119736832, 0);
		// Beyond a Decimal's 96 bits the value is a float.
		for f in [1e30, -1e30, 1e300, f64::MAX, f64::MIN] {
			assert!(
				matches!(through_index_key(&Number::Float(f)), Number::Float(g) if g == f),
				"{f:e}"
			);
		}
		for f in [f64::INFINITY, f64::NEG_INFINITY] {
			assert!(matches!(through_index_key(&Number::Float(f)), Number::Float(g) if g == f));
		}
		assert!(
			matches!(through_index_key(&Number::Float(f64::NAN)), Number::Float(g) if g.is_nan())
		);
	}

	#[test]
	fn an_index_key_decodes_a_number_to_a_decimal_only_between_a_millionth_and_2_pow_96() {
		// A Decimal's own range reads back as itself, scale included.
		for text in [
			"0.000001",
			"-0.000001",
			"0.0000010000000000000000000001",
			"1.50",
			"100.000",
			"79228162514264337593543950335",
			"-79228162514264337593543950335",
			"7.9228162514264337593543950335",
		] {
			let d = decimal(text);
			assert_decimal(through_index_key(&Number::Decimal(d)), d.mantissa(), d.scale());
		}
		// A magnitude below a millionth reads back as the nearest float.
		for (text, float) in [
			("0.0000001", "1e-7"),
			("-0.0000001", "-1e-7"),
			("0.0000009999999999999999999999", "9.999999999999999999999e-7"),
			("0.0000000000000000000000000001", "1e-28"),
		] {
			let nearest: f64 = float.parse().unwrap();
			match through_index_key(&Number::Decimal(decimal(text))) {
				Number::Float(g) => assert_eq!(g.to_bits(), nearest.to_bits(), "{text}: {g:e}"),
				other @ (Number::Int(_)
				| Number::Decimal(_)
				| Number::I8(_)
				| Number::U8(_)
				| Number::I16(_)
				| Number::U16(_)
				| Number::I32(_)
				| Number::U32(_)
				| Number::U64(_)
				| Number::F16(_)
				| Number::F32(_)
				| Number::BigDecimal(_)) => {
					panic!("{text} decoded as {other:?}, not a float")
				}
			}
		}
		// A float is on one side or the other of 2^96 = 79228162514264337593543950336.
		assert!(matches!(through_index_key(&Number::Float(2f64.powi(96))), Number::Float(_)));
		assert!(matches!(
			through_index_key(&Number::Float(2f64.powi(96) * (1.0 - f64::EPSILON))),
			Number::Decimal(_)
		));
	}

	#[test]
	fn an_index_key_decodes_an_int_to_the_decimal_of_that_value() {
		for i in [0, 1, -1, 7, i64::MAX, i64::MIN, 1_000_000, -1_000_000_007] {
			assert_decimal(through_index_key(&Number::Int(i)), i128::from(i), 0);
		}
	}

	/// Whatever a number is written as, an index key reads it back as a number of
	/// about the same value, never one an exponent away.
	#[test]
	fn an_index_key_decodes_to_the_value_it_was_written_from_up_to_rounding() {
		use rand::SeedableRng;
		use rand::rngs::StdRng;

		let mut rng = StdRng::seed_from_u64(0x1DE7);

		// Every f64 bit pattern, so every magnitude and every digit string.
		for _ in 0..60_000 {
			let f = f64::from_bits(rng.random());
			if f.is_nan() {
				continue;
			}
			let original = LexDecimal::from_f64(f);
			let decoded = through_index_key(&Number::Float(f));
			match decoded {
				// A float is the one it was written from.
				Number::Float(g) if f.is_finite() => {
					assert_eq!(g.to_bits(), f.to_bits(), "{f:e} decoded as {g:e}")
				}
				Number::Float(g) => assert_eq!(g, f, "{f:e}"),
				// A Decimal keeps 28 fractional digits, so a value of at least a
				// millionth keeps at least 22 significant ones.
				Number::Decimal(_) => {
					assert!(agrees(original, &decoded, 22), "{f:e} decoded as {decoded:?}");
				}
				Number::Int(_)
				| Number::I8(_)
				| Number::U8(_)
				| Number::I16(_)
				| Number::U16(_)
				| Number::I32(_)
				| Number::U32(_)
				| Number::U64(_)
				| Number::F16(_)
				| Number::F32(_)
				| Number::BigDecimal(_) => panic!("{f:e} decoded as {decoded:?}"),
			}
		}

		// Floats with a short mantissa at every power of ten, where the digit
		// string is exact rather than a 39-digit expansion.
		for e in -324..=308 {
			for m in ["1", "9.999999999999999", "1.2345678901234567", "5", "7.1"] {
				let Ok(f) = format!("{m}e{e}").parse::<f64>() else {
					continue;
				};
				// A mantissa at the top of the range rounds up past `f64::MAX`.
				if !f.is_finite() {
					continue;
				}
				for f in [f, -f] {
					let original = LexDecimal::from_f64(f);
					let decoded = through_index_key(&Number::Float(f));
					assert!(agrees(original, &decoded, 22), "{f:e} decoded as {decoded:?}");
				}
			}
		}

		// Every Decimal: those in range come back as themselves, and the rest as a
		// float of the same value.
		for _ in 0..60_000 {
			let mantissa = (rng.random::<u128>() >> rng.random_range(32..128)) as i128;
			let mantissa = if rng.random() {
				-mantissa
			} else {
				mantissa
			};
			let d = Decimal::from_i128_with_scale(mantissa, rng.random_range(0..=28));
			let decoded = through_index_key(&Number::Decimal(d));
			if d.is_zero() || d.abs() >= decimal("0.000001") {
				match decoded {
					Number::Decimal(g) if d.is_zero() => assert!(g.is_zero(), "{d} decoded as {g}"),
					Number::Decimal(g) => {
						assert_eq!(g.serialize(), d.serialize(), "{d} decoded as {g}")
					}
					other @ (Number::Int(_)
					| Number::Float(_)
					| Number::I8(_)
					| Number::U8(_)
					| Number::I16(_)
					| Number::U16(_)
					| Number::I32(_)
					| Number::U32(_)
					| Number::U64(_)
					| Number::F16(_)
					| Number::F32(_)
					| Number::BigDecimal(_)) => {
						panic!("{d} decoded as {other:?}")
					}
				}
			} else {
				match decoded {
					Number::Float(g) => {
						let original = LexDecimal::from_decimal(d);
						assert!(agrees(original, &decoded, 15), "{d} decoded as {g:e}");
					}
					other @ (Number::Int(_)
					| Number::Decimal(_)
					| Number::I8(_)
					| Number::U8(_)
					| Number::I16(_)
					| Number::U16(_)
					| Number::I32(_)
					| Number::U32(_)
					| Number::U64(_)
					| Number::F16(_)
					| Number::F32(_)
					| Number::BigDecimal(_)) => {
						panic!("{d} decoded as {other:?}, not a float")
					}
				}
			}
		}

		// Every i64.
		for _ in 0..20_000 {
			let i = rng.random::<i64>() >> rng.random_range(0..64);
			assert_decimal(through_index_key(&Number::Int(i)), i128::from(i), 0);
		}
	}

	/// A decoded float that a Decimal parser reads positionally is the Decimal it
	/// reads, to the last digit and the scale, so a decoded value does not move.
	#[test]
	fn an_index_key_decodes_a_float_to_the_decimal_its_text_parses_to() {
		use rand::SeedableRng;
		use rand::rngs::StdRng;

		let mut rng = StdRng::seed_from_u64(0x7E47);
		let mut checked = 0;
		for _ in 0..60_000 {
			// Magnitudes of a millionth up to 2^96, written positionally.
			let f = rng.random::<f64>() * 10f64.powi(rng.random_range(-6..=28));
			let f = if rng.random() {
				-f
			} else {
				f
			};
			let text = LexDecimal::from_f64(f).to_string();
			if text.contains(['e', 'E']) {
				continue;
			}
			let Ok(parsed) = Decimal::from_str_radix(&text, 10) else {
				continue;
			};
			checked += 1;
			match through_index_key(&Number::Float(f)) {
				Number::Decimal(d) => assert_eq!(d.serialize(), parsed.serialize(), "{text}: {d}"),
				other @ (Number::Int(_)
				| Number::Float(_)
				| Number::I8(_)
				| Number::U8(_)
				| Number::I16(_)
				| Number::U16(_)
				| Number::I32(_)
				| Number::U32(_)
				| Number::U64(_)
				| Number::F16(_)
				| Number::F32(_)
				| Number::BigDecimal(_)) => {
					panic!("{f:e} decoded as {other:?}, not the decimal {parsed}")
				}
			}
		}
		assert!(checked > 20_000, "only {checked} positional floats were generated");
	}

	/// Reading a Decimal from the text of the decoded digits, and a float where that
	/// text is not one, is a decoder of its own for the values it reads right. The
	/// decoder under test agrees with it wherever it is right, in value and in kind,
	/// so a value is decoded differently only where the text read gets it wrong: each
	/// such case is checked, and none is required, since the text read may get the
	/// whole sample right.
	#[test]
	fn an_index_key_decodes_every_value_a_text_read_gets_right_as_it_does() {
		use rand::SeedableRng;
		use rand::rngs::StdRng;

		fn read_from_text(key: &[u8]) -> Number {
			let d128 = DecimalLexEncoder::decode(key).unwrap();
			match Decimal::from_str_radix(&d128.to_string(), 10) {
				Ok(d) => Number::Decimal(d),
				Err(_) => Number::Float(d128.to_f64()),
			}
		}

		let mut rng = StdRng::seed_from_u64(0x7E47_0002);
		let mut numbers = Vec::new();
		for _ in 0..60_000 {
			numbers.push(Number::Float(f64::from_bits(rng.random())));
			numbers
				.push(Number::Float(rng.random::<f64>() * 10f64.powi(rng.random_range(-40..40))));
			let mantissa = (rng.random::<u128>() >> rng.random_range(32..128)) as i128;
			numbers.push(Number::Decimal(Decimal::from_i128_with_scale(
				if rng.random() {
					-mantissa
				} else {
					mantissa
				},
				rng.random_range(0..=28),
			)));
			numbers.push(Number::Int(rng.random::<i64>() >> rng.random_range(0..64)));
		}
		for n in numbers {
			let key = n.as_decimal_buf();
			let (decoded, read) = (Number::from_decimal_buf(&key).unwrap(), read_from_text(&key));
			let same = match (&decoded, &read) {
				(Number::Decimal(a), Number::Decimal(b)) => a.serialize() == b.serialize(),
				(Number::Float(a), Number::Float(b)) => a.to_bits() == b.to_bits() || a == b,
				(
					Number::Int(_)
					| Number::Float(_)
					| Number::Decimal(_)
					| Number::I8(_)
					| Number::U8(_)
					| Number::I16(_)
					| Number::U16(_)
					| Number::I32(_)
					| Number::U32(_)
					| Number::U64(_)
					| Number::F16(_)
					| Number::F32(_)
					| Number::BigDecimal(_),
					Number::Int(_)
					| Number::Float(_)
					| Number::Decimal(_)
					| Number::I8(_)
					| Number::U8(_)
					| Number::I16(_)
					| Number::U16(_)
					| Number::I32(_)
					| Number::U32(_)
					| Number::U64(_)
					| Number::F16(_)
					| Number::F32(_)
					| Number::BigDecimal(_),
				) => false,
			};
			if same {
				continue;
			}
			let written = DecimalLexEncoder::decode(&key).unwrap();
			assert!(
				written.is_finite()
					&& !matches!(
						written,
						LexDecimal::Finite {
							coefficient: 0,
							..
						}
					) && !agrees(written, &read, 9)
					&& agrees(written, &decoded, 15),
				"{n:?} decoded as {decoded:?}, which differs from {read:?}, read from text"
			);
		}
	}

	#[test]
	fn as_int_lossless_accepts_valid() {
		assert_eq!(Number::Int(0).as_int_lossless(), Some(0));
		assert_eq!(Number::Int(i64::MIN).as_int_lossless(), Some(i64::MIN));
		assert_eq!(Number::Int(i64::MAX).as_int_lossless(), Some(i64::MAX));
		assert_eq!(Number::Float(0.0).as_int_lossless(), Some(0));
		assert_eq!(Number::Float(-0.0).as_int_lossless(), Some(0));
		assert_eq!(Number::Float(7.0).as_int_lossless(), Some(7));
		// `i64::MIN` is exactly representable as `f64` and is the inclusive lower bound.
		assert_eq!(Number::Float(i64::MIN as f64).as_int_lossless(), Some(i64::MIN));
		assert_eq!(Number::Decimal(Decimal::ZERO).as_int_lossless(), Some(0));
		assert_eq!(Number::Decimal(Decimal::ONE).as_int_lossless(), Some(1));
		assert_eq!(Number::Decimal(Decimal::NEGATIVE_ONE).as_int_lossless(), Some(-1));
	}

	#[test]
	fn as_int_lossless_rejects_invalid() {
		assert_eq!(Number::Float(1.5).as_int_lossless(), None);
		assert_eq!(Number::Float(-1.5).as_int_lossless(), None);
		assert_eq!(Number::Float(f64::NAN).as_int_lossless(), None);
		assert_eq!(Number::Float(f64::INFINITY).as_int_lossless(), None);
		assert_eq!(Number::Float(f64::NEG_INFINITY).as_int_lossless(), None);
		// `i64::MAX as f64` rounds up to 2^63, which is *not* in `i64` range —
		// the exclusive upper bound is `-(i64::MIN as f64)`.
		assert_eq!(Number::Float(i64::MAX as f64).as_int_lossless(), None);
		assert_eq!(Number::Float(1e30).as_int_lossless(), None);
		assert_eq!(Number::Float(-1e30).as_int_lossless(), None);
		assert_eq!(
			Number::Decimal(Decimal::from_str_normalized("1.5").unwrap()).as_int_lossless(),
			None,
		);
	}

	#[test]
	fn as_array_index_accepts_valid() {
		assert_eq!(Number::Int(0).as_array_index(), Some(0));
		assert_eq!(Number::Int(7).as_array_index(), Some(7));
		assert_eq!(Number::Float(0.0).as_array_index(), Some(0));
		assert_eq!(Number::Float(-0.0).as_array_index(), Some(0));
		assert_eq!(Number::Float(3.0).as_array_index(), Some(3));
		assert_eq!(Number::Decimal(Decimal::ZERO).as_array_index(), Some(0));
		assert_eq!(Number::Decimal(Decimal::ONE).as_array_index(), Some(1));
	}

	#[test]
	fn as_array_index_rejects_invalid() {
		assert_eq!(Number::Int(-1).as_array_index(), None);
		assert_eq!(Number::Int(i64::MIN).as_array_index(), None);
		assert_eq!(Number::Float(-1.0).as_array_index(), None);
		assert_eq!(Number::Float(1.5).as_array_index(), None);
		assert_eq!(Number::Float(f64::NAN).as_array_index(), None);
		assert_eq!(Number::Float(f64::INFINITY).as_array_index(), None);
		assert_eq!(Number::Float(f64::NEG_INFINITY).as_array_index(), None);
		// 1e30 is far beyond usize::MAX and not exactly representable as usize.
		assert_eq!(Number::Float(1e30).as_array_index(), None);
		assert_eq!(Number::Decimal(Decimal::NEGATIVE_ONE).as_array_index(), None);
		assert_eq!(
			Number::Decimal(Decimal::from_str_normalized("1.5").unwrap()).as_array_index(),
			None,
		);
	}

	#[test]
	fn fixed_int_is_unchanged_regardless_of_precision() {
		assert_eq!(Number::Int(101).fixed(0), Number::Int(101));
		assert_eq!(Number::Int(101).fixed(2), Number::Int(101));
		assert_eq!(Number::Int(101).fixed(319), Number::Int(101));
		assert_eq!(Number::Int(-7).fixed(5), Number::Int(-7));
	}

	#[test]
	fn fixed_float_rounds_to_requested_precision() {
		assert_eq!(Number::Float(101.5).fixed(2), Number::Float(101.5));
		assert_eq!(Number::Float(101.1111111).fixed(2), Number::Float(101.11));
		// Subnormal at precision past f64's significant-digit range: the
		// trailing digits get truncated, producing a smaller-but-finite
		// neighbour rather than 0, inf, or NaN.
		assert_eq!(
			Number::Float(2.2250738585072014e-308).fixed(319),
			Number::Float(2.22507385851e-308),
		);
	}

	#[test]
	fn fixed_float_preserves_non_finite() {
		// The Float arm relies on `format!`/`parse` round-tripping every
		// f64 — including NaN and ±inf. Pin that invariant here so a future
		// change to the formatter cannot silently break the `expect` in
		// `Number::fixed`.
		let Number::Float(nan) = Number::Float(f64::NAN).fixed(2) else {
			panic!("expected Number::Float(NaN)");
		};
		assert!(nan.is_nan());
		assert_eq!(Number::Float(f64::INFINITY).fixed(2), Number::Float(f64::INFINITY));
		assert_eq!(Number::Float(f64::NEG_INFINITY).fixed(2), Number::Float(f64::NEG_INFINITY));
	}

	#[test]
	fn fixed_decimal_uses_round_dp() {
		let d = Decimal::from_str_normalized("1.2345").unwrap();
		assert_eq!(
			Number::Decimal(d).fixed(2),
			Number::Decimal(Decimal::from_str_normalized("1.23").unwrap()),
		);
	}

	/// A number as a record key stores it, and as a scan reads it back.
	fn through_record_key(n: &Number) -> (Vec<u8>, Number) {
		let key = storekey::encode_vec(n).unwrap();
		let back = storekey::decode_borrow::<Number>(&key)
			.unwrap_or_else(|e| panic!("the key of {n:?} does not decode: {e}"));
		(key, back)
	}

	/// A record id read out of a key is used to fetch the record again, which
	/// re-encodes it, so decoding must give back the value the key was written
	/// from: the same float bit for bit, not a neighbour of it, and not an error.
	fn assert_float_key_round_trips(f: f64) {
		let (key, back) = through_record_key(&Number::Float(f));
		let Number::Float(g) = back else {
			panic!("{f:e} decoded as {back:?}, not a float");
		};
		// The sign of a zero and the payload of a NaN are not stored.
		let same =
			g.to_bits() == f.to_bits() || (f == 0.0 && g == 0.0) || (f.is_nan() && g.is_nan());
		assert!(same, "{f:e} decoded as {g:e}");
		assert_eq!(
			through_record_key(&Number::Float(g)).0,
			key,
			"{f:e} does not re-encode to its key"
		);
	}

	#[test]
	fn a_float_key_decodes_to_the_float_it_was_written_from() {
		for f in [
			0.0,
			-0.0,
			0.1,
			0.2 + 0.1,
			1.5,
			5.0,
			1.6330027767028172,
			std::f64::consts::PI,
			9007199254740994.0,
			f64::EPSILON,
			f64::INFINITY,
			f64::NEG_INFINITY,
			f64::NAN,
			// Past the largest magnitude a `Decimal` holds, ~7.9e28.
			1e28,
			7.9e28,
			1e29,
			1e30,
			-1e30,
			1e300,
			f64::MAX,
			f64::MIN,
			// Below the smallest scale a `Decimal` holds, and the subnormals.
			1e-6,
			1e-7,
			5.6527459748369866e-8,
			1e-20,
			1e-28,
			1e-29,
			1e-300,
			f64::MIN_POSITIVE,
			5e-324,
		] {
			assert_float_key_round_trips(f);
		}
		// Every decimal exponent, over mantissas of every length.
		for e in -324..=308 {
			for m in ["1", "9.999999999999999", "1.2345678901234567", "5", "7.1"] {
				if let Ok(f) = format!("{m}e{e}").parse::<f64>() {
					assert_float_key_round_trips(f);
					assert_float_key_round_trips(-f);
				}
			}
		}
		let mut rng = rand::rng();
		for _ in 0..20_000 {
			assert_float_key_round_trips(f64::from_bits(rng.random()));
			assert_float_key_round_trips(rng.random::<f64>());
			assert_float_key_round_trips(
				rng.random::<f64>() * 10f64.powi(rng.random_range(-30..30)),
			);
		}
	}

	#[test]
	fn a_decimal_key_decodes_to_the_decimal_it_was_written_from() {
		fn assert_round_trips(d: Decimal) {
			let (key, back) = through_record_key(&Number::Decimal(d));
			assert!(matches!(back, Number::Decimal(g) if g == d), "{d} decoded as {back:?}");
			assert_eq!(through_record_key(&back).0, key, "{d} does not re-encode to its key");
		}
		for d in [
			"0",
			"1",
			"-1",
			"1.5",
			"1.50",
			"100.000",
			"1000000",
			// A scale of 7 or more is printed in scientific notation by `LexDecimal`.
			"0.0000001",
			"-0.0000000121",
			"0.0000000000000000000000000001",
			"7.9228162514264337593543950335",
			"79228162514264337593543950335",
			"-79228162514264337593543950335",
		] {
			assert_round_trips(d.parse().unwrap());
		}
		assert_round_trips(Decimal::MAX);
		assert_round_trips(Decimal::MIN);
		let mut rng = rand::rng();
		for _ in 0..20_000 {
			let mantissa = (rng.random::<u128>() >> rng.random_range(32..128)) as i128;
			let mantissa = if rng.random() {
				-mantissa
			} else {
				mantissa
			};
			assert_round_trips(Decimal::from_i128_with_scale(mantissa, rng.random_range(0..=28)));
		}
	}

	#[test]
	fn an_int_key_decodes_to_the_int_it_was_written_from() {
		let mut ints = vec![0, 1, -1, i64::MAX, i64::MIN];
		for e in 0..19 {
			let p = 10i64.pow(e);
			ints.extend([p, -p, p - 1, p + 1, 5 * p]);
		}
		for i in ints {
			let (key, back) = through_record_key(&Number::Int(i));
			assert!(matches!(back, Number::Int(g) if g == i), "{i} decoded as {back:?}");
			assert_eq!(through_record_key(&back).0, key);
		}
	}

	/// What a released binary stored for these numbers. The bytes are the format
	/// of every record id and key that holds one, so they must not change: a
	/// scan on a newer release decodes exactly these.
	#[test]
	fn the_stored_bytes_of_a_numeric_key_are_frozen() {
		fn hex(b: &[u8]) -> String {
			b.iter().map(|b| format!("{b:02x}")).collect()
		}
		for (n, stored) in [
			(Number::Float(1e30), "a01937211111111111111112a995735949767001000003"),
			(Number::Float(0.1), "a01918211111111111111116662226234236893813229001000003"),
			(Number::Float(5e-324), "a017d35a51767569523576552876798a3979332483480f01000003"),
			(Number::Decimal("0.0000001".parse().unwrap()), "a019122001000004"),
			(Number::Int(1000), "a0191c21110f01000002"),
		] {
			let key = storekey::encode_vec(&n).unwrap();
			assert_eq!(hex(&key), stored, "{n:?}");
		}
	}

	#[test]
	fn number_kind_discriminants_are_stable() {
		assert_eq!(NumberKind::Int as u8, 0);
		assert_eq!(NumberKind::Float as u8, 1);
		assert_eq!(NumberKind::Decimal as u8, 2);
		assert_eq!(NumberKind::I8 as u8, 3);
		assert_eq!(NumberKind::U8 as u8, 4);
		assert_eq!(NumberKind::I16 as u8, 5);
		assert_eq!(NumberKind::U16 as u8, 6);
		assert_eq!(NumberKind::I32 as u8, 7);
		assert_eq!(NumberKind::U32 as u8, 8);
		assert_eq!(NumberKind::U64 as u8, 9);
		assert_eq!(NumberKind::F16 as u8, 10);
		assert_eq!(NumberKind::F32 as u8, 11);
	}

	#[test]
	fn through_record_key_handles_all_extended_number_types() {
		let cases = [
			Number::Int(-123456789),
			Number::Float(1234.5678),
			Number::Decimal(Decimal::from_str_normalized("9876.54321").unwrap()),
			Number::I8(-42),
			Number::U8(200),
			Number::I16(-1234),
			Number::U16(54321),
			Number::I32(-100000),
			Number::U32(2000000),
			Number::U64(1000000000000),
			Number::F16(half::f16::from_f32(1.5)),
			Number::F32(2.75),
		];
		for n in cases {
			let (key, back) = through_record_key(&n);
			assert_eq!(back, n, "failed roundtrip for {n:?}");
			assert_eq!(through_record_key(&back).0, key);
		}
	}

	#[test]
	fn revision_round_trips_and_freezes_wire_bytes_for_all_variants() {
		let dec = Decimal::from_str_normalized("9876.54321").unwrap();
		let mut dec_expected = vec![1, 2];
		dec.serialize_revisioned(&mut dec_expected).unwrap();

		let cases: &[(Number, &[u8])] = &[
			// Revision 1 (discriminants 0, 1, 2):
			(Number::Int(0), &[1, 0, 0]),
			(Number::Int(-1), &[1, 0, 1]),
			(Number::Int(42), &[1, 0, 84]),
			(Number::Float(1.5), &[1, 1, 0, 0, 0, 0, 0, 0, 0xf8, 0x3f]),
			(Number::Decimal(dec), &dec_expected),
			// Revision 2 (discriminants 3..=11):
			(Number::I8(-42), &[2, 3, (-42i8) as u8]),
			(Number::U8(200), &[2, 4, 200]),
			(Number::I16(-1), &[2, 5, 1]),
			(Number::I16(-300), &[2, 5, 251, 87, 2]),
			(Number::U16(200), &[2, 6, 200]),
			(Number::U16(1000), &[2, 6, 251, 232, 3]),
			(Number::I32(-1), &[2, 7, 1]),
			(Number::U32(200), &[2, 8, 200]),
			(Number::U64(200), &[2, 9, 200]),
			(Number::F16(half::f16::from_f32(1.5)), &[2, 10, 251, 0x00, 0x3e]),
			(Number::F32(2.75), &[2, 11, 0x00, 0x00, 0x30, 0x40]),
		];
		for (n, expected) in cases {
			let mut buf = Vec::new();
			n.serialize_revisioned(&mut buf).unwrap();
			assert_eq!(buf.as_slice(), *expected, "frozen revision bytes changed for {n:?}");
			let mut reader = buf.as_slice();
			let back = Number::deserialize_revisioned(&mut reader).unwrap();
			assert_eq!(back, *n, "failed revision roundtrip for {n:?}");
			assert!(reader.is_empty(), "trailing bytes after decoding {n:?}");
		}

		// Revision 2 also accepts discriminants 0, 1, 2 (`Int`, `Float`, `Decimal`):
		let rev2_int = [2u8, 0, 84];
		assert_eq!(Number::deserialize_revisioned(&mut &rev2_int[..]).unwrap(), Number::Int(42));

		// Revision 1 rejects sized discriminants 3..=11:
		for disc in 3u8..=11 {
			let bad = [1u8, disc, 0, 0, 0, 0];
			assert!(
				Number::deserialize_revisioned(&mut &bad[..]).is_err(),
				"revision 1 should reject discriminant {disc}"
			);
		}
	}

	#[test]
	fn decimal_arithmetic_rejects_non_finite_and_out_of_range_floats() {
		let dec_one = Number::Decimal(Decimal::ONE);
		let unrepresentable = [
			Number::F16(half::f16::INFINITY),
			Number::F16(half::f16::NEG_INFINITY),
			Number::F16(half::f16::NAN),
			Number::F32(f32::INFINITY),
			Number::F32(f32::NEG_INFINITY),
			Number::F32(f32::NAN),
			Number::F32(f32::MAX),
			Number::F32(f32::MIN),
		];
		for x in unrepresentable {
			assert!(
				dec_one.clone().try_add(x.clone()).is_err(),
				"expected try_add error for {x:?}"
			);
			assert!(
				x.clone().try_add(dec_one.clone()).is_err(),
				"expected try_add error for {x:?}"
			);
			assert!(
				dec_one.clone().try_sub(x.clone()).is_err(),
				"expected try_sub error for {x:?}"
			);
			assert!(
				dec_one.clone().try_mul(x.clone()).is_err(),
				"expected try_mul error for {x:?}"
			);
			assert!(
				dec_one.clone().try_div(x.clone()).is_err(),
				"expected try_div error for {x:?}"
			);
			assert!(
				dec_one.clone().try_pow(x.clone()).is_err(),
				"expected try_pow error for {x:?}"
			);
		}

		// Infallible operators fall back to f64 arithmetic when Decimal conversion fails:
		assert_eq!(dec_one.clone() + Number::F32(f32::INFINITY), Number::Float(f64::INFINITY));
		assert_eq!(
			dec_one.clone() * Number::F16(half::f16::NEG_INFINITY),
			Number::Float(f64::NEG_INFINITY)
		);
		let Number::Float(nan) = dec_one.clone() + Number::F32(f32::NAN) else {
			panic!("expected Number::Float(NaN)");
		};
		assert!(nan.is_nan());
		assert_eq!(dec_one + Number::F32(f32::MAX), Number::Float(1.0 + f32::MAX as f64));
	}

	#[test]
	fn lerp_and_lerp_angle_decimal_overflow_do_not_panic() {
		let max = Number::Decimal(Decimal::MAX);
		let min = Number::Decimal(Decimal::MIN);
		let half = Number::Decimal("0.5".parse().unwrap());
		let two = Number::Decimal(Decimal::from(2));

		let res = half.clone().lerp(min.clone(), max.clone());
		assert!(matches!(res, Number::Float(f) if f.is_finite()));

		let res2 = two.lerp(Number::Decimal(Decimal::ZERO), max.clone());
		assert!(matches!(res2, Number::Float(f) if f.is_finite()));

		let res_angle = half.lerp_angle(min, max);
		assert!(matches!(res_angle, Number::Float(f) if f.is_finite()));
	}

	#[test]
	fn float_decimal_non_finite_pow_zero_negative_and_product_overflow() {
		let one_dec = Number::Decimal(Decimal::ONE);
		assert!(
			matches!(Number::Float(f64::NAN) + one_dec.clone(), Number::Float(f) if f.is_nan())
		);
		assert!(Number::Float(f64::NAN).try_add(one_dec.clone()).is_err());
		assert_eq!(Number::Float(f64::INFINITY) + one_dec.clone(), Number::Float(f64::INFINITY));
		assert!(Number::Float(f64::INFINITY).try_add(one_dec.clone()).is_err());
		assert_eq!(Number::Float(1e30) + one_dec.clone(), Number::Float(1e30));
		assert!(Number::Float(1e30).try_add(one_dec).is_err());

		let zero_dec = Number::Decimal(Decimal::ZERO);
		assert!(zero_dec.clone().try_pow(Number::Decimal(-Decimal::ONE)).is_err());
		assert!(zero_dec.clone().try_pow(Number::Int(-1)).is_err());
		assert!(zero_dec.try_pow(Number::Float(-1.0)).is_err());

		let prod: Number = [Number::Int(i64::MAX), Number::Int(2)].into_iter().product();
		assert_eq!(prod, Number::Decimal(Decimal::from(i64::MAX) * Decimal::from(2)));
	}

	#[test]
	fn a_bigdecimal_power_is_bounded_by_its_positional_size() {
		// One significant digit, but 1001 positions: the power would hold 145 million digits.
		let wide = BigDecimal::from_digits(false, b"1", 1000).expect("in range");
		let base = Number::BigDecimal(Box::new(wide));
		assert!(base.clone().try_pow(Number::Int(145_455)).is_err());
		let small = base.try_pow(Number::Int(2)).expect("within the bound");
		let Number::BigDecimal(small) = small else {
			panic!("a bigdecimal power is a bigdecimal");
		};
		assert_eq!(small.coefficient_digits().len(), 2001);
	}
}

#[cfg(test)]
mod bigdecimal_key_tests {
	//! The key encoding of a `bigdecimal` among every other number kind. Every generator draws
	//! from a fixed seed, so a failure reproduces.

	use std::str::FromStr;

	use rust_decimal::Decimal;
	use surrealdb_types::BigDecimal;

	use super::{Number, NumberKind};
	use crate::expr::decimal::{DecimalLexEncoder, KeyDecimal};
	use crate::val::IndexFormat;

	struct Rng(u64);

	impl Rng {
		fn next(&mut self) -> u64 {
			// xorshift64*
			self.0 ^= self.0 >> 12;
			self.0 ^= self.0 << 25;
			self.0 ^= self.0 >> 27;
			self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
		}

		fn below(&mut self, n: u64) -> u64 {
			self.next() % n
		}
	}

	fn big(text: &str) -> BigDecimal {
		BigDecimal::from_str(text).unwrap_or_else(|e| panic!("{text}: {e}"))
	}

	fn bignum(text: &str) -> Number {
		Number::BigDecimal(Box::new(big(text)))
	}

	/// The positional text of `digits` at `scale`, negated when `negative`.
	fn text(negative: bool, digits: &str, scale: usize) -> String {
		let mut text = digits.to_owned();
		if scale > 0 {
			if text.len() <= scale {
				text = format!("{}{text}", "0".repeat(scale + 1 - text.len()));
			}
			text.insert(text.len() - scale, '.');
		}
		if negative {
			text.insert(0, '-');
		}
		text
	}

	/// A bigdecimal of 40 or 65 digits, or of 1 to 80, at a scale of 0 to 70, of either sign,
	/// trailing zeros and zero included.
	fn gen_big(rng: &mut Rng) -> BigDecimal {
		let len = match rng.below(4) {
			0 => 40,
			1 => 65,
			_ => 1 + rng.below(80) as usize,
		};
		let mut digits: String = (0..len).map(|_| char::from(b'0' + rng.below(10) as u8)).collect();
		if rng.below(4) == 0 {
			digits.push_str("000");
		}
		big(&text(rng.below(2) == 0, &digits, rng.below(71) as usize))
	}

	/// A number of a kind other than a bigdecimal whose key holds its exact value: an integer, a
	/// decimal, or a float that is a short binary fraction.
	fn gen_other(rng: &mut Rng) -> Number {
		match rng.below(5) {
			0 => Number::Int(rng.next() as i64 >> rng.below(64)),
			1 => Number::U64(rng.next()),
			2 => Number::Float(((rng.next() as i64) >> (11 + rng.below(53))) as f64 / 8.0),
			_ => {
				let mantissa = ((rng.next() as i64) >> rng.below(64)) as i128;
				Number::Decimal(Decimal::from_i128_with_scale(mantissa, rng.below(20) as u32))
			}
		}
	}

	/// The key a normalised index writes for `n`: a decimal or bigdecimal without trailing zeros
	/// after its point.
	fn normalised_key(n: &Number) -> Vec<u8> {
		match n {
			Number::Decimal(d) => Number::Decimal(d.normalize()).as_decimal_buf(),
			Number::BigDecimal(d) => Number::BigDecimal(Box::new(d.normalized())).as_decimal_buf(),
			Number::Int(_)
			| Number::Float(_)
			| Number::I8(_)
			| Number::U8(_)
			| Number::I16(_)
			| Number::U16(_)
			| Number::I32(_)
			| Number::U32(_)
			| Number::U64(_)
			| Number::F16(_)
			| Number::F32(_) => n.as_decimal_buf(),
		}
	}

	/// A bigdecimal has the bytes of the `decimal`, `int` or float with its coefficient and
	/// scale, so none of their bytes moves.
	#[test]
	fn a_bigdecimal_is_spelled_as_the_decimal_of_its_coefficient_and_scale() {
		for text in [
			"0",
			"1",
			"-1",
			"1.5",
			"1.50",
			"-1.50",
			"1500",
			"0.001",
			"-0.000100",
			"12345.6789",
			"79228162514264337593543950335",
			"-7.9228162514264337593543950335",
		] {
			let decimal = Number::Decimal(Decimal::from_str(text).expect("a decimal"));
			assert_eq!(bignum(text).as_decimal_buf(), decimal.as_decimal_buf(), "{text}");
		}
		for int in [0i64, 1, -1, 7, 1000, -1000, i64::MAX, i64::MIN] {
			assert_eq!(
				bignum(&int.to_string()).as_decimal_buf(),
				Number::Int(int).as_decimal_buf(),
				"{int}"
			);
		}
		assert_eq!(bignum("2.5").as_decimal_buf(), Number::Float(2.5).as_decimal_buf());
		assert_eq!(bignum("-1024").as_decimal_buf(), Number::Float(-1024.0).as_decimal_buf());
	}

	/// In a normalised index, equal values of every kind share one key, and the key order is the
	/// numeric order, for bigdecimals of 40 and 65 digits mixed with ints, u64s, decimals and
	/// floats, negatives and zero.
	#[test]
	fn bigdecimal_keys_order_numerically_among_every_kind() {
		let mut rng = Rng(0x5EED_B16D_EC00_0001);
		let mut numbers: Vec<Number> = vec![
			bignum("0"),
			bignum("-0.000"),
			Number::Int(0),
			Number::Float(-0.0),
			Number::Decimal(Decimal::ZERO),
			bignum("1.5"),
			bignum("1.50"),
			Number::Decimal(Decimal::new(150, 2)),
			Number::Float(1.5),
			bignum("1"),
			Number::Int(1),
			Number::Decimal(Decimal::new(100, 2)),
			bignum(&"9".repeat(40)),
			bignum(&format!("1{}", "0".repeat(39))),
			bignum(&format!("-{}", "9".repeat(65))),
			bignum(&format!("0.{}1", "0".repeat(60))),
			bignum(&format!("-0.{}1", "0".repeat(60))),
			Number::U64(u64::MAX),
			bignum(&u64::MAX.to_string()),
			bignum("18446744073709551616"),
			Number::Int(i64::MIN),
			bignum("-9223372036854775809"),
		];
		for _ in 0..2_000 {
			numbers.push(Number::BigDecimal(Box::new(gen_big(&mut rng))));
			numbers.push(gen_other(&mut rng));
		}
		// Values next to one another, so that kinds interleave.
		for _ in 0..500 {
			let other = gen_other(&mut rng);
			let Some(b) = other.as_bigdecimal_cast() else {
				continue;
			};
			let nudge = big(&format!("0.{}1", "0".repeat(rng.below(60) as usize)));
			numbers.push(other);
			numbers.push(Number::BigDecimal(Box::new(b.normalized())));
			numbers.push(Number::BigDecimal(Box::new(b.checked_add(&nudge).expect("adds"))));
			numbers.push(Number::BigDecimal(Box::new(b.checked_sub(&nudge).expect("subtracts"))));
		}
		numbers.sort();
		let keys: Vec<Vec<u8>> = numbers.iter().map(normalised_key).collect();
		for (pair, key) in numbers.windows(2).zip(keys.windows(2)) {
			match pair[0].cmp(&pair[1]) {
				std::cmp::Ordering::Less => assert!(
					key[0] < key[1],
					"{} < {} but their keys do not order so",
					pair[0],
					pair[1]
				),
				std::cmp::Ordering::Equal => {
					assert_eq!(key[0], key[1], "{} == {} but their keys differ", pair[0], pair[1])
				}
				std::cmp::Ordering::Greater => panic!("sorted numbers out of order"),
			}
		}
	}

	/// Scales past the two-byte range take the escaped form, which orders above and below every
	/// two-byte form of its sign.
	#[test]
	fn wide_scales_order_around_the_two_byte_range() {
		let powers = [-280_000i64, -6_000, -5_379, -5_378, -1, 0, 1, 6_177, 6_178, 140_000];
		let mut positives: Vec<BigDecimal> = powers
			.iter()
			.map(|p| BigDecimal::from_digits(false, b"7", *p).expect("in range"))
			.collect();
		positives.push(BigDecimal::from_digits(false, b"75", 140_000).expect("in range"));
		let negatives: Vec<BigDecimal> = positives.iter().rev().map(|b| -b.clone()).collect();
		let ordered: Vec<BigDecimal> = negatives.into_iter().chain(positives).collect();
		for pair in ordered.windows(2) {
			assert!(pair[0] < pair[1]);
			let a = Number::BigDecimal(Box::new(pair[0].clone())).as_decimal_buf();
			let b = Number::BigDecimal(Box::new(pair[1].clone())).as_decimal_buf();
			assert!(a < b, "{a:02x?} !< {b:02x?}");
		}
		// The two-byte range spells every int, decimal and float, so the extremes of the floats
		// sort inside the wide forms.
		let tiny = BigDecimal::from_digits(false, b"1", -6_000).expect("in range");
		let huge = BigDecimal::from_digits(false, b"1", 6_178).expect("in range");
		let tiny = Number::BigDecimal(Box::new(tiny)).as_decimal_buf();
		let huge = Number::BigDecimal(Box::new(huge)).as_decimal_buf();
		assert!(tiny > Number::Int(0).as_decimal_buf());
		assert!(tiny < Number::Float(f64::from_bits(1)).as_decimal_buf());
		assert!(huge > Number::Float(f64::MAX).as_decimal_buf());
		assert!(huge < Number::Float(f64::INFINITY).as_decimal_buf());
	}

	/// A key decodes back to the bigdecimal it was written from, scale included, under the
	/// record-id format; under the index format a wide value decodes to the same value.
	#[test]
	fn bigdecimal_keys_round_trip() {
		let mut rng = Rng(0x5EED_B16D_EC00_0002);
		let mut values: Vec<BigDecimal> = (0..3_000).map(|_| gen_big(&mut rng)).collect();
		for exponent in [-280_000i64, -5_379, 6_178, 140_000] {
			values.push(BigDecimal::from_digits(true, b"123", exponent).expect("in range"));
		}
		for value in values {
			let n = Number::BigDecimal(Box::new(value.clone()));
			let buf = n.as_decimal_buf();
			let back = Number::from_decimal_buf_kind(&buf, NumberKind::BigDecimal)
				.unwrap_or_else(|e| panic!("{value}: {e}"));
			let Number::BigDecimal(back) = back else {
				panic!("{value} decodes to {back:?}");
			};
			assert_eq!(*back, value);
			// Zero keeps no scale; every other value keeps the one it was written with.
			if !value.is_zero() {
				assert_eq!(back.scale(), value.scale(), "{value}");
			}
			assert_eq!(Number::BigDecimal(back).as_decimal_buf(), buf, "{value}");
			let indexed = Number::from_decimal_buf(&buf).unwrap_or_else(|e| panic!("{value}: {e}"));
			if matches!(DecimalLexEncoder::decode_key(&buf), Ok(KeyDecimal::Wide { .. })) {
				assert_eq!(indexed, n, "{value}");
			}
			// Through storekey, as an index and a record id write it.
			let key = storekey::encode_vec_format::<IndexFormat, _>(&n).expect("encodes");
			storekey::decode_borrow_format::<IndexFormat, Number>(&key).expect("decodes");
			let key = storekey::encode_vec(&n).expect("encodes");
			let back: Number = storekey::decode_borrow(&key).expect("decodes");
			if value.is_zero() {
				assert_eq!(back, n, "{value}");
			} else {
				assert_eq!(back.to_string(), n.to_string(), "{value}");
			}
		}
	}

	/// Decoding never panics: on random bytes, and on every truncation and one-byte change of
	/// valid keys of every form.
	#[test]
	fn decoding_arbitrary_key_bytes_never_panics() {
		let mut rng = Rng(0x5EED_B16D_EC00_0003);
		let decode_all = |bytes: &[u8]| {
			let _ = DecimalLexEncoder::decode_key(bytes);
			let _ = DecimalLexEncoder::decode(bytes);
			let _ = Number::from_decimal_buf(bytes);
			for kind in [NumberKind::BigDecimal, NumberKind::Int, NumberKind::Decimal] {
				let _ = Number::from_decimal_buf_kind(bytes, kind);
			}
		};
		for _ in 0..20_000 {
			let len = rng.below(24) as usize;
			let bytes: Vec<u8> = (0..len).map(|_| rng.next() as u8).collect();
			decode_all(&bytes);
		}
		let mut valid: Vec<Vec<u8>> = (0..300)
			.map(|_| Number::BigDecimal(Box::new(gen_big(&mut rng))).as_decimal_buf())
			.collect();
		for exponent in [-280_000i64, -5_379, 6_178, 20_000] {
			for negative in [false, true] {
				let b = BigDecimal::from_digits(negative, b"98765", exponent).expect("in range");
				valid.push(Number::BigDecimal(Box::new(b)).as_decimal_buf());
			}
		}
		// A long key is cut and changed at its head, its tail and 32 points through its body:
		// decoding a key of thousands of bytes is slow, so every position of one would
		// make the test quadratic.
		for bytes in valid {
			let len = bytes.len();
			let stride = (len / 32).max(1);
			let positions = (0..len).filter(|at| *at < 16 || len - at <= 16 || at % stride == 0);
			for at in positions {
				decode_all(&bytes[..at]);
				for _ in 0..4 {
					let mut mutant = bytes.clone();
					mutant[at] = rng.next() as u8;
					decode_all(&mutant);
				}
			}
		}
		// A wide ordinal that names a scale past what a bigdecimal holds is refused, as is one
		// with a zero byte.
		let mut beyond = vec![0xA0, 0xFF];
		beyond.extend([0xFF; 8]);
		beyond.extend([0x20, 0x00]);
		assert!(Number::from_decimal_buf_kind(&beyond, NumberKind::BigDecimal).is_err());
		let mut zero_byte = vec![0xA0, 0xFF, 0x00];
		zero_byte.extend([0x01; 7]);
		zero_byte.extend([0x20, 0x00]);
		assert!(DecimalLexEncoder::decode_key(&zero_byte).is_err());
	}
}
