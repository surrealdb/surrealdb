#![deny(clippy::wildcard_enum_match_arm)]

use std::hash::{Hash, Hasher};
use std::iter::once;

use common::fmt::{EscapeObjectKey, Float, Fmt, QuoteStr, SqlDatetime, SqlDuration};
use rust_decimal::Decimal;
use surrealdb_strand::Strand;
use surrealdb_types::{BigDecimal, SqlFormat, ToSql, write_sql};

use crate::expr::{CoverStmts, Expr, RecordIdLit};
use crate::val::{
	Bytes, Date, Datetime, Duration, File, Geometry, Interval, Regex, Time, Timestamp, Uuid,
};

// Note: Literal uses `Strand` (not `String`) for the inner value of
// `Literal::String` so that converting between `Value` and `Expr` via
// `into_literal()` is a zero-cost move rather than a heap allocation.
// `Literal` is not revisioned, so there is no on-disk compatibility
// concern with changing the inner type.

/// A literal value, should be computed to get an actual value.
///
/// # Note regarding equality.
/// A literal is equal to an other literal if it is the exact same byte
/// representation, so normal float rules regarding equality do not apply, i.e.
/// if `a != b` then `Literal::Float(a)` could still be equal
/// to `Literal::Float(b)` in the case of `NaN` floats for example. Also
/// surrealql rules regarding number equality are not observed, 1f != 1dec.

#[derive(Clone, Debug)]
pub enum Literal {
	None,
	Null,
	// An unbounded range, i.e. `..` without any start or end bound.
	UnboundedRange,
	Bool(bool),
	Float(f64),
	Integer(i64),
	Decimal(Decimal),
	/// An arbitrary-precision decimal number
	BigDecimal(Box<BigDecimal>),
	String(Strand),
	Bytes(Bytes),
	Regex(Regex),
	RecordId(RecordIdLit),
	Array(Vec<Expr>),
	Set(Vec<Expr>),
	Object(Vec<ObjectEntry>),
	Duration(Duration),
	Datetime(Datetime),
	Uuid(Uuid),
	Geometry(Geometry),
	File(File),
	I8(i8),
	U8(u8),
	I16(i16),
	U16(u16),
	I32(i32),
	U32(u32),
	U64(u64),
	F16(half::f16),
	F32(f32),
	Date(Date),
	Time(Time),
	Timestamp(Timestamp),
	Interval(Interval),
}

impl Literal {
	pub fn is_static(&self) -> bool {
		match self {
			Literal::None
			| Literal::Null
			| Literal::UnboundedRange
			| Literal::Bool(_)
			| Literal::Float(_)
			| Literal::Integer(_)
			| Literal::Decimal(_)
			| Literal::BigDecimal(_)
			| Literal::String(_)
			| Literal::Bytes(_)
			| Literal::Regex(_)
			| Literal::Duration(_)
			| Literal::Datetime(_)
			| Literal::Date(_)
			| Literal::Time(_)
			| Literal::Timestamp(_)
			| Literal::Interval(_)
			| Literal::Uuid(_)
			| Literal::File(_)
			| Literal::Geometry(_)
			| Literal::I8(_)
			| Literal::U8(_)
			| Literal::I16(_)
			| Literal::U16(_)
			| Literal::I32(_)
			| Literal::U32(_)
			| Literal::U64(_)
			| Literal::F16(_)
			| Literal::F32(_) => true,
			Literal::RecordId(record_id_lit) => record_id_lit.is_static(),
			Literal::Array(exprs) => exprs.iter().all(|x| x.is_static()),
			Literal::Set(exprs) => exprs.iter().all(|x| x.is_static()),
			Literal::Object(items) => items.iter().all(|x| x.value.is_static()),
		}
	}

	/// Whether evaluating this literal can modify data. Composite literals
	/// (arrays, sets, objects, record ids) evaluate their element expressions
	/// in place, so those must be inspected; scalars cannot write.
	pub fn read_only(&self) -> bool {
		match self {
			Literal::None
			| Literal::Null
			| Literal::UnboundedRange
			| Literal::Bool(_)
			| Literal::Float(_)
			| Literal::Integer(_)
			| Literal::Decimal(_)
			| Literal::BigDecimal(_)
			| Literal::String(_)
			| Literal::Bytes(_)
			| Literal::Regex(_)
			| Literal::Duration(_)
			| Literal::Datetime(_)
			| Literal::Date(_)
			| Literal::Time(_)
			| Literal::Timestamp(_)
			| Literal::Interval(_)
			| Literal::Uuid(_)
			| Literal::File(_)
			| Literal::Geometry(_)
			| Literal::I8(_)
			| Literal::U8(_)
			| Literal::I16(_)
			| Literal::U16(_)
			| Literal::I32(_)
			| Literal::U32(_)
			| Literal::U64(_)
			| Literal::F16(_)
			| Literal::F32(_) => true,
			Literal::RecordId(record_id_lit) => record_id_lit.read_only(),
			Literal::Array(exprs) => exprs.iter().all(|x| x.read_only()),
			Literal::Set(exprs) => exprs.iter().all(|x| x.read_only()),
			Literal::Object(items) => items.iter().all(|x| x.value.read_only()),
		}
	}

	/// The value this literal denotes, when it denotes one on its own.
	///
	/// `Some` when no part of the literal needs the evaluator: the scalars, and
	/// the collections whose every element is itself directly convertible. The
	/// result is required to equal what evaluating the literal produces, so a
	/// caller may use either interchangeably. In particular an object's entries
	/// are ordered and a repeated key resolves to its last occurrence, matching
	/// evaluation.
	///
	/// `None` for anything whose value is not a property of the literal alone:
	/// a record id with a generated key (the key is allocated when the literal
	/// is evaluated), and any collection containing such an element. `None` is
	/// always a safe answer, so a caller must treat it as "ask the evaluator"
	/// rather than as "not a value".
	///
	/// This is the shape a `.surql` import consists of almost entirely, and
	/// answering it without the evaluator is what keeps import off the async
	/// stack: see the callers in the literal evaluator and in the
	/// planner.
	pub fn as_static_value(&self) -> Option<crate::val::Value> {
		use crate::val::Value;

		let value = match self {
			Literal::None => Value::None,
			Literal::Null => Value::Null,
			Literal::UnboundedRange => Value::Range(Box::new(crate::val::Range::unbounded())),
			Literal::Bool(x) => Value::Bool(*x),
			Literal::Float(x) => Value::Number(crate::val::Number::Float(*x)),
			Literal::Integer(x) => Value::Number(crate::val::Number::Int(*x)),
			Literal::Decimal(x) => Value::Number(crate::val::Number::Decimal(*x)),
			Literal::BigDecimal(x) => Value::Number(crate::val::Number::BigDecimal(x.clone())),
			Literal::String(x) => Value::String(x.clone()),
			Literal::Bytes(x) => Value::Bytes(x.clone()),
			Literal::Regex(x) => Value::Regex(x.clone()),
			Literal::Duration(x) => Value::Duration(*x),
			Literal::Datetime(x) => Value::Datetime(*x),
			Literal::Date(x) => Value::Date(*x),
			Literal::Time(x) => Value::Time(*x),
			Literal::Timestamp(x) => Value::Timestamp(*x),
			Literal::Interval(x) => Value::Interval(*x),
			Literal::Uuid(x) => Value::Uuid(*x),
			Literal::Geometry(x) => Value::Geometry(x.clone()),
			Literal::File(x) => Value::File(x.clone()),
			Literal::I8(x) => Value::Number(crate::val::Number::I8(*x)),
			Literal::U8(x) => Value::Number(crate::val::Number::U8(*x)),
			Literal::I16(x) => Value::Number(crate::val::Number::I16(*x)),
			Literal::U16(x) => Value::Number(crate::val::Number::U16(*x)),
			Literal::I32(x) => Value::Number(crate::val::Number::I32(*x)),
			Literal::U32(x) => Value::Number(crate::val::Number::U32(*x)),
			Literal::U64(x) => Value::Number(crate::val::Number::U64(*x)),
			Literal::F16(x) => Value::Number(crate::val::Number::F16(*x)),
			Literal::F32(x) => Value::Number(crate::val::Number::F32(*x)),
			// Only the key forms that are values in their own right. A
			// generated key is allocated during evaluation, and the compound
			// keys hold expressions.
			Literal::RecordId(rid) => {
				use crate::expr::RecordIdKeyLit;
				let key = match &rid.key {
					RecordIdKeyLit::Number(x) => crate::val::RecordIdKey::Number(*x),
					RecordIdKeyLit::String(x) => crate::val::RecordIdKey::String(x.clone()),
					RecordIdKeyLit::Uuid(x) => crate::val::RecordIdKey::Uuid(*x),
					RecordIdKeyLit::Array(_)
					| RecordIdKeyLit::Object(_)
					| RecordIdKeyLit::Generate(_)
					| RecordIdKeyLit::Range(_) => return None,
				};
				Value::RecordId(crate::val::RecordId::new(rid.table.clone(), key))
			}
			Literal::Array(exprs) => {
				let mut values = Vec::with_capacity(exprs.len());
				for e in exprs {
					values.push(e.as_static_value()?);
				}
				Value::Array(crate::val::Array(values))
			}
			Literal::Set(exprs) => {
				let mut set = crate::val::Set::new();
				for e in exprs {
					set.insert(e.as_static_value()?);
				}
				Value::Set(set)
			}
			// Built through a sorted map, exactly as evaluation builds it, so
			// key ordering and duplicate-key resolution cannot drift apart.
			Literal::Object(items) => {
				if let Some(geometry) = crate::parser::geometry::collect_geometry(items) {
					return Some(Value::Geometry(geometry));
				}
				let mut map = std::collections::BTreeMap::new();
				for i in items {
					map.insert(i.key.clone(), i.value.as_static_value()?);
				}
				Value::Object(crate::val::Object::from(map))
			}
		};
		Some(value)
	}
}

impl PartialEq for Literal {
	fn eq(&self, other: &Self) -> bool {
		match (self, other) {
			(Literal::None, Literal::None) => true,
			(Literal::Null, Literal::Null) => true,
			(Literal::UnboundedRange, Literal::UnboundedRange) => true,
			(Literal::Bool(a), Literal::Bool(b)) => a == b,
			(Literal::Float(a), Literal::Float(b)) => a.to_bits() == b.to_bits(),
			(Literal::Integer(a), Literal::Integer(b)) => a == b,
			(Literal::Decimal(a), Literal::Decimal(b)) => a == b,
			// Value equality ignores trailing zeros; a literal is the same only as written.
			(Literal::BigDecimal(a), Literal::BigDecimal(b)) => a == b && a.scale() == b.scale(),
			(Literal::String(a), Literal::String(b)) => a == b,
			(Literal::Bytes(a), Literal::Bytes(b)) => a == b,
			(Literal::Regex(a), Literal::Regex(b)) => a == b,
			(Literal::RecordId(a), Literal::RecordId(b)) => a == b,
			(Literal::Array(a), Literal::Array(b)) => a == b,
			(Literal::Set(a), Literal::Set(b)) => a == b,
			(Literal::Object(a), Literal::Object(b)) => a == b,
			(Literal::Duration(a), Literal::Duration(b)) => a == b,
			(Literal::Datetime(a), Literal::Datetime(b)) => a == b,
			(Literal::Date(a), Literal::Date(b)) => a == b,
			(Literal::Time(a), Literal::Time(b)) => a == b,
			(Literal::Timestamp(a), Literal::Timestamp(b)) => a == b,
			(Literal::Interval(a), Literal::Interval(b)) => a.identical(b),
			(Literal::Uuid(a), Literal::Uuid(b)) => a == b,
			(Literal::Geometry(a), Literal::Geometry(b)) => a == b,
			(Literal::File(a), Literal::File(b)) => a == b,
			(Literal::I8(a), Literal::I8(b)) => a == b,
			(Literal::U8(a), Literal::U8(b)) => a == b,
			(Literal::I16(a), Literal::I16(b)) => a == b,
			(Literal::U16(a), Literal::U16(b)) => a == b,
			(Literal::I32(a), Literal::I32(b)) => a == b,
			(Literal::U32(a), Literal::U32(b)) => a == b,
			(Literal::U64(a), Literal::U64(b)) => a == b,
			(Literal::F16(a), Literal::F16(b)) => a.to_bits() == b.to_bits(),
			(Literal::F32(a), Literal::F32(b)) => a.to_bits() == b.to_bits(),
			// Every variant must appear above. This arm is for mismatched
			// variants only: a variant missing from the list falls through to
			// it and compares unequal to itself, which breaks the `Eq` asserted
			// below and silently defeats every structural comparison of an
			// expression tree containing it.
			_ => false,
		}
	}
}
impl Eq for Literal {}

impl Hash for Literal {
	fn hash<H: Hasher>(&self, state: &mut H) {
		std::mem::discriminant(self).hash(state);
		match self {
			Literal::None => {}
			Literal::Null => {}
			Literal::UnboundedRange => {}
			Literal::Bool(x) => x.hash(state),
			Literal::Float(x) => x.to_bits().hash(state),
			Literal::BigDecimal(x) => x.hash(state),
			Literal::Integer(x) => x.hash(state),
			Literal::Decimal(x) => x.hash(state),
			Literal::String(x) => x.hash(state),
			Literal::Bytes(x) => x.hash(state),
			Literal::Regex(x) => x.hash(state),
			Literal::RecordId(x) => x.hash(state),
			Literal::Array(x) => x.hash(state),
			Literal::Set(x) => x.hash(state),
			Literal::Object(x) => x.hash(state),
			Literal::Duration(x) => x.hash(state),
			Literal::Datetime(x) => x.hash(state),
			Literal::Date(x) => x.hash(state),
			Literal::Time(x) => x.hash(state),
			Literal::Timestamp(x) => x.hash(state),
			Literal::Interval(x) => x.hash(state),
			Literal::Uuid(x) => x.hash(state),
			Literal::Geometry(x) => x.hash(state),
			Literal::File(x) => x.hash(state),
			Literal::I8(x) => x.hash(state),
			Literal::U8(x) => x.hash(state),
			Literal::I16(x) => x.hash(state),
			Literal::U16(x) => x.hash(state),
			Literal::I32(x) => x.hash(state),
			Literal::U32(x) => x.hash(state),
			Literal::U64(x) => x.hash(state),
			Literal::F16(x) => x.to_bits().hash(state),
			Literal::F32(x) => x.to_bits().hash(state),
		}
	}
}

impl ToSql for Literal {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		match self {
			Literal::None => f.push_str("NONE"),
			Literal::Null => f.push_str("NULL"),
			Literal::UnboundedRange => f.push_str(".."),
			Literal::Bool(x) => {
				if *x {
					f.push_str("true");
				} else {
					f.push_str("false");
				}
			}
			Literal::Float(float) => write_sql!(f, fmt, "{}", Float(*float)),
			Literal::Integer(x) => f.push_str(&x.to_string()),
			Literal::I8(v) => {
				f.push_str(&v.to_string());
				f.push_str("i8");
			}
			Literal::U8(v) => {
				f.push_str(&v.to_string());
				f.push_str("u8");
			}
			Literal::I16(v) => {
				f.push_str(&v.to_string());
				f.push_str("i16");
			}
			Literal::U16(v) => {
				f.push_str(&v.to_string());
				f.push_str("u16");
			}
			Literal::I32(v) => {
				f.push_str(&v.to_string());
				f.push_str("i32");
			}
			Literal::U32(v) => {
				f.push_str(&v.to_string());
				f.push_str("u32");
			}
			Literal::U64(v) => {
				f.push_str(&v.to_string());
				f.push_str("u64");
			}
			Literal::F16(v) => surrealdb_types::number::fmt_f16_sql(*v, f),
			Literal::F32(v) => surrealdb_types::number::fmt_f32_sql(*v, f),
			Literal::Decimal(d) => d.fmt_sql(f, fmt),
			Literal::BigDecimal(d) => {
				surrealdb_types::Number::BigDecimal(d.clone()).fmt_sql(f, fmt)
			}
			Literal::String(strand) => write_sql!(f, fmt, "{}", QuoteStr(strand)),
			Literal::Bytes(bytes) => write_sql!(f, fmt, "b\"{}\"", hex::encode_upper(&bytes.0)),
			Literal::Regex(regex) => {
				let pattern = regex.to_string().replace('/', "\\/");
				write_sql!(f, fmt, "/{}/", &pattern);
			}
			Literal::RecordId(record_id_lit) => record_id_lit.fmt_sql(f, fmt),
			Literal::Array(exprs) => {
				f.push('[');
				if !exprs.is_empty() {
					let fmt = fmt.increment();
					if fmt.is_pretty() {
						f.push('\n');
						fmt.write_indent(f);
					}
					for (i, expr) in exprs.iter().enumerate() {
						if i > 0 {
							fmt.write_separator(f);
						}
						CoverStmts(expr).fmt_sql(f, fmt);
					}
					if fmt.is_pretty() {
						f.push('\n');
						// One level less indentation for closing bracket
						if let SqlFormat::Indented(level) = fmt
							&& level > 0
						{
							for _ in 0..(level - 1) {
								f.push('\t');
							}
						}
					}
				}
				f.push(']');
			}
			Literal::Set(exprs) => {
				f.push('{');
				if !exprs.is_empty() {
					let fmt = fmt.increment();
					if fmt.is_pretty() {
						f.push('\n');
						fmt.write_indent(f);
					}
					for (i, expr) in exprs.iter().enumerate() {
						if i > 0 {
							fmt.write_separator(f);
						} else if let Expr::Literal(Literal::RecordId(_)) = *expr {
							f.push('(');
							expr.fmt_sql(f, fmt);
							f.push(')');
							continue;
						}
						CoverStmts(expr).fmt_sql(f, fmt);
					}

					if exprs.len() == 1 {
						f.push(',');
					}

					if fmt.is_pretty() {
						f.push('\n');
						// One level less indentation for closing bracket
						if let SqlFormat::Indented(level) = fmt
							&& level > 0
						{
							for _ in 0..(level - 1) {
								f.push('\t');
							}
						}
					}
				} else {
					f.push(',');
				}
				f.push('}');
			}
			Literal::Object(items) => {
				if fmt.is_pretty() {
					f.push('{');
				} else {
					f.push_str("{ ");
				}
				if !items.is_empty() {
					let fmt = fmt.increment();
					if fmt.is_pretty() {
						f.push('\n');
						fmt.write_indent(f);
					}
					for (i, entry) in items.iter().enumerate() {
						if i > 0 {
							fmt.write_separator(f);
						}
						write_sql!(
							f,
							fmt,
							"{}: {}",
							EscapeObjectKey(&entry.key),
							CoverStmts(&entry.value)
						);
					}
					if fmt.is_pretty() {
						f.push('\n');
						// One level less indentation for closing bracket
						if let SqlFormat::Indented(level) = fmt
							&& level > 0
						{
							for _ in 0..(level - 1) {
								f.push('\t');
							}
						}
					}
				}
				if fmt.is_pretty() {
					f.push('}');
				} else {
					f.push_str(" }");
				}
			}
			Literal::Duration(duration) => SqlDuration(duration.0).fmt_sql(f, fmt),
			Literal::Datetime(datetime) => SqlDatetime(datetime.0).fmt_sql(f, fmt),
			Literal::Date(v) => v.fmt_sql(f, fmt),
			Literal::Time(v) => v.fmt_sql(f, fmt),
			Literal::Timestamp(v) => v.fmt_sql(f, fmt),
			Literal::Interval(v) => v.fmt_sql(f, fmt),
			Literal::Uuid(uuid) => write_sql!(f, fmt, "u{}", QuoteStr(&uuid.to_string())),
			Literal::Geometry(geometry) => SqlGeometry(geometry).fmt_sql(f, fmt),
			Literal::File(file) => file.fmt_sql(f, fmt),
		}
	}
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct ObjectEntry {
	pub key: Strand,
	pub value: Expr,
}

impl ToSql for ObjectEntry {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		write_sql!(f, fmt, "{}: {}", EscapeObjectKey(self.key.as_str()), self.value);
	}
}

#[cfg(test)]
mod equality_tests {
	use rust_decimal::Decimal;

	use super::Literal;
	use crate::val::Strand;

	/// `Eq` is asserted for `Literal`, so every variant must equal itself.
	///
	/// The impl is hand-written with a `_ => false` fallthrough, so a variant
	/// omitted from it compares unequal to itself rather than failing to
	/// compile. That breaks any structural comparison of an expression
	/// containing it: a change-detector sees a spurious difference, and an
	/// index-guard match sees none.
	#[test]
	fn every_variant_equals_itself() {
		// The exhaustive match is the point: it makes adding a variant a
		// compile error here, so the sample list cannot fall behind the enum
		// the way a hand-maintained subset does.
		fn _forces_this_list_to_be_updated(l: &Literal) {
			match l {
				Literal::None
				| Literal::Null
				| Literal::UnboundedRange
				| Literal::Bool(_)
				| Literal::Float(_)
				| Literal::Integer(_)
				| Literal::Decimal(_)
				| Literal::BigDecimal(_)
				| Literal::String(_)
				| Literal::Bytes(_)
				| Literal::Regex(_)
				| Literal::RecordId(_)
				| Literal::Array(_)
				| Literal::Set(_)
				| Literal::Object(_)
				| Literal::Duration(_)
				| Literal::Datetime(_)
				| Literal::Date(_)
				| Literal::Time(_)
				| Literal::Timestamp(_)
				| Literal::Interval(_)
				| Literal::Uuid(_)
				| Literal::Geometry(_)
				| Literal::File(_)
				| Literal::I8(_)
				| Literal::U8(_)
				| Literal::I16(_)
				| Literal::U16(_)
				| Literal::I32(_)
				| Literal::U32(_)
				| Literal::U64(_)
				| Literal::F16(_)
				| Literal::F32(_) => {}
			}
		}
		let samples = [
			Literal::None,
			Literal::Null,
			Literal::UnboundedRange,
			Literal::Bool(true),
			Literal::Float(1.5),
			Literal::Float(-0.0),
			Literal::Integer(7),
			Literal::Decimal(Decimal::new(150, 2)),
			Literal::String(Strand::new("s")),
			Literal::Bytes(crate::val::Bytes::from(vec![1u8])),
			Literal::Regex("a".parse::<crate::val::Regex>().unwrap()),
			Literal::RecordId(crate::expr::RecordIdLit {
				table: "t".into(),
				key: crate::expr::RecordIdKeyLit::Number(1),
			}),
			Literal::Array(vec![]),
			Literal::Set(vec![]),
			Literal::Object(vec![]),
			Literal::Duration(crate::val::Duration::from_secs(1)),
			Literal::Datetime(crate::val::Datetime::MIN_UTC),
			Literal::Uuid(crate::val::Uuid::nil()),
			Literal::Geometry(crate::val::Geometry::Point(geo_types::Point::new(1.0, 2.0))),
			Literal::File(crate::val::File::new("b".to_owned(), "p".to_owned())),
			Literal::I8(1),
			Literal::U8(1),
			Literal::I16(1),
			Literal::U16(1),
			Literal::I32(1),
			Literal::U32(1),
			Literal::U64(1),
			Literal::F16(half::f16::from_f32(1.5)),
			Literal::F16(half::f16::from_f32(-0.0)),
			Literal::F32(1.5),
			Literal::F32(-0.0),
		];
		for s in samples {
			assert_eq!(s, s.clone(), "{s:?} must equal itself");
		}
	}

	/// `Float` compares by bit pattern, so the two zeroes are distinct — they
	/// render differently (`-0f` vs `0f`), and treating them as equal would let
	/// a rewrite that changes one into the other be dropped as a no-op.
	#[test]
	fn signed_zero_is_not_equal_to_zero() {
		assert_ne!(Literal::Float(-0.0), Literal::Float(0.0));
		assert_ne!(Literal::F16(half::f16::from_f32(-0.0)), Literal::F16(half::f16::from_f32(0.0)));
		assert_ne!(Literal::F32(-0.0), Literal::F32(0.0));
	}
}

#[cfg(test)]
mod static_value_tests {
	use surrealdb_strand::Strand;

	use super::{Literal, ObjectEntry};
	use crate::expr::record_id::RecordIdKeyGen;
	use crate::expr::{Expr, RecordIdKeyLit, RecordIdLit};
	use crate::val::Value;

	fn entry(key: &str, value: Literal) -> ObjectEntry {
		ObjectEntry {
			key: Strand::new(key),
			value: Expr::Literal(value),
		}
	}

	/// An object's entries come out ordered by key regardless of source order,
	/// because the value has to be indistinguishable from the evaluated one.
	#[test]
	fn object_entries_are_ordered_by_key() {
		let lit =
			Literal::Object(vec![entry("z", Literal::Integer(1)), entry("a", Literal::Integer(2))]);
		let Some(Value::Object(obj)) = lit.as_static_value() else {
			panic!("a literal object of literals is a value");
		};
		let keys: Vec<&str> = obj.iter().map(|(k, _)| k.as_str()).collect();
		assert_eq!(keys, ["a", "z"]);
	}

	/// A repeated key resolves to its last occurrence, matching what inserting
	/// each entry in source order produces.
	#[test]
	fn repeated_object_key_keeps_the_last() {
		let lit =
			Literal::Object(vec![entry("a", Literal::Integer(1)), entry("a", Literal::Integer(3))]);
		let Some(Value::Object(obj)) = lit.as_static_value() else {
			panic!("a literal object of literals is a value");
		};
		assert_eq!(obj.len(), 1);
		assert_eq!(obj.get("a"), Some(&Value::Number(crate::val::Number::Int(3))));
	}

	/// A generated record-id key is allocated during evaluation, so a literal
	/// carrying one is not a value on its own — and neither is any collection
	/// holding it, or the fast path would hand out ids the evaluator never
	/// allocated.
	#[test]
	fn generated_record_id_key_is_not_static() {
		let lit = Literal::RecordId(RecordIdLit {
			table: "t".into(),
			key: RecordIdKeyLit::Generate(RecordIdKeyGen::Ulid),
		});
		assert!(lit.as_static_value().is_none());
		let nested = Literal::Object(vec![entry("id", lit.clone())]);
		assert!(nested.as_static_value().is_none());
		let in_array = Literal::Array(vec![Expr::Literal(lit)]);
		assert!(in_array.as_static_value().is_none());
	}

	/// A collection is a value only if every element is, so one non-literal
	/// element withholds the whole collection from the fast path.
	#[test]
	fn non_literal_element_withholds_the_collection() {
		let lit = Literal::Array(vec![
			Expr::Literal(Literal::Integer(1)),
			Expr::Idiom(crate::expr::Idiom::field("f".to_owned())),
		]);
		assert!(lit.as_static_value().is_none());
	}

	/// Simple record-id keys are values, which is what makes an exported
	/// record's `id` field take the fast path.
	#[test]
	fn simple_record_id_keys_are_static() {
		for key in [
			RecordIdKeyLit::Number(1),
			RecordIdKeyLit::String(Strand::new("a")),
			RecordIdKeyLit::Uuid(crate::val::Uuid::nil()),
		] {
			let lit = Literal::RecordId(RecordIdLit {
				table: "t".into(),
				key,
			});
			assert!(matches!(lit.as_static_value(), Some(Value::RecordId(_))));
		}
	}
}

struct SqlGeometry<'a>(&'a Geometry);
impl ToSql for SqlGeometry<'_> {
	fn fmt_sql(&self, f: &mut String, _fmt: SqlFormat) {
		let sql_fmt = SqlFormat::SingleLine;
		match self.0 {
			Geometry::Point(v) => {
				write_sql!(f, sql_fmt, "({}, {})", Float(v.x()), Float(v.y()))
			}
			Geometry::Line(v) => write_sql!(
				f,
				sql_fmt,
				"{{ type: 'LineString', coordinates: [{}] }}",
				Fmt::comma_separated(v.points().map(|v| Fmt::new(v, |v, f, fmt| write_sql!(
					f,
					fmt,
					"[{}, {}]",
					Float(v.x()),
					Float(v.y())
				))))
			),
			Geometry::Polygon(v) => write_sql!(
				f,
				sql_fmt,
				"{{ type: 'Polygon', coordinates: [{}] }}",
				Fmt::comma_separated(once(v.exterior()).chain(v.interiors()).map(|v| Fmt::new(
					v,
					|v, f, fmt| write_sql!(
						f,
						fmt,
						"[{}]",
						Fmt::comma_separated(v.points().map(|v| Fmt::new(
							v,
							|v, f, fmt| write_sql!(f, fmt, "[{}, {}]", Float(v.x()), Float(v.y()))
						)))
					)
				)))
			),
			Geometry::MultiPoint(v) => {
				write_sql!(
					f,
					sql_fmt,
					"{{ type: 'MultiPoint', coordinates: [{}] }}",
					Fmt::comma_separated(v.iter().map(|v| Fmt::new(v, |v, f, fmt| write_sql!(
						f,
						fmt,
						"[{}, {}]",
						Float(v.x()),
						Float(v.y())
					))))
				)
			}
			Geometry::MultiLine(v) => {
				write_sql!(
					f,
					sql_fmt,
					"{{ type: 'MultiLineString', coordinates: [{}] }}",
					Fmt::comma_separated(v.iter().map(|v| Fmt::new(v, |v, f, fmt| write_sql!(
						f,
						fmt,
						"[{}]",
						Fmt::comma_separated(v.points().map(|v| Fmt::new(
							v,
							|v, f, fmt| write_sql!(f, fmt, "[{}, {}]", Float(v.x()), Float(v.y()))
						)))
					))))
				)
			}
			Geometry::MultiPolygon(v) => {
				write_sql!(
					f,
					sql_fmt,
					"{{ type: 'MultiPolygon', coordinates: [{}] }}",
					Fmt::comma_separated(v.iter().map(|v| Fmt::new(v, |v, f, fmt| {
						write_sql!(
							f,
							fmt,
							"[{}]",
							Fmt::comma_separated(once(v.exterior()).chain(v.interiors()).map(
								|v| Fmt::new(v, |v, f, fmt| write_sql!(
									f,
									fmt,
									"[{}]",
									Fmt::comma_separated(v.points().map(|v| Fmt::new(
										v,
										|v, f, fmt| write_sql!(
											f,
											fmt,
											"[{}, {}]",
											Float(v.x()),
											Float(v.y())
										)
									)))
								))
							))
						)
					}))),
				)
			}
			Geometry::Collection(v) => {
				write_sql!(
					f,
					sql_fmt,
					"{{ type: 'GeometryCollection', geometries: [{}] }}",
					Fmt::comma_separated(v.iter().map(SqlGeometry))
				)
			}
		}
	}
}
