//use async_graphql::dynamic::Object;
use chrono::{DateTime, Utc};
use common::fmt::{EscapeObjectKey, Float, QuoteStr, SqlDatetime, SqlDuration};
use rust_decimal::Decimal;
use surrealdb_strand::Strand;
use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::file::File;
use crate::{CoverStmts, Expr, RecordIdLit};

#[derive(Clone, Debug)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum Literal {
	None,
	Null,
	// and unbounded range: `..`
	UnboundedRange,
	Bool(bool),
	Float(f64),
	Integer(i64),
	Decimal(
		#[cfg_attr(feature = "arbitrary", arbitrary(with = crate::arbitrary::arb_decimal))] Decimal,
	),
	Duration(std::time::Duration),

	String(Strand),
	RecordId(RecordIdLit),
	Datetime(DateTime<Utc>),
	Uuid(uuid::Uuid),
	Regex(
		#[cfg_attr(feature = "arbitrary", arbitrary(with = crate::arbitrary::arb_regex))]
		regex::Regex,
	),

	Array(Vec<Expr>),
	Set(Vec<Expr>),
	Object(Vec<ObjectEntry>),
	Geometry(geo::Geometry<f64>),
	File(File),
	Bytes(
		#[cfg_attr(feature = "arbitrary", arbitrary(with = crate::arbitrary::arb_bytes))]
		bytes::Bytes,
	),
}

impl PartialEq for Literal {
	fn eq(&self, other: &Self) -> bool {
		match (self, other) {
			(Literal::None, Literal::None) => true,
			(Literal::Null, Literal::Null) => true,
			(Literal::Bool(a), Literal::Bool(b)) => a == b,
			(Literal::Float(a), Literal::Float(b)) => a.to_bits() == b.to_bits(),
			(Literal::Integer(a), Literal::Integer(b)) => a == b,
			(Literal::Decimal(a), Literal::Decimal(b)) => a == b,
			(Literal::String(a), Literal::String(b)) => a == b,
			(Literal::Bytes(a), Literal::Bytes(b)) => a == b,
			// `regex::Regex` has no `PartialEq`; compare the source pattern instead.
			(Literal::Regex(a), Literal::Regex(b)) => a.as_str() == b.as_str(),
			(Literal::RecordId(a), Literal::RecordId(b)) => a == b,
			(Literal::Array(a), Literal::Array(b)) => a == b,
			(Literal::Set(a), Literal::Set(b)) => a == b,
			(Literal::Object(a), Literal::Object(b)) => a == b,
			(Literal::Duration(a), Literal::Duration(b)) => a == b,
			(Literal::Datetime(a), Literal::Datetime(b)) => a == b,
			(Literal::Uuid(a), Literal::Uuid(b)) => a == b,
			(Literal::Geometry(a), Literal::Geometry(b)) => a == b,
			(Literal::File(a), Literal::File(b)) => a == b,
			// Payload-free, so equal by construction. Without this arm it falls
			// to `_ => false` and `impl Eq` below asserts a reflexivity that
			// does not hold.
			(Literal::UnboundedRange, Literal::UnboundedRange) => true,
			// Deliberately last, and deliberately not a catch-all for variants
			// that were simply forgotten: every variant needs an arm above.
			// `every_variant_equals_itself` is what enforces that.
			_ => false,
		}
	}
}
impl Eq for Literal {}

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
			Literal::Decimal(d) => d.fmt_sql(f, fmt),
			Literal::String(strand) => write_sql!(f, fmt, "{}", QuoteStr(strand)),
			Literal::Bytes(bytes) => write_sql!(f, fmt, "b\"{}\"", hex::encode_upper(bytes)),
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
			Literal::Duration(duration) => SqlDuration(*duration).fmt_sql(f, fmt),
			Literal::Datetime(datetime) => SqlDatetime(*datetime).fmt_sql(f, fmt),
			Literal::Uuid(uuid) => write_sql!(f, fmt, "u{}", QuoteStr(&uuid.to_string())),
			// Delegate to the public Geometry's rendering: float coordinates get
			// the `f` suffix there, unlike the runtime Geometry's bare-f64
			// printing. Cold, AST-printing path, not the exec hot path.
			Literal::Geometry(geometry) => {
				surrealdb_types::Geometry::from(geometry.clone()).fmt_sql(f, fmt)
			}
			Literal::File(file) => file.fmt_sql(f, fmt),
		}
	}
}

/// A hack to convert objects to geometries like they previously would.
/// If it fails to convert to geometry it just returns an object like previous
/// behaviour>
///
/// The behaviour around geometries needs to be improved but until then this is
/// her to ensure they still work like they previously would.

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
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
mod tests {
	use super::*;

	/// One value per variant.
	///
	/// The exhaustive match is the point: it makes adding a variant a compile
	/// error here, so the sample list cannot silently fall behind the enum the
	/// way a hand-maintained list does.
	fn one_of_each() -> Vec<Literal> {
		fn _forces_this_list_to_be_updated(l: &Literal) {
			match l {
				Literal::None
				| Literal::Null
				| Literal::UnboundedRange
				| Literal::Bool(_)
				| Literal::Float(_)
				| Literal::Integer(_)
				| Literal::Decimal(_)
				| Literal::Duration(_)
				| Literal::String(_)
				| Literal::RecordId(_)
				| Literal::Datetime(_)
				| Literal::Uuid(_)
				| Literal::Regex(_)
				| Literal::Array(_)
				| Literal::Set(_)
				| Literal::Object(_)
				| Literal::Geometry(_)
				| Literal::File(_)
				| Literal::Bytes(_) => {}
			}
		}
		vec![
			Literal::None,
			Literal::Null,
			Literal::UnboundedRange,
			Literal::Bool(true),
			Literal::Float(1.5),
			Literal::Integer(7),
			Literal::Decimal(Decimal::new(150, 2)),
			Literal::Duration(std::time::Duration::from_secs(1)),
			Literal::String(Strand::new("s")),
			Literal::RecordId(RecordIdLit {
				table: "t".into(),
				key: crate::RecordIdKeyLit::Number(1),
			}),
			Literal::Datetime(DateTime::<Utc>::from_timestamp(0, 0).unwrap()),
			Literal::Uuid(uuid::Uuid::nil()),
			Literal::Regex("a".parse().unwrap()),
			Literal::Array(vec![]),
			Literal::Set(vec![]),
			Literal::Object(vec![]),
			Literal::Geometry(geo::Geometry::Point(geo::Point::new(1.0, 2.0))),
			Literal::File(File {
				bucket: "b".to_owned(),
				key: "p".to_owned(),
			}),
			Literal::Bytes(bytes::Bytes::from_static(b"x")),
		]
	}

	/// Every variant must equal itself.
	///
	/// `PartialEq` is hand-written with a `_ => false` fallthrough, so a variant
	/// omitted from it compares unequal to itself rather than failing to
	/// compile — while `impl Eq` asserts the opposite. `sql::Expr` derives its
	/// equality from this, and `sql::Ast` from that, so a single missing arm
	/// makes structural comparison of any parsed query containing that literal
	/// silently false.
	#[test]
	fn every_variant_equals_itself() {
		for l in one_of_each() {
			assert_eq!(l, l.clone(), "{l:?} must equal itself");
		}
	}

	/// The two zeroes render differently (`-0f` vs `0f`), so treating them as
	/// equal would let a change-detector miss a real edit.
	#[test]
	fn signed_zero_is_not_equal_to_zero() {
		assert_ne!(Literal::Float(0.0), Literal::Float(-0.0));
	}
}
