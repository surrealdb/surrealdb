use std::hash::{Hash, Hasher};

use rust_decimal::Decimal;
use surrealdb_strand::Strand;
use surrealdb_types::{SqlFormat, ToSql};

use crate::expr::{Expr, RecordIdLit};
use crate::val::{Bytes, Datetime, Duration, File, Geometry, Regex, Uuid};

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
			| Literal::String(_)
			| Literal::Bytes(_)
			| Literal::Regex(_)
			| Literal::Duration(_)
			| Literal::Datetime(_)
			| Literal::Uuid(_)
			| Literal::File(_)
			| Literal::Geometry(_) => true,
			Literal::RecordId(record_id_lit) => record_id_lit.is_static(),
			Literal::Array(exprs) => exprs.iter().all(|x| x.is_static()),
			Literal::Set(exprs) => exprs.iter().all(|x| x.is_static()),
			Literal::Object(items) => items.iter().all(|x| x.value.is_static()),
		}
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
			(Literal::String(a), Literal::String(b)) => a == b,
			(Literal::Bytes(a), Literal::Bytes(b)) => a == b,
			(Literal::Regex(a), Literal::Regex(b)) => a == b,
			(Literal::RecordId(a), Literal::RecordId(b)) => a == b,
			(Literal::Array(a), Literal::Array(b)) => a == b,
			(Literal::Set(a), Literal::Set(b)) => a == b,
			(Literal::Object(a), Literal::Object(b)) => a == b,
			(Literal::Duration(a), Literal::Duration(b)) => a == b,
			(Literal::Datetime(a), Literal::Datetime(b)) => a == b,
			(Literal::Uuid(a), Literal::Uuid(b)) => a == b,
			(Literal::Geometry(a), Literal::Geometry(b)) => a == b,
			(Literal::File(a), Literal::File(b)) => a == b,
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
			Literal::Uuid(x) => x.hash(state),
			Literal::Geometry(x) => x.hash(state),
			Literal::File(x) => x.hash(state),
		}
	}
}

impl ToSql for Literal {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		let lit: crate::sql::Literal = self.clone().into();
		lit.fmt_sql(f, fmt);
	}
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct ObjectEntry {
	pub key: Strand,
	pub value: Expr,
}

impl ToSql for ObjectEntry {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		let entry: crate::sql::literal::ObjectEntry = self.clone().into();
		entry.fmt_sql(f, fmt);
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
				| Literal::String(_)
				| Literal::Bytes(_)
				| Literal::Regex(_)
				| Literal::RecordId(_)
				| Literal::Array(_)
				| Literal::Set(_)
				| Literal::Object(_)
				| Literal::Duration(_)
				| Literal::Datetime(_)
				| Literal::Uuid(_)
				| Literal::Geometry(_)
				| Literal::File(_) => {}
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
			Literal::Geometry(crate::val::Geometry::Point(geo::Point::new(1.0, 2.0))),
			Literal::File(crate::val::File::new("b".to_owned(), "p".to_owned())),
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
	}
}
