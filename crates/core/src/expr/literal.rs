use crate::expr::{Expr, RecordIdLit};
use crate::key::sequence::st;
use crate::val::{Bytes, Closure, Datetime, Duration, File, Geometry, Regex, Strand, Uuid};
use revision::revisioned;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::hash::{Hash, Hasher};

/// A literal value, should be computed to get an actual value.
///
/// # Note regarding equality.
/// A literal is equal to an other literal if it is the exact same byte representation, so normal float rules
/// regarding equality do not apply, i.e. if `a != b` then `Literal::Float(a)` could still be equal
/// to `Literal::Float(b)` in the case of `NaN` floats for example.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename = "$surrealdb::private::sql::Value")]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum Literal {
	None,
	Null,
	Bool(bool),
	Float(f64),
	Integer(i64),
	//TODO: Possibly remove wrapper.
	Decimal(Decimal),
	Strand(Strand),
	Bytes(Bytes),
	//TODO: Possibly remove wrapper.
	Regex(Regex),
	RecordId(RecordIdLit),
	Array(Vec<Expr>),
	Object(Vec<(String, Expr)>),
	Duration(Duration),
	Datetime(Datetime),
	Uuid(Uuid),
	Geometry(Geometry),
	File(File),
	Closure(Box<Closure>),
	Point(f64, f64),
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
			(Literal::Strand(a), Literal::Strand(b)) => a == b,
			(Literal::Bytes(a), Literal::Bytes(b)) => a == b,
			(Literal::Regex(a), Literal::Regex(b)) => a == b,
			(Literal::RecordId(a), Literal::RecordId(b)) => a == b,
			(Literal::Array(a), Literal::Array(b)) => a == b,
			(Literal::Object(a), Literal::Object(b)) => a == b,
			(Literal::Duration(a), Literal::Duration(b)) => a == b,
			(Literal::Datetime(a), Literal::Datetime(b)) => a == b,
			(Literal::Uuid(a), Literal::Uuid(b)) => a == b,
			(Literal::Geometry(a), Literal::Geometry(b)) => a == b,
			(Literal::File(a), Literal::File(b)) => a == b,
			(Literal::Closure(a), Literal::Closure(b)) => a == b,
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
			Literal::Bool(x) => x.hash(state),
			Literal::Float(x) => x.to_bits().hash(state),
			Literal::Integer(x) => x.hash(state),
			Literal::Decimal(x) => x.hash(state),
			Literal::Strand(x) => x.hash(state),
			Literal::Bytes(x) => x.hash(state),
			Literal::Regex(x) => x.hash(state),
			Literal::RecordId(x) => x.hash(state),
			Literal::Array(x) => x.hash(state),
			Literal::Object(x) => x.hash(state),
			Literal::Duration(x) => x.hash(state),
			Literal::Datetime(x) => x.hash(state),
			Literal::Uuid(x) => x.hash(state),
			Literal::Geometry(x) => x.hash(state),
			Literal::File(x) => x.hash(state),
			Literal::Closure(x) => x.hash(state),
		}
	}
}

pub struct ObjectEntry {
	pub key: String,
	pub value: Expr,
}
