use crate::sql::{
	Bytes, Closure, Datetime, Duration, Expr, File, Geometry, RecordIdLit, Regex, Strand, Uuid,
};
use rust_decimal::Decimal;

#[derive(Clone, Debug, PartialEq, PartialOrd)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum Literal {
	None,
	Null,
	Bool(bool),
	Float(f64),
	Integer(i64),
	//TODO: Possibly remove wrapper.
	Decimal(Decimal),
	Duration(Duration),

	Strand(Strand),
	RecordId(RecordIdLit),
	Datetime(Datetime),
	Uuid(Uuid),
	Regex(Regex),

	//TODO: Possibly remove wrapper.
	Array(Vec<Expr>),
	Object(Vec<ObjectEntry>),
	Geometry(Geometry),
	File(File),
	Bytes(Bytes),
	Closure(Box<Closure>),

	Point(f64, f64),
}

impl From<Literal> for crate::expr::Literal {
	fn from(value: Literal) -> Self {
		match value {
			Literal::None => crate::expr::Literal::None,
			Literal::Null => crate::expr::Literal::Null,
			Literal::Bool(x) => crate::expr::Literal::Bool(x),
			Literal::Float(x) => crate::expr::Literal::Float(x),
			Literal::Integer(x) => crate::expr::Literal::Integer(x),
			Literal::Decimal(decimal) => crate::expr::Literal::Decimal(decimal),
			Literal::Duration(duration) => crate::expr::Literal::Duration(duration.into()),
			Literal::Strand(strand) => crate::expr::Literal::Strand(strand.into()),
			Literal::RecordId(record_id_lit) => {
				crate::expr::Literal::RecordId(record_id_lit.into())
			}
			Literal::Datetime(datetime) => crate::expr::Literal::Datetime(datetime.into()),
			Literal::Uuid(uuid) => crate::expr::Literal::Uuid(uuid.into()),
			Literal::Regex(regex) => crate::expr::Literal::Regex(regex.into()),
			Literal::Array(exprs) => crate::expr::Literal::Array(exprs.into()),
			Literal::Object(items) => crate::expr::Literal::Object(items.into()),
			Literal::Geometry(geometry) => crate::expr::Literal::Geometry(geometry.into()),
			Literal::File(file) => crate::expr::Literal::File(file.into()),
			Literal::Bytes(bytes) => crate::expr::Literal::Bytes(bytes.into()),
			Literal::Closure(closure) => crate::expr::Literal::Closure(closure.into()),
			Literal::Point(a, b) => crate::expr::Literal::Point(a, b),
		}
	}
}

impl From<crate::expr::Literal> for Literal {
	fn from(value: Literal) -> Self {
		match value {
			crate::expr::Literal::None => Literal::None,
			crate::expr::Literal::Null => Literal::Null,
			crate::expr::Literal::Bool(x) => Literal::Bool(x),
			crate::expr::Literal::Float(x) => Literal::Float(x),
			crate::expr::Literal::Integer(x) => Literal::Integer(x),
			crate::expr::Literal::Decimal(decimal) => Literal::Decimal(decimal),
			crate::expr::Literal::Duration(duration) => Literal::Duration(duration),
			crate::expr::Literal::Strand(strand) => Literal::Strand(strand),
			crate::expr::Literal::RecordId(record_id_lit) => Literal::RecordId(record_id_lit),
			crate::expr::Literal::Datetime(datetime) => Literal::Datetime(datetime),
			crate::expr::Literal::Uuid(uuid) => Literal::Uuid(uuid),
			crate::expr::Literal::Regex(regex) => Literal::Regex(regex),
			crate::expr::Literal::Array(exprs) => Literal::Array(exprs),
			crate::expr::Literal::Object(items) => Literal::Object(items),
			crate::expr::Literal::Geometry(geometry) => Literal::Geometry(geometry),
			crate::expr::Literal::File(file) => Literal::File(file),
			crate::expr::Literal::Bytes(bytes) => Literal::Bytes(bytes),
			crate::expr::Literal::Closure(closure) => Literal::Closure(closure),
			crate::expr::Literal::Point(a, b) => Literal::Point(a, b),
		}
	}
}

pub struct ObjectEntry {
	pub key: String,
	pub value: Expr,
}
