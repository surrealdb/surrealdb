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

pub struct ObjectEntry {
	pub key: String,
	pub value: Expr,
}
