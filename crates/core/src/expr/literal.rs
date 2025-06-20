use crate::expr::{Expr, File, RecordIdLit, Regex, Uuid};
use crate::val::{Bytes, Closure, Datetime, Duration, Geometry, Strand};
use revision::revisioned;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[revisioned(revision = 1)]
#[derive(Clone, Debug, PartialEq, PartialOrd, Serialize, Deserialize)]
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
}

pub struct ObjectEntry {
	pub key: String,
	pub value: Expr,
}
