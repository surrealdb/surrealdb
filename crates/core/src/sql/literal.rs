use std::fmt;

use crate::sql::{Closure, Expr, RecordIdLit, Regex};
use crate::val::{Bytes, Datetime, Duration, File, Geometry, Strand, Uuid};
use rust_decimal::Decimal;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum Literal {
	None,
	Null,
	// and unbounded range: `..`
	UnboundedRange,
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
}

impl From<Literal> for crate::expr::Literal {
	fn from(value: Literal) -> Self {
		match value {
			Literal::None => crate::expr::Literal::None,
			Literal::Null => crate::expr::Literal::Null,
			Literal::UnboundedRange => crate::expr::Literal::UnboundedRange,
			Literal::Bool(x) => crate::expr::Literal::Bool(x),
			Literal::Float(x) => crate::expr::Literal::Float(x),
			Literal::Integer(x) => crate::expr::Literal::Integer(x),
			Literal::Decimal(decimal) => crate::expr::Literal::Decimal(decimal),
			Literal::Duration(duration) => crate::expr::Literal::Duration(duration),
			Literal::Strand(strand) => crate::expr::Literal::Strand(strand),
			Literal::RecordId(record_id_lit) => {
				crate::expr::Literal::RecordId(record_id_lit.into())
			}
			Literal::Datetime(datetime) => crate::expr::Literal::Datetime(datetime),
			Literal::Uuid(uuid) => crate::expr::Literal::Uuid(uuid.into()),
			Literal::Regex(regex) => crate::expr::Literal::Regex(regex.into()),
			Literal::Array(exprs) => {
				crate::expr::Literal::Array(exprs.into_iter().map(From::from).collect())
			}
			Literal::Object(items) => {
				crate::expr::Literal::Object(items.into_iter().map(From::from).collect())
			}
			Literal::Geometry(geometry) => crate::expr::Literal::Geometry(geometry),
			Literal::File(file) => crate::expr::Literal::File(file),
			Literal::Bytes(bytes) => crate::expr::Literal::Bytes(bytes),
			Literal::Closure(closure) => crate::expr::Literal::Closure(Box::new((*closure).into())),
		}
	}
}

impl From<crate::expr::Literal> for Literal {
	fn from(value: crate::expr::Literal) -> Self {
		match value {
			crate::expr::Literal::None => Literal::None,
			crate::expr::Literal::Null => Literal::Null,
			crate::expr::Literal::UnboundedRange => Literal::UnboundedRange,
			crate::expr::Literal::Bool(x) => Literal::Bool(x),
			crate::expr::Literal::Float(x) => Literal::Float(x),
			crate::expr::Literal::Integer(x) => Literal::Integer(x),
			crate::expr::Literal::Decimal(decimal) => Literal::Decimal(decimal),
			crate::expr::Literal::Duration(duration) => Literal::Duration(duration),
			crate::expr::Literal::Strand(strand) => Literal::Strand(strand),
			crate::expr::Literal::RecordId(record_id_lit) => {
				Literal::RecordId(record_id_lit.into())
			}
			crate::expr::Literal::Datetime(datetime) => Literal::Datetime(datetime),
			crate::expr::Literal::Uuid(uuid) => Literal::Uuid(uuid),
			crate::expr::Literal::Regex(regex) => Literal::Regex(regex),
			crate::expr::Literal::Array(exprs) => {
				Literal::Array(exprs.into_iter().map(From::from).collect())
			}
			crate::expr::Literal::Object(items) => {
				Literal::Object(items.into_iter().map(From::from).collect())
			}
			crate::expr::Literal::Geometry(geometry) => Literal::Geometry(geometry),
			crate::expr::Literal::File(file) => Literal::File(file),
			crate::expr::Literal::Bytes(bytes) => Literal::Bytes(bytes),
			crate::expr::Literal::Closure(closure) => Literal::Closure(Box::new((*closure).into())),
		}
	}
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ObjectEntry {
	pub key: String,
	pub value: Expr,
}

impl From<ObjectEntry> for crate::expr::literal::ObjectEntry {
	fn from(value: ObjectEntry) -> Self {
		crate::expr::literal::ObjectEntry {
			key: value.key,
			value: value.value.into(),
		}
	}
}

impl From<crate::expr::literal::ObjectEntry> for ObjectEntry {
	fn from(value: crate::expr::literal::ObjectEntry) -> Self {
		ObjectEntry {
			key: value.key,
			value: value.value.into(),
		}
	}
}

impl fmt::Display for ObjectEntry {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "{}: {}", self.key, self.value)
	}
}
