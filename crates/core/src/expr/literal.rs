use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::expr::{Expr, FlowResult, RecordIdLit, fmt::Fmt};
use crate::val::{
	Array, Bytes, Closure, Datetime, Duration, File, Geometry, Number, Object, Range, Regex,
	Strand, Uuid, Value,
};
use reblessive::tree::Stk;
use revision::revisioned;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt;
use std::hash::{Hash, Hasher};

/// A literal value, should be computed to get an actual value.
///
/// # Note regarding equality.
/// A literal is equal to an other literal if it is the exact same byte representation, so normal float rules
/// regarding equality do not apply, i.e. if `a != b` then `Literal::Float(a)` could still be equal
/// to `Literal::Float(b)` in the case of `NaN` floats for example. Also surrealql rules regarding
/// number equality are not observed, 1f != 1dec.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename = "$surrealdb::private::sql::Value")]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum Literal {
	None,
	Null,
	// An unbounded range, i.e. `..` without any start or end bound.
	UnboundedRange,
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
	Object(Vec<ObjectEntry>),
	Duration(Duration),
	Datetime(Datetime),
	Uuid(Uuid),
	Geometry(Geometry),
	File(File),
	Closure(Box<Closure>),
}

impl Literal {
	pub(crate) fn is_static(&self) -> bool {
		match self {
			Literal::None
			| Literal::Null
			| Literal::UnboundedRange
			| Literal::Bool(_)
			| Literal::Float(_)
			| Literal::Integer(_)
			| Literal::Decimal(_)
			| Literal::Strand(_)
			| Literal::Bytes(_)
			| Literal::Regex(_)
			| Literal::Duration(_)
			| Literal::Datetime(_)
			| Literal::Uuid(_)
			| Literal::File(_)
			| Literal::Geometry(_) => true,
			Literal::RecordId(record_id_lit) => record_id_lit.is_static(),
			Literal::Array(exprs) => exprs.iter().all(|x| x.is_static()),
			Literal::Object(items) => items.iter().all(|x| x.value.is_static()),
			Literal::Closure(_) => false,
		}
	}

	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> FlowResult<Value> {
		let res = match self {
			Literal::None => Value::None,
			Literal::Null => Value::Null,
			Literal::UnboundedRange => Value::Range(Box::new(Range::unbounded())),
			Literal::Bool(x) => Value::Bool(*x),
			Literal::Float(x) => Value::Number(Number::Float(*x)),
			Literal::Integer(i) => Value::Number(Number::Int(*i)),
			Literal::Decimal(d) => Value::Number(Number::Decimal(*d)),
			Literal::Strand(strand) => Value::Strand(strand.clone()),
			Literal::Bytes(bytes) => Value::Bytes(bytes.clone()),
			Literal::Regex(regex) => Value::Regex(regex.clone()),
			Literal::RecordId(record_id_lit) => {
				Value::Thing(record_id_lit.compute(stk, ctx, opt, doc).await?)
			}
			Literal::Array(exprs) => {
				let mut array = Vec::with_capacity(exprs.len());
				for e in exprs.iter() {
					array.push(e.compute(stk, ctx, opt, doc).await?);
				}
				Value::Array(Array(array))
			}
			// TODO: Geometry matching.
			Literal::Object(items) => {
				let mut map = BTreeMap::new();
				for i in items.iter() {
					let v = i.value.compute(stk, ctx, opt, doc).await?;
					map.insert(i.key.clone(), v);
				}
				Value::Object(Object(map))
			}
			Literal::Duration(duration) => Value::Duration(*duration),
			Literal::Datetime(datetime) => Value::Datetime(datetime.clone()),
			Literal::Uuid(uuid) => Value::Uuid(*uuid),
			Literal::Geometry(geometry) => Value::Geometry(geometry.clone()),
			Literal::File(file) => Value::File(file.clone()),
			Literal::Closure(closure) => Value::Closure(closure.clone()),
		};
		Ok(res)
	}
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
			Literal::UnboundedRange => {}
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

impl fmt::Display for Literal {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			Literal::None => "NONE".fmt(f),
			Literal::Null => "NULL".fmt(f),
			Literal::UnboundedRange => "..".fmt(f),
			Literal::Bool(x) => {
				if *x {
					"true".fmt(f)
				} else {
					"false".fmt(f)
				}
			}
			Literal::Float(float) => {
				if float.is_finite() {
					write!(f, "{float}f")
				} else {
					write!(f, "{float}")
				}
			}
			Literal::Integer(x) => x.fmt(f),
			Literal::Decimal(d) => write!(f, "{d}dec"),
			Literal::Strand(strand) => strand.fmt(f),
			Literal::Bytes(bytes) => bytes.fmt(f),
			Literal::Regex(regex) => regex.fmt(f),
			Literal::RecordId(record_id_lit) => record_id_lit.fmt(f),
			Literal::Array(exprs) => write!(f, "[{}]", Fmt::comma_separated(exprs.iter())),
			Literal::Object(items) => write!(f, "{{{}}}", Fmt::comma_separated(items.iter())),
			Literal::Duration(duration) => duration.fmt(f),
			Literal::Datetime(datetime) => datetime.fmt(f),
			Literal::Uuid(uuid) => uuid.fmt(f),
			Literal::Geometry(geometry) => geometry.fmt(f),
			Literal::File(file) => file.fmt(f),
			Literal::Closure(closure) => closure.fmt(f),
		}
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct ObjectEntry {
	pub key: String,
	pub value: Expr,
}

impl fmt::Display for ObjectEntry {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "{}: {}", self.key, self.value)
	}
}
