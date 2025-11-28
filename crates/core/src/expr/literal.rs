use std::collections::BTreeMap;
use std::hash::{Hash, Hasher};

use reblessive::tree::Stk;
use rust_decimal::Decimal;
use surrealdb_types::{SqlFormat, ToSql};

use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::expr::{Expr, FlowResult, RecordIdLit};
use crate::val::{
	Array, Bytes, Datetime, Duration, File, Geometry, Number, Object, Range, Regex, Uuid, Value,
};

/// A literal value, should be computed to get an actual value.
///
/// # Note regarding equality.
/// A literal is equal to an other literal if it is the exact same byte
/// representation, so normal float rules regarding equality do not apply, i.e.
/// if `a != b` then `Literal::Float(a)` could still be equal
/// to `Literal::Float(b)` in the case of `NaN` floats for example. Also
/// surrealql rules regarding number equality are not observed, 1f != 1dec.

#[derive(Clone, Debug)]
pub(crate) enum Literal {
	None,
	Null,
	// An unbounded range, i.e. `..` without any start or end bound.
	UnboundedRange,
	Bool(bool),
	Float(f64),
	Integer(i64),
	//TODO: Possibly remove wrapper.
	Decimal(Decimal),
	String(String),
	Bytes(Bytes),
	//TODO: Possibly remove wrapper.
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
	pub(crate) fn is_static(&self) -> bool {
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
			Literal::String(strand) => Value::String(strand.clone()),
			Literal::Bytes(bytes) => Value::Bytes(bytes.clone()),
			Literal::Regex(regex) => Value::Regex(regex.clone()),
			Literal::RecordId(record_id_lit) => {
				Value::RecordId(record_id_lit.compute(stk, ctx, opt, doc).await?)
			}
			Literal::Array(exprs) => {
				let mut array = Vec::with_capacity(exprs.len());
				for e in exprs.iter() {
					array.push(stk.run(|stk| e.compute(stk, ctx, opt, doc)).await?);
				}
				Value::Array(Array(array))
			}
			Literal::Set(exprs) => {
				let mut set = crate::val::Set::new();
				for e in exprs.iter() {
					let v = stk.run(|stk| e.compute(stk, ctx, opt, doc)).await?;
					set.insert(v);
				}
				Value::Set(set)
			}
			Literal::Object(items) => {
				let mut map = BTreeMap::new();
				for i in items.iter() {
					let v = stk.run(|stk| i.value.compute(stk, ctx, opt, doc)).await?;
					map.insert(i.key.clone(), v);
				}
				Value::Object(Object(map))
			}
			Literal::Duration(duration) => Value::Duration(*duration),
			Literal::Datetime(datetime) => Value::Datetime(datetime.clone()),
			Literal::Uuid(uuid) => Value::Uuid(*uuid),
			Literal::Geometry(geometry) => Value::Geometry(geometry.clone()),
			Literal::File(file) => Value::File(file.clone()),
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
pub(crate) struct ObjectEntry {
	pub key: String,
	pub value: Expr,
}

impl ToSql for ObjectEntry {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		let entry: crate::sql::literal::ObjectEntry = self.clone().into();
		entry.fmt_sql(f, fmt);
	}
}
