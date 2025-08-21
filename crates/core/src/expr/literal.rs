use std::collections::BTreeMap;
use std::fmt::{self, Write as _};
use std::hash::{Hash, Hasher};

use reblessive::tree::Stk;
use rust_decimal::Decimal;

use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::expr::escape::EscapeKey;
use crate::expr::fmt::{Fmt, Pretty, is_pretty, pretty_indent};
use crate::expr::{Expr, FlowResult, RecordIdLit};
use crate::val::{
	Array, Bytes, Closure, Datetime, Duration, File, Geometry, Number, Object, Range, Regex,
	Strand, Uuid, Value,
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
				Value::RecordId(record_id_lit.compute(stk, ctx, opt, doc).await?)
			}
			Literal::Array(exprs) => {
				let mut array = Vec::with_capacity(exprs.len());
				for e in exprs.iter() {
					array.push(stk.run(|stk| e.compute(stk, ctx, opt, doc)).await?);
				}
				Value::Array(Array(array))
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
		let mut f = Pretty::from(f);
		match self {
			Literal::None => write!(f, "NONE"),
			Literal::Null => write!(f, "NULL"),
			Literal::UnboundedRange => write!(f, ".."),
			Literal::Bool(x) => {
				if *x {
					write!(f, "true")
				} else {
					write!(f, "false")
				}
			}
			Literal::Float(float) => {
				if float.is_finite() {
					write!(f, "{float}f")
				} else {
					write!(f, "{float}")
				}
			}
			Literal::Integer(x) => write!(f, "{x}"),
			Literal::Decimal(d) => write!(f, "{d}dec"),
			Literal::Strand(strand) => write!(f, "{strand}"),
			Literal::Bytes(bytes) => write!(f, "{bytes}"),
			Literal::Regex(regex) => write!(f, "{regex}"),
			Literal::RecordId(record_id_lit) => write!(f, "{record_id_lit}"),
			Literal::Array(exprs) => {
				f.write_char('[')?;
				if !exprs.is_empty() {
					let indent = pretty_indent();
					write!(f, "{}", Fmt::pretty_comma_separated(exprs.as_slice()))?;
					drop(indent);
				}
				f.write_char(']')
			}
			Literal::Object(items) => {
				if is_pretty() {
					f.write_char('{')?;
				} else {
					f.write_str("{ ")?;
				}
				if !items.is_empty() {
					let indent = pretty_indent();
					write!(
						f,
						"{}",
						Fmt::pretty_comma_separated(items.iter().map(|args| Fmt::new(
							args,
							|entry, f| write!(f, "{}: {}", EscapeKey(&entry.key), entry.value)
						)),)
					)?;
					drop(indent);
				}
				if is_pretty() {
					f.write_char('}')
				} else {
					f.write_str(" }")
				}
			}
			Literal::Duration(duration) => write!(f, "{duration}"),
			Literal::Datetime(datetime) => write!(f, "{datetime}"),
			Literal::Uuid(uuid) => write!(f, "{uuid}"),
			Literal::Geometry(geometry) => write!(f, "{geometry}"),
			Literal::File(file) => write!(f, "{file}"),
			Literal::Closure(closure) => write!(f, "{closure}"),
		}
	}
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct ObjectEntry {
	pub key: String,
	pub value: Expr,
}

impl fmt::Display for ObjectEntry {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "{}: {}", self.key, self.value)
	}
}
