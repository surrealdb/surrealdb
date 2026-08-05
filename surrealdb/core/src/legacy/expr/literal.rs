use std::collections::BTreeMap;

use reblessive::tree::Stk;

use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::expr::FlowResult;
use crate::expr::literal::Literal;
use crate::val::{Array, Number, Object, Range, Value};

/// Computes one element of a collection literal.
///
/// An expression that is itself a literal needing no evaluation is converted
/// directly. Everything else is evaluated on the async stack as usual.
///
/// The direct conversion is not a micro-optimisation: `stk.run` pushes a task
/// onto the reblessive stack, and that push (writing the future's state into
/// the stack allocation and moving the result back out) costs far more than
/// building the value for entries that are plain scalars. Collection literals
/// are the shape a `.surql` import consists of almost entirely, so the number
/// of pushes per record is what governs import throughput.
async fn compute_element(
	e: &crate::expr::Expr,
	stk: &mut Stk,
	ctx: &FrozenContext,
	opt: &Options,
	doc: Option<&CursorDoc>,
) -> FlowResult<Value> {
	if let Some(v) = e.as_static_value() {
		return Ok(v);
	}
	stk.run(|stk| crate::legacy::expr_compute(e, stk, ctx, opt, doc)).await
}

/// Process this type returning a computed simple Value
#[instrument(level = "trace", name = "Literal::compute", skip_all)]
pub(crate) async fn literal_compute(
	this: &Literal,
	stk: &mut Stk,
	ctx: &FrozenContext,
	opt: &Options,
	doc: Option<&CursorDoc>,
) -> FlowResult<Value> {
	let res = match this {
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
		Literal::RecordId(record_id_lit) => Value::RecordId(
			crate::legacy::record_id_lit_compute(record_id_lit, stk, ctx, opt, doc).await?,
		),
		Literal::Array(exprs) => {
			let mut array = Vec::with_capacity(exprs.len());
			for e in exprs.iter() {
				array.push(compute_element(e, stk, ctx, opt, doc).await?);
			}
			Value::Array(Array(array))
		}
		Literal::Set(exprs) => {
			let mut set = crate::val::Set::new();
			for e in exprs.iter() {
				let v = stk.run(|stk| crate::legacy::expr_compute(e, stk, ctx, opt, doc)).await?;
				set.insert(v);
			}
			Value::Set(set)
		}
		Literal::Object(items) => {
			let mut map = BTreeMap::new();
			for i in items.iter() {
				let v = compute_element(&i.value, stk, ctx, opt, doc).await?;
				map.insert(i.key.clone(), v);
			}
			Value::Object(Object::from(map))
		}
		Literal::Duration(duration) => Value::Duration(*duration),
		Literal::Datetime(datetime) => Value::Datetime(*datetime),
		Literal::Uuid(uuid) => Value::Uuid(*uuid),
		Literal::Geometry(geometry) => Value::Geometry(geometry.clone()),
		Literal::File(file) => Value::File(file.clone()),
	};
	Ok(res)
}
