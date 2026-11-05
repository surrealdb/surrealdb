//! The function a query trace's marking pass writes around a record link the
//! statement's result reads through ([`crate::expr::logical::trace`]).
//!
//! `link(value, holder, field)` answers `value`, the value of `field` of
//! `holder`, unchanged. Where the statement's request asked for `links`, it
//! records `[holder, field, target]` for `value` and for each record id among
//! its elements. A `NONE` holder is one the marking could not name without
//! evaluating it twice: nothing is recorded, and the trace is partial. The function's name holds a
//! character no query can write, so only the marking pass calls it, and it is exempt from the
//! capabilities that allow or deny the functions a query names.

use anyhow::Result;
use surrealdb_rpc::TraceKinds;

use crate::exec::EvalContext;
use crate::exec::function::{BuiltinScalarFn, ScalarExec, ScalarFunction, Signature};
use crate::exec::ir::iface::Fallibility;
use crate::expr::Kind;
use crate::expr::logical::EffectSet;
use crate::val::Value;

fn link_impl(ctx: &EvalContext<'_>, args: Vec<Value>) -> Result<Value> {
	let mut args = args.into_iter();
	let value = args.next().unwrap_or(Value::None);
	let (Some(holder), Some(Value::String(field))) = (args.next(), args.next()) else {
		return Ok(value);
	};
	let Some(trace) = ctx.exec_ctx.ctx().query_trace() else {
		return Ok(value);
	};
	// `NONE`: the marking could not name the holder without evaluating its
	// expression twice, so the link the result reads through goes unrecorded.
	if matches!(holder, Value::None) {
		if matches!(value, Value::RecordId(_) | Value::Array(_) | Value::Set(_)) {
			trace.mark_partial(TraceKinds::LINKS);
		}
		return Ok(value);
	}
	// The holder is a record id, or a record read whole, which names itself.
	let holder = if let Value::RecordId(id) = holder {
		id
	} else if let Value::Object(object) = &holder
		&& let Some(Value::RecordId(id)) = object.get("id")
	{
		id.clone()
	} else {
		return Ok(value);
	};
	if let Value::RecordId(target) = &value {
		trace.link(holder, field.to_string(), target.clone());
	} else if let Value::Array(items) = &value {
		for item in items.iter() {
			if let Value::RecordId(target) = item {
				trace.link(holder.clone(), field.to_string(), target.clone());
			}
		}
	} else if let Value::Set(items) = &value {
		for item in items.iter() {
			if let Value::RecordId(target) = item {
				trace.link(holder.clone(), field.to_string(), target.clone());
			}
		}
	}
	Ok(value)
}

/// `link(value, holder, field)`. See the [module](self).
#[allow(non_upper_case_globals)]
pub const TraceLink: BuiltinScalarFn = BuiltinScalarFn {
	name: crate::expr::logical::trace::LINK,
	// Reading the row keeps it from being evaluated before the row is known.
	effects: EffectSet::READS_ROW,
	fallibility: Fallibility::INFALLIBLE,
	closure_args: &[],
	signature: || {
		Signature::new()
			.arg("value", Kind::Any)
			.arg("holder", Kind::Any)
			.arg("field", Kind::String)
			.returns(Kind::Any)
	},
	exec: ScalarExec::Context(link_impl),
};

fn anchor_impl(ctx: &EvalContext<'_>, args: Vec<Value>) -> Result<Value> {
	let mut args = args.into_iter();
	let value = args.next().unwrap_or(Value::None);
	let partial = matches!(args.next(), Some(Value::Bool(true)));
	if let Some(trace) = ctx.exec_ctx.ctx().query_trace() {
		if partial {
			trace.mark_partial(TraceKinds::LINKS);
		} else {
			trace.report(Vec::new(), &value);
		}
	}
	Ok(value)
}

/// `anchor(value)`: the value of a step that cuts what the traced lookups
/// before it answered, such as an index or a filter. It answers `value`
/// unchanged, and reports what those lookups walked, cut to what leads to it.
/// `anchor(value, true)` is a step that cuts link reads it cannot tell apart:
/// it reports nothing, and marks the trace partial.
#[allow(non_upper_case_globals)]
pub const TraceAnchor: BuiltinScalarFn = BuiltinScalarFn {
	name: crate::expr::logical::trace::ANCHOR,
	// Reading the row keeps it from being evaluated before the row is known.
	effects: EffectSet::READS_ROW,
	fallibility: Fallibility::INFALLIBLE,
	closure_args: &[],
	signature: || {
		Signature::new().arg("value", Kind::Any).optional("partial", Kind::Bool).returns(Kind::Any)
	},
	exec: ScalarExec::Context(anchor_impl),
};

/// The internal functions of a query trace.
pub(super) const SCALARS: &[&dyn ScalarFunction] = &[&TraceAnchor, &TraceLink];
