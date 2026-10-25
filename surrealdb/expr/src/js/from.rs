#![deny(clippy::wildcard_enum_match_arm)]

use chrono::{TimeZone, Utc};
use js::prelude::This;
use js::{Coerced, Ctx, Error, Exception, FromAtom, FromJs};
use rust_decimal::Decimal;
use surrealdb_cnf::GENERATION_ALLOCATION_LIMIT;

use crate::js::classes;
use crate::val::{Array, Bytes, Datetime, Geometry, Object, Value};

/// Maximum number of value nodes a single [`Value::from_js`] call may materialise.
///
/// Capped at `i32::MAX` because QuickJS array indexing asserts that indices fit in `i32`.
fn max_js_value_nodes() -> usize {
	(*GENERATION_ALLOCATION_LIMIT).min(i32::MAX as usize)
}

fn value_too_large_error() -> Error {
	Error::new_from_js_message(
		"js value",
		"value",
		format!("JavaScript value exceeds maximum size of {} elements", max_js_value_nodes()),
	)
}

fn array_from_js<'js>(
	ctx: &Ctx<'js>,
	arr: &js::Array<'js>,
	depth: usize,
	remaining: &mut usize,
) -> Result<Value, Error> {
	// QuickJS tags array lengths above `i32::MAX` as `Float64`, which causes
	// `rquickjs::Array::len` to panic on its `JS_IsInt` assertion. Read the
	// `length` property directly as a `js::Value` and reject non-`i32` or
	// negative lengths before allocating or indexing.
	let len_val: js::Value<'js> = arr.as_object().get(js::atom::PredefinedAtom::Length)?;
	let Some(len) = len_val.as_int().and_then(|n| usize::try_from(n).ok()) else {
		return Err(value_too_large_error());
	};
	if len > *remaining {
		return Err(value_too_large_error());
	}
	let mut x = Array::with_capacity(len);
	for i in 0..len {
		let v: js::Value<'js> = arr.get(i)?;
		x.push(value_from_js(ctx, v, depth + 1, remaining)?);
	}
	Ok(x.into())
}

impl<'js> FromJs<'js> for Value {
	fn from_js(ctx: &Ctx<'js>, val: js::Value<'js>) -> Result<Self, Error> {
		let mut remaining = max_js_value_nodes();
		value_from_js(ctx, val, 1, &mut remaining)
	}
}

/// [`Value::from_js`] carrying the nesting depth of `val` and remaining node budget.
///
/// SECURITY: a script can return an arbitrarily deeply nested value or a shallow
/// directed acyclic graph with shared references whose tree expansion is
/// exponential in depth, and this conversion is natively recursive, so bound
/// both nesting depth and total materialised nodes. Building a deep or wide DAG
/// needs no JS-side recursion (`a = [a, a]` in a flat loop), so the script stack
/// limit never engages; at stock config an unbounded return value either overflows
/// the host stack (`panic = "abort"`) or exhausts memory. A `FromJs` error carries
/// its message in Rust rather than via a pending JS exception, so it survives the
/// async promise resolution the return value is converted through.
fn value_from_js<'js>(
	ctx: &Ctx<'js>,
	val: js::Value<'js>,
	depth: usize,
	remaining: &mut usize,
) -> Result<Value, Error> {
	if depth > crate::val::MAX_VALUE_DEPTH {
		return Err(Error::new_from_js_message(
			"js value",
			"value",
			crate::expr::Error::ValueTooDeep {
				max: crate::val::MAX_VALUE_DEPTH,
			}
			.to_string(),
		));
	}
	let Some(next_remaining) = remaining.checked_sub(1) else {
		return Err(value_too_large_error());
	};
	*remaining = next_remaining;
	match val.type_of() {
		js::Type::Undefined => Ok(Value::None),
		js::Type::Null => Ok(Value::Null),
		js::Type::Bool => Ok(Value::from(val.as_bool().expect("type checked as bool"))),
		js::Type::Int => Ok(Value::from(val.as_int().expect("type checked as int") as f64)),
		js::Type::Float => Ok(Value::from(val.as_float().expect("type checked as float"))),
		js::Type::String => {
			Ok(Value::from(val.into_string().expect("type checked as string").to_string()?))
		}
		js::Type::Array => {
			let v = val.as_array().expect("type checked as array");
			array_from_js(ctx, v, depth, remaining)
		}
		js::Type::BigInt => {
			// TODO: Optimize this if rquickjs ever supports better conversion methods.
			let str = Coerced::<String>::from_js(ctx, val)?;
			if let Ok(i) = str.parse::<i64>() {
				return Ok(Value::from(i));
			}
			match str.parse::<Decimal>() {
				Ok(x) => Ok(Value::from(x)),
				Err(e) => Err(Exception::from_message(ctx.clone(), &e.to_string())?.throw()),
			}
		}
		js::Type::Object | js::Type::Exception => {
			// Extract the value as an object
			let v = val.into_object().expect("type checked as object");
			// Check to see if this object is an error
			if v.is_error() {
				let e: String = v.get(js::atom::PredefinedAtom::Message)?;
				let (Ok(e) | Err(e)) = Exception::from_message(ctx.clone(), &e).map(|x| x.throw());
				return Err(e);
			}
			// Check to see if this object is a record
			if let Some(v) = v.as_class::<classes::record::Record>() {
				let borrow = v.borrow();
				let v: &classes::record::Record = &borrow;
				return Ok(v.value.clone().into());
			}
			// Check to see if this object is a duration
			if let Some(v) = v.as_class::<classes::duration::Duration>() {
				let borrow = v.borrow();
				let v: &classes::duration::Duration = &borrow;
				return match &v.value {
					Some(v) => Ok((*v).into()),
					None => Ok(Value::None),
				};
			}
			// Check to see if this object is a uuid
			if let Some(v) = v.as_class::<classes::uuid::Uuid>() {
				let borrow = v.borrow();
				let v: &classes::uuid::Uuid = &borrow;
				return match &v.value {
					Some(v) => Ok((*v).into()),
					None => Ok(Value::None),
				};
			}
			// Check to see if this object is a file
			if let Some(v) = v.as_class::<classes::file::File>() {
				let borrow = v.borrow();
				let v: &classes::file::File = &borrow;
				return Ok(v.value.clone().into());
			}

			// Check to see if this object is a range
			if let Some(v) = v.as_class::<classes::range::Range>() {
				let borrow = v.borrow();
				let v: &classes::range::Range = &borrow;
				// A range built from ranges nests without any array or object
				// between the levels, so its depth is judged here, from the levels the conversion
				// has already entered: the value the
				// walk would clone and drop recursively is refused first.
				let range = Value::Range(Box::new(v.value.clone()));
				crate::val::check_value_depth_from(&range, depth)
					.map_err(|e| Error::new_from_js_message("Range", "value", e.to_string()))?;
				return Ok(range);
			}

			if let Some(v) = v.as_typed_array::<u8>() {
				// SAFETY: the slice aliases memory owned by the JS engine and is copied
				// out below before any JavaScript can run, so it cannot be detached or
				// reallocated while it is borrowed.
				let Some(data) = (unsafe { v.as_bytes() }) else {
					return Err(Error::new_from_js_message(
						"Uint8Array",
						"Bytes",
						"Uint8Array data was consumed.",
					));
				};

				return Ok(Value::Bytes(Bytes::from(data.to_vec())));
			}

			// Check to see if this object is a date
			let date: js::Object = ctx.globals().get(js::atom::PredefinedAtom::Date)?;
			if (v).is_instance_of(&date) {
				let f: js::Function = v.get("getTime")?;
				let m: i64 = f.call((This(v),))?;
				let d = Utc
					.timestamp_millis_opt(m)
					.single()
					.ok_or_else(|| Error::new_from_js("Date", "invalid timestamp"))?;
				return Ok(Datetime::from(d).into());
			}

			// Check to see if this object is an array
			if let Some(v) = v.as_array() {
				return array_from_js(ctx, v, depth, remaining);
			}

			// Check to see if this object is a function
			if v.as_function().is_some() {
				return Ok(Value::None);
			}
			// This object is a normal object
			let mut x = Object::default();
			for i in v.props() {
				let (k, v) = i?;
				let k = String::from_atom(k)?;
				let v = value_from_js(ctx, v, depth + 1, remaining)?;
				x.insert(k, v);
			}

			if let Some(x) = Geometry::try_from_object(&x) {
				return Ok(x.into());
			}

			Ok(x.into())
		}
		js::Type::Uninitialized
		| js::Type::Symbol
		| js::Type::Constructor
		| js::Type::Function
		| js::Type::Promise
		| js::Type::Proxy
		| js::Type::Module
		| js::Type::Unknown => Ok(Value::Null),
	}
}

#[cfg(test)]
mod tests {
	use js::{Context, FromJs, Runtime};

	use crate::val::Value;

	#[test]
	fn sparse_array_with_length_above_i32_max_returns_error() {
		let rt = Runtime::new().unwrap();
		let ctx = Context::full(&rt).unwrap();
		ctx.with(|ctx| {
			let js_val: js::Value =
				ctx.eval("(() => { let a = []; a.length = 2147483648; return a; })()").unwrap();
			let res = Value::from_js(&ctx, js_val);
			assert!(res.is_err());
		});
	}

	#[test]
	fn shared_reference_dag_with_exponential_expansion_returns_error() {
		let rt = Runtime::new().unwrap();
		let ctx = Context::full(&rt).unwrap();
		ctx.with(|ctx| {
			let js_val: js::Value = ctx
				.eval(
					"(() => { let a = [1, 2]; for (let i = 0; i < 30; i++) a = [a, a]; return a; })()",
				)
				.unwrap();
			let res = Value::from_js(&ctx, js_val);
			assert!(res.is_err());
		});
	}
}
