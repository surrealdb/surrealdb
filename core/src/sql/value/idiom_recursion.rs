use crate::{
	cnf::IDIOM_RECURSION_LIMIT,
	ctx::Context,
	dbs::Options,
	doc::CursorDoc,
	err::Error,
	sql::{
		part::{Recurse, RecursionPlan},
		Array, Part,
	},
};

use super::Value;
use reblessive::tree::Stk;

// Method used to check if the value
// inside a recursed idiom path is final
pub(crate) fn is_final(v: &Value) -> bool {
	match v {
		Value::None => true,
		Value::Null => true,
		Value::Array(v) => v.is_empty() || v.is_all_none_or_null(),
		_ => false,
	}
}

pub(crate) fn get_final(v: &Value) -> Value {
	match v {
		Value::Array(_) => Value::Array(Array(vec![])),
		Value::Null => Value::Null,
		_ => Value::None,
	}
}

pub(crate) fn clean_iteration(v: Value) -> Value {
	if let Value::Array(v) = v {
		Value::from(v.0.into_iter().filter(|v| !is_final(v)).collect::<Vec<Value>>()).flatten()
	} else {
		v
	}
}

pub(crate) async fn compute_idiom_recursion(
	stk: &mut Stk,
	ctx: &Context,
	opt: &Options,
	doc: Option<&CursorDoc>,
	recurse: &Recurse,
	i: &u32,
	value: &Value,
	next: &[Part],
	plan: &Option<RecursionPlan>,
) -> Result<Value, Error> {
	// Find minimum and maximum amount of iterations
	let min = recurse.min()?;
	let max = recurse.max()?;
	let limit = *IDIOM_RECURSION_LIMIT as u32;

	// We recursed and found a final value, let's return
	// it for the previous iteration to pick up on this
	if plan.is_some() && is_final(value) {
		return Ok(get_final(value));
	}

	// Counter for the local loop and current value
	let mut i = i.to_owned();
	let mut current = value.clone();

	if plan.is_some() {
		// If we have reached the maximum amount of iterations,
		// we can return the current value and break the loop.
		if let Some(max) = max {
			if i >= max {
				return Ok(current);
			}
		} else if i >= limit {
			return Err(Error::IdiomRecursionLimitExceeded {
				limit,
			});
		}
	}

	println!("plan {:?}", plan);

	loop {
		// Bump iteration
		i += 1;

		let v = stk.run(|stk| current.get(stk, &ctx, opt, doc, next)).await?;

		// println!("i {i}");
		// println!("b {value}");
		// println!("a {v}");
		let v = match plan {
			Some(ref p) => p.compute(stk, ctx, opt, doc, recurse, &i, &v, next, plan).await?,
			_ => v,
		};

		let v = clean_iteration(v);

		// Process the value for this iteration
		match v {
			// We reached a final value
			v if is_final(&v) || v == current => {
				return Ok(match i <= min {
					// If we have not yet reached the minimum amount of
					// required iterations it's a dead end, and we return NONE
					true => get_final(&v),

					// If the value is final, and we reached the minimum
					// amount of required iterations, we can return the value
					false => current,
				});
			}
			v => {
				// Otherwise we can update the value and
				// continue to the next iteration.
				current = v.to_owned();
			}
		};

		// If we have reached the maximum amount of iterations,
		// we can return the current value and break the loop.
		if let Some(max) = max {
			if i >= max {
				return Ok(current);
			}
		} else if i >= limit {
			return Err(Error::IdiomRecursionLimitExceeded {
				limit,
			});
		}

		// If we recursed, we should not continue the loop,
		// as the loop will continue on the whole value, and
		// not on the potentially nested value which triggered
		// the recurse, resulting in a potential infinite loop
		if plan.is_some() {
			return Ok(current);
		}
	}
}
