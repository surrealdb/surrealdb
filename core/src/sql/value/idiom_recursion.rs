use crate::{
	cnf::IDIOM_RECURSION_LIMIT,
	ctx::{Context, MutableContext},
	dbs::Options,
	doc::CursorDoc,
	err::Error,
	sql::Array,
};

use super::Value;
use reblessive::tree::Stk;

// Method used to check if the value
// inside a recursed idiom path is final
fn is_final(v: &Value) -> bool {
	match v {
		Value::None => true,
		Value::Null => true,
		Value::Array(v) => v.is_empty() || v.is_all_none_or_null(),
		_ => false,
	}
}

fn get_final(v: &Value) -> Value {
	match v {
		Value::Array(_) => Value::Array(Array(vec![])),
		Value::Null => Value::Null,
		_ => Value::None,
	}
}

pub(crate) async fn compute_idiom_recursion(
	stk: &mut Stk,
	ctx: &Context,
	opt: &Options,
	doc: Option<&CursorDoc>,
	value: &Value,
	recursed: bool,
) -> Result<Value, Error> {
	// Get recursion context
	let (i, recurse, next) = match ctx.idiom_recursion() {
		Some((i, recurse, next)) => (i, recurse, next),
		_ => return Err(Error::Unreachable("Not recursing".into())),
	};

	// We recursed and found a final value, let's return
	// it for the previous iteration to pick up on this
	if recursed && is_final(value) {
		return Ok(get_final(value));
	}

	// Find minimum and maximum amount of iterations
	let min = recurse.min()?;
	let max = recurse.max()?;
	let limit = *IDIOM_RECURSION_LIMIT as i64;

	// Counter for the local loop and current value
	let mut i = i.to_owned();
	let mut current = value.clone();

	if recursed {
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

	loop {
		// Bump iteration
		let mut ctx = MutableContext::new(ctx);
		ctx.bump_idiom_recursion();
		let ctx = ctx.freeze();
		i += 1;

		// Obtain the processed value for this iteration
		let v = stk.run(|stk| current.get(stk, &ctx, opt, doc, next.as_slice())).await?.flatten();

		// When using the Continue Recurse (@) symbol nested
		// in the idiom path "next", we have iterated further
		// than we are aware of here, in which case we can
		// break the loop
		let nested_iteration = match ctx.idiom_recursion_iterated() {
			Some(i) => i,
			None => i,
		};

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
			// We iterated in a nested recursion invocation
			v if i < nested_iteration => {
				return Ok(match nested_iteration < min {
					// If we have not yet reached the minimum amount of
					// required iterations it's a dead end, and we return NONE
					true => get_final(&v),

					// If the value is final, and we reached the minimum
					// amount of required iterations, we can return the value
					false => v,
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
		if recursed {
			return Ok(current);
		}
	}
}
