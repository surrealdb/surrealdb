use crate::{
	cnf::IDIOM_RECURSION_LIMIT,
	ctx::Context,
	dbs::Options,
	doc::CursorDoc,
	err::Error,
	sql::{part::RecursionPlan, Array, Part},
};

use super::Value;
use reblessive::tree::Stk;

#[derive(Clone, Copy, Debug)]
pub struct Recursion<'a> {
	pub min: &'a u32,
	pub max: Option<&'a u32>,
	pub iterated: &'a u32,
	pub current: &'a Value,
	pub path: &'a [Part],
	pub plan: Option<&'a RecursionPlan>,
}

impl<'a> Recursion<'a> {
	pub fn with_iterated(self, iterated: &'a u32) -> Self {
		Self {
			iterated,
			..self
		}
	}

	pub fn with_current(self, current: &'a Value) -> Self {
		Self {
			current,
			..self
		}
	}
}

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

pub(crate) async fn compute_idiom_recursion<'a>(
	stk: &mut Stk,
	ctx: &Context,
	opt: &Options,
	doc: Option<&CursorDoc>,
	rec: Recursion<'a>,
) -> Result<Value, Error> {
	// Find the recursion limit
	let limit = *IDIOM_RECURSION_LIMIT as u32;

	// We recursed and found a final value, let's return
	// it for the previous iteration to pick up on this
	if rec.plan.is_some() && is_final(rec.current) {
		return Ok(get_final(rec.current));
	}

	// Counter for the local loop and current value
	let mut i = rec.iterated.to_owned();
	let mut current = rec.current.to_owned();

	if rec.plan.is_some() {
		// If we have reached the maximum amount of iterations,
		// we can return the current value and break the loop.
		if let Some(max) = rec.max {
			if &i >= max {
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
		i += 1;

		// Process the path, not accounting for any recursive plans
		let v = stk.run(|stk| current.get(stk, ctx, opt, doc, rec.path)).await?;
		let v = match rec.plan {
			// We found a recursion plan, let's apply it
			Some(p) => {
				p.compute(stk, ctx, opt, doc, rec.with_iterated(&i).with_current(&v)).await?
			}
			_ => v,
		};

		// Clean up any dead ends when we encounter an array
		let v = clean_iteration(v);

		// Process the value for this iteration
		match v {
			// We reached a final value
			v if is_final(&v) || v == current => {
				return Ok(match &i <= rec.min {
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
				current = v;
			}
		};

		// If we have reached the maximum amount of iterations,
		// we can return the current value and break the loop.
		if let Some(max) = rec.max {
			if &i >= max {
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
		if rec.plan.is_some() {
			return Ok(current);
		}
	}
}
