use anyhow::{Result, bail};
use reblessive::tree::Stk;

use crate::cnf::IDIOM_RECURSION_LIMIT;
use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::expr::part::{RecurseInstruction, RecursionPlan};
use crate::expr::{FlowResultExt as _, Part};
use crate::val::{Array, Value};

#[derive(Clone, Copy, Debug)]
pub struct Recursion<'a> {
	pub min: u32,
	pub max: Option<u32>,
	pub iterated: u32,
	pub current: &'a Value,
	pub path: &'a [Part],
	pub plan: Option<&'a RecursionPlan>,
	pub instruction: Option<&'a RecurseInstruction>,
}

impl<'a> Recursion<'a> {
	pub fn with_iterated(self, iterated: u32) -> Self {
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

pub(crate) async fn compute_idiom_recursion(
	stk: &mut Stk,
	ctx: &Context,
	opt: &Options,
	doc: Option<&CursorDoc>,
	rec: Recursion<'_>,
) -> Result<Value> {
	// Find the recursion limit
	let limit = *IDIOM_RECURSION_LIMIT as u32;
	// Do we recursion instead of looping?
	let marked_recursive = rec.plan.is_some();

	// We recursed and found a final value, let's return
	// it for the previous iteration to pick up on this
	if marked_recursive && is_final(rec.current) {
		return Ok(get_final(rec.current));
	}

	// Counter for the local loop and current value
	let mut i = rec.iterated.to_owned();
	let mut current = rec.current.to_owned();
	let mut finished = vec![];

	// Recurse instructions always collect their input
	// into the finished collection. In this case, we
	// ignore the current value and return the finished instead.
	macro_rules! output {
		() => {
			if rec.instruction.is_some() {
				Value::from(finished)
			} else {
				current
			}
		};
	}

	if marked_recursive {
		// If we have reached the maximum amount of iterations,
		// we can return the current value and break the loop.
		if let Some(max) = rec.max {
			if i >= max {
				return Ok(current);
			}
		} else if i >= limit {
			bail!(Error::IdiomRecursionLimitExceeded {
				limit,
			});
		}
	}

	loop {
		// Bump iteration
		i += 1;

		// Process the path, not accounting for any recursive plans
		let v = match rec.instruction {
			Some(instruction) => {
				instruction
					.compute(
						stk,
						ctx,
						opt,
						doc,
						rec.with_iterated(i).with_current(&current),
						&mut finished,
					)
					.await?
			}
			_ => stk.run(|stk| current.get(stk, ctx, opt, doc, rec.path)).await.catch_return()?,
		};

		// Check for any recursion plans
		let v = match rec.plan {
			// We found a recursion plan, let's apply it
			Some(p) => p.compute(stk, ctx, opt, doc, rec.with_iterated(i).with_current(&v)).await?,
			_ => v,
		};

		// Clean up any dead ends when we encounter an array
		let v = if rec.instruction.is_none() {
			clean_iteration(v)
		} else {
			v
		};

		// Process the value for this iteration
		match v {
			// We reached a final value
			v if is_final(&v) || v == current => {
				let res: Value = match rec.instruction {
					// If we have a recurse instruction, and we have not yet
					// reached the minimum amount of required iterations, we
					// return an empty array.
					Some(_) if i < rec.min => Array::new().into(),
					// If we did reach minimum depth, the finished collection
					// could have collected values. Let's return them.
					Some(_) => Value::from(finished),

					// If we have not yet reached the minimum amount of
					// required iterations it's a dead end, and we return NONE
					None if i <= rec.min => get_final(&v),
					// If the value is final, and we reached the minimum
					// amount of required iterations, we can return the value
					None => output!(),
				};

				return Ok(res);
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
			if i >= max {
				return Ok(output!());
			}
		} else if i >= limit {
			bail!(Error::IdiomRecursionLimitExceeded {
				limit,
			});
		}

		// If we recursed, we should not continue the loop,
		// as the loop will continue on the whole value, and
		// not on the potentially nested value which triggered
		// the recurse, resulting in a potential infinite loop
		if marked_recursive {
			return Ok(current);
		}
	}
}
