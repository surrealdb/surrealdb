use anyhow::{Result, bail};
use reblessive::tree::Stk;

use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::exe::FlowResultExt as _;
use crate::exec::Error as ExecError;
use crate::expr::idiom::recursion::{Recursion, clean_iteration, get_final, is_final};
use crate::val::{Array, Value};

pub(crate) async fn compute_idiom_recursion(
	stk: &mut Stk,
	ctx: &FrozenContext,
	opt: &Options,
	doc: Option<&CursorDoc>,
	rec: Recursion<'_>,
) -> Result<Value> {
	// Find the recursion limit
	let limit = ctx.config.exec.idiom_recursion_limit;
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
			bail!(ExecError::IdiomRecursionLimitExceeded {
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
				crate::legacy::recurse_instruction_compute(
					instruction,
					stk,
					ctx,
					opt,
					doc,
					rec.with_iterated(i).with_current(&current),
					&mut finished,
				)
				.await?
			}
			_ => stk
				.run(|stk| crate::legacy::value_get(&current, stk, ctx, opt, doc, rec.path))
				.await
				.catch_return()?,
		};

		// Check for any recursion plans
		let v = match rec.plan {
			// We found a recursion plan, let's apply it
			Some(p) => {
				crate::legacy::recursion_plan_compute(
					p,
					stk,
					ctx,
					opt,
					doc,
					rec.with_iterated(i).with_current(&v),
				)
				.await?
			}
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
					Some(_) if i < rec.min => Value::Array(Array::new()),
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
			bail!(ExecError::IdiomRecursionLimitExceeded {
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
