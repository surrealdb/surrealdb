use anyhow::Result;
use reblessive::tree::Stk;

use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::EngineError;
use crate::exe::{FlowResultExt as _, try_join_all_buffered};
use crate::expr::idiom::recursion::{self, Recursion, clean_iteration, is_final};
use crate::expr::part::{Part, RecurseInstruction, RecursionPlan};
use crate::legacy::compute_idiom_recursion;
use crate::val::{Array, RecordId, Value};

#[instrument(level = "trace", name = "RecursionPlan::compute", skip_all)]
pub(crate) async fn recursion_plan_compute<'a>(
	this: &RecursionPlan,
	stk: &mut Stk,
	ctx: &FrozenContext,
	opt: &Options,
	doc: Option<&CursorDoc>,
	rec: Recursion<'a>,
) -> Result<Value> {
	match rec.current {
		Value::Array(value) => stk
			.scope(|scope| {
				let futs = value.iter().map(|value| {
					scope.run(|stk| {
						let rec = rec.with_current(value);
						crate::legacy::recursion_plan_compute_inner(this, stk, ctx, opt, doc, rec)
					})
				});
				try_join_all_buffered(futs, ctx.config.max_concurrent_tasks)
			})
			.await
			.map(Into::into),
		_ => {
			stk.run(|stk| {
				crate::legacy::recursion_plan_compute_inner(this, stk, ctx, opt, doc, rec)
			})
			.await
		}
	}
}

pub(crate) async fn recursion_plan_compute_inner<'a>(
	this: &RecursionPlan,
	stk: &mut Stk,
	ctx: &FrozenContext,
	opt: &Options,
	doc: Option<&CursorDoc>,
	rec: Recursion<'a>,
) -> Result<Value> {
	match this {
		RecursionPlan::Repeat => compute_idiom_recursion(stk, ctx, opt, doc, rec).await,
		RecursionPlan::Destructure {
			parts,
			field,
			before,
			plan,
			after,
		} => {
			let v = stk
				.run(|stk| crate::legacy::value_get(rec.current, stk, ctx, opt, doc, before))
				.await
				.catch_return()?;
			let v = crate::legacy::recursion_plan_compute(
				plan,
				stk,
				ctx,
				opt,
				doc,
				rec.with_current(&v),
			)
			.await?;
			let v = stk
				.run(|stk| crate::legacy::value_get(&v, stk, ctx, opt, doc, after))
				.await
				.catch_return()?;
			let v = clean_iteration(v);

			if rec.iterated < rec.min && is_final(&v) {
				// We do not use get_final here, because it's not a result
				// the user will see, it's rather about path elimination
				// By returning NONE, an array to be eliminated will be
				// filled with NONE, and thus eliminated
				return Ok(Value::None);
			}

			let path = &[Part::Destructure(parts.to_owned())];
			match stk
				.run(|stk| crate::legacy::value_get(rec.current, stk, ctx, opt, doc, path))
				.await
				.catch_return()?
			{
				Value::Object(mut obj) => {
					obj.insert(field.clone(), v);
					Ok(Value::Object(obj))
				}
				Value::None => Ok(Value::None),
				v => Err(anyhow::Error::new(EngineError::unreachable(format_args!(
					"Expected an object or none, found {}.",
					v.kind_of()
				)))),
			}
		}
	}
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn walk_paths(
	stk: &mut Stk,
	ctx: &FrozenContext,
	opt: &Options,
	doc: Option<&CursorDoc>,
	recursion: Recursion<'_>,
	finished: &mut Vec<Value>,
	inclusive: bool,
	expects: Option<&Value>,
) -> Result<Value> {
	let mut open: Vec<Value> = vec![];
	let paths = match recursion.current {
		Value::Array(v) => &v.0,
		v => &vec![v.to_owned()],
	};

	for path in paths.iter() {
		let path = match path {
			Value::Array(v) => &v.0,
			v => &vec![v.to_owned()],
		};
		let Some(last) = path.last() else {
			continue;
		};
		let res = stk
			.run(|stk| crate::legacy::value_get(last, stk, ctx, opt, doc, recursion.path))
			.await
			.catch_return()?;

		if recursion::is_final(&res) || &res == last {
			if expects.is_none()
				&& (recursion.iterated > 1 || inclusive)
				&& recursion.iterated >= recursion.min
			{
				finished.push(path.to_owned().into());
			}
			continue;
		}

		let steps = match res {
			Value::Array(v) => v.0,
			v => vec![v],
		};

		let reached_max = recursion.max.is_some_and(|max| recursion.iterated >= max);
		for step in steps.iter() {
			let val = if recursion.iterated == 1 && !inclusive {
				Value::from(vec![step.to_owned()])
			} else {
				let mut path = path.to_owned();
				path.push(step.to_owned());
				Value::from(path)
			};
			if let Some(expects) = expects
				&& step == expects
			{
				let steps = match val {
					Value::Array(v) => v.0,
					v => vec![v],
				};
				for step in steps {
					finished.push(step);
				}
				return Ok(Value::None);
			}
			if reached_max {
				if (Option::<&Value>::None).is_none() {
					finished.push(val);
				}
			} else {
				open.push(val);
			}
		}
	}

	Ok(Value::Array(Array(open)))
}

pub(crate) async fn recurse_instruction_compute(
	this: &RecurseInstruction,
	stk: &mut Stk,
	ctx: &FrozenContext,
	opt: &Options,
	doc: Option<&CursorDoc>,
	rec: Recursion<'_>,
	finished: &mut Vec<Value>,
) -> Result<Value> {
	match this {
		RecurseInstruction::Path {
			inclusive,
		} => walk_paths(stk, ctx, opt, doc, rec, finished, *inclusive, None).await,
		RecurseInstruction::Shortest {
			expects,
			inclusive,
		} => {
			let expects = stk
				.run(|stk| crate::legacy::expr_compute(expects, stk, ctx, opt, doc))
				.await
				.catch_return()?
				.coerce_to::<RecordId>()?
				.into();
			walk_paths(stk, ctx, opt, doc, rec, finished, *inclusive, Some(&expects)).await
		}
		RecurseInstruction::Collect {
			inclusive,
		} => {
			// If we are inclusive, we add the starting point to the collection
			if rec.iterated == 1 && *inclusive {
				match rec.current {
					Value::Array(v) => {
						for v in v.iter() {
							if !finished.contains(v) {
								finished.push(v.to_owned());
							}
						}
					}
					v => {
						if !finished.contains(v) {
							finished.push(v.to_owned());
						}
					}
				};
			}

			// Apply the recursed path to the current values
			let res = stk
				.run(|stk| crate::legacy::value_get(rec.current, stk, ctx, opt, doc, rec.path))
				.await
				.catch_return()?;
			// Clean the iteration
			let res = clean_iteration(res);

			// Persist any new values from the result, only at or beyond min depth
			if rec.iterated >= rec.min {
				match &res {
					Value::Array(v) => {
						for v in v.iter() {
							if !finished.contains(v) {
								finished.push(v.to_owned());
							}
						}
					}
					v => {
						if !finished.contains(v) {
							finished.push(v.to_owned());
						}
					}
				};
			}

			// Continue
			Ok(res)
		}
	}
}
