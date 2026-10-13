//! Applying the walk of an `OMIT` path ([`Instr::Omit`](super::program::Instr)).
//!
//! A path of fields and literal indices is removed from an object by
//! [`Instr::RemovePath`](super::program::Instr). Any other names its locations
//! only once walked: a filter selects the elements to remove, a parameter
//! spells an index, `.*` empties a container, a destructure lists fields. The
//! walk follows the steps to every location they reach, innermost first so a
//! removal never invalidates a position the walk is still holding, and removes
//! what the action names at each.
//!
//! A field or a destructure — a step, or the action — met at an array applies
//! to each of its elements: `o.b` removes `b` from every element of an array
//! `o` holds. An index numbers a position in an array and spells a key in an
//! A step whose target is only known at run time is a pipeline of the program.
//! It runs over the element the step is applied to, or over `NONE` for an index.

use std::pin::Pin;

use super::interp::State;
use super::payload::{Walk, WalkAction, WalkStep};
use super::program::Program;
use super::write;
use crate::exec::{ExecutionContext, FlowResult};
use crate::expr::ControlFlow;
use crate::expr::logical::LogicalDestructureField;
use crate::val::{Object, Value};

type Walked<'a> = Pin<Box<dyn Future<Output = FlowResult<()>> + Send + 'a>>;

/// Removes from `value` the locations `walk` names.
pub(super) async fn apply(
	program: &Program,
	walk: &Walk<u16>,
	value: &mut Value,
	state: &mut State,
	ctx: &ExecutionContext,
) -> FlowResult<()> {
	descend(program, &walk.steps, &walk.action, value, state, ctx).await
}

/// The index a value spells: a position in an array, and a key in an object.
fn index_of(value: &Value) -> Option<usize> {
	match value {
		Value::Number(n) => usize::try_from(n.to_int()).ok(),
		_ => None,
	}
}

fn descend<'a>(
	program: &'a Program,
	steps: &'a [WalkStep<u16>],
	action: &'a WalkAction<u16>,
	value: &'a mut Value,
	state: &'a mut State,
	ctx: &'a ExecutionContext,
) -> Walked<'a> {
	Box::pin(async move {
		let Some((step, rest)) = steps.split_first() else {
			return perform(program, action, value, state, ctx).await;
		};
		match step {
			WalkStep::Field(_) | WalkStep::Destructure(_) if matches!(value, Value::Array(_)) => {
				if let Value::Array(arr) = value {
					for element in arr.iter_mut() {
						descend(program, steps, action, element, state, ctx).await?;
					}
				}
			}
			WalkStep::Field(name) => {
				if let Value::Object(obj) = value
					&& let Some(nested) = obj.get_mut(name.as_str())
				{
					descend(program, rest, action, nested, state, ctx).await?;
				}
			}
			WalkStep::Index(index) => {
				descend_index(program, *index, rest, action, value, state, ctx).await?;
			}
			WalkStep::IndexAt(pipeline) => {
				let index =
					write::run_image(program, *pipeline, Value::None, None, state, ctx).await?;
				if let Some(index) = index_of(&index) {
					descend_index(program, index, rest, action, value, state, ctx).await?;
				}
			}
			WalkStep::Matching(pipeline) => {
				if let Value::Array(arr) = value {
					for item in arr.iter_mut() {
						let held =
							write::run_image(program, *pipeline, item.clone(), None, state, ctx)
								.await?;
						if held.is_truthy() {
							descend(program, rest, action, item, state, ctx).await?;
						}
					}
				}
			}
			WalkStep::Each => match value {
				Value::Object(obj) => {
					for (_, nested) in obj.iter_mut() {
						descend(program, rest, action, nested, state, ctx).await?;
					}
				}
				Value::Array(arr) => {
					for nested in arr.iter_mut() {
						descend(program, rest, action, nested, state, ctx).await?;
					}
				}
				_ => {}
			},
			WalkStep::First => {
				if let Value::Array(arr) = value
					&& let Some(nested) = arr.first_mut()
				{
					descend(program, rest, action, nested, state, ctx).await?;
				}
			}
			WalkStep::Last => {
				if let Value::Array(arr) = value
					&& let Some(nested) = arr.last_mut()
				{
					descend(program, rest, action, nested, state, ctx).await?;
				}
			}
			WalkStep::Nowhere => {}
			WalkStep::Destructure(fields) => {
				if let Value::Object(obj) = value {
					for field in fields {
						let name = match field {
							LogicalDestructureField::Field(name)
							| LogicalDestructureField::All(name)
							| LogicalDestructureField::Destructure(name, _) => name,
							// An aliased entry names an expression and a rest entry names no single
							// field, so neither is a location to walk to.
							LogicalDestructureField::Aliased(..)
							| LogicalDestructureField::Rest => {
								continue;
							}
						};
						if let Some(nested) = obj.get_mut(name.as_str()) {
							descend(program, rest, action, nested, state, ctx).await?;
						}
					}
				}
			}
		}
		Ok(())
	})
}

/// Applies an index step, whether the index is a literal or evaluated at run time.
async fn descend_index(
	program: &Program,
	index: usize,
	rest: &[WalkStep<u16>],
	action: &WalkAction<u16>,
	value: &mut Value,
	state: &mut State,
	ctx: &ExecutionContext,
) -> FlowResult<()> {
	match value {
		Value::Array(arr) => {
			if let Some(nested) = arr.get_mut(index) {
				descend(program, rest, action, nested, state, ctx).await?;
			}
		}
		Value::Object(obj) => {
			if let Some(nested) = obj.get_mut(index.to_string().as_str()) {
				descend(program, rest, action, nested, state, ctx).await?;
			}
		}
		_ => {}
	}
	Ok(())
}

/// Removes what `action` names inside `value`.
fn perform<'a>(
	program: &'a Program,
	action: &'a WalkAction<u16>,
	value: &'a mut Value,
	state: &'a mut State,
	ctx: &'a ExecutionContext,
) -> Walked<'a> {
	Box::pin(async move {
		match action {
			WalkAction::Nothing => {}
			WalkAction::RemoveField(_) | WalkAction::Destructure(_)
				if matches!(value, Value::Array(_)) =>
			{
				if let Value::Array(arr) = value {
					for element in arr.iter_mut() {
						perform(program, action, element, state, ctx).await?;
					}
				}
			}
			WalkAction::RemoveField(name) => {
				if let Value::Object(obj) = value {
					obj.remove(name.as_str());
				}
			}
			WalkAction::RemoveIndex(index) => remove_index(value, *index),
			WalkAction::RemoveIndexAt(pipeline) => {
				let index =
					write::run_image(program, *pipeline, Value::None, None, state, ctx).await?;
				if let Some(index) = index_of(&index) {
					remove_index(value, index);
				}
			}
			WalkAction::RemoveMatching(pipeline) => {
				if let Value::Array(arr) = value {
					// Every predicate is evaluated against the array before any removal, so a
					// removal never changes which elements a later predicate sees.
					let mut keep = Vec::with_capacity(arr.len());
					for item in arr.iter() {
						let held =
							write::run_image(program, *pipeline, item.clone(), None, state, ctx)
								.await?;
						keep.push(!held.is_truthy());
					}
					let mut keep = keep.into_iter();
					arr.retain(|_| keep.next().unwrap_or(true));
				}
			}
			WalkAction::RemoveFirst => {
				if let Value::Array(arr) = value
					&& !arr.is_empty()
				{
					arr.remove(0);
				}
			}
			WalkAction::RemoveLast => {
				if let Value::Array(arr) = value
					&& let Some(last) = arr.len().checked_sub(1)
				{
					arr.remove(last);
				}
			}
			WalkAction::Clear => match value {
				Value::Object(obj) => obj.clear(),
				Value::Array(arr) => arr.clear(),
				_ => {}
			},
			WalkAction::Destructure(fields) => {
				if let Value::Object(obj) = value {
					remove_fields(obj, fields)?;
				}
			}
		}
		Ok(())
	})
}

/// Removes the element an index numbers from an array, closing the gap, or the
/// key it spells from an object.
fn remove_index(value: &mut Value, index: usize) {
	match value {
		Value::Array(arr) if index < arr.len() => {
			arr.remove(index);
		}
		Value::Object(obj) => {
			obj.remove(index.to_string().as_str());
		}
		_ => {}
	}
}

/// Removes the locations a destructure lists from an object.
///
/// A field entry removes that field. A `{ a.* }` entry empties the container in
/// field `a` and keeps the field. An aliased entry names an expression and a rest
/// entry names no single field, so neither is a location to remove. Reaching
/// either raises an error. The error is raised during removal, not when the plan
/// is built, so a statement whose source yields no rows never reaches the entry
/// and raises nothing.
fn remove_fields(obj: &mut Object, fields: &[LogicalDestructureField]) -> FlowResult<()> {
	for field in fields {
		match field {
			LogicalDestructureField::Field(name) => {
				obj.remove(name.as_str());
			}
			LogicalDestructureField::All(name) => match obj.get_mut(name.as_str()) {
				Some(Value::Object(inner)) => inner.clear(),
				Some(Value::Array(inner)) => inner.clear(),
				_ => {}
			},
			LogicalDestructureField::Destructure(name, nested) => {
				if let Some(Value::Object(inner)) = obj.get_mut(name.as_str()) {
					remove_fields(inner, nested)?;
				}
			}
			LogicalDestructureField::Aliased(..) => {
				return Err(ControlFlow::Err(anyhow::Error::new(
					crate::expr::Error::UnsupportedDestructure {
						variant: "An aliased".into(),
					},
				)));
			}
			LogicalDestructureField::Rest => {
				return Err(ControlFlow::Err(anyhow::Error::new(
					crate::expr::Error::UnsupportedDestructure {
						variant: "A rest".into(),
					},
				)));
			}
		}
	}
	Ok(())
}
