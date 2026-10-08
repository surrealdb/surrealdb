//! `OMIT` paths, compiled.
//!
//! An `OMIT` path names a location to remove from the projected row. Most of a
//! path is written down — a field name, a literal index, `.*`, a destructure —
//! and is walked structurally. The rest names its location only once evaluated:
//! a filter selects the elements to remove, a parameter spells the index, and
//! those sub-expressions are compiled here with the rest of the statement and
//! evaluated per row.
//!
//! A path is held flat: the steps that *reach* a location, then the one action
//! that removes something there. Flattening is what makes a `Broadcast` — which
//! applies a whole path to everything another path reaches — nothing more than
//! the concatenation of the two, and what gives a destructure a reaching form
//! (`a.{ b }.x`) beside its removing one (`a.{ b }`).

use std::sync::Arc;

use crate::exec::{BoxFut, EvalContext, FlowResult, PhysicalExpr};
use crate::expr::ControlFlow;
use crate::expr::logical::LogicalDestructureField;
use crate::val::{Object, Strand, Value};

/// One step of the walk to the location a path names. Each reaches zero or
/// more values inside the value it is applied to.
#[derive(Debug, Clone)]
pub(crate) enum OmitStep {
	/// `.name`: the field of an object.
	Field(Strand),
	/// `[0]`: the element an array numbers, or the key an object spells.
	Index(usize),
	/// `[$i]`: the same, where the index is known only once evaluated.
	IndexAt(Arc<dyn PhysicalExpr>),
	/// `[WHERE p]`: every element the predicate holds for.
	Matching(Arc<dyn PhysicalExpr>),
	/// `.*`: every child of an object or array.
	Each,
	/// `[0]` at the front, and `[$]` at the back.
	First,
	Last,
	/// `.{ a, b }` before a further step: each field the destructure lists.
	Destructure(Arc<[LogicalDestructureField]>),
	/// A step that reaches no location — a method call answers with a value,
	/// not a place. Nothing under it is reached either, so the path removes
	/// nothing.
	Nowhere,
}

/// What a path removes where its steps land.
#[derive(Debug, Clone)]
pub(crate) enum OmitAction {
	/// `.name`: the field leaves the object.
	RemoveField(Strand),
	/// `[0]`: the element leaves the array, closing the gap, or the key
	/// leaves the object.
	RemoveIndex(usize),
	/// `[$i]`: the same, evaluated.
	RemoveIndexAt(Arc<dyn PhysicalExpr>),
	/// `[WHERE p]`: every element the predicate holds for leaves the array.
	RemoveMatching(Arc<dyn PhysicalExpr>),
	RemoveFirst,
	RemoveLast,
	/// `.*`: the object or array is emptied, and stays.
	Clear,
	/// `.{ a, b }`: the listed fields leave the object.
	Destructure(Arc<[LogicalDestructureField]>),
	/// A step that names no location — a method call answers with a value,
	/// not a place — so the path removes nothing. Kept rather than declined
	/// so the statement still plans: removing nothing is what it does.
	Nothing,
}

/// One compiled `OMIT` path.
#[derive(Debug, Clone)]
pub(crate) struct OmitPath {
	pub(crate) steps: Vec<OmitStep>,
	pub(crate) action: OmitAction,
}

impl OmitPath {
	/// The name of the field this path removes, when it removes a field of
	/// the row itself and nothing deeper. A caller reasoning about which
	/// output names survive the projection asks this rather than the steps.
	pub(crate) fn single_field(&self) -> Option<&str> {
		match (&self.steps[..], &self.action) {
			([], OmitAction::RemoveField(name)) => Some(name.as_str()),
			_ => None,
		}
	}

	/// Applies the path to `value`, removing what it names.
	pub async fn apply(&self, value: &mut Value, ctx: &EvalContext<'_>) -> FlowResult<()> {
		walk(value, &self.steps, &self.action, ctx).await
	}
}

/// The index a value spells: a position in an array, and a key in an object.
fn index_of(value: &Value) -> Option<usize> {
	match value {
		Value::Number(n) => usize::try_from(n.to_int()).ok(),
		_ => None,
	}
}

/// Reaches the locations `steps` names inside `value` and performs `action` at
/// each, innermost first so a removal never invalidates a position the walk is
/// still holding.
///
/// A field or a destructure — a step, or the action — met at an array applies
/// to each of its elements: `o.b`
/// removes `b` from every element of an array `o` holds.
fn walk<'a>(
	value: &'a mut Value,
	steps: &'a [OmitStep],
	action: &'a OmitAction,
	ctx: &'a EvalContext<'a>,
) -> BoxFut<'a, FlowResult<()>> {
	Box::pin(async move {
		let Some((step, rest)) = steps.split_first() else {
			return perform(value, action, ctx).await;
		};
		match step {
			OmitStep::Field(_) | OmitStep::Destructure(_) if matches!(value, Value::Array(_)) => {
				if let Value::Array(arr) = value {
					for element in arr.iter_mut() {
						walk(element, steps, action, ctx).await?;
					}
				}
			}
			OmitStep::Field(name) => {
				if let Value::Object(obj) = value
					&& let Some(nested) = obj.get_mut(name.as_str())
				{
					walk(nested, rest, action, ctx).await?;
				}
			}
			OmitStep::Index(idx) => descend_index(value, *idx, rest, action, ctx).await?,
			OmitStep::IndexAt(expr) => {
				let idx = expr.evaluate(ctx.clone()).await?;
				if let Some(idx) = index_of(&idx) {
					descend_index(value, idx, rest, action, ctx).await?;
				}
			}
			OmitStep::Matching(predicate) => {
				if let Value::Array(arr) = value {
					for item in arr.iter_mut() {
						if predicate.evaluate(ctx.with_value(item)).await?.is_truthy() {
							walk(item, rest, action, ctx).await?;
						}
					}
				}
			}
			OmitStep::Each => match value {
				Value::Object(obj) => {
					for (_, nested) in obj.iter_mut() {
						walk(nested, rest, action, ctx).await?;
					}
				}
				Value::Array(arr) => {
					for nested in arr.iter_mut() {
						walk(nested, rest, action, ctx).await?;
					}
				}
				_ => {}
			},
			OmitStep::First => {
				if let Value::Array(arr) = value
					&& let Some(nested) = arr.first_mut()
				{
					walk(nested, rest, action, ctx).await?;
				}
			}
			OmitStep::Last => {
				if let Value::Array(arr) = value
					&& let Some(nested) = arr.last_mut()
				{
					walk(nested, rest, action, ctx).await?;
				}
			}
			OmitStep::Nowhere => {}
			OmitStep::Destructure(fields) => {
				if let Value::Object(obj) = value {
					for field in fields.iter() {
						let name = match field {
							LogicalDestructureField::Field(name)
							| LogicalDestructureField::All(name)
							| LogicalDestructureField::Destructure(name, _) => name,
							// Names an expression, or no single field, not a location to reach.
							LogicalDestructureField::Aliased(..)
							| LogicalDestructureField::Rest => {
								continue;
							}
						};
						if let Some(nested) = obj.get_mut(name.as_str()) {
							walk(nested, rest, action, ctx).await?;
						}
					}
				}
			}
		}
		Ok(())
	})
}

/// One index step, shared by its literal and evaluated spellings.
fn descend_index<'a>(
	value: &'a mut Value,
	idx: usize,
	rest: &'a [OmitStep],
	action: &'a OmitAction,
	ctx: &'a EvalContext<'a>,
) -> BoxFut<'a, FlowResult<()>> {
	Box::pin(async move {
		match value {
			Value::Array(arr) => {
				if let Some(nested) = arr.get_mut(idx) {
					walk(nested, rest, action, ctx).await?;
				}
			}
			Value::Object(obj) => {
				if let Some(nested) = obj.get_mut(idx.to_string().as_str()) {
					walk(nested, rest, action, ctx).await?;
				}
			}
			_ => {}
		}
		Ok(())
	})
}

/// Removes what `action` names inside `value`: a field or a destructure from
/// each element of an array, as [`walk`] reaches one.
fn perform<'a>(
	value: &'a mut Value,
	action: &'a OmitAction,
	ctx: &'a EvalContext<'a>,
) -> BoxFut<'a, FlowResult<()>> {
	Box::pin(perform_boxed(value, action, ctx))
}

async fn perform_boxed(
	value: &mut Value,
	action: &OmitAction,
	ctx: &EvalContext<'_>,
) -> FlowResult<()> {
	match action {
		OmitAction::Nothing => {}
		OmitAction::RemoveField(_) | OmitAction::Destructure(_)
			if matches!(value, Value::Array(_)) =>
		{
			if let Value::Array(arr) = value {
				for element in arr.iter_mut() {
					perform(element, action, ctx).await?;
				}
			}
		}
		OmitAction::RemoveField(name) => {
			if let Value::Object(obj) = value {
				obj.remove(name.as_str());
			}
		}
		OmitAction::RemoveIndex(idx) => remove_index(value, *idx),
		OmitAction::RemoveIndexAt(expr) => {
			let idx = expr.evaluate(ctx.clone()).await?;
			if let Some(idx) = index_of(&idx) {
				remove_index(value, idx);
			}
		}
		OmitAction::RemoveMatching(predicate) => {
			if let Value::Array(arr) = value {
				// Decided over the array as it stands, then applied, so a
				// removal never shifts an element past the predicate.
				let mut keep = Vec::with_capacity(arr.len());
				for item in arr.iter() {
					keep.push(!predicate.evaluate(ctx.with_value(item)).await?.is_truthy());
				}
				let mut keep = keep.into_iter();
				arr.retain(|_| keep.next().unwrap_or(true));
			}
		}
		OmitAction::RemoveFirst => {
			if let Value::Array(arr) = value
				&& !arr.is_empty()
			{
				arr.remove(0);
			}
		}
		OmitAction::RemoveLast => {
			if let Value::Array(arr) = value
				&& let Some(last) = arr.len().checked_sub(1)
			{
				arr.remove(last);
			}
		}
		OmitAction::Clear => match value {
			Value::Object(obj) => obj.clear(),
			Value::Array(arr) => arr.clear(),
			_ => {}
		},
		OmitAction::Destructure(fields) => {
			if let Value::Object(obj) = value {
				destructure_fields(obj, fields)?;
			}
		}
	}
	Ok(())
}

/// An index numbers a position in an array and spells a key in an object, and
/// removes what it names in either.
fn remove_index(value: &mut Value, idx: usize) {
	match value {
		Value::Array(arr) if idx < arr.len() => {
			arr.remove(idx);
		}
		Value::Object(obj) => {
			obj.remove(idx.to_string().as_str());
		}
		_ => {}
	}
}

/// Removes the locations a destructure lists from an object.
///
/// Errs on an aliased entry, which names an expression rather than a location
/// to remove. The error belongs to the removal rather than to the plan: a
/// statement whose source yields no row performs no removal, and reaches no
/// aliased entry to object to.
pub(crate) fn destructure_fields(
	obj: &mut Object,
	fields: &[LogicalDestructureField],
) -> FlowResult<()> {
	for field in fields {
		match field {
			LogicalDestructureField::Field(name) => {
				obj.remove(name.as_str());
			}
			// `{ a.* }` names the contents of `a` rather than `a` itself, so
			// the container it holds is emptied and the field stays.
			LogicalDestructureField::All(name) => match obj.get_mut(name.as_str()) {
				Some(Value::Object(inner)) => inner.clear(),
				Some(Value::Array(inner)) => inner.clear(),
				_ => {}
			},
			LogicalDestructureField::Destructure(name, nested) => {
				if let Some(Value::Object(inner)) = obj.get_mut(name.as_str()) {
					destructure_fields(inner, nested)?;
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
