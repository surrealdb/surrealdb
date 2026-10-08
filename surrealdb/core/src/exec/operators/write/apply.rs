//! The five record mutators: what a data clause's assignments do to a record.
//!
//! A `SET` clause is a list of `(place, operator, value)` triples whose
//! right-hand sides have already been evaluated. Applying one walks the
//! [`WritePlace`] into the record and mutates what it lands on, and which of
//! the five mutators does the mutating is the assignment operator's choice:
//!
//! | Operator | Mutator |
//! |----------|---------|
//! | `=` with a value | [`set`] |
//! | `=` with `NONE` | [`del`] — assigning nothing is removing |
//! | `+=` | [`increment`] |
//! | `-=` | [`decrement`] |
//! | `+?` | [`extend`] |
//!
//! # The three walks differ at every step
//!
//! [`set`] descends while the record has somewhere to descend to, and where
//! the path runs out of record it *builds* the rest: a dead end is filled with
//! the objects the remaining steps name, so `SET a.b.c = 1` on a record with
//! no `a` leaves `{ a: { b: { c: 1 } } }`. [`del`] does the opposite — it stops
//! where the record stops, because there is nothing to remove down a path the
//! record never had.
//!
//! They also disagree about what a step selects. A step that names a *set* of
//! places — `[*]`, `[WHERE …]` — broadcasts the rest of the walk to every
//! member in both, but at the end of a path `[*]` assigns to each member under
//! [`set`] and clears the whole container under [`del`]. A lookup step is an
//! object key to [`set`] and nothing at all to [`del`]. [`read`] is a third
//! walk again: it builds nothing, removes nothing, and reads a step that names
//! many members as the collection of all of them. That is why there is no
//! shared walk primitive here: parameterising one walk by all of that would be
//! three walks in one body.
//!
//! # The three compound mutators read first
//!
//! [`increment`], [`decrement`] and [`extend`] are a read, a combine and a
//! [`set`]. The read is [`read`] — a walk of the place's own steps, in the
//! frames [`place::subscript`] and [`place::accepts`] define, so that what is
//! combined with is the value at the address the result is written back to.
//! It runs against the record **as it stands**, so assignments earlier in the
//! same clause are visible to a compound assignment later in it. What is
//! combined with is therefore the record's value, not the caller's reduced
//! view of it; the right-hand side it is combined with was evaluated against
//! the reduced view before any of this ran.
//!
//! A place that names nothing in the record reads `NONE`, and each compound
//! mutator has its own reading of that: `+=` starts from zero for a number and
//! from a one-element array for anything else, `-=` from zero for a number and
//! abandons the assignment otherwise, `+?` from the empty array.

use std::sync::Arc;

use surrealdb_collections::Entry;
use surrealdb_types::ToSql;

use super::clauses::{WriteAssignment, WriteData};
use super::place::{PlaceEval, PlaceStep, TreeEval};
use super::row::ComputedInput;
use crate::doc::alter::ComputedAssignment;
use crate::exec::{BoxFut, ExecutionContext};
use crate::expr::{AssignOperator, ControlFlow, Error as ExprError};
use crate::val::array::Uniq;
use crate::val::{Number, Object, Strand, TryAdd, TrySub, Value};

// ===========================================================================
// A clause, applied
// ===========================================================================

/// Apply an evaluated `SET` clause to `target`, in the order it was written.
///
/// `clause` and `computed` are the same clause: the compiled assignments and
/// the values they evaluated to, paired by position, which is the order
/// [`evaluate_data`](super::stages::data) produced them in. The place comes
/// from the compiled side because that is where the path's own expressions
/// live; the operator and the value come from the evaluated side, because
/// those are what this record's evaluation produced.
///
/// Each assignment is applied before the next is read, so
/// `SET a = 1, b += a` combines with the assigned `a`.
pub(crate) async fn apply_set(
	target: &mut Value,
	clause: &[WriteAssignment],
	computed: &[ComputedAssignment],
	exec: &ExecutionContext,
) -> Result<(), ControlFlow> {
	let places = clause.iter().map(|assignment| assignment.place.steps());
	apply_set_with(target, places, computed, &mut TreeEval(exec)).await
}

/// [`apply_set`] over the places of any form of step: `places` are the
/// assignments' steps in written order, and `ev` evaluates what they embed.
pub(crate) async fn apply_set_with<'a, X: PlaceEval>(
	target: &mut Value,
	places: impl ExactSizeIterator<Item = &'a [PlaceStep<X::Expr>]> + Send,
	computed: &[ComputedAssignment],
	ev: &mut X,
) -> Result<(), ControlFlow>
where
	X::Expr: 'a,
{
	if places.len() != computed.len() {
		return Err(ControlFlow::Err(anyhow::anyhow!(
			"a SET clause of {} assignments evaluated to {}",
			places.len(),
			computed.len()
		)));
	}
	for (steps, computed) in places.zip(computed) {
		let value = computed.value.clone();
		match &computed.operator {
			// Assigning nothing is removing: `SET a = NONE` leaves no `a`
			// rather than an `a` holding `NONE`.
			AssignOperator::Assign => match value {
				Value::None => del(target, steps, ev).await?,
				value => set_with(target, steps, value, ev).await?,
			},
			AssignOperator::Add => increment(target, steps, value, ev).await?,
			AssignOperator::Subtract => decrement(target, steps, value, ev).await?,
			AssignOperator::Extend => extend(target, steps, value, ev).await?,
		}
	}
	Ok(())
}

/// The synthetic value `$input` names for one record.
///
/// Every clause but `SET` already carries the value the statement supplied, so
/// it is that value. A `SET` clause carries assignments rather than a value, so
/// the value is what those assignments make of an *empty object*: a compound
/// operator there combines with `NONE`, which is what it would combine with on
/// a record this statement is creating.
///
/// `data` and `input` are the same clause, compiled and evaluated. A pair that
/// disagrees is a routing mistake rather than a shape this can guess at, and is
/// refused.
pub(crate) async fn materialise_input(
	data: &WriteData,
	input: &ComputedInput,
	exec: &ExecutionContext,
) -> Result<Arc<Value>, ControlFlow> {
	match (data, input) {
		(_, ComputedInput::Patch(value))
		| (_, ComputedInput::Merge(value))
		| (_, ComputedInput::Replace(value))
		| (_, ComputedInput::Content(value)) => Ok(Arc::clone(value)),
		// An `UNSET` says what to take away, which is no value at all.
		(_, ComputedInput::Unset(_)) => Ok(Arc::new(Value::None)),
		(WriteData::Set(clause), ComputedInput::Set(computed)) => {
			let mut input = Value::Object(Object::default());
			apply_set(&mut input, clause, computed, exec).await?;
			Ok(Arc::new(input))
		}
		(_, ComputedInput::Set(_)) => Err(ControlFlow::Err(anyhow::anyhow!(
			"an evaluated SET clause paired with a clause that is not a SET"
		))),
	}
}

// ===========================================================================
// Assignment
// ===========================================================================

/// Write `val` at the place `steps` names.
///
/// Descends while the record has somewhere to descend to. Three things end the
/// descent:
///
/// - the steps run out, and `val` is written where the walk landed;
/// - the walk lands on a value no step can descend into — a record id, `NULL`, `NONE`, or a missing
///   object key — and the remaining steps are *built* as nested objects around `val` by [`assign`];
/// - a step lands on something it does not address at all, and the assignment is abandoned. A field
///   on a number names nothing, and naming nothing writes nothing.
///
/// A step that selects a set of places broadcasts the rest of the walk to each
/// member, so one assignment can write many.
pub(crate) fn set<'a>(
	this: &'a mut Value,
	steps: &'a [PlaceStep],
	val: Value,
	exec: &'a ExecutionContext,
) -> BoxFut<'a, Result<(), ControlFlow>> {
	Box::pin(async move { set_with(this, steps, val, &mut TreeEval(exec)).await })
}

/// [`set`] over steps of any form, their embedded expressions evaluated by
/// `ev`.
pub(crate) fn set_with<'a, X: PlaceEval>(
	this: &'a mut Value,
	steps: &'a [PlaceStep<X::Expr>],
	val: Value,
	ev: &'a mut X,
) -> BoxFut<'a, Result<(), ControlFlow>> {
	Box::pin(async move {
		// The empty place is the record itself.
		if steps.is_empty() {
			*this = val;
			return Ok(());
		}
		let mut place = this;
		let mut at = 0;
		while at < steps.len() {
			// The steps from the current one on, which is what an abandoned
			// descent rebuilds and what a broadcast re-walks.
			let from_here = &steps[at..];
			// Any step into one of these is guaranteed to find nothing, so the
			// whole remaining path is built here instead.
			if matches!(place, Value::RecordId(_) | Value::Null | Value::None) {
				return assign(place, val, from_here, ev).await;
			}
			let rest = &steps[at + 1..];
			match &steps[at] {
				// A lookup is a constant object key like a field, spelled as
				// the lookup renders.
				PlaceStep::Field(name) | PlaceStep::Lookup(name) => match place {
					Value::Object(obj) => match obj.entry(name.as_str()) {
						Entry::Vacant(vacant) => {
							let dead_end = vacant.insert(Value::None);
							return assign(dead_end, val, rest, ev).await;
						}
						Entry::Occupied(occupied) => place = occupied.into_mut(),
					},
					// A key named on an array is named on each of its
					// elements, so `a.b = 1` sets `b` on every element of `a`.
					Value::Array(arr) => {
						for element in arr.iter_mut() {
							set_with(element, from_here, val.clone(), ev).await?;
						}
						return Ok(());
					}
					_ => return Ok(()),
				},
				PlaceStep::Subscript {
					expr,
					..
				} => {
					let key = ev.subscript(expr).await?;
					match place {
						// On an object the subscript is a key: a string is the
						// key itself, anything else the text it renders to.
						Value::Object(obj) => {
							let key = match key {
								Value::String(key) => key.as_str().to_owned(),
								key => key.to_sql(),
							};
							match obj.entry(key) {
								Entry::Vacant(vacant) => {
									let dead_end = vacant.insert(Value::None);
									return assign(dead_end, val, rest, ev).await;
								}
								Entry::Occupied(occupied) => place = occupied.into_mut(),
							}
						}
						Value::Array(arr) => match key {
							// A range selects a run of elements, and the rest
							// of the walk applies to each.
							Value::Range(range) => {
								let range = range
									.coerce_to_typed::<i64>()
									.map_err(|err| ControlFlow::Err(anyhow::Error::new(err)))?;
								if let Some(selected) = range.slice_mut(arr) {
									for element in selected.iter_mut() {
										set_with(element, rest, val.clone(), ev).await?;
									}
								}
								return Ok(());
							}
							Value::Number(index) => {
								let Some(index) = index.as_array_index() else {
									return Ok(());
								};
								// An index past the end names no element, and
								// an assignment does not grow the array.
								let Some(element) = arr.get_mut(index) else {
									return Ok(());
								};
								place = element;
							}
							_ => return Ok(()),
						},
						_ => return Ok(()),
					}
				}
				PlaceStep::First => match place {
					Value::Array(arr) => {
						let Some(element) = arr.first_mut() else {
							return Ok(());
						};
						place = element;
					}
					Value::Set(set) => {
						let Some(element) = set.first_mut() else {
							return Ok(());
						};
						place = element;
					}
					_ => return Ok(()),
				},
				PlaceStep::Last => match place {
					Value::Array(arr) => {
						let Some(element) = arr.last_mut() else {
							return Ok(());
						};
						place = element;
					}
					Value::Set(set) => {
						let Some(element) = set.last_mut() else {
							return Ok(());
						};
						place = element;
					}
					_ => return Ok(()),
				},
				// Every member, and nothing when the value has no members.
				PlaceStep::All => {
					match place {
						Value::Array(arr) => {
							for element in arr.iter_mut() {
								set_with(element, rest, val.clone(), ev).await?;
							}
						}
						Value::Object(obj) => {
							for (_, element) in obj.iter_mut() {
								set_with(element, rest, val.clone(), ev).await?;
							}
						}
						_ => (),
					}
					return Ok(());
				}
				PlaceStep::Where(pred) => {
					let Value::Array(arr) = place else {
						return Ok(());
					};
					// A literal index written after the filter counts within
					// the selection: `a[WHERE …][0]` is the first accepted
					// element. The selection is walked as an array of its own
					// and its members written back where they came from.
					if matches!(
						rest.first(),
						Some(PlaceStep::Subscript {
							literal_index: true,
							..
						})
					) {
						let mut accepted = Vec::new();
						let mut positions = Vec::new();
						for (position, element) in arr.iter().enumerate() {
							if ev.accepts(pred, element).await? {
								accepted.push(element.clone());
								positions.push(position);
							}
						}
						let mut selection = Value::from(accepted);
						set_with(&mut selection, rest, val, ev).await?;
						for (index, position) in positions.into_iter().enumerate() {
							arr[position] = element_at(&selection, index);
						}
						return Ok(());
					}
					for element in arr.iter_mut() {
						if ev.accepts(pred, element).await? {
							set_with(element, rest, val.clone(), ev).await?;
						}
					}
					return Ok(());
				}
				// The walk has landed on something it does not address, and
				// naming nothing writes nothing.
				PlaceStep::Unaddressed => return Ok(()),
			}
			at += 1;
		}
		*place = val;
		Ok(())
	})
}

/// Build the steps a record does not have around `val`, and write the result.
///
/// Each step from the outside in wraps what it holds in a one-key object, so
/// `a.b.c` reached at a dead end after `a` leaves `{ b: { c: val } }` there.
/// The steps are walked in reverse, so a subscript among them is evaluated
/// from the innermost outwards.
///
/// Only the three steps that name a key can be built: a step that selects a
/// member of a container — `[0]`, `[$]`, `[*]`, `[WHERE …]` — names no key to
/// create one under, and abandons the assignment. So does a subscript that
/// evaluates to something no key can be made from.
async fn assign<X: PlaceEval>(
	place: &mut Value,
	mut val: Value,
	steps: &[PlaceStep<X::Expr>],
	ev: &mut X,
) -> Result<(), ControlFlow> {
	for step in steps.iter().rev() {
		let key: Strand = match step {
			PlaceStep::Field(name) | PlaceStep::Lookup(name) => name.as_str().into(),
			PlaceStep::Subscript {
				expr,
				..
			} => match ev.subscript(expr).await? {
				Value::String(key) => key,
				Value::RecordId(key) => key.to_sql().into(),
				Value::Number(key) => key.to_sql().into(),
				Value::Range(key) => key.to_sql().into(),
				_ => return Ok(()),
			},
			PlaceStep::Where(_)
			| PlaceStep::First
			| PlaceStep::Last
			| PlaceStep::All
			| PlaceStep::Unaddressed => {
				return Ok(());
			}
		};
		let mut object = Object::default();
		object.insert(key, val);
		val = object.into();
	}
	*place = val;
	Ok(())
}

// ===========================================================================
// Removal
// ===========================================================================

/// Remove what the place `steps` names.
///
/// Stops where the record stops: a key the record does not have, an index past
/// the end of an array, or a step into a value that is neither an object nor an
/// array, all leave the record alone. At the last step what the walk landed on
/// is removed — a key from an object, an element from an array, every member
/// under `[*]`, and every accepted element under `[WHERE …]`.
///
/// A step it does not address on an object removes nothing; on an array it is
/// broadcast to the elements, which is what makes `UNSET a.b` reach the `b` of
/// every element of an array `a`.
fn del<'a, X: PlaceEval>(
	this: &'a mut Value,
	steps: &'a [PlaceStep<X::Expr>],
	ev: &'a mut X,
) -> BoxFut<'a, Result<(), ControlFlow>> {
	Box::pin(async move {
		// The empty place is the record itself, and a record is not something
		// a removal can take away.
		let Some(step) = steps.first() else {
			return Ok(());
		};
		let last = steps.len() == 1;
		let rest = &steps[1..];
		match this {
			Value::Object(obj) => match step {
				PlaceStep::All => {
					if last {
						obj.clear();
					} else {
						for value in obj.values_mut() {
							del(value, rest, ev).await?;
						}
					}
				}
				PlaceStep::Field(name) => {
					if last {
						obj.remove(name.as_str());
					} else if let Some(value) =
						obj.get_mut(name.as_str()).filter(|value| !value.is_nullish())
					{
						del(value, rest, ev).await?;
					}
				}
				PlaceStep::Subscript {
					expr,
					..
				} => {
					// Only the three subscript results that name an object key
					// do anything; a key is the text a number or a record id
					// renders to, or a string itself.
					let key = match ev.subscript(expr).await? {
						Value::Number(key) => key.to_sql(),
						Value::String(key) => key.as_str().to_owned(),
						Value::RecordId(key) => key.to_sql(),
						_ => return Ok(()),
					};
					if last {
						obj.remove(key.as_str());
					} else if let Some(value) =
						obj.get_mut(key.as_str()).filter(|value| !value.is_nullish())
					{
						del(value, rest, ev).await?;
					}
				}
				// A lookup, a filter and the two positional steps name nothing
				// on an object.
				PlaceStep::Lookup(_)
				| PlaceStep::Where(_)
				| PlaceStep::First
				| PlaceStep::Last
				| PlaceStep::Unaddressed => {}
			},
			Value::Array(arr) => match step {
				PlaceStep::All => {
					if last {
						arr.clear();
					} else {
						for element in arr.iter_mut() {
							del(element, rest, ev).await?;
						}
					}
				}
				PlaceStep::First => {
					if last {
						if !arr.is_empty() {
							arr.remove(0);
						}
					} else if let Some(element) = arr.first_mut() {
						del(element, rest, ev).await?;
					}
				}
				PlaceStep::Last => {
					if last {
						if !arr.is_empty() {
							let index = arr.len() - 1;
							arr.remove(index);
						}
					} else if let Some(element) = arr.last_mut() {
						del(element, rest, ev).await?;
					}
				}
				PlaceStep::Where(pred) => {
					// At the end of the path the filter names the elements
					// themselves, and they go.
					if last {
						let mut kept = Vec::new();
						for element in arr.iter() {
							if !ev.accepts(pred, element).await? {
								kept.push(element.clone());
							}
						}
						arr.0 = kept;
					} else if matches!(rest.first(), Some(PlaceStep::Subscript { .. })) {
						// A subscript written after the filter counts within
						// the selection, so the selection is walked as an array
						// of its own. A member the walk removed from it is
						// removed from the array too; the others are written
						// back.
						let mut accepted = Vec::new();
						let mut positions = Vec::new();
						for (position, element) in arr.iter().enumerate() {
							if ev.accepts(pred, element).await? {
								accepted.push(element.clone());
								positions.push(position);
							}
						}
						let mut selection = Value::from(accepted);
						del(&mut selection, rest, ev).await?;
						// Back to front, so a removal does not shift a position
						// still to be written. What is removed is counted in
						// the selection, what is written back in the array.
						for (index, position) in positions.into_iter().enumerate().rev() {
							match element_at(&selection, index) {
								Value::None => {
									arr.remove(index);
								}
								element => arr[position] = element,
							}
						}
					} else {
						for element in arr.iter_mut() {
							if ev.accepts(pred, element).await? {
								del(element, rest, ev).await?;
							}
						}
					}
				}
				PlaceStep::Subscript {
					expr,
					..
				} => {
					// Only a number indexes an array; anything else names no
					// element of it.
					let Value::Number(index) = ev.subscript(expr).await? else {
						return Ok(());
					};
					let Some(index) = index.as_array_index() else {
						return Ok(());
					};
					if last {
						if arr.len() > index {
							arr.remove(index);
						}
					} else if let Some(element) = arr.get_mut(index) {
						del(element, rest, ev).await?;
					}
				}
				// A key named on an array is named on each of its elements.
				PlaceStep::Field(_) | PlaceStep::Lookup(_) => {
					for element in arr.iter_mut() {
						del(element, steps, ev).await?;
					}
				}
				PlaceStep::Unaddressed => {}
			},
			// Nothing else holds anything a place can take away.
			_ => (),
		}
		Ok(())
	})
}

// ===========================================================================
// The compound read
// ===========================================================================

/// What the place `steps` names in `this`.
///
/// This is a read, not a walk to a place: where [`set`] builds what the record
/// does not have and [`del`] stops where the record stops, this yields the
/// value that is there and `NONE` where there is none. It is also broader than
/// either: a step that names a set of values — `[*]`, `[WHERE …]`, a key named
/// on an array — reads *all* of them as one collection, so a compound
/// assignment at such a place combines with the collection and writes the
/// combined result to each member.
///
/// Each step reads what a value of that kind holds:
///
/// - A **field** or a **lookup** is a constant object key. On an array or a set it is named on each
///   member instead, and the results are a collection of the same kind.
/// - A **subscript** is an object key — a string as itself, a number or a record id as the text it
///   renders to — and an index or a range over an array or a set. Any other value names nothing.
/// - **`[WHERE …]`** keeps the members the predicate accepts, and the rest of the path continues on
///   that selection: an index after it therefore counts within the selection.
/// - The **first** and **last** steps are the first and last member of an array or a set.
/// - **`[*]`** is every member of an array or a set; on an object it is the object itself, so the
///   rest of the path continues on it unchanged.
///
/// A step lands nowhere — an absent key, an index past the end, a value of any
/// other kind — and the read is `NONE`, which each compound mutator has its own
/// reading of.
///
/// A record id is one of those other kinds: the read stops there rather than
/// fetching the record it names. A step past a link addresses a field of
/// another record, and this assignment writes to none of it — it replaces the
/// link — so combining with a remote value would write a result derived from a
/// record the statement never touched.
///
/// The read carries no `$input`: a compound assignment's right-hand side was
/// evaluated with that binding in scope, the address it is written to was not.
pub(crate) fn read<'a>(
	this: &'a Value,
	steps: &'a [PlaceStep],
	exec: &'a ExecutionContext,
) -> BoxFut<'a, Result<Value, ControlFlow>> {
	Box::pin(async move { read_with(this, steps, &mut TreeEval(exec)).await })
}

/// [`read`] over steps of any form, their embedded expressions evaluated by
/// `ev`.
pub(crate) fn read_with<'a, X: PlaceEval>(
	this: &'a Value,
	steps: &'a [PlaceStep<X::Expr>],
	ev: &'a mut X,
) -> BoxFut<'a, Result<Value, ControlFlow>> {
	Box::pin(async move {
		// The empty place is the record itself.
		let Some(step) = steps.first() else {
			return Ok(this.clone());
		};
		let rest = &steps[1..];
		match this {
			Value::Object(obj) => match step {
				// A lookup is a constant object key like a field, spelled as
				// the lookup renders.
				PlaceStep::Field(name) | PlaceStep::Lookup(name) => match obj.get(name.as_str()) {
					Some(value) => read_with(value, rest, ev).await,
					None => Ok(Value::None),
				},
				PlaceStep::Subscript {
					expr,
					..
				} => {
					let key = match ev.subscript(expr).await? {
						Value::String(key) => key.as_str().to_owned(),
						Value::Number(key) => key.to_sql(),
						Value::RecordId(key) => key.to_sql(),
						_ => return Ok(Value::None),
					};
					match obj.get(key.as_str()) {
						Some(value) => read_with(value, rest, ev).await,
						None => Ok(Value::None),
					}
				}
				// Every value of an object is the object, so the rest of the
				// path reads it as it stands.
				PlaceStep::All => read_with(this, rest, ev).await,
				// An object holds no members to select from by position or by
				// predicate.
				PlaceStep::Where(_)
				| PlaceStep::First
				| PlaceStep::Last
				| PlaceStep::Unaddressed => Ok(Value::None),
			},
			Value::Array(arr) => match step {
				// A key named on an array is named on each of its elements, and
				// the read is every element's value.
				PlaceStep::Field(_) | PlaceStep::Lookup(_) => {
					let mut out = Vec::with_capacity(arr.len());
					for element in arr.iter() {
						out.push(read_with(element, steps, ev).await?);
					}
					Ok(Value::from(out))
				}
				PlaceStep::All => {
					let mut out = Vec::with_capacity(arr.len());
					for element in arr.iter() {
						out.push(read_with(element, rest, ev).await?);
					}
					Ok(Value::from(out))
				}
				PlaceStep::First => match arr.first() {
					Some(element) => read_with(element, rest, ev).await,
					None => Ok(Value::None),
				},
				PlaceStep::Last => match arr.last() {
					Some(element) => read_with(element, rest, ev).await,
					None => Ok(Value::None),
				},
				PlaceStep::Where(pred) => {
					let mut accepted = Vec::new();
					for element in arr.iter() {
						if ev.accepts(pred, element).await? {
							accepted.push(element.clone());
						}
					}
					let selection = Value::from(accepted);
					read_with(&selection, rest, ev).await
				}
				PlaceStep::Subscript {
					expr,
					..
				} => match ev.subscript(expr).await? {
					Value::Number(index) => {
						match index.as_array_index().and_then(|index| arr.get(index)) {
							Some(element) => read_with(element, rest, ev).await,
							None => Ok(Value::None),
						}
					}
					// A range selects a run of elements, read as an array of
					// its own.
					Value::Range(range) => {
						let range = range
							.coerce_to_typed::<i64>()
							.map_err(|err| ControlFlow::Err(anyhow::Error::new(err)))?;
						let selection = range
							.slice(arr.as_slice())
							.map(|selected| Value::from(selected.to_vec()))
							.unwrap_or(Value::None);
						read_with(&selection, rest, ev).await
					}
					// Only a number and a range name elements of an array.
					_ => Ok(Value::None),
				},
				PlaceStep::Unaddressed => Ok(Value::None),
			},
			// A set holds what an array holds, and reads back as a set.
			Value::Set(set) => match step {
				PlaceStep::Field(_) | PlaceStep::Lookup(_) => {
					let mut out = Vec::new();
					for element in set.iter() {
						out.push(read_with(element, steps, ev).await?);
					}
					Ok(Value::Set(out.into_iter().collect()))
				}
				PlaceStep::All => {
					let mut out = Vec::new();
					for element in set.iter() {
						out.push(read_with(element, rest, ev).await?);
					}
					Ok(Value::Set(out.into_iter().collect()))
				}
				PlaceStep::First => match set.first() {
					Some(element) => read_with(element, rest, ev).await,
					None => Ok(Value::None),
				},
				PlaceStep::Last => match set.last() {
					Some(element) => read_with(element, rest, ev).await,
					None => Ok(Value::None),
				},
				PlaceStep::Where(pred) => {
					let mut accepted = Vec::new();
					for element in set.iter() {
						if ev.accepts(pred, element).await? {
							accepted.push(element.clone());
						}
					}
					let selection = Value::Set(accepted.into_iter().collect());
					read_with(&selection, rest, ev).await
				}
				PlaceStep::Subscript {
					expr,
					..
				} => match ev.subscript(expr).await? {
					Value::Number(index) => {
						match index.as_array_index().and_then(|index| set.nth(index)) {
							Some(element) => read_with(element, rest, ev).await,
							None => Ok(Value::None),
						}
					}
					Value::Range(range) => {
						let range = range
							.coerce_to_typed::<i64>()
							.map_err(|err| ControlFlow::Err(anyhow::Error::new(err)))?;
						let members: Vec<Value> = set.iter().cloned().collect();
						let selection = range
							.slice(members.as_slice())
							.map(|selected| Value::Set(selected.iter().cloned().collect()))
							.unwrap_or(Value::None);
						read_with(&selection, rest, ev).await
					}
					_ => Ok(Value::None),
				},
				PlaceStep::Unaddressed => Ok(Value::None),
			},
			// Nothing else holds a place to read: a record id is the link
			// itself rather than the record it names, and no other kind has
			// members a step addresses.
			_ => Ok(Value::None),
		}
	})
}

// ===========================================================================
// The compound mutators
// ===========================================================================

/// `+=`: add, concatenate or append, by what the place already holds.
///
/// A number adds, a string concatenates, an object merges, an array takes the
/// members of an array or set and otherwise gains one element, and a set gains
/// members. A place holding nothing starts from zero for a number, from the
/// duration, array or set itself, and from a one-element array for anything
/// else. Anything else is `TryAdd`'s question, and a pair it refuses fails the
/// statement.
async fn increment<X: PlaceEval>(
	target: &mut Value,
	steps: &[PlaceStep<X::Expr>],
	val: Value,
	ev: &mut X,
) -> Result<(), ControlFlow> {
	let next = match read_with(target, steps, ev).await? {
		Value::None => match val {
			Value::Number(x) => Value::Number(Number::Int(0) + x),
			Value::Duration(x) => Value::Duration(x),
			Value::Array(x) => Value::Array(x),
			Value::Set(x) => Value::Set(x),
			x => Value::from(vec![x]),
		},
		Value::Array(current) => match val {
			Value::Array(x) => Value::Array(current.concat(x)),
			Value::Set(x) => Value::Array(current.concat_set(x)),
			x => Value::Array(current.with_push(x)),
		},
		Value::Set(mut current) => match val {
			Value::Set(x) => {
				current.0.extend(x);
				Value::Set(current)
			}
			Value::Array(x) => {
				current.0.extend(x.0);
				Value::Set(current)
			}
			x => {
				current.insert(x);
				Value::Set(current)
			}
		},
		current => current.try_add(val).map_err(ControlFlow::Err)?,
	};
	set_with(target, steps, next, ev).await
}

/// `-=`: subtract, or remove members.
///
/// An array or a set loses the members the right-hand side names — every
/// occurrence of each, not one. A place holding nothing becomes the negated
/// number, and for anything but a number the assignment is abandoned: there is
/// nothing to take away and nothing sensible to leave. Anything else is
/// `TrySub`'s question.
async fn decrement<X: PlaceEval>(
	target: &mut Value,
	steps: &[PlaceStep<X::Expr>],
	val: Value,
	ev: &mut X,
) -> Result<(), ControlFlow> {
	let next = match read_with(target, steps, ev).await? {
		Value::Array(current) => match val {
			Value::Array(x) => Value::from(current.remove_all(&x.0)),
			Value::Set(x) => Value::from(current.remove_all_set(&x)),
			x => Value::from(current.remove_value(&x)),
		},
		Value::Set(mut current) => {
			match val {
				Value::Array(x) => {
					for item in x {
						current.remove(&item);
					}
				}
				Value::Set(x) => {
					for item in x {
						current.remove(&item);
					}
				}
				x => {
					current.remove(&x);
				}
			}
			Value::from(current)
		}
		Value::None => match val {
			Value::Number(x) => Value::from(Number::from(0) - x),
			_ => return Ok(()),
		},
		current => current.try_sub(val).map_err(ControlFlow::Err)?,
	};
	set_with(target, steps, next, ev).await
}

/// `+?`: union into an array.
///
/// The result is deduplicated, which is what separates this from `+=`. A place
/// holding nothing becomes the right-hand side as an array; a place holding
/// anything but an array or nothing fails the statement, because there is no
/// reading of a union with a scalar.
async fn extend<X: PlaceEval>(
	target: &mut Value,
	steps: &[PlaceStep<X::Expr>],
	val: Value,
	ev: &mut X,
) -> Result<(), ControlFlow> {
	let next = match read_with(target, steps, ev).await? {
		Value::Array(current) => match val {
			Value::Array(x) => Value::from(current.concat(x).uniq()),
			x => Value::from(current.with_push(x).uniq()),
		},
		Value::None => match val {
			Value::Array(x) => Value::from(x),
			x => Value::from(vec![x]),
		},
		current => {
			return Err(ControlFlow::Err(anyhow::Error::new(ExprError::TryExtend(
				current.kind_of().to_owned(),
			))));
		}
	};
	set_with(target, steps, next, ev).await
}

// ===========================================================================
// Shared helpers
// ===========================================================================

/// The element at `index` of an array value, or `NONE` where there is none.
///
/// The two filter steps walk their selection as an array and read it back
/// through this, so an index the walk removed reads as absent rather than
/// shifting the one after it into its place.
fn element_at(value: &Value, index: usize) -> Value {
	match value {
		Value::Array(arr) => arr.get(index).cloned().unwrap_or(Value::None),
		_ => Value::None,
	}
}

#[cfg(all(test, feature = "kv-mem"))]
#[allow(clippy::unwrap_used)]
mod tests {
	use super::*;
	use crate::exec::operators::test_util::{
		eval, eval_on, physical_expr, root_ctx, val, write_place,
	};

	/// Apply a `SET` clause to `record`, the way the stage does: each place
	/// compiled, each right-hand side already evaluated.
	async fn apply(record: &str, clause: &[(&str, AssignOperator, &str)]) -> Result<Value, String> {
		apply_in(&root_ctx(), record, clause).await
	}

	/// As [`apply`], but in a caller-supplied context, for a clause whose
	/// places read the statement's own scope.
	async fn apply_in(
		ctx: &ExecutionContext,
		record: &str,
		clause: &[(&str, AssignOperator, &str)],
	) -> Result<Value, String> {
		let mut target = val(record).await;
		let mut compiled = Vec::new();
		let mut computed = Vec::new();
		for (path, operator, value) in clause {
			let place = write_place(path, ctx).await;
			computed.push(ComputedAssignment {
				place: place.path().clone(),
				operator: operator.clone(),
				value: val(value).await,
			});
			compiled.push(WriteAssignment {
				place,
				operator: operator.clone(),
				value: physical_expr(value, ctx).await,
			});
		}
		match apply_set(&mut target, &compiled, &computed, ctx).await {
			Ok(()) => Ok(target),
			Err(ControlFlow::Err(err)) => Err(err.to_string()),
			Err(other) => panic!("expected a value or a failure, got {other:?}"),
		}
	}

	/// `SET <place> = <value>`, applied to `record`.
	async fn assign_one(record: &str, place: &str, value: &str) -> Value {
		apply(record, &[(place, AssignOperator::Assign, value)])
			.await
			.expect("the assignment should succeed")
	}

	/// The value `src` evaluates to, for comparing a result against.
	async fn expect(src: &str) -> Value {
		val(src).await
	}

	// =======================================================================
	// Assignment
	// =======================================================================

	/// A path the record does not have is built rather than abandoned: that is
	/// what makes `CREATE` with a nested `SET` produce anything at all.
	#[tokio::test]
	async fn an_absent_path_is_built_on_the_way_to_the_assignment() {
		assert_eq!(
			assign_one("{ }", "test.something.allow", "999").await,
			expect("{ test: { something: { allow: 999 } } }").await
		);
		// Only the missing tail is built; what the record already had stands.
		assert_eq!(
			assign_one("{ test: { other: NULL } }", "test.something.allow", "999").await,
			expect("{ test: { other: NULL, something: { allow: 999 } } }").await
		);
		// A record that is nothing at all is the same case: the whole path is
		// the missing tail.
		assert_eq!(
			assign_one("NONE", "test.something", "999").await,
			expect("{ test: { something: 999 } }").await
		);
	}

	/// A step into a value that holds no such place writes nothing. The
	/// statement succeeds: naming nothing is not an error, it is naming
	/// nothing.
	#[tokio::test]
	async fn a_step_that_addresses_nothing_writes_nothing() {
		assert_eq!(
			assign_one("{ test: { something: 123 } }", "test.something.wrong", "999").await,
			expect("{ test: { something: 123 } }").await
		);
		// An index past the end of an array names no element, and an
		// assignment does not grow the array to reach it.
		assert_eq!(
			assign_one("{ test: [1, 2] }", "test[5]", "999").await,
			expect("{ test: [1, 2] }").await
		);
	}

	/// A key named on an array is named on each of its elements, so one
	/// assignment writes many — with or without an explicit `[*]`.
	#[tokio::test]
	async fn a_key_named_on_an_array_is_written_on_every_element() {
		let record = "{ test: [{ age: 34 }, { age: 36 }] }";
		let both = expect("{ test: [{ age: 21 }, { age: 21 }] }").await;
		assert_eq!(assign_one(record, "test.age", "21").await, both);
		assert_eq!(assign_one(record, "test[*].age", "21").await, both);
		// `[*]` at the end of the path assigns to each member rather than
		// clearing the container.
		assert_eq!(assign_one(record, "test[*]", "21").await, expect("{ test: [21, 21] }").await);
	}

	/// A filter selects the elements written to, and what follows it applies
	/// to each of them.
	#[tokio::test]
	async fn a_filter_selects_the_elements_written_to() {
		let record = "{ test: [{ age: 34 }, { age: 36 }] }";
		assert_eq!(
			assign_one(record, "test[WHERE age > 35].age", "21").await,
			expect("{ test: [{ age: 34 }, { age: 21 }] }").await
		);
		// With nothing after it the filter names the elements themselves.
		assert_eq!(
			assign_one(record, "test[WHERE age > 35]", "21").await,
			expect("{ test: [{ age: 34 }, 21] }").await
		);
	}

	/// A literal index written after a filter counts within the selection, not
	/// within the array: `[WHERE …][0]` is the first accepted element. An index
	/// that is an expression rather than a literal does not take that reading —
	/// it is walked against each accepted element, where a number names an
	/// object key.
	#[tokio::test]
	async fn a_literal_index_after_a_filter_counts_within_the_selection() {
		let record = "{ test: [{ age: 34 }, { age: 36 }] }";
		assert_eq!(
			assign_one(record, "test[WHERE age > 30][0].age", "21").await,
			expect("{ test: [{ age: 21 }, { age: 36 }] }").await
		);
		assert_eq!(
			assign_one(record, "test[WHERE age > 30][0]", "21").await,
			expect("{ test: [21, { age: 36 }] }").await
		);
		assert_eq!(
			assign_one(record, "test[WHERE age > 30][0 + 0].age", "21").await,
			expect("{ test: [{ age: 34, '0': { age: 21 } }, { age: 36, '0': { age: 21 } }] }")
				.await
		);
	}

	/// A subscript is an object key on an object and an index on an array, and
	/// a key that is not a string is the text it renders to.
	#[tokio::test]
	async fn a_subscript_is_a_key_on_an_object_and_an_index_on_an_array() {
		assert_eq!(
			assign_one("{ test: [123, 456, 789] }", "test[1]", "999").await,
			expect("{ test: [123, 999, 789] }").await
		);
		// An index that is not a literal is evaluated when the place is
		// walked, and indexes just the same.
		assert_eq!(
			assign_one("{ test: [123, 456, 789] }", "test[1 + 1]", "999").await,
			expect("{ test: [123, 456, 999] }").await
		);
		assert_eq!(
			assign_one("{ test: { } }", "test['inner']", "true").await,
			expect("{ test: { inner: true } }").await
		);
		// A record id names the key it renders to, both where the key is built
		// and where it already exists.
		assert_eq!(
			assign_one("{ test: { } }", "test.other[city:london]", "true").await,
			expect("{ test: { other: { 'city:london': true } } }").await
		);
	}

	/// A range subscript selects a run of elements, and what follows applies to
	/// each.
	#[tokio::test]
	async fn a_range_subscript_writes_every_element_it_spans() {
		assert_eq!(
			assign_one("{ test: [1, 2, 3, 4] }", "test[1..3]", "9").await,
			expect("{ test: [1, 9, 9, 4] }").await
		);
	}

	/// The two positional steps address one element each, and address nothing
	/// on an empty array.
	#[tokio::test]
	async fn the_positional_steps_address_one_element() {
		assert_eq!(
			assign_one("{ test: [1, 2, 3] }", "test[$]", "9").await,
			expect("{ test: [1, 2, 9] }").await
		);
		assert_eq!(assign_one("{ test: [] }", "test[$]", "9").await, expect("{ test: [] }").await);
	}

	// =======================================================================
	// Removal
	// =======================================================================

	/// Assigning nothing removes: `SET a = NONE` leaves no `a` rather than an
	/// `a` holding `NONE`.
	#[tokio::test]
	async fn assigning_none_removes_the_place() {
		assert_eq!(
			assign_one("{ test: { other: NULL, something: 123 } }", "test.something", "NONE").await,
			expect("{ test: { other: NULL } }").await
		);
		assert_eq!(
			assign_one("{ test: [123, 456, 789] }", "test[1]", "NONE").await,
			expect("{ test: [123, 789] }").await
		);
	}

	/// A removal stops where the record stops: it builds nothing, and a path
	/// the record never had leaves it alone.
	#[tokio::test]
	async fn a_removal_builds_nothing() {
		let record = "{ test: { other: NULL, something: 123 } }";
		assert_eq!(assign_one(record, "test.something.wrong", "NONE").await, expect(record).await);
		assert_eq!(assign_one(record, "test.other.something", "NONE").await, expect(record).await);
	}

	/// A filter at the end of a path names the accepted elements themselves,
	/// and they go; a step after it reaches into each of them instead.
	#[tokio::test]
	async fn a_filtered_removal_drops_the_elements_it_accepts() {
		let record = "{ test: [{ name: 'A', age: 34 }, { name: 'B', age: 36 }] }";
		assert_eq!(
			assign_one(record, "test[WHERE age > 35]", "NONE").await,
			expect("{ test: [{ name: 'A', age: 34 }] }").await
		);
		assert_eq!(
			assign_one(record, "test[WHERE age > 35].age", "NONE").await,
			expect("{ test: [{ name: 'A', age: 34 }, { name: 'B' }] }").await
		);
	}

	/// A subscript after a filter counts within the selection, and a member the
	/// walk removed from the selection is removed from the array.
	#[tokio::test]
	async fn a_subscript_after_a_filter_removes_from_the_selection() {
		let record = "{ test: [{ name: 'A', age: 34 }, { name: 'B', age: 36 }] }";
		assert_eq!(
			assign_one(record, "test[WHERE age > 30][0]", "NONE").await,
			expect("{ test: [{ name: 'B', age: 36 }] }").await
		);
	}

	/// `[*]` clears the container at the end of a removal's path, where the
	/// same step at the end of an assignment's path writes to each member.
	#[tokio::test]
	async fn all_clears_the_container_at_the_end_of_a_removal() {
		assert_eq!(
			assign_one("{ test: [1, 2] }", "test[*]", "NONE").await,
			expect("{ test: [] }").await
		);
		assert_eq!(
			assign_one("{ test: { a: 1, b: 2 } }", "test[*]", "NONE").await,
			expect("{ test: { } }").await
		);
		assert_eq!(
			assign_one("{ test: [{ a: 1, b: 2 }] }", "test[*].a", "NONE").await,
			expect("{ test: [{ b: 2 }] }").await
		);
	}

	/// A key named on an array is removed from each of its elements.
	#[tokio::test]
	async fn a_key_named_on_an_array_is_removed_from_every_element() {
		assert_eq!(
			assign_one("{ test: [{ a: 1, b: 2 }, { a: 3 }] }", "test.a", "NONE").await,
			expect("{ test: [{ b: 2 }, { }] }").await
		);
	}

	// =======================================================================
	// The compound operators
	// =======================================================================

	/// A compound operator against a place the record does not hold combines
	/// with `NONE`, and each of the three reads that differently.
	#[tokio::test]
	async fn a_compound_operator_against_an_absent_place_starts_from_nothing() {
		assert_eq!(
			apply("{ }", &[("n", AssignOperator::Add, "10")]).await.unwrap(),
			expect("{ n: 10 }").await
		);
		assert_eq!(
			apply("{ }", &[("n", AssignOperator::Subtract, "10")]).await.unwrap(),
			expect("{ n: -10 }").await
		);
		// `+=` with anything but a number, a duration or a collection starts a
		// one-element array.
		assert_eq!(
			apply("{ }", &[("n", AssignOperator::Add, "'x'")]).await.unwrap(),
			expect("{ n: ['x'] }").await
		);
		assert_eq!(
			apply("{ }", &[("n", AssignOperator::Extend, "1")]).await.unwrap(),
			expect("{ n: [1] }").await
		);
		// `-=` with anything but a number has nothing to take away and nothing
		// to leave, so the assignment is abandoned and no key appears.
		assert_eq!(
			apply("{ }", &[("n", AssignOperator::Subtract, "'x'")]).await.unwrap(),
			expect("{ }").await
		);
	}

	/// What each compound operator does to a place the record does hold.
	#[tokio::test]
	async fn the_compound_operators_combine_with_what_the_place_holds() {
		assert_eq!(
			apply("{ n: 100 }", &[("n", AssignOperator::Add, "10")]).await.unwrap(),
			expect("{ n: 110 }").await
		);
		assert_eq!(
			apply("{ n: 'hello' }", &[("n", AssignOperator::Add, "' world'")]).await.unwrap(),
			expect("{ n: 'hello world' }").await
		);
		// `+=` appends to an array without deduplicating; `+?` unions.
		assert_eq!(
			apply("{ n: [1, 2] }", &[("n", AssignOperator::Add, "[2, 3]")]).await.unwrap(),
			expect("{ n: [1, 2, 2, 3] }").await
		);
		assert_eq!(
			apply("{ n: [1, 2] }", &[("n", AssignOperator::Extend, "[2, 3]")]).await.unwrap(),
			expect("{ n: [1, 2, 3] }").await
		);
		// `-=` removes every occurrence of what it names.
		assert_eq!(
			apply("{ n: [1, 2, 1] }", &[("n", AssignOperator::Subtract, "1")]).await.unwrap(),
			expect("{ n: [2] }").await
		);
	}

	/// A compound operator reads the record as it stands, so an assignment
	/// earlier in the same clause is what it combines with.
	#[tokio::test]
	async fn a_compound_operator_reads_the_assignments_before_it() {
		assert_eq!(
			apply("{ }", &[("n", AssignOperator::Assign, "5"), ("n", AssignOperator::Add, "1")])
				.await
				.unwrap(),
			expect("{ n: 6 }").await
		);
	}

	/// A compound operator reads through the same path it writes to, so it
	/// combines with the element the place names rather than the container.
	#[tokio::test]
	async fn a_compound_operator_reads_the_place_it_writes() {
		assert_eq!(
			apply("{ test: [100, 200, 300] }", &[("test[1]", AssignOperator::Add, "10")])
				.await
				.unwrap(),
			expect("{ test: [100, 210, 300] }").await
		);
	}

	/// A place's subscript names its key with no record in scope, and the read
	/// resolves it in that same frame, so a compound operator combines with the
	/// value at the key it writes to.
	///
	/// `k ?? $k` resolves in both scopes and to different keys: with the record
	/// in scope the field `k` names `m`, and with only the statement's scope the
	/// field is nothing and the parameter names `n`. The place is the parameter's
	/// key, so `m` must be left alone.
	#[tokio::test]
	async fn a_compound_operator_reads_the_key_its_subscript_writes() {
		let ctx = root_ctx().with_param("k", Value::from("n"));
		let record = "{ k: 'm', a: { n: 1, m: 100 } }";
		// The two frames disagree, which is what makes the assertion below
		// discriminating: with the record in scope the subscript names `m`,
		// and in the frame a place's subscript is evaluated in it names `n`.
		assert_eq!(eval_on("k ?? $k", &val(record).await, &ctx).await.unwrap(), val("'m'").await);
		assert_eq!(eval("k ?? $k", &ctx).await.unwrap(), val("'n'").await);
		assert_eq!(
			apply_in(&ctx, record, &[("a[k ?? $k]", AssignOperator::Add, "10")]).await.unwrap(),
			expect("{ k: 'm', a: { n: 11, m: 100 } }").await
		);
	}

	/// A pair no compound operator has a reading of fails the statement rather
	/// than writing a guess.
	#[tokio::test]
	async fn a_combination_with_no_reading_fails_the_statement() {
		assert!(
			apply("{ n: 100 }", &[("n", AssignOperator::Add, "'hello'")]).await.is_err(),
			"a number and a string do not add"
		);
		assert!(
			apply("{ n: { a: 1 } }", &[("n", AssignOperator::Subtract, "10")]).await.is_err(),
			"an object and a number do not subtract"
		);
		let refused = apply("{ n: 100 }", &[("n", AssignOperator::Extend, "[1]")])
			.await
			.expect_err("a number cannot be extended");
		assert!(refused.contains("int"), "the refusal must name the kind, got: {refused}");
	}

	// =======================================================================
	// The compound read
	// =======================================================================

	/// What the place `path` names in `record`, which is what a compound
	/// operator combines with.
	async fn read_at(record: &str, path: &str) -> Value {
		let ctx = root_ctx();
		let place = write_place(path, &ctx).await;
		let target = val(record).await;
		read(&target, place.steps(), &ctx).await.expect("the read should succeed")
	}

	/// The read yields what the record holds and `NONE` where it holds nothing:
	/// it neither builds the path an assignment would nor stops short the way a
	/// removal does.
	#[tokio::test]
	async fn the_read_of_an_object_is_the_key_a_step_names() {
		let record = "{ a: { b: 1 } }";
		assert_eq!(read_at(record, "a.b").await, expect("1").await);
		assert_eq!(read_at(record, "a").await, expect("{ b: 1 }").await);
		// An absent key, and a step past one, name nothing.
		assert_eq!(read_at(record, "a.missing").await, Value::None);
		assert_eq!(read_at(record, "missing.b.c").await, Value::None);
		// A lookup is the constant key it renders to, which is the key the
		// write walk writes.
		assert_eq!(read_at("{ a: { '->b': 1 } }", "a->b").await, expect("1").await);
		// The empty place is the record itself.
		let ctx = root_ctx();
		let target = val(record).await;
		assert_eq!(read(&target, &[], &ctx).await.unwrap(), target);
	}

	/// A subscript is an object key and an array index, and a value that names
	/// neither reads nothing.
	#[tokio::test]
	async fn the_read_of_a_subscript_is_a_key_or_an_index() {
		// On an object a string is the key itself, and a number or a record id
		// the text it renders to.
		assert_eq!(read_at("{ a: { b: 'x' } }", "a['b']").await, expect("'x'").await);
		assert_eq!(read_at("{ a: { '1': 'x' } }", "a[1]").await, expect("'x'").await);
		assert_eq!(
			read_at("{ a: { 'city:london': 'x' } }", "a[city:london]").await,
			expect("'x'").await
		);
		assert_eq!(read_at("{ a: { b: 'x' } }", "a[true]").await, Value::None);
		// On an array only a number and a range name elements, and an index
		// past the end names none.
		assert_eq!(read_at("{ a: [10, 20, 30] }", "a[1]").await, expect("20").await);
		assert_eq!(read_at("{ a: [10, 20, 30] }", "a[5]").await, Value::None);
		assert_eq!(read_at("{ a: [10, 20, 30] }", "a['b']").await, Value::None);
		assert_eq!(read_at("{ a: [10, 20, 30, 40] }", "a[1..3]").await, expect("[20, 30]").await);
	}

	/// A step that names a set of members reads all of them as one collection,
	/// which is what a compound operator at such a place combines with — and
	/// what it then writes back to every member.
	#[tokio::test]
	async fn the_read_of_many_members_is_one_collection() {
		let record = "{ a: [{ n: 1 }, { n: 2 }] }";
		// A key named on an array is named on each element, with or without an
		// explicit `[*]`.
		assert_eq!(read_at(record, "a.n").await, expect("[1, 2]").await);
		assert_eq!(read_at(record, "a[*].n").await, expect("[1, 2]").await);
		assert_eq!(
			apply(record, &[("a[*].n", AssignOperator::Add, "10")]).await.unwrap(),
			expect("{ a: [{ n: [1, 2, 10] }, { n: [1, 2, 10] }] }").await
		);
		// Every value of an object is the object, so the rest of the path reads
		// it as it stands.
		assert_eq!(read_at("{ a: { n: 1 } }", "a[*].n").await, expect("1").await);
		// A filter keeps the elements it accepts and the rest of the path
		// continues on that selection, so an index after it counts within the
		// selection rather than within the array.
		assert_eq!(read_at(record, "a[WHERE n > 1].n").await, expect("[2]").await);
		assert_eq!(read_at(record, "a[WHERE n > 0][1].n").await, expect("2").await);
		// The positional steps name one element each, and none of an empty
		// array.
		assert_eq!(read_at(record, "a[0].n").await, expect("1").await);
		assert_eq!(read_at(record, "a[$].n").await, expect("2").await);
		assert_eq!(read_at("{ a: [] }", "a[$]").await, Value::None);
	}

	/// A set holds what an array holds and reads back as a set.
	#[tokio::test]
	async fn the_read_of_a_set_is_a_set() {
		let record = "{ a: <set>[{ n: 1 }, { n: 2 }] }";
		assert_eq!(read_at(record, "a.n").await, expect("<set>[1, 2]").await);
		assert_eq!(read_at(record, "a[*].n").await, expect("<set>[1, 2]").await);
		assert_eq!(read_at(record, "a[WHERE n > 1].n").await, expect("<set>[2]").await);
		assert_eq!(read_at(record, "a[0].n").await, expect("1").await);
		assert_eq!(read_at(record, "a[$].n").await, expect("2").await);
		let numbers = "{ a: <set>[10, 20, 30, 40] }";
		assert_eq!(read_at(numbers, "a[1]").await, expect("20").await);
		assert_eq!(read_at(numbers, "a[1..3]").await, expect("<set>[20, 30]").await);
	}

	/// The read stops at a record link rather than fetching the record it
	/// names. A step past a link addresses a field of another record, and the
	/// assignment writes to none of it — it replaces the link — so the value
	/// combined with is the one at the address written to.
	#[tokio::test]
	async fn the_read_stops_at_a_record_link() {
		assert_eq!(read_at("{ friend: person:bob }", "friend.n").await, Value::None);
		// Where the path ends at the link, the link is the value.
		assert_eq!(read_at("{ friend: person:bob }", "friend").await, expect("person:bob").await);
		assert_eq!(
			apply("{ friend: person:bob }", &[("friend.n", AssignOperator::Add, "1")])
				.await
				.unwrap(),
			expect("{ friend: { n: 1 } }").await
		);
	}

	/// Nothing else holds a place to read, so the compound operators see `NONE`
	/// and take their absent-place reading of it.
	#[tokio::test]
	async fn the_read_of_a_value_with_no_members_is_nothing() {
		assert_eq!(read_at("{ a: 1 }", "a.b").await, Value::None);
		assert_eq!(read_at("{ a: 'text' }", "a[0]").await, Value::None);
		assert_eq!(read_at("{ a: NULL }", "a.b").await, Value::None);
	}

	// =======================================================================
	// `$input`
	// =======================================================================

	/// `$input` for a `SET` clause is what its assignments make of an empty
	/// object, so a compound operator there resolves against `NONE` the way it
	/// would on a record being created.
	#[tokio::test]
	async fn input_is_a_set_clause_applied_to_nothing() {
		let ctx = root_ctx();
		let place = write_place("n", &ctx).await;
		let computed = vec![ComputedAssignment {
			place: place.path().clone(),
			operator: AssignOperator::Add,
			value: val("10").await,
		}];
		let clause = WriteData::Set(vec![WriteAssignment {
			place,
			operator: AssignOperator::Add,
			value: physical_expr("10", &ctx).await,
		}]);
		let input = ComputedInput::Set(computed);
		let materialised = materialise_input(&clause, &input, &ctx).await.unwrap();
		assert_eq!(*materialised, expect("{ n: 10 }").await);
	}

	/// Every other clause form already carries the value the statement
	/// supplied, and an `UNSET` supplies none.
	#[tokio::test]
	async fn input_for_the_other_clause_forms_is_the_value_itself() {
		let ctx = root_ctx();
		let supplied = Arc::new(expect("{ a: 1 }").await);
		let clause = WriteData::Content(physical_expr("{ a: 1 }", &ctx).await);
		let input = ComputedInput::Content(Arc::clone(&supplied));
		assert_eq!(*materialise_input(&clause, &input, &ctx).await.unwrap(), *supplied);

		let clause = WriteData::Unset(vec![write_place("a", &ctx).await]);
		let input = ComputedInput::Unset(vec![crate::expr::Idiom::field("a".to_owned())]);
		assert_eq!(*materialise_input(&clause, &input, &ctx).await.unwrap(), Value::None);
	}
}
