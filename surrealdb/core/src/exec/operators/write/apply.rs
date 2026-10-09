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

use surrealdb_collections::Entry;
use surrealdb_types::ToSql;

use super::place::{PlaceEval, PlaceStep};
use crate::doc::alter::ComputedAssignment;
use crate::exec::BoxFut;
use crate::expr::{AssignOperator, ControlFlow, Error as ExprError};
use crate::val::array::Uniq;
use crate::val::{Number, Object, Strand, TryAdd, TrySub, Value};

// ===========================================================================
// A clause, applied
// ===========================================================================

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

// ===========================================================================
// Assignment
// ===========================================================================

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

	// =======================================================================
	// Assignment
	// =======================================================================

	// =======================================================================
	// Removal
	// =======================================================================

	// =======================================================================
	// The compound operators
	// =======================================================================

	// =======================================================================
	// The compound read
	// =======================================================================

	// =======================================================================
	// `$input`
	// =======================================================================
}
