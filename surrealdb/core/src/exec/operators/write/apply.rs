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
			AssignOperator::Assign => {
				if let Value::None = value {
					del(target, steps, ev).await?;
				} else {
					set_with(target, steps, value, ev).await?;
				}
			}
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
				PlaceStep::Field(name) | PlaceStep::Lookup(name) => {
					if let Value::Object(obj) = place {
						match obj.entry(name.as_str()) {
							Entry::Vacant(vacant) => {
								let dead_end = vacant.insert(Value::None);
								return assign(dead_end, val, rest, ev).await;
							}
							Entry::Occupied(occupied) => place = occupied.into_mut(),
						}
					} else if let Value::Array(arr) = place {
						for element in arr.iter_mut() {
							set_with(element, from_here, val.clone(), ev).await?;
						}
						return Ok(());
					} else {
						return Ok(());
					}
				}
				PlaceStep::Subscript {
					expr,
					..
				} => {
					let key = ev.subscript(expr).await?;
					if let Value::Object(obj) = place {
						let key = if let Value::String(key) = key {
							key.as_str().to_owned()
						} else {
							key.to_sql()
						};
						match obj.entry(key) {
							Entry::Vacant(vacant) => {
								let dead_end = vacant.insert(Value::None);
								return assign(dead_end, val, rest, ev).await;
							}
							Entry::Occupied(occupied) => place = occupied.into_mut(),
						}
					} else if let Value::Array(arr) = place {
						if let Value::Range(range) = key {
							let range = range
								.coerce_to_typed::<i64>()
								.map_err(|err| ControlFlow::Err(anyhow::Error::new(err)))?;
							if let Some(selected) = range.slice_mut(arr) {
								for element in selected.iter_mut() {
									set_with(element, rest, val.clone(), ev).await?;
								}
							}
							return Ok(());
						} else if let Value::Number(index) = key {
							let Some(index) = index.as_array_index() else {
								return Ok(());
							};
							// An index past the end names no element, and
							// an assignment does not grow the array.
							let Some(element) = arr.get_mut(index) else {
								return Ok(());
							};
							place = element;
						} else {
							return Ok(());
						}
					} else {
						return Ok(());
					}
				}
				PlaceStep::First => {
					if let Value::Array(arr) = place {
						let Some(element) = arr.first_mut() else {
							return Ok(());
						};
						place = element;
					} else if let Value::Set(set) = place {
						let Some(element) = set.first_mut() else {
							return Ok(());
						};
						place = element;
					} else {
						return Ok(());
					}
				}
				PlaceStep::Last => {
					if let Value::Array(arr) = place {
						let Some(element) = arr.last_mut() else {
							return Ok(());
						};
						place = element;
					} else if let Value::Set(set) = place {
						let Some(element) = set.last_mut() else {
							return Ok(());
						};
						place = element;
					} else {
						return Ok(());
					}
				}
				// Every member, and nothing when the value has no members.
				PlaceStep::All => {
					if let Value::Array(arr) = place {
						for element in arr.iter_mut() {
							set_with(element, rest, val.clone(), ev).await?;
						}
					} else if let Value::Object(obj) = place {
						for (_, element) in obj.iter_mut() {
							set_with(element, rest, val.clone(), ev).await?;
						}
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
				Value::None
				| Value::Null
				| Value::Bool(_)
				| Value::Duration(_)
				| Value::Datetime(_)
				| Value::Date(_)
				| Value::Time(_)
				| Value::Timestamp(_)
				| Value::Interval(_)
				| Value::Uuid(_)
				| Value::Array(_)
				| Value::Set(_)
				| Value::Object(_)
				| Value::Geometry(_)
				| Value::Bytes(_)
				| Value::Table(_)
				| Value::File(_)
				| Value::Regex(_)
				| Value::Closure(_) => return Ok(()),
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
		if let Value::Object(obj) = this {
			match step {
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
						Value::None
						| Value::Null
						| Value::Bool(_)
						| Value::Duration(_)
						| Value::Datetime(_)
						| Value::Date(_)
						| Value::Time(_)
						| Value::Timestamp(_)
						| Value::Interval(_)
						| Value::Uuid(_)
						| Value::Array(_)
						| Value::Set(_)
						| Value::Object(_)
						| Value::Geometry(_)
						| Value::Bytes(_)
						| Value::Table(_)
						| Value::File(_)
						| Value::Regex(_)
						| Value::Range(_)
						| Value::Closure(_) => return Ok(()),
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
			}
		} else if let Value::Array(arr) = this {
			match step {
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
					} else if let [
						PlaceStep::Subscript {
							expr,
							..
						},
						tail @ ..,
					] = rest
					{
						// A subscript written after the filter counts within
						// the selection, and maps the selected index directly
						// to its position in the array.
						let mut positions = Vec::new();
						for (position, element) in arr.iter().enumerate() {
							if ev.accepts(pred, element).await? {
								positions.push(position);
							}
						}
						if let Value::Number(index) = ev.subscript(expr).await?
							&& let Some(index) = index.as_array_index()
							&& let Some(&position) = positions.get(index)
						{
							if tail.is_empty() {
								arr.remove(position);
							} else if let Some(element) = arr.get_mut(position) {
								del(element, tail, ev).await?;
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
			}
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
						Value::None
						| Value::Null
						| Value::Bool(_)
						| Value::Duration(_)
						| Value::Datetime(_)
						| Value::Date(_)
						| Value::Time(_)
						| Value::Timestamp(_)
						| Value::Interval(_)
						| Value::Uuid(_)
						| Value::Array(_)
						| Value::Set(_)
						| Value::Object(_)
						| Value::Geometry(_)
						| Value::Bytes(_)
						| Value::Table(_)
						| Value::File(_)
						| Value::Regex(_)
						| Value::Range(_)
						| Value::Closure(_) => return Ok(Value::None),
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
				} => {
					let sub = ev.subscript(expr).await?;
					if let Value::Number(index) = sub {
						match index.as_array_index().and_then(|index| arr.get(index)) {
							Some(element) => read_with(element, rest, ev).await,
							None => Ok(Value::None),
						}
					} else if let Value::Range(range) = sub {
						let range = range
							.coerce_to_typed::<i64>()
							.map_err(|err| ControlFlow::Err(anyhow::Error::new(err)))?;
						let selection = range
							.slice(arr.as_slice())
							.map(|selected| Value::from(selected.to_vec()))
							.unwrap_or(Value::None);
						read_with(&selection, rest, ev).await
					} else {
						Ok(Value::None)
					}
				}
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
				} => {
					let sub = ev.subscript(expr).await?;
					if let Value::Number(index) = sub {
						match index.as_array_index().and_then(|index| set.nth(index)) {
							Some(element) => read_with(element, rest, ev).await,
							None => Ok(Value::None),
						}
					} else if let Value::Range(range) = sub {
						let range = range
							.coerce_to_typed::<i64>()
							.map_err(|err| ControlFlow::Err(anyhow::Error::new(err)))?;
						let members: Vec<Value> = set.iter().cloned().collect();
						let selection = range
							.slice(members.as_slice())
							.map(|selected| Value::Set(selected.iter().cloned().collect()))
							.unwrap_or(Value::None);
						read_with(&selection, rest, ev).await
					} else {
						Ok(Value::None)
					}
				}
				PlaceStep::Unaddressed => Ok(Value::None),
			},
			// Nothing else holds a place to read: a record id is the link
			// itself rather than the record it names, and no other kind has
			// members a step addresses.
			Value::None
			| Value::Null
			| Value::Bool(_)
			| Value::Number(_)
			| Value::String(_)
			| Value::Duration(_)
			| Value::Datetime(_)
			| Value::Date(_)
			| Value::Time(_)
			| Value::Timestamp(_)
			| Value::Interval(_)
			| Value::Uuid(_)
			| Value::Geometry(_)
			| Value::Bytes(_)
			| Value::Table(_)
			| Value::RecordId(_)
			| Value::File(_)
			| Value::Regex(_)
			| Value::Range(_)
			| Value::Closure(_) => Ok(Value::None),
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
			other @ (Value::None
			| Value::Null
			| Value::Bool(_)
			| Value::String(_)
			| Value::Datetime(_)
			| Value::Date(_)
			| Value::Time(_)
			| Value::Timestamp(_)
			| Value::Interval(_)
			| Value::Uuid(_)
			| Value::Object(_)
			| Value::Geometry(_)
			| Value::Bytes(_)
			| Value::Table(_)
			| Value::RecordId(_)
			| Value::File(_)
			| Value::Regex(_)
			| Value::Range(_)
			| Value::Closure(_)) => Value::from(vec![other]),
		},
		Value::Array(current) => {
			if let Value::Array(x) = val {
				Value::Array(current.concat(x))
			} else if let Value::Set(x) = val {
				Value::Array(current.concat_set(x))
			} else {
				Value::Array(current.with_push(val))
			}
		}
		Value::Set(mut current) => {
			if let Value::Set(x) = val {
				current.0.extend(x);
				Value::Set(current)
			} else if let Value::Array(x) = val {
				current.0.extend(x.0);
				Value::Set(current)
			} else {
				current.insert(val);
				Value::Set(current)
			}
		}
		current @ (Value::Null
		| Value::Bool(_)
		| Value::Number(_)
		| Value::String(_)
		| Value::Duration(_)
		| Value::Datetime(_)
		| Value::Date(_)
		| Value::Time(_)
		| Value::Timestamp(_)
		| Value::Interval(_)
		| Value::Uuid(_)
		| Value::Object(_)
		| Value::Geometry(_)
		| Value::Bytes(_)
		| Value::Table(_)
		| Value::RecordId(_)
		| Value::File(_)
		| Value::Regex(_)
		| Value::Range(_)
		| Value::Closure(_)) => current.try_add(val).map_err(ControlFlow::Err)?,
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
		Value::Array(current) => {
			if let Value::Array(x) = val {
				Value::from(current.remove_all(&x.0))
			} else if let Value::Set(x) = val {
				Value::from(current.remove_all_set(&x))
			} else {
				Value::from(current.remove_value(&val))
			}
		}
		Value::Set(mut current) => {
			if let Value::Array(x) = val {
				for item in x {
					current.remove(&item);
				}
			} else if let Value::Set(x) = val {
				for item in x {
					current.remove(&item);
				}
			} else {
				current.remove(&val);
			}
			Value::from(current)
		}
		Value::None => {
			if let Value::Number(x) = val {
				Value::from(Number::from(0) - x)
			} else {
				return Ok(());
			}
		}
		current @ (Value::Null
		| Value::Bool(_)
		| Value::Number(_)
		| Value::String(_)
		| Value::Duration(_)
		| Value::Datetime(_)
		| Value::Date(_)
		| Value::Time(_)
		| Value::Timestamp(_)
		| Value::Interval(_)
		| Value::Uuid(_)
		| Value::Object(_)
		| Value::Geometry(_)
		| Value::Bytes(_)
		| Value::Table(_)
		| Value::RecordId(_)
		| Value::File(_)
		| Value::Regex(_)
		| Value::Range(_)
		| Value::Closure(_)) => current.try_sub(val).map_err(ControlFlow::Err)?,
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
	let target_val = read_with(target, steps, ev).await?;
	let next = if let Value::Array(current) = target_val {
		if let Value::Array(x) = val {
			Value::from(current.concat(x).uniq())
		} else {
			Value::from(current.with_push(val).uniq())
		}
	} else if let Value::None = target_val {
		if let Value::Array(x) = val {
			Value::from(x)
		} else {
			Value::from(vec![val])
		}
	} else {
		return Err(ControlFlow::Err(anyhow::Error::new(ExprError::TryExtend(
			target_val.kind_of().to_owned(),
		))));
	};
	set_with(target, steps, next, ev).await
}

// ===========================================================================
// Shared helpers
// ===========================================================================

/// The element at `index` of an array value, or `NONE` where there is none.
fn element_at(value: &Value, index: usize) -> Value {
	if let Value::Array(arr) = value {
		arr.get(index).cloned().unwrap_or(Value::None)
	} else {
		Value::None
	}
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
	use super::*;
	use crate::val::Array;

	enum StepExpr {
		Subscript(Value),
		EvenInt,
	}

	struct TestEval;

	impl PlaceEval for TestEval {
		type Expr = StepExpr;

		fn subscript<'a>(
			&'a mut self,
			expr: &'a Self::Expr,
		) -> BoxFut<'a, Result<Value, ControlFlow>> {
			Box::pin(async move {
				match expr {
					StepExpr::Subscript(index) => Ok(index.clone()),
					StepExpr::EvenInt => Ok(Value::None),
				}
			})
		}

		fn accepts<'a>(
			&'a mut self,
			expr: &'a Self::Expr,
			element: &'a Value,
		) -> BoxFut<'a, Result<bool, ControlFlow>> {
			Box::pin(async move {
				match expr {
					StepExpr::EvenInt => {
						if let Value::Number(Number::Int(n)) = element {
							Ok(*n % 2 == 0)
						} else if let Value::Object(obj) = element {
							Ok(matches!(
								obj.get("v"),
								Some(Value::Number(Number::Int(n))) if *n % 2 == 0
							))
						} else {
							Ok(false)
						}
					}
					StepExpr::Subscript(_) => Ok(false),
				}
			})
		}
	}

	/// `{ a: [1, 2, 3, 4, 6] }` after deleting `a[WHERE even][index]`.
	async fn del_where_subscript(index: Value) -> Option<Value> {
		let mut doc = Value::Object(Object::from_iter([(
			"a".to_string(),
			Value::Array(Array::from(vec![
				Value::from(1_i64),
				Value::from(2_i64),
				Value::from(3_i64),
				Value::from(4_i64),
				Value::from(6_i64),
			])),
		)]));
		let steps = vec![
			PlaceStep::Field("a".to_string()),
			PlaceStep::Where(StepExpr::EvenInt),
			PlaceStep::Subscript {
				expr: StepExpr::Subscript(index),
				literal_index: true,
			},
		];
		del(&mut doc, &steps, &mut TestEval).await.unwrap();

		let Value::Object(obj) = &doc else {
			panic!("expected object");
		};
		obj.get("a").cloned()
	}

	#[tokio::test]
	async fn del_where_subscript_removes_selected_position_without_corrupting_array() {
		// Matching positions for EvenInt are [1, 3, 4] (values 2, 4, 6).
		// Removing selection[0] (value 2 at array position 1) must remove index 1
		// and leave [1, 3, 4, 6] intact.
		assert_eq!(
			del_where_subscript(Value::from(0_i64)).await,
			Some(Value::Array(Array::from(vec![
				Value::from(1_i64),
				Value::from(3_i64),
				Value::from(4_i64),
				Value::from(6_i64),
			])))
		);
	}

	#[tokio::test]
	async fn del_where_subscript_accepts_every_exact_numeric_index() {
		// selection[1] is value 4 at array position 3, whichever exact numeric
		// variant names it.
		let expected = Some(Value::Array(Array::from(vec![
			Value::from(1_i64),
			Value::from(2_i64),
			Value::from(3_i64),
			Value::from(6_i64),
		])));
		assert_eq!(del_where_subscript(Value::from(1.0_f64)).await, expected);
		assert_eq!(
			del_where_subscript(Value::Number(Number::Decimal(rust_decimal::Decimal::ONE))).await,
			expected
		);
		// A fractional or negative index names no element, so nothing is removed.
		let untouched = Some(Value::Array(Array::from(vec![
			Value::from(1_i64),
			Value::from(2_i64),
			Value::from(3_i64),
			Value::from(4_i64),
			Value::from(6_i64),
		])));
		assert_eq!(del_where_subscript(Value::from(0.5_f64)).await, untouched);
		assert_eq!(del_where_subscript(Value::from(-1_i64)).await, untouched);
	}
}
