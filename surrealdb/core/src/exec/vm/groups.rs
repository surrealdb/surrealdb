//! An aggregate's groups, their states side by side in slabs.
//!
//! Every group of one aggregate has the same shape: an accumulator for each
//! call of each field, and a first value for each field. [`Groups`] keeps
//! group `g`'s accumulators at `accumulators[g * width .. (g + 1) * width]`
//! and its first values at `firsts[g * fields .. (g + 1) * fields]`, so
//! opening a group pushes onto two vectors whose capacity is reused, and a
//! built-in aggregate's state ([`AccumulatorState`]) is held inline. Opening a
//! group allocates only for its key, and nothing at all under `GROUP ALL` or
//! for a single key of a scalar kind.
//!
//! Two rows belong to the same group iff their keys compare equal under
//! `Value: Ord`, so the key index is ordered (the operator tree's `GroupMap`
//! says why a hash does not serve), and the groups are finished in key order. A single key is
//! indexed by its value, a compound key by the vector of its values; both order as the
//! vector of the key's values does.

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::exec::function::{AccumulatorState, AggregateFunction};
use crate::val::Value;

/// The groups of one aggregate. See the module documentation.
pub(super) struct Groups {
	index: KeyIndex,
	/// The key the row being folded is building, one value per key; empty
	/// under `GROUP ALL`.
	key: Vec<Value>,
	/// The group the row selected.
	current: usize,
	/// Where each field's accumulators start within a group's, with the
	/// total last: field `f`'s are `starts[f] .. starts[f + 1]`.
	starts: Arc<[u32]>,
	accumulators: Vec<AccumulatorState>,
	firsts: Vec<Option<Value>>,
	/// How many groups are open.
	len: usize,
}

/// A group's place by its key.
enum KeyIndex {
	/// `GROUP ALL`: one group, open from the start.
	All,
	One(BTreeMap<Value, usize>),
	Many(BTreeMap<Vec<Value>, usize>),
}

/// A finished group's key, as the drain hands it over.
pub(super) enum Key {
	All,
	One(Value),
	Many(Vec<Value>),
}

impl Key {
	pub(super) fn as_slice(&self) -> &[Value] {
		match self {
			Key::All => &[],
			Key::One(value) => std::slice::from_ref(value),
			Key::Many(values) => values,
		}
	}
}

/// One group's states, borrowed from the slabs.
pub(super) struct GroupMut<'g> {
	starts: &'g [u32],
	accumulators: &'g mut [AccumulatorState],
	firsts: &'g mut [Option<Value>],
}

impl GroupMut<'_> {
	/// The accumulators of field `field`, one per call.
	pub(super) fn accumulators(&mut self, field: usize) -> &mut [AccumulatorState] {
		let (start, end) = (self.starts[field] as usize, self.starts[field + 1] as usize);
		&mut self.accumulators[start..end]
	}

	/// The first value of field `field`, `None` until evaluated.
	pub(super) fn first(&mut self, field: usize) -> &mut Option<Value> {
		&mut self.firsts[field]
	}
}

/// The groups of an aggregate once no row will be folded into them, in key
/// order.
pub(super) struct Finished {
	keys: Vec<(Key, usize)>,
	starts: Arc<[u32]>,
	accumulators: Vec<AccumulatorState>,
	firsts: Vec<Option<Value>>,
}

impl Finished {
	/// How many groups there are.
	pub(super) fn len(&self) -> usize {
		self.keys.len()
	}

	/// The `at`th group in key order: its key and its states.
	pub(super) fn group(&mut self, at: usize) -> (&[Value], GroupMut<'_>) {
		let (key, idx) = &self.keys[at];
		let width = *self.starts.last().unwrap_or(&0) as usize;
		let fields = self.starts.len() - 1;
		let group = GroupMut {
			starts: &self.starts,
			accumulators: &mut self.accumulators[idx * width..(idx + 1) * width],
			firsts: &mut self.firsts[idx * fields..(idx + 1) * fields],
		};
		(key.as_slice(), group)
	}
}

impl Groups {
	/// An aggregate's groups before any row has been folded into them: with
	/// `keys` grouping keys, and an accumulator for each of `functions`, by
	/// field and call.
	pub(super) fn new(keys: usize, functions: &[Vec<Arc<dyn AggregateFunction>>]) -> Self {
		let mut starts = Vec::with_capacity(functions.len() + 1);
		let mut at = 0u32;
		starts.push(at);
		for calls in functions {
			at += calls.len() as u32;
			starts.push(at);
		}
		let mut groups = Self {
			index: match keys {
				0 => KeyIndex::All,
				1 => KeyIndex::One(BTreeMap::new()),
				_ => KeyIndex::Many(BTreeMap::new()),
			},
			key: vec![Value::None; keys],
			current: 0,
			starts: starts.into(),
			accumulators: Vec::new(),
			firsts: Vec::new(),
			len: 0,
		};
		if keys == 0 {
			groups.open(functions);
		}
		groups
	}

	fn fields(&self) -> usize {
		self.starts.len() - 1
	}

	fn width(&self) -> usize {
		*self.starts.last().unwrap_or(&0) as usize
	}

	/// Push a fresh group's states and answer its index.
	fn open(&mut self, functions: &[Vec<Arc<dyn AggregateFunction>>]) -> usize {
		for calls in functions {
			self.accumulators.extend(calls.iter().map(|function| function.accumulator_state()));
		}
		self.firsts.extend((0..functions.len()).map(|_| None));
		self.len += 1;
		self.len - 1
	}

	/// Set key `index` of the row being folded.
	pub(super) fn set_key(&mut self, index: usize, value: Value) {
		if let Some(slot) = self.key.get_mut(index) {
			*slot = value;
		}
	}

	/// Select the group of the key the row has built, opening it with fresh
	/// states of `functions` when no earlier row opened it.
	pub(super) fn select(&mut self, functions: &[Vec<Arc<dyn AggregateFunction>>]) {
		let found = match &self.index {
			KeyIndex::All => Some(0),
			KeyIndex::One(index) => index.get(&self.key[0]).copied(),
			KeyIndex::Many(index) => index.get(self.key.as_slice()).copied(),
		};
		self.current = match found {
			Some(idx) => idx,
			None => {
				let idx = self.open(functions);
				match &mut self.index {
					KeyIndex::All => {}
					KeyIndex::One(index) => {
						index.insert(self.key[0].clone(), idx);
					}
					KeyIndex::Many(index) => {
						index.insert(self.key.clone(), idx);
					}
				}
				idx
			}
		};
	}

	/// The first value of field `field` of the selected group.
	pub(super) fn first(&self, field: usize) -> &Option<Value> {
		&self.firsts[self.current * self.fields() + field]
	}

	/// The first value of field `field` of the selected group, to set.
	pub(super) fn first_mut(&mut self, field: usize) -> &mut Option<Value> {
		let fields = self.fields();
		&mut self.firsts[self.current * fields + field]
	}

	/// The accumulator of call `call` of field `field` of the selected group.
	pub(super) fn accumulator(
		&mut self,
		field: usize,
		call: usize,
	) -> Option<&mut AccumulatorState> {
		let (start, end) = (self.starts[field] as usize, self.starts[field + 1] as usize);
		if start + call >= end {
			return None;
		}
		let at = self.current * self.width() + start + call;
		self.accumulators.get_mut(at)
	}

	/// The groups in key order, for their finish.
	pub(super) fn finish(self) -> Finished {
		let keys = match self.index {
			KeyIndex::All => vec![(Key::All, 0)],
			KeyIndex::One(index) => index.into_iter().map(|(k, i)| (Key::One(k), i)).collect(),
			KeyIndex::Many(index) => index.into_iter().map(|(k, i)| (Key::Many(k), i)).collect(),
		};
		Finished {
			keys,
			starts: self.starts,
			accumulators: self.accumulators,
			firsts: self.firsts,
		}
	}

	/// Fold `part`, the groups a later batch of the same aggregate built,
	/// into these: a group only `part` opened is moved in whole, and a group
	/// both opened has each accumulator merged and each first value kept
	/// unless this side has none.
	pub(super) fn absorb(&mut self, part: Groups) -> anyhow::Result<()> {
		if self.starts != part.starts || !self.index.same_kind(&part.index) {
			anyhow::bail!("a region merged groups of another kind");
		}
		let (width, fields) = (self.width(), self.fields());
		let mut part = part.finish();
		for (key, from) in std::mem::take(&mut part.keys) {
			let found = match (&self.index, &key) {
				(KeyIndex::All, Key::All) => Some(0),
				(KeyIndex::One(index), Key::One(value)) => index.get(value).copied(),
				(KeyIndex::Many(index), Key::Many(values)) => index.get(values).copied(),
				_ => anyhow::bail!("a region merged groups of another kind"),
			};
			let accumulators = &mut part.accumulators[from * width..(from + 1) * width];
			let firsts = &mut part.firsts[from * fields..(from + 1) * fields];
			match found {
				Some(into) => {
					for (j, state) in accumulators.iter_mut().enumerate() {
						let state = std::mem::replace(state, AccumulatorState::placeholder());
						self.accumulators[into * width + j].merge(state)?;
					}
					for (j, value) in firsts.iter_mut().enumerate() {
						let slot = &mut self.firsts[into * fields + j];
						if slot.is_none() {
							*slot = value.take();
						}
					}
				}
				None => {
					let idx = self.len;
					self.len += 1;
					self.accumulators.extend(
						accumulators
							.iter_mut()
							.map(|s| std::mem::replace(s, AccumulatorState::placeholder())),
					);
					self.firsts.extend(firsts.iter_mut().map(Option::take));
					match (&mut self.index, key) {
						(KeyIndex::One(index), Key::One(value)) => {
							index.insert(value, idx);
						}
						(KeyIndex::Many(index), Key::Many(values)) => {
							index.insert(values, idx);
						}
						_ => anyhow::bail!("a region merged groups of another kind"),
					}
				}
			}
		}
		Ok(())
	}
}

impl KeyIndex {
	fn same_kind(&self, other: &KeyIndex) -> bool {
		matches!(
			(self, other),
			(KeyIndex::All, KeyIndex::All)
				| (KeyIndex::One(_), KeyIndex::One(_))
				| (KeyIndex::Many(_), KeyIndex::Many(_))
		)
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::exec::function::FunctionRegistry;
	use crate::val::Number;

	fn functions() -> Vec<Vec<Arc<dyn AggregateFunction>>> {
		let registry = FunctionRegistry::with_builtins();
		let get = |name| Arc::clone(registry.get_aggregate(name).expect("a built-in aggregate"));
		vec![vec![], vec![get("count"), get("math::sum")]]
	}

	fn fold(
		groups: &mut Groups,
		functions: &[Vec<Arc<dyn AggregateFunction>>],
		rows: &[(i64, i64)],
	) {
		for &(key, value) in rows {
			groups.set_key(0, Value::Number(Number::Int(key)));
			groups.select(functions);
			if groups.first(0).is_none() {
				*groups.first_mut(0) = Some(Value::Number(Number::Int(key * 10)));
			}
			groups.accumulator(1, 0).expect("count").update(Value::None).expect("counts");
			groups
				.accumulator(1, 1)
				.expect("sum")
				.update(Value::Number(Number::Int(value)))
				.expect("sums");
		}
	}

	fn answers(groups: Groups) -> Vec<(Value, Value, Value, Value)> {
		let mut finished = groups.finish();
		(0..finished.len())
			.map(|at| {
				let (key, mut group) = finished.group(at);
				let key = key[0].clone();
				let first = group.first(0).clone().unwrap_or(Value::None);
				let accs = group.accumulators(1);
				(key, first, accs[0].finalize().expect("count"), accs[1].finalize().expect("sum"))
			})
			.collect()
	}

	fn int(v: i64) -> Value {
		Value::Number(Number::Int(v))
	}

	#[test]
	fn groups_fold_and_finish_in_key_order() {
		let functions = functions();
		let mut groups = Groups::new(1, &functions);
		fold(&mut groups, &functions, &[(3, 1), (1, 2), (3, 4), (2, 8), (1, 16)]);
		assert_eq!(
			answers(groups),
			vec![
				(int(1), int(10), int(2), int(18)),
				(int(2), int(20), int(1), int(8)),
				(int(3), int(30), int(2), int(5)),
			]
		);
	}

	#[test]
	fn absorbing_a_later_batch_answers_the_serial_fold() {
		let functions = functions();
		let rows = [(3, 1), (1, 2), (3, 4), (2, 8), (1, 16), (4, 32)];
		let mut serial = Groups::new(1, &functions);
		fold(&mut serial, &functions, &rows);
		let mut main = Groups::new(1, &functions);
		fold(&mut main, &functions, &rows[..3]);
		let mut part = Groups::new(1, &functions);
		fold(&mut part, &functions, &rows[3..]);
		main.absorb(part).expect("same shape");
		assert_eq!(answers(main), answers(serial));
	}

	#[test]
	fn group_all_has_its_one_group_from_the_start() {
		let functions = functions();
		let groups = Groups::new(0, &functions);
		let answers: Vec<_> = {
			let mut finished = groups.finish();
			assert_eq!(finished.len(), 1);
			let (key, mut group) = finished.group(0);
			assert!(key.is_empty());
			group.accumulators(1).iter().map(|a| a.finalize().expect("final")).collect()
		};
		assert_eq!(answers, vec![int(0), int(0)]);
	}
}
