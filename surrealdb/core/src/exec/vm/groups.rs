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
//! `Value: Ord`, so the key index is ordered, and the groups are finished in
//! key order. A single key is indexed by its value, and a compound key by the
//! vector of its values. Both order the way that vector does.

use std::collections::BTreeMap;
use std::sync::Arc;

use super::partition::Splitters;
use crate::exec::function::{AccumulatorState, AggregateFunction};
use crate::exec::memory::runs::Record;
use crate::val::{Number, Value};

/// What a group holds beyond its key and slots: its place in the key index.
const GROUP_OVERHEAD: usize = 48;

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
	/// What the open groups hold, estimated: each group's key and slots
	/// when it opens, each first value when it is set, and each value an
	/// accumulator that keeps its inputs takes ([`Self::grew`]).
	bytes: usize,
	/// The terms of the accumulators this part leaves to the merge to fold,
	/// in the order its rows handed them over ([`Self::log_folds`]).
	logged: Option<Box<Logged>>,
}

/// The values an aggregate's morsel hands to some of its accumulators, kept
/// instead of folded, so that the merge folds them in the order of the serial
/// run. Only `GROUP ALL`'s one group keeps a log.
///
/// An accumulator that folds numbers one at a time ([`Accumulator::update`] of
/// the sum, mean, variance and deviation) skips every value that is not a
/// number, and folds a number. The log keeps the numbers in the order they were
/// handed over and nothing else.
///
/// The numbers of a slot are kept in blocks, each allocated at its full size
/// before the rows it holds the numbers of run ([`Groups::open_log_block`]), so
/// no number is ever moved once logged and no block grows.
struct Logged {
	/// The places in a group's accumulators of the accumulators that are
	/// logged, ascending.
	slots: Arc<[usize]>,
	/// By slot, the blocks of numbers, in the order the numbers were handed
	/// over; the last block of a slot is the one being filled.
	numbers: Vec<Vec<Vec<Number>>>,
	/// The numbers a block of a slot holds.
	block: usize,
	/// What the numbers hold, in bytes.
	bytes: usize,
	/// What keeps the memory the blocks hold charged to the query, released
	/// with the log.
	holds: Vec<Box<dyn std::any::Any + Send + Sync>>,
}

impl Logged {
	/// Append what `later`, the log of the rows after these, holds.
	fn append(&mut self, later: Logged) -> anyhow::Result<()> {
		anyhow::ensure!(self.slots == later.slots, "a region merged logs of other accumulators");
		self.bytes += later.bytes;
		for (mine, theirs) in self.numbers.iter_mut().zip(later.numbers) {
			mine.extend(theirs);
		}
		self.holds.extend(later.holds);
		Ok(())
	}

	/// Open a block of `block` numbers for each slot, which the next `block` rows fill.
	fn open(&mut self, block: usize) {
		self.block = block;
		for blocks in &mut self.numbers {
			blocks.push(Vec::with_capacity(block));
		}
	}
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

/// Whether every accumulator of `functions` can save its state, as groups
/// that spill must: a group set that cannot is held as bytes the query cannot
/// spill.
pub(super) fn savable(functions: &[Vec<Arc<dyn AggregateFunction>>]) -> bool {
	functions.iter().flatten().all(|function| function.accumulator_state().save().is_some())
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
			bytes: 0,
			logged: None,
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
		self.bytes += crate::exec::memory::size::values_bytes(&self.key)
			+ self.width() * std::mem::size_of::<AccumulatorState>()
			+ functions.len() * std::mem::size_of::<Option<Value>>()
			+ GROUP_OVERHEAD;
		self.len - 1
	}

	/// What the open groups hold, estimated.
	pub(super) fn bytes(&self) -> usize {
		self.bytes
	}

	/// Count `bytes` more held by the selected group: a first value set, or
	/// a value an accumulator that keeps its inputs took.
	pub(super) fn grew(&mut self, bytes: usize) {
		self.bytes += bytes;
	}

	/// How many keys a group has; none under `GROUP ALL`.
	pub(super) fn keys(&self) -> usize {
		self.key.len()
	}

	/// Whether these groups may be spilled: grouped by a key, so more than
	/// one group can open.
	pub(super) fn spillable(&self) -> bool {
		!matches!(self.index, KeyIndex::All)
	}

	/// Every open group as a saved record in key order — its key, then its
	/// accumulators' saved states and its first values — leaving no group
	/// open. `None`, with the groups left as they are, when an accumulator's
	/// state cannot be saved.
	pub(super) fn save_all(&mut self) -> Option<Vec<Record>> {
		if !self.spillable() {
			return None;
		}
		self.save()
	}

	/// Every group saved, `GROUP ALL`'s one under an empty key; consumes the
	/// groups. `None` when an accumulator's state cannot be saved.
	pub(super) fn into_saved(mut self) -> Option<Vec<Record>> {
		self.save()
	}

	/// Whether no group is open.
	pub(super) fn is_empty(&self) -> bool {
		self.len == 0
	}

	/// How many groups are open.
	pub(super) fn len(&self) -> usize {
		self.len
	}

	/// The key of every `stride`th open group, first one included, in key order; none under
	/// `GROUP ALL`.
	pub(super) fn sample(&self, stride: usize) -> Vec<Vec<Value>> {
		let stride = stride.max(1);
		match &self.index {
			KeyIndex::All => Vec::new(),
			KeyIndex::One(index) => index.keys().step_by(stride).map(|k| vec![k.clone()]).collect(),
			KeyIndex::Many(index) => index.keys().step_by(stride).cloned().collect(),
		}
	}

	/// These groups divided by `splitters` into one set of groups per partition, in partition
	/// order, every group moved whole into the partition its key routes to. A key that compares
	/// equal to another routes with it, so equal keys always meet in one partition, and the
	/// partitions' keys follow one another in key order. `GROUP ALL`'s one group is not divided:
	/// it is the only part.
	pub(super) fn split(self, splitters: &Splitters) -> Vec<Groups> {
		if !self.spillable() {
			return vec![self];
		}
		let (width, fields, total, count) = (self.width(), self.fields(), self.bytes, self.len);
		let mut parts: Vec<Groups> =
			(0..splitters.partitions()).map(|_| self.empty_like()).collect();
		let mut finished = self.finish();
		for (key, from) in std::mem::take(&mut finished.keys) {
			let part = &mut parts[splitters.route(key.as_slice())];
			let at = part.len;
			part.len += 1;
			part.accumulators.extend(
				finished.accumulators[from * width..(from + 1) * width]
					.iter_mut()
					.map(|state| std::mem::replace(state, AccumulatorState::placeholder())),
			);
			part.firsts.extend(
				finished.firsts[from * fields..(from + 1) * fields].iter_mut().map(Option::take),
			);
			match (&mut part.index, key) {
				(KeyIndex::One(index), Key::One(value)) => {
					index.insert(value, at);
				}
				(KeyIndex::Many(index), Key::Many(values)) => {
					index.insert(values, at);
				}
				_ => unreachable!("a keyed group's key is of its groups' kind"),
			}
		}
		// What the groups hold is shared out by how many each part holds, the last part that
		// holds any taking the remainder, so the parts hold what the whole did.
		let mut left = total;
		let last = parts.iter().rposition(|part| part.len > 0);
		for (at, part) in parts.iter_mut().enumerate() {
			part.bytes = match (Some(at) == last, count) {
				(_, 0) => 0,
				(true, _) => left,
				(false, count) => (total as u128 * part.len as u128 / count as u128) as usize,
			};
			left = left.saturating_sub(part.bytes);
		}
		parts
	}

	/// No groups, of the shape these are.
	fn empty_like(&self) -> Self {
		Self {
			index: match &self.index {
				KeyIndex::All => KeyIndex::All,
				KeyIndex::One(_) => KeyIndex::One(BTreeMap::new()),
				KeyIndex::Many(_) => KeyIndex::Many(BTreeMap::new()),
			},
			key: vec![Value::None; self.key.len()],
			current: 0,
			starts: Arc::clone(&self.starts),
			accumulators: Vec::new(),
			firsts: Vec::new(),
			len: 0,
			bytes: 0,
			logged: None,
		}
	}

	/// [`Self::save_all`] for every kind of groups: `GROUP ALL`'s one group
	/// is saved under an empty key.
	fn save(&mut self) -> Option<Vec<Record>> {
		let (width, fields) = (self.width(), self.fields());
		let index = match &mut self.index {
			KeyIndex::All => match self.len {
				1 => vec![(Vec::new(), 0usize)],
				_ => Vec::new(),
			},
			KeyIndex::One(index) => {
				std::mem::take(index).into_iter().map(|(k, i)| (vec![k], i)).collect::<Vec<_>>()
			}
			KeyIndex::Many(index) => std::mem::take(index).into_iter().collect(),
		};
		let mut records = Vec::with_capacity(index.len());
		let mut saved_all = true;
		for (key, at) in &index {
			let mut states = Vec::with_capacity(width);
			for state in &self.accumulators[at * width..(at + 1) * width] {
				match state.save() {
					Some(saved) => states.push(saved),
					None => {
						saved_all = false;
						break;
					}
				}
			}
			if !saved_all {
				break;
			}
			let mut firsts = Vec::with_capacity(fields * 2);
			for first in &self.firsts[at * fields..(at + 1) * fields] {
				firsts.push(first.clone().unwrap_or(Value::None));
				firsts.push(Value::Bool(first.is_some()));
			}
			let row =
				Value::Array(vec![Value::Array(states.into()), Value::Array(firsts.into())].into());
			records.push((key.clone(), row));
		}
		if !saved_all {
			// Put the index back as it was.
			match &mut self.index {
				KeyIndex::One(map) => {
					map.extend(index.into_iter().map(|(mut k, i)| (k.remove(0), i)))
				}
				KeyIndex::Many(map) => map.extend(index),
				KeyIndex::All => {}
			}
			return None;
		}
		self.accumulators.clear();
		self.firsts.clear();
		self.len = 0;
		self.bytes = 0;
		Some(records)
	}

	/// Fold a record [`Self::save_all`] answered into these groups: a key no
	/// group holds opens one with the record's states, and a key one holds
	/// has each state merged after its own and each first value kept unless
	/// it has none.
	pub(super) fn load_saved(
		&mut self,
		key: Vec<Value>,
		row: Value,
		functions: &[Vec<Arc<dyn AggregateFunction>>],
	) -> anyhow::Result<()> {
		let Value::Array(parts) = row else {
			anyhow::bail!("A spilled group is not an array");
		};
		let mut parts = parts.0.into_iter();
		let (Some(Value::Array(states)), Some(Value::Array(firsts))) = (parts.next(), parts.next())
		else {
			anyhow::bail!("A spilled group is malformed");
		};
		for (index, value) in key.into_iter().enumerate() {
			self.set_key(index, value);
		}
		let before = self.len;
		self.select(functions);
		let opened = self.len > before;
		let (width, fields) = (self.width(), self.fields());
		let base = self.current * width;
		let mut at = 0;
		for calls in functions {
			for function in calls {
				let state = states.0.get(at).cloned().unwrap_or(Value::None);
				self.bytes += crate::exec::memory::size::value_bytes(&state);
				if opened {
					self.accumulators[base + at].load(state)?;
				} else {
					let mut later = function.accumulator_state();
					later.load(state)?;
					self.accumulators[base + at].merge(later)?;
				}
				at += 1;
			}
		}
		let mut firsts = firsts.0.into_iter();
		for field in 0..fields {
			let (value, present) = (firsts.next(), firsts.next());
			let slot = &mut self.firsts[self.current * fields + field];
			if slot.is_none() && matches!(present, Some(Value::Bool(true))) {
				let value = value.unwrap_or(Value::None);
				self.bytes += crate::exec::memory::size::value_bytes(&value);
				*slot = Some(value);
			}
		}
		Ok(())
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

	/// Keep the values the accumulators at `slots`, ascending, of `GROUP ALL`'s one group are
	/// handed, in the order they are handed over, instead of folding them: a merge that folds the
	/// groups of the morsels in the order of the serial run folds the kept values as the rows
	/// would have. The groups of a key take no log. No block is open until
	/// [`Self::open_log_block`].
	pub(super) fn log_folds(&mut self, slots: &Arc<[usize]>) {
		debug_assert!(slots.windows(2).all(|pair| pair[0] < pair[1]), "slots ascend: {slots:?}");
		if slots.is_empty() || !matches!(self.index, KeyIndex::All) {
			return;
		}
		let kept = slots.len();
		self.logged = Some(Box::new(Logged {
			slots: Arc::clone(slots),
			numbers: vec![Vec::new(); kept],
			block: 0,
			bytes: 0,
			holds: Vec::new(),
		}));
	}

	/// Open a block of `numbers` numbers for each logged accumulator, for the rows to come: a row
	/// hands each accumulator at most one number, so `numbers` rows fill no more than the block.
	/// Answers the bytes the blocks reserve, which the caller charges, `0` where nothing is
	/// logged.
	pub(super) fn open_log_block(&mut self, numbers: usize) -> usize {
		let Some(logged) = self.logged.as_mut() else {
			return 0;
		};
		logged.open(numbers);
		Self::block_bytes(logged.slots.len(), numbers)
	}

	/// The bytes `slots` logged accumulators reserve for a block of `numbers` numbers each.
	pub(super) fn block_bytes(slots: usize, numbers: usize) -> usize {
		slots.saturating_mul(numbers).saturating_mul(std::mem::size_of::<Number>())
	}

	/// Keep `hold` charged for as long as the log holds the blocks it charged.
	pub(super) fn hold_log(&mut self, hold: Box<dyn std::any::Any + Send + Sync>) {
		if let Some(logged) = self.logged.as_mut() {
			logged.holds.push(hold);
		}
	}

	/// Take `value`, handed to call `call` of field `field`, into the log when that accumulator is
	/// logged ([`Self::log_folds`]); give it back when it is not, for the accumulator to fold.
	///
	/// A number goes into the room of the last block [`Self::open_log_block`] opened, whose bytes
	/// the caller charged. A block holds the numbers of the rows it was opened for, and a row
	/// hands an accumulator at most one, so a number with no room left is an internal error: no
	/// block is opened here, since nothing would charge it.
	#[inline]
	pub(super) fn log(
		&mut self,
		field: usize,
		call: usize,
		value: Value,
	) -> anyhow::Result<Option<Value>> {
		let Some(logged) = self.logged.as_mut() else {
			return Ok(Some(value));
		};
		let (start, end) = (self.starts[field] as usize, self.starts[field + 1] as usize);
		if start + call >= end {
			return Ok(Some(value));
		}
		let Ok(at) = logged.slots.binary_search(&(start + call)) else {
			return Ok(Some(value));
		};
		// An absent or non-numeric value is skipped by every logged aggregate, so only a
		// number is kept.
		if let Value::Number(number) = value {
			let Some(block) =
				logged.numbers[at].last_mut().filter(|block| block.len() < block.capacity())
			else {
				anyhow::bail!("a row handed a logged accumulator a number past its block");
			};
			block.push(number);
			logged.bytes += std::mem::size_of::<Number>();
		}
		Ok(None)
	}

	/// What the values kept for the merge hold, in bytes.
	pub(super) fn logged_bytes(&self) -> usize {
		self.logged.as_ref().map_or(0, |logged| logged.bytes)
	}

	/// Fold the values `later` kept, the log of the rows after the ones these groups hold: into
	/// the accumulators of `GROUP ALL`'s group in order, or after the values these groups keep.
	fn fold_logged(&mut self, later: Logged) -> anyhow::Result<()> {
		if let Some(mine) = self.logged.as_mut() {
			return mine.append(later);
		}
		anyhow::ensure!(
			matches!(self.index, KeyIndex::All) && self.len == 1,
			"a region folded a log into groups of a key"
		);
		let fold = |state: &mut AccumulatorState, value: Value| {
			if let Err(e) = state.update(value) {
				tracing::debug!(error = %e, "Accumulator update failed, skipping value");
			}
		};
		let Logged {
			slots,
			numbers,
			..
		} = later;
		for (slot, numbers) in slots.iter().zip(numbers) {
			let state = self
				.accumulators
				.get_mut(*slot)
				.ok_or_else(|| anyhow::anyhow!("a logged accumulator is not in the group"))?;
			for number in numbers.into_iter().flatten() {
				fold(state, Value::Number(number));
			}
		}
		Ok(())
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
		self.bytes += part.bytes;
		let mut part = part;
		let logged = part.logged.take();
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
						// A logged accumulator holds nothing to merge: its values are folded
						// below.
						if logged
							.as_ref()
							.is_some_and(|logged| logged.slots.binary_search(&j).is_ok())
						{
							continue;
						}
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
		match logged {
			Some(logged) => self.fold_logged(*logged),
			None => Ok(()),
		}
	}
}

/// Saved groups in key order ([`Groups::save_all`]), folded a batch of keys
/// at a time: the saved states of one key are loaded in the order the merge
/// answers them, which is the order they were saved in.
pub(super) struct SavedFold {
	merge: super::spill::Merge,
	next: Option<Record>,
	keys: usize,
	functions: Vec<Vec<Arc<dyn AggregateFunction>>>,
	batch: usize,
	room: usize,
	done: bool,
}

impl SavedFold {
	/// A fold of `merge`'s records, groups of `keys` keys with `functions`,
	/// a chunk ending at `batch` keys or once it holds more than `room`
	/// bytes.
	pub(super) fn new(
		merge: super::spill::Merge,
		keys: usize,
		functions: Vec<Vec<Arc<dyn AggregateFunction>>>,
		batch: usize,
		room: usize,
	) -> Self {
		Self {
			merge,
			next: None,
			keys,
			functions,
			batch,
			room,
			done: false,
		}
	}

	/// The next batch of keys, every saved state of each folded in source
	/// order; `None` once every key is folded.
	pub(super) async fn next_chunk(
		&mut self,
		ctx: &crate::exec::ExecutionContext,
	) -> crate::exec::FlowResult<Option<Groups>> {
		use crate::expr::ControlFlow;
		if self.done {
			return Ok(None);
		}
		let mut chunk = Groups::new(self.keys, &self.functions);
		let mut taken = 0;
		let mut last: Option<Vec<Value>> = None;
		let mut drained = true;
		loop {
			let record = match self.next.take() {
				Some(record) => record,
				None => match self.merge.next(ctx).await? {
					Some(record) => record,
					None => break,
				},
			};
			if last.as_ref().is_none_or(|l| l.cmp(&record.0) != std::cmp::Ordering::Equal) {
				if taken == self.batch || (taken > 0 && chunk.bytes() > self.room) {
					self.next = Some(record);
					drained = false;
					break;
				}
				taken += 1;
				last = Some(record.0.clone());
			}
			chunk.load_saved(record.0, record.1, &self.functions).map_err(ControlFlow::Err)?;
		}
		if taken == 0 {
			self.done = true;
			return Ok(None);
		}
		self.done = drained;
		Ok(Some(chunk))
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

impl Groups {
	/// Write these groups for the merge of another engine to read back ([`Self::get_wire`]): the
	/// kind of key, every group's key, its accumulators' saved states and its first values, and
	/// the values `GROUP ALL` keeps for the merge to fold. `None`, with nothing written, when an
	/// accumulator's state cannot be saved.
	pub(super) fn put_wire(&self) -> Option<Vec<u8>> {
		let mut w = super::morsel::wire::Writer::default();
		w.usize(self.key.len());
		w.usize(self.len);
		w.usize(self.bytes);
		let keyed: Vec<(Vec<Value>, usize)> = match &self.index {
			KeyIndex::All => (0..self.len).map(|at| (Vec::new(), at)).collect(),
			KeyIndex::One(index) => index.iter().map(|(k, &i)| (vec![k.clone()], i)).collect(),
			KeyIndex::Many(index) => index.iter().map(|(k, &i)| (k.clone(), i)).collect(),
		};
		w.usize(keyed.len());
		for (key, at) in &keyed {
			for value in key {
				w.value(value).ok()?;
			}
			w.usize(*at);
		}
		w.usize(self.accumulators.len());
		for state in &self.accumulators {
			w.value(&state.save()?).ok()?;
		}
		w.usize(self.firsts.len());
		for first in &self.firsts {
			match first {
				None => w.u8(0),
				Some(value) => {
					w.u8(1);
					w.value(value).ok()?;
				}
			}
		}
		match &self.logged {
			None => w.u8(0),
			Some(logged) => {
				w.u8(1);
				w.usize(logged.slots.len());
				for (at, slot) in logged.slots.iter().enumerate() {
					w.usize(*slot);
					let numbers: usize = logged.numbers[at].iter().map(Vec::len).sum();
					w.usize(numbers);
					for number in logged.numbers[at].iter().flatten() {
						w.value(&Value::Number(number.clone())).ok()?;
					}
				}
				w.usize(logged.bytes);
			}
		}
		Some(w.into_bytes())
	}

	/// The groups [`Self::put_wire`] wrote, whose accumulators are those of `functions`, by field
	/// and call.
	pub(super) fn get_wire(
		bytes: &[u8],
		functions: &[Vec<Arc<dyn AggregateFunction>>],
	) -> anyhow::Result<Self> {
		let mut r = super::morsel::wire::Reader::raw(bytes);
		let keys = r.usize()?;
		let mut groups = Self::new(keys, functions);
		// `GROUP ALL` opens its one group from the start; the written groups replace it.
		groups.accumulators.clear();
		groups.firsts.clear();
		groups.len = r.usize()?;
		groups.bytes = r.usize()?;
		for _ in 0..r.usize()? {
			let key = (0..keys).map(|_| r.value()).collect::<anyhow::Result<Vec<_>>>()?;
			let at = r.usize()?;
			anyhow::ensure!(at < groups.len, "a morsel's group lies past its groups");
			match &mut groups.index {
				KeyIndex::All => {}
				KeyIndex::One(index) => {
					let key = key.into_iter().next().unwrap_or(Value::None);
					index.insert(key, at);
				}
				KeyIndex::Many(index) => {
					index.insert(key, at);
				}
			}
		}
		let width = groups.width();
		let states = r.usize()?;
		anyhow::ensure!(states == width * groups.len, "a morsel's groups hold other accumulators");
		for _ in 0..groups.len {
			for calls in functions {
				for function in calls {
					let mut state = function.accumulator_state();
					state.load(r.value()?)?;
					groups.accumulators.push(state);
				}
			}
		}
		let firsts = r.usize()?;
		anyhow::ensure!(
			firsts == groups.fields() * groups.len,
			"a morsel's groups hold other first values"
		);
		for _ in 0..firsts {
			let first = match r.u8()? {
				0 => None,
				_ => Some(r.value()?),
			};
			groups.firsts.push(first);
		}
		if r.u8()? == 1 {
			let kept = r.usize()?;
			let mut slots = Vec::with_capacity(kept);
			let mut numbers = Vec::with_capacity(kept);
			for _ in 0..kept {
				slots.push(r.usize()?);
				let count = r.usize()?;
				let mut block = Vec::with_capacity(count);
				for _ in 0..count {
					match r.value()? {
						Value::Number(number) => block.push(number),
						Value::None
						| Value::Null
						| Value::Bool(_)
						| Value::String(_)
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
						| Value::RecordId(_)
						| Value::File(_)
						| Value::Regex(_)
						| Value::Range(_)
						| Value::Closure(_) => anyhow::bail!("a morsel's kept term is not a number"),
					}
				}
				numbers.push(vec![block]);
			}
			let bytes = r.usize()?;
			groups.logged = Some(Box::new(Logged {
				slots: slots.into(),
				numbers,
				block: 0,
				bytes,
				holds: Vec::new(),
			}));
		}
		r.done()?;
		Ok(groups)
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

	/// Groups divided by range into partitions, each partition's shares of the morsels folded in
	/// the order of the morsels, answer the serial fold in key order, whatever the splitters.
	#[test]
	fn groups_split_by_range_and_folded_answer_the_serial_fold() {
		let functions = functions();
		let morsels: [&[(i64, i64)]; 4] = [
			&[(3, 1), (1, 2), (3, 4), (2, 8), (11, 5)],
			&[(1, 16), (4, 32), (9, 1)],
			&[],
			&[(3, 7), (12, 9), (4, 2), (1, 1), (0, 6), (10, 3)],
		];
		let mut serial = Groups::new(1, &functions);
		for rows in morsels {
			fold(&mut serial, &functions, rows);
		}
		let expected = answers(serial);
		for splitters in [
			Splitters::single(),
			Splitters::new(vec![vec![int(3)]]),
			Splitters::new(vec![vec![int(2)], vec![int(4)], vec![int(10)]]),
			// A splitter no key reaches leaves partitions with no groups.
			Splitters::new(vec![vec![int(-5)], vec![int(3)], vec![int(100)]]),
		] {
			let mut partitions: Vec<Vec<Groups>> =
				(0..splitters.partitions()).map(|_| Vec::new()).collect();
			for rows in morsels {
				let mut groups = Groups::new(1, &functions);
				fold(&mut groups, &functions, rows);
				let bytes = groups.bytes();
				let split = groups.split(&splitters);
				assert_eq!(split.len(), splitters.partitions());
				assert_eq!(split.iter().map(Groups::bytes).sum::<usize>(), bytes);
				for (partition, share) in partitions.iter_mut().zip(split) {
					partition.push(share);
				}
			}
			let mut got = Vec::new();
			for shares in partitions {
				let mut shares = shares.into_iter();
				let mut folded = shares.next().expect("a share of the first morsel");
				for share in shares {
					folded.absorb(share).expect("same shape");
				}
				got.extend(answers(folded));
			}
			assert_eq!(got, expected, "{splitters:?}");
		}
	}

	/// The values of a `GROUP ALL` over a sum, a mean, a deviation, a variance and a count, such
	/// that the order they are folded in changes the low bits of the floats, and some of them
	/// are absent or of another kind.
	fn float_terms() -> Vec<Value> {
		(0..400i64)
			.map(|i| match i {
				_ if i % 31 == 0 => Value::String("a string".to_owned().into()),
				_ if i % 17 == 0 => Value::None,
				_ if i % 13 == 0 => Value::Null,
				_ if i % 11 == 0 => Value::Number(Number::Int(i)),
				_ => Value::Number(Number::Float(
					i as f64 * 0.1
						+ if i % 2 == 0 {
							1e15
						} else {
							-1e15
						},
				)),
			})
			.collect()
	}

	fn float_functions() -> Vec<Vec<Arc<dyn AggregateFunction>>> {
		let registry = FunctionRegistry::with_builtins();
		let get = |name| Arc::clone(registry.get_aggregate(name).expect("a built-in aggregate"));
		vec![vec![
			get("math::sum"),
			get("math::mean"),
			get("math::stddev"),
			get("math::variance"),
			get("count"),
		]]
	}

	/// The calls of `float_functions` that fold floats.
	const FLOATS: [usize; 4] = [0, 1, 2, 3];

	fn floats() -> Arc<[usize]> {
		Arc::from(FLOATS)
	}

	/// Hand `terms` to every call of `groups`, as the instruction that folds a row does: to the log
	/// where the call is logged, to the accumulator where it is not.
	fn hand_over(groups: &mut Groups, terms: &[Value]) {
		groups.open_log_block(terms.len());
		for term in terms {
			for call in 0..5 {
				if let Some(value) =
					groups.log(0, call, term.clone()).expect("room in the block opened for it")
				{
					let _ = groups.accumulator(0, call).expect("an accumulator").update(value);
				}
			}
		}
	}

	/// What each accumulator of the one group answers, or the failure it answers with, as text: the
	/// bits of a float are part of what it prints.
	fn finals(groups: Groups) -> Vec<String> {
		let mut finished = groups.finish();
		let (_, mut group) = finished.group(0);
		group
			.accumulators(0)
			.iter()
			.map(|state| match state.finalize() {
				Ok(value) => format!("{value:?}"),
				Err(e) => format!("{e}"),
			})
			.collect()
	}

	/// Logged values folded by the merge in the order of the parts answer what the serial fold
	/// answers, bit for bit, whatever the parts' sizes, and whether the parts are merged one by
	/// one into the statement's groups or into one another first.
	#[test]
	fn logged_terms_fold_as_the_serial_fold_does() {
		let functions = float_functions();
		let terms = float_terms();
		let mut serial = Groups::new(0, &functions);
		hand_over(&mut serial, &terms);
		let expected = finals(serial);
		// An order-sensitive fold: the same terms, in another order, answer something else.
		let mut reversed = Groups::new(0, &functions);
		hand_over(&mut reversed, &terms.iter().rev().cloned().collect::<Vec<_>>());
		assert_ne!(finals(reversed)[..4], expected[..4], "the terms do not tell the order");
		for cuts in
			[vec![200], vec![1, 399], vec![100, 101, 250], vec![0, 0, 400], vec![7, 8, 9, 300]]
		{
			let mut parts = Vec::new();
			let mut from = 0;
			for cut in cuts.iter().copied().chain([terms.len()]) {
				let mut part = Groups::new(0, &functions);
				part.log_folds(&floats());
				hand_over(&mut part, &terms[from..cut]);
				parts.push(part);
				from = cut;
			}
			let kept: usize = parts.iter().map(Groups::logged_bytes).sum();
			assert!(kept > 0);
			// One into the statement's groups after another.
			let mut main = Groups::new(0, &functions);
			for part in
				parts.iter_mut().map(|part| std::mem::replace(part, Groups::new(0, &functions)))
			{
				main.absorb(part).expect("same shape");
			}
			assert_eq!(main.logged_bytes(), 0);
			assert_eq!(finals(main), expected, "{cuts:?}, one by one");
		}
		// Parts folded into one another keep their terms in order for the statement's groups.
		let mut parts: Vec<Groups> = terms
			.chunks(97)
			.map(|chunk| {
				let mut part = Groups::new(0, &functions);
				part.log_folds(&floats());
				hand_over(&mut part, chunk);
				part
			})
			.collect();
		let mut right = parts.split_off(2);
		let mut left = parts.remove(0);
		left.absorb(parts.remove(0)).expect("same shape");
		let mut rest = right.remove(0);
		for part in right {
			rest.absorb(part).expect("same shape");
		}
		assert!(left.logged_bytes() > 0 && rest.logged_bytes() > 0);
		let mut main = Groups::new(0, &functions);
		main.absorb(left).expect("same shape");
		main.absorb(rest).expect("same shape");
		assert_eq!(finals(main), expected, "pairs first");
	}

	/// The terms of a slot are kept in blocks allocated at their full size and never grown: the
	/// block a chunk opens holds the terms of its rows, and the memory the blocks charge is
	/// released with the log, once the merge has folded it.
	#[test]
	fn logged_terms_are_kept_in_blocks_that_never_grow() {
		let functions = float_functions();
		let terms: Vec<Value> = (0..50).map(|i| Value::Number(Number::Float(i as f64))).collect();
		let mut part = Groups::new(0, &functions);
		part.log_folds(&floats());
		let token = Arc::new(());
		for chunk in terms.chunks(8) {
			assert_eq!(part.open_log_block(8), Groups::block_bytes(FLOATS.len(), 8));
			part.hold_log(Box::new(Arc::clone(&token)));
			for term in chunk {
				for call in 0..5 {
					if let Some(value) =
						part.log(0, call, term.clone()).expect("room in the block opened for it")
					{
						let _ = part.accumulator(0, call).expect("an accumulator").update(value);
					}
				}
			}
		}
		let logged = part.logged.as_ref().expect("a log");
		for blocks in &logged.numbers {
			assert_eq!(blocks.iter().map(Vec::len).sum::<usize>(), terms.len());
			assert!(blocks.iter().all(|block| block.len() <= block.capacity()));
			assert!(blocks.iter().all(|block| block.capacity() == 8), "a block grew");
		}
		assert_eq!(part.logged_bytes(), terms.len() * FLOATS.len() * std::mem::size_of::<Number>());
		assert_eq!(Arc::strong_count(&token), 1 + terms.chunks(8).count());
		let mut main = Groups::new(0, &functions);
		main.absorb(part).expect("same shape");
		assert_eq!(Arc::strong_count(&token), 1, "the charge outlived the log");
	}

	/// A term past the room of the block opened for it is an error: a block nothing charged is
	/// never opened to hold it, and the terms before it stay where they were put.
	#[test]
	fn a_term_past_its_block_is_an_error() {
		let functions = float_functions();
		let mut part = Groups::new(0, &functions);
		part.log_folds(&floats());
		part.open_log_block(2);
		for term in [1.0, 2.0] {
			assert_eq!(part.log(0, 0, Value::Number(Number::Float(term))).expect("room"), None);
		}
		let bytes = part.logged_bytes();
		assert!(part.log(0, 0, Value::Number(Number::Float(3.0))).is_err());
		assert_eq!(part.logged_bytes(), bytes);
		let logged = part.logged.as_ref().expect("a log");
		assert!(logged.numbers.iter().all(|blocks| blocks.len() == 1));
	}

	/// A group of a key keeps no log: its values fold where they are handed over.
	#[test]
	fn a_group_of_a_key_logs_nothing() {
		let functions = functions();
		let mut groups = Groups::new(1, &functions);
		groups.log_folds(&Arc::from([0, 1]));
		groups.set_key(0, int(1));
		groups.select(&functions);
		assert_eq!(groups.log(1, 1, int(5)).expect("no log"), Some(int(5)));
		assert_eq!(groups.logged_bytes(), 0);
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
