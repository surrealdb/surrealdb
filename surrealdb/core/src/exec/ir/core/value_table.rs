//! Value numbering within one statement.

use std::collections::HashMap;

use smallvec::SmallVec;

use crate::exec::ir::iface::identity::{AttrDigest, ValueKey};
use crate::exec::ir::iface::ids::{OpId, RegionId, ValueId};

/// The values a statement has computed, by identity key.
///
/// The table holds the keys of one statement at a time: keys are compared only within a
/// statement, and entering another one forgets the previous statement's keys. Frame and snapshot
/// are part of a key, so the table never shares a value across frames or snapshots. A key that
/// is opaque, by its `opaque` field or by an attribute digest that is [`AttrDigest::OPAQUE`], is
/// equal to no other key, so it is never held and never matched.
///
/// Within a statement the keys are scoped by region: a key is held for the innermost region
/// entered when it was inserted ([`ValueTable::enter_regions`]), and leaving that region forgets
/// it, so a value of one arm is never offered to a sibling arm.
///
/// A key's attribute digest is a candidate match only. Two keys can be equal while their ops'
/// attributes differ, when the digests collide, so a key holds every value inserted under it and
/// a lookup answers the one its caller confirms has equal attributes
/// ([`OpKind::attrs_eq`](crate::exec::ir::op_kind::OpKind::attrs_eq)).
///
/// The table builds no key; `analysis::identity::key` does. It does not check that the held
/// value is visible where it is shared: a caller that replaces a value by an earlier one checks
/// [`Ir::dominates`](super::ir::Ir::dominates) first.
#[derive(Clone, Debug, Default)]
pub(crate) struct ValueTable {
	statement: Option<OpId>,
	values: HashMap<ValueKey, SmallVec<[ValueId; 2]>>,
	/// The regions entered within the statement, outermost first, each with the keys and values
	/// inserted while it was the innermost.
	scopes: Vec<(RegionId, Vec<(ValueKey, ValueId)>)>,
}

impl ValueTable {
	/// An empty table, outside every statement.
	pub(crate) fn new() -> Self {
		Self::default()
	}

	/// Makes `statement` the statement keys are held for, as
	/// [`enclosing_statement`](super::walk::enclosing_statement) answers it. Entering a
	/// statement other than the current one forgets every held key; entering the current one
	/// changes nothing.
	pub(crate) fn enter_statement(&mut self, statement: Option<OpId>) {
		if self.statement != statement {
			self.values.clear();
			self.scopes.clear();
			self.statement = statement;
		}
	}

	/// Makes `chain`, the regions that enclose the next op within the statement, outermost first,
	/// the regions keys are held for. The keys of every entered region not on `chain` are
	/// forgotten; the regions of `chain` not yet entered are entered.
	pub(crate) fn enter_regions(&mut self, chain: &[RegionId]) {
		let kept = self
			.scopes
			.iter()
			.zip(chain)
			.take_while(|((entered, _), region)| entered == *region)
			.count();
		for (_, held) in self.scopes.drain(kept..) {
			for (key, value) in held {
				if let Some(values) = self.values.get_mut(&key) {
					values.retain(|held| *held != value);
					if values.is_empty() {
						self.values.remove(&key);
					}
				}
			}
		}
		self.scopes.extend(chain[kept..].iter().map(|region| (*region, Vec::new())));
	}

	/// The statement keys are held for.
	pub(crate) fn statement(&self) -> Option<OpId> {
		self.statement
	}

	/// The value an earlier call held for `key` that `same` confirms, if any: `same(earlier)`
	/// answers whether the op that defines `earlier` has the attributes of the op that defines
	/// `value`. Otherwise holds `value` for `key` beside the values already held for it and
	/// answers `None`. An opaque key is never held, so it always answers `None`.
	pub(crate) fn lookup_or_insert(
		&mut self,
		key: ValueKey,
		value: ValueId,
		mut same: impl FnMut(ValueId) -> bool,
	) -> Option<ValueId> {
		if key.opaque.is_some() || key.attrs == AttrDigest::OPAQUE {
			return None;
		}
		match self.values.get_mut(&key) {
			Some(held) => {
				if let Some(earlier) = held.iter().copied().find(|earlier| same(*earlier)) {
					return Some(earlier);
				}
				held.push(value);
			}
			None => {
				self.values.insert(key.clone(), SmallVec::from_elem(value, 1));
			}
		}
		if let Some((_, keys)) = self.scopes.last_mut() {
			keys.push((key, value));
		}
		None
	}

	/// The number of values held for the current statement.
	pub(crate) fn len(&self) -> usize {
		self.values.values().map(SmallVec::len).sum()
	}

	/// Whether no key is held.
	pub(crate) fn is_empty(&self) -> bool {
		self.values.is_empty()
	}
}
