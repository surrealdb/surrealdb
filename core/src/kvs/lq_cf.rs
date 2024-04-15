use std::collections::BTreeMap;

use crate::kvs::lq_structs::{LqIndexKey, LqIndexValue, LqSelector};
use crate::vs::{conv, Versionstamp};

pub(crate) struct LiveQueryTracker {
	// Map of Live Query identifier (ns+db+tb) for change feed tracking
	// the mapping is to a list of affected live queries
	local_live_queries: BTreeMap<LqIndexKey, LqIndexValue>,
	// Set of tracked change feeds with associated watermarks
	// This is updated with new/removed live queries and improves cf request performance
	// The Versionstamp associated is scanned inclusive of first value, so it must contain the earliest NOT read value
	// So if VS=2 has been processed, the correct value here is VS=3
	cf_watermarks: BTreeMap<LqSelector, Versionstamp>,
}

impl LiveQueryTracker {
	pub(crate) const fn new() -> Self {
		Self {
			local_live_queries: Default::default(),
			cf_watermarks: Default::default(),
		}
	}

	pub(crate) fn register_live_query(&self) {}

	pub(crate) fn unregister_live_query(&self) {}

	/// This will update the watermark of all live queries, regardless of their individual state
	pub(crate) fn update_watermark(&mut self, selector: &LqSelector, watermark: &Versionstamp) {
		// TODO: The updated watermark needs to reflect the earliest live query
		// This is because each live query is in a different tracked state
		// the tracked watermarks on change feed are shared
		// And we always want to capture the earliest watermark
		let mut current = self.cf_watermarks.get(selector).unwrap_or(&[0; 10]);
		let current = conv::to_u128_be(*current);
		let proposed = conv::to_u128_be(*watermark);
		if proposed > current {
			// Update change feed tracking
			self.cf_watermarks.insert(selector.clone(), *watermark);

			// Update live query tracking
			self.local_live_queries.iter_mut().for_each(|(index, value)| {
				if index.selector != *selector {
					return;
				}
				value.vs = *watermark;
			});
		}
	}

	pub(crate) fn get_watermarks(&self) -> &BTreeMap<LqSelector, Versionstamp> {
		return &self.cf_watermarks;
	}

	/// This is to iterate the change feed trackers by index
	/// It is useful in situations where you want to hold a mutable reference, but still need
	/// to iterate over it normally
	/// This will break if values are added or removed, so keep the write lock while iterating
	/// This can be improved by having droppable trackers/iterators returned
	pub(crate) fn get_watermark_by_enum_index(
		&self,
		index: usize,
	) -> Option<(&LqSelector, &Versionstamp)> {
		self.cf_watermarks.nth(index)
	}

	pub(crate) fn is_empty(&self) -> bool {
		self.local_live_queries.is_empty()
	}

	/// Find the necessary Live Query information for a given selector
	pub(crate) fn live_queries_for_selector(&self, selector: &LqSelector) -> Vec<(LqIndexKey, LqIndexValue)> {
			self.local_live_queries.
				iter()
				.filter(|(k, _)| k.selector == *selector)
				.flat_map(|(lq_index, lq_values)| {
					lq_values.iter().cloned().map(|x| (lq_index.clone(), x))
				})
				.to_owned()
				.collect()
		}
	}
}
