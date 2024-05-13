use crate::dbs::node::Timestamp;
use std::collections::BTreeMap;

use crate::kvs::lq_structs::{KillEntry, LqEntry, LqIndexKey, LqIndexValue, LqSelector};
use crate::vs::{conv, Versionstamp};

/// We often want to increment by 1, but the 2 least significant bytes are unused
const ONE_SHIFTED: u128 = 1 << 16;

/// The datastore needs to track live queries that it owns as an engine. The db API and drivers
/// start tasks that poll the database for changes that are broadcast to relevant live queries.
///
/// This struct tracks live queries against change feeds so that the correct watermarks are used
/// across differently versioned live queries. It provides convenience, correctness and separation
/// of concerns.
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
			local_live_queries: BTreeMap::new(),
			cf_watermarks: BTreeMap::new(),
		}
	}

	/// Add another Live Query to track, given the Versionstamp to stream from
	pub(crate) fn register_live_query(
		&mut self,
		lq_index_key: &LqEntry,
		live_query_vs: Versionstamp,
	) -> Result<(), &'static str> {
		// See if we are already tracking the query
		let k = lq_index_key.as_key();
		if self.local_live_queries.contains_key(&k) {
			return Err("Live query registered twice");
		}
		let v = lq_index_key.as_value(live_query_vs, Timestamp::default());
		let selector = k.selector.clone();
		self.local_live_queries.insert(k, v);

		// Check if we need to add a watermark for change feeds
		match self.cf_watermarks.get(&selector) {
			Some(existing_watermark) => {
				// if we are tracking a later watermark than the one committed, then we need to move the watermark backwards
				// Each individual live query will track its own watermark, so they will not get picked up when replaying older events
				let current_u128 = conv::to_u128_be(*existing_watermark);
				let proposed_u128 = conv::to_u128_be(live_query_vs);
				if proposed_u128 < current_u128 {
					self.cf_watermarks.insert(selector, live_query_vs);
				}
			}
			None => {
				// This default watermark is bad - it will catch up from the start of the change feed
				self.cf_watermarks.insert(selector, live_query_vs);
			}
		}
		Ok(())
	}

	pub(crate) fn unregister_live_query(&mut self, kill_entry: &KillEntry) {
		// Because the information available from a kill statement is limited, we need to find a relevant kill query
		let found: Option<(LqIndexKey, LqIndexValue)> = self
			.local_live_queries
			.iter()
			.filter(|(k, _)| {
				// Get all the live queries in the ns/db pair. We don't know table
				k.selector.ns == kill_entry.ns && k.selector.db == kill_entry.db
			})
			.filter_map(|(k, v)| {
				if v.stm.id == kill_entry.live_id {
					Some((k.clone(), v.clone()))
				} else {
					None
				}
			})
			.next();
		match found {
			None => {
				// TODO(SUR-336): Make Live Query ID validation available at statement level, perhaps via transaction
				warn!(
					"Could not find live query {:?} to kill in ns/db pair {:?} / {:?}",
					&kill_entry, &kill_entry.ns, &kill_entry.db
				);
			}
			Some(found) => {
				self.local_live_queries.remove(&found.0);
				// TODO remove the watermarks
			}
		};
	}

	/// This will update the watermark of all live queries, regardless of their individual state
	pub(crate) fn update_watermark_live_query(
		&mut self,
		live_query: &LqIndexKey,
		watermark: &Versionstamp,
	) -> Result<(), &'static str> {
		let lq_data = self.local_live_queries.get_mut(live_query).ok_or("Live query not found")?;
		let current_lq_vs = conv::to_u128_be(lq_data.vs);
		let proposed_vs = conv::to_u128_be(*watermark);
		if proposed_vs >= current_lq_vs {
			// We now need to increase the watermark so that scanning does not pick up the current observed
			let new_proposed = proposed_vs + ONE_SHIFTED;
			lq_data.vs = conv::try_u128_to_versionstamp(new_proposed)
				.map_err(|_| "Could not convert to versionstamp")?;

			// We need to drop the borrow and keep the data
			let lq_data = lq_data.clone();

			// Since we modified, we now check if we need to update the change feed watermark
			let valid_lqs = Self::live_queries_for_selector_impl(
				&self.local_live_queries,
				&live_query.selector,
			);
			// Find the minimum watermark
			let min_watermark =
				valid_lqs.iter().map(|(_, v)| conv::to_u128_be(v.vs)).min().unwrap();
			// Get the current watermark
			let current_watermark =
				conv::to_u128_be(*self.cf_watermarks.get(&live_query.selector).unwrap());
			if min_watermark > current_watermark {
				self.cf_watermarks.insert(live_query.selector.clone(), lq_data.vs);
			}
		}
		Ok(())
	}

	pub(crate) fn get_watermarks(&self) -> &BTreeMap<LqSelector, Versionstamp> {
		&self.cf_watermarks
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
		self.cf_watermarks.iter().nth(index)
	}

	pub(crate) fn is_empty(&self) -> bool {
		self.local_live_queries.is_empty()
	}

	/// Find the necessary Live Query information for a given selector
	pub(crate) fn live_queries_for_selector(
		&self,
		selector: &LqSelector,
	) -> Vec<(LqIndexKey, LqIndexValue)> {
		Self::live_queries_for_selector_impl(&self.local_live_queries, selector)
	}

	fn live_queries_for_selector_impl(
		local_live_queries: &BTreeMap<LqIndexKey, LqIndexValue>,
		selector: &LqSelector,
	) -> Vec<(LqIndexKey, LqIndexValue)> {
		local_live_queries
			.iter()
			.filter(|(k, _)| k.selector == *selector)
			.map(|(k, v)| (k.clone(), v.clone()))
			.collect()
	}
}

#[cfg(test)]
mod test {
	use super::*;
	use crate::sql::statements::LiveStatement;
	use crate::sql::{Table, Uuid, Value};
	use std::str::FromStr;

	const NS: &str = "test_namespace";
	const DB: &str = "test_database";
	const TB: &str = "test_table";
	const DEFAULT_WATERMARK: [u8; 10] = [0; 10];

	#[test]
	fn registering_lq_tracks_cf() {
		let mut tracker = LiveQueryTracker::new();
		assert!(tracker.is_empty());
		let lq_entry = an_lq_entry(
			Uuid::from_str("36a35c76-8912-4b28-987a-4dcf276422c0").unwrap(),
			NS,
			DB,
			TB,
		);
		tracker.register_live_query(&lq_entry, DEFAULT_WATERMARK).unwrap();

		assert_eq!(tracker.get_watermarks().len(), 1);
	}

	#[test]
	fn can_progress_a_live_query() {
		let mut tracker = LiveQueryTracker::new();
		assert!(tracker.is_empty());
		let lq_entry = an_lq_entry(
			Uuid::from_str("ffac79b6-39e7-45bb-901c-2cda393e4f8a").unwrap(),
			NS,
			DB,
			TB,
		);

		// We set any watermark to start with
		tracker.register_live_query(&lq_entry, DEFAULT_WATERMARK).unwrap();
		assert_tracker_has_watermark(
			&tracker,
			NS.to_string(),
			DB.to_string(),
			TB.to_string(),
			DEFAULT_WATERMARK,
		);

		// Progress the watermark
		let proposed_watermark = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
		tracker.update_watermark_live_query(&lq_entry.as_key(), &proposed_watermark).unwrap();
		let new_watermark = increment_versionstamp(proposed_watermark);
		assert_tracker_has_watermark(
			&tracker,
			NS.to_string(),
			DB.to_string(),
			TB.to_string(),
			new_watermark,
		);
	}

	#[test]
	fn progressed_live_queries_that_get_removed_clear_cf_watermark() {
		let mut tracker = LiveQueryTracker::new();
		assert!(tracker.is_empty());

		// Add lq
		let lq_entry = an_lq_entry(
			Uuid::from_str("97d28595-0297-4b77-9806-58ec726e21f1").unwrap(),
			NS,
			DB,
			TB,
		);
		tracker.register_live_query(&lq_entry, DEFAULT_WATERMARK).unwrap();

		// Check watermark
		let lq_selector = LqSelector {
			ns: NS.to_string(),
			db: DB.to_string(),
			tb: TB.to_string(),
		};
		assert_tracker_has_watermark(
			&tracker,
			lq_selector.ns.clone(),
			lq_selector.db.clone(),
			lq_selector.tb.clone(),
			DEFAULT_WATERMARK,
		);

		// Progress watermark
		let proposed_watermark = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
		tracker.update_watermark_live_query(&lq_entry.as_key(), &proposed_watermark).unwrap();
		let mut modified_watermark = proposed_watermark;
		modified_watermark[7] += 1;
		assert_tracker_has_watermark(
			&tracker,
			lq_selector.ns.clone(),
			lq_selector.db.clone(),
			lq_selector.tb.clone(),
			modified_watermark,
		);
	}

	#[test]
	fn two_live_queries_one_in_catchup() {
		let mut tracker = LiveQueryTracker::new();
		assert!(tracker.is_empty());

		// Add lq
		let lq1 = an_lq_entry(
			Uuid::from_str("4b93a192-9f5f-4014-aa2e-93ecff8ad2e6").unwrap(),
			NS,
			DB,
			TB,
		);
		tracker.register_live_query(&lq1, DEFAULT_WATERMARK).unwrap();

		// Check watermark is "default"
		let wms = tracker.get_watermarks();
		assert_eq!(wms.len(), 1);
		let (selector, watermark) = wms.iter().next().unwrap();
		assert_eq!(
			selector,
			&LqSelector {
				ns: NS.to_string(),
				db: DB.to_string(),
				tb: TB.to_string(),
			}
		);
		assert_eq!(watermark, &DEFAULT_WATERMARK);

		// Progress the watermark
		let progressed_watermark = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
		tracker.update_watermark_live_query(&lq1.as_key(), &progressed_watermark).unwrap();

		// Add a second live query
		let lq2 = an_lq_entry(
			Uuid::from_str("ec023004-c657-49f9-8688-33e4ab490fd2").unwrap(),
			NS,
			DB,
			TB,
		);

		// Check the watermark is shared - it has moved backwards
		tracker.register_live_query(&lq2, DEFAULT_WATERMARK).unwrap();
		assert_tracker_has_watermark(
			&tracker,
			NS.to_string(),
			DB.to_string(),
			TB.to_string(),
			DEFAULT_WATERMARK,
		);

		// But the individual live query watermarks are intact
		let tracked_live_queries = tracker.live_queries_for_selector(&LqSelector {
			ns: NS.to_string(),
			db: DB.to_string(),
			tb: TB.to_string(),
		});
		let progressed_watermark = increment_versionstamp(progressed_watermark);
		assert_eq!(tracked_live_queries.len(), 2);
		assert_eq!(tracked_live_queries[0].1.vs, progressed_watermark);
		assert_eq!(tracked_live_queries[1].1.vs, DEFAULT_WATERMARK);
	}

	/// Fixture to provide necessary data for a tracked live query
	fn an_lq_entry(live_id: Uuid, ns: &str, db: &str, tb: &str) -> LqEntry {
		LqEntry {
			live_id,
			ns: ns.to_string(),
			db: db.to_string(),
			stm: LiveStatement {
				id: live_id,
				node: Default::default(),
				expr: Default::default(),
				what: Value::Table(Table(tb.to_string())),
				cond: None,
				fetch: None,
				archived: None,
				session: None,
				auth: None,
			},
		}
	}

	/// Validate there is only a single watermark with the given data
	fn assert_tracker_has_watermark(
		tracker: &LiveQueryTracker,
		ns: String,
		db: String,
		tb: String,
		vs: Versionstamp,
	) {
		assert_eq!(tracker.get_watermarks().len(), 1);
		let (selector, watermark) = tracker.get_watermarks().iter().next().unwrap();
		assert_eq!(
			selector,
			&LqSelector {
				ns,
				db,
				tb
			}
		);
		assert_eq!(watermark, &vs);
	}

	fn increment_versionstamp(vs: Versionstamp) -> Versionstamp {
		let u128_be = conv::to_u128_be(vs);
		let incremented = u128_be + ONE_SHIFTED;
		conv::try_u128_to_versionstamp(incremented).unwrap()
	}
}
