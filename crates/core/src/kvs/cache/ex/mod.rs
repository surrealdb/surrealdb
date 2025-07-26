//! Manages cached records with expiration logic.
//!
//! Tracks records with expiration timestamps and supports efficient cleanup
//! once they are considered expired.

use crate::expr::Id;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::time::Instant;

/// Represents a unique record in the datastore using namespace, database, table, and ID.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct Record {
	pub ns: String,
	pub db: String,
	pub tb: String,
	pub id: Id,
}

/// A record along with its expiration time.
pub struct ExpireItem {
	pub record: Record,
	pub time: Instant,
}

impl Record {
	pub fn new(ns: String, db: String, tb: String, id: Id) -> Record {
		Self {
			ns,
			db,
			tb,
			id,
		}
	}
}

/// An in-memory structure for tracking expiring records.
///
/// Internally maintains:
/// - `keys`: maps a record to its expiration time for quick lookup.
/// - `times`: maps an expiration time to a set of records expiring at that moment.
pub struct ExpireCache {
	keys: Arc<RwLock<HashMap<Record, Instant>>>,
	times: Arc<RwLock<BTreeMap<Instant, HashSet<Record>>>>,
}

impl ExpireCache {
	/// Creates a new, empty expiration cache.
	pub fn new() -> Self {
		Self {
			keys: Arc::new(RwLock::new(HashMap::new())),
			times: Arc::new(RwLock::new(BTreeMap::new())),
		}
	}

	/// Inserts a record and its expiration time into the cache.
	pub async fn insert(&self, item: ExpireItem) {
		self.keys.write().await.insert(item.record.clone(), item.time);
		self.times.write().await.entry(item.time).or_default().insert(item.record);
	}

	/// Retrieves the earliest expiration time and its associated records, if any.
	pub async fn earlier_keys(&self) -> Option<(Instant, HashSet<Record>)> {
		self.times.read().await.iter().next().map(|(t, ks)| (*t, ks.clone()))
	}

	/// Removes the earliest set of expired records.
	pub async fn remove_earlier(&self) {
		let mut times_guard = self.times.write().await;
		if let Some((&t, _)) = times_guard.iter().next() {
			if let Some(ks) = times_guard.remove(&t) {
				let mut keys_guard = self.keys.write().await;
				for k in &ks {
					keys_guard.remove(k);
				}
			}
		}
	}
}
