//! The datastore's own knobs.
//!
//! Unlike the per-query limits, these are read for as long as the datastore
//! lives. Two size work the datastore itself drives: the cross-transaction
//! definition cache, and the batch each export scan pulls. The other two select
//! and bound the live-query delivery path it runs. The datastore is the lowest
//! layer that reads them, so it owns them; the write path reads the selected
//! engine downward from here to decide whether a record change is captured as a
//! live-query event.

use std::fmt;
use std::str::FromStr;
use std::time::Duration;

use surrealdb_cnf as cnf;

/// Selects which live-query execution engine the datastore uses.
///
/// See [`crate::lq`] for the architecture. The default is
/// [`LiveQueryEngine::Inline`], so a datastore only moves off the inline
/// delivery path when this is set explicitly.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum LiveQueryEngine {
	/// Legacy engine: per-subscriber matching, permission checks, and projection
	/// run inline on the mutator's transaction path before commit, so write cost
	/// scales with the number of live subscribers on the written table.
	#[default]
	Inline,
	/// Inverted engine: the mutator only persists before/after values, and a
	/// per-node router performs per-subscriber matching off the write path, so
	/// write throughput is independent of the subscriber count.
	Router,
}

impl fmt::Display for LiveQueryEngine {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::Inline => f.write_str("inline"),
			Self::Router => f.write_str("router"),
		}
	}
}

impl FromStr for LiveQueryEngine {
	type Err = String;

	fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
		match s.to_lowercase().as_str() {
			"inline" => Ok(Self::Inline),
			"router" => Ok(Self::Router),
			v => Err(format!("Invalid live query engine: '{v}'. Expected 'inline' or 'router'")),
		}
	}
}

/// Sizes and live-query settings held for the datastore's lifetime.
#[derive(Clone, Debug)]
pub(crate) struct DatastoreConfig {
	/// Specifies the number of definitions which can be cached across transactions
	/// (default: 1,000)
	pub datastore_cache_size: usize,
	/// The maximum number of keys that should be scanned at once for export queries
	/// (default: 1000)
	pub export_batch_size: u32,
	/// Selects the live-query execution engine (default: [`LiveQueryEngine::Inline`]).
	pub live_query_engine: LiveQueryEngine,
	/// Retention window for the dedicated live-query event keyspace (only used when
	/// `live_query_engine` is `Router`). Sized to cover subscriber reconnect and
	/// rolling-upgrade windows during which a subscriber may need to replay missed
	/// events. Independent of any user-defined `CHANGEFEED` retention (default: 1h).
	pub live_query_retention: Duration,
}

impl Default for DatastoreConfig {
	fn default() -> Self {
		Self {
			datastore_cache_size: 1_000,
			export_batch_size: 1000,
			live_query_engine: LiveQueryEngine::Inline,
			live_query_retention: Duration::from_secs(3600),
		}
	}
}

impl cnf::Config for DatastoreConfig {
	fn parse(&mut self, map: &cnf::ConfigMap) {
		map.parse_key("datastore_cache_size", &mut self.datastore_cache_size)
			.parse_key("export_batch_size", &mut self.export_batch_size)
			.parse_key("live_query_engine", &mut self.live_query_engine)
			.parse_key_with("live_query_retention", &mut self.live_query_retention, |x| {
				cnf::parse_duration(x).ok()
			});
	}
}
