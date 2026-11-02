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
use surrealdb_kvs::Release;

use crate::kvs::DatastoreError;

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
	/// Host-driven engine: the write path neither captures record changes for a
	/// router nor notifies inline, and writes no change feed. `LIVE` and `KILL`
	/// still commit their subscription and node keys in the statement's
	/// transaction. The host observes the committed record changes itself and
	/// asks the datastore to build each subscription's notification with
	/// [`Datastore::live_notifications`](crate::kvs::Datastore::live_notifications).
	External,
}

impl LiveQueryEngine {
	/// Whether a write notifies the table's subscriptions on its own path,
	/// before its transaction commits.
	pub(crate) fn notifies_inline(self) -> bool {
		self == Self::Inline
	}

	/// Whether a write captures its record change into the live-event keyspace
	/// for the router to replay.
	pub(crate) fn captures_events(self) -> bool {
		self == Self::Router
	}

	/// Whether a write appends to the table's change feed.
	pub(crate) fn writes_changefeed(self) -> bool {
		self != Self::External
	}
}

impl fmt::Display for LiveQueryEngine {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::Inline => f.write_str("inline"),
			Self::Router => f.write_str("router"),
			Self::External => f.write_str("external"),
		}
	}
}

impl FromStr for LiveQueryEngine {
	type Err = String;

	fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
		match s.to_lowercase().as_str() {
			"inline" => Ok(Self::Inline),
			"router" => Ok(Self::Router),
			"external" => Ok(Self::External),
			v => Err(format!(
				"Invalid live query engine: '{v}'. Expected 'inline', 'router' or 'external'"
			)),
		}
	}
}

/// The feature level an operator asked a datastore to be created at, or raised
/// to, as the `feature_level` setting spells it.
///
/// Parsing never fails, because a setting that cannot be parsed has to reach
/// the builder, which refuses it: the configuration reader only warns and keeps
/// the default. [`Self::resolve`] validates it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum FeatureLevelSetting {
	/// No pin was configured.
	#[default]
	Unset,
	/// A release, as `M.m`, or this build's release as `current`. Not yet
	/// checked against the releases this build can hold.
	Pin(Release),
	/// Keep a store that is at [`surrealdb_kvs::FeatureLevel::Legacy`] when it is opened at that
	/// level: nothing raises it, neither the operator's pin nor the single-process
	/// raise. A store that records a level, and a store the build creates, are not
	/// affected, so the hold names no release and never lowers anything.
	Legacy,
	/// A setting that is neither `current`, `legacy` nor a release of the form
	/// `M.m`.
	Invalid,
}

impl FromStr for FeatureLevelSetting {
	type Err = std::convert::Infallible;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		if s.eq_ignore_ascii_case("current") {
			return Ok(Self::Pin(Release::CURRENT));
		}
		if s.eq_ignore_ascii_case("legacy") {
			return Ok(Self::Legacy);
		}
		Ok(Release::parse(s).map_or(Self::Invalid, Self::Pin))
	}
}

impl FeatureLevelSetting {
	/// The validated pin: `None` where none is configured or the setting is the
	/// Legacy hold, and an error where the setting is malformed or names a
	/// release outside [`Release::FIRST_STORED`]`..=`[`Release::CURRENT`].
	pub(crate) fn resolve(self) -> Result<Option<Release>, DatastoreError> {
		match self {
			Self::Unset | Self::Legacy => Ok(None),
			Self::Invalid => Err(DatastoreError::FeatureLevelPinInvalid),
			Self::Pin(release) => validate_feature_level_pin(release).map(Some),
		}
	}

	/// Whether the setting is the Legacy hold.
	pub(crate) const fn holds_legacy(self) -> bool {
		matches!(self, Self::Legacy)
	}
}

/// Checks that `release` is one this build can hold as a feature level: not
/// newer than this build, and not older than the first release with one.
pub(crate) fn validate_feature_level_pin(release: Release) -> Result<Release, DatastoreError> {
	if release > Release::CURRENT {
		return Err(DatastoreError::FeatureLevelAboveRelease {
			target: release,
			current: Release::CURRENT,
		});
	}
	if release < Release::FIRST_STORED {
		return Err(DatastoreError::FeatureLevelBelowFirst {
			first: Release::FIRST_STORED,
		});
	}
	Ok(release)
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
	/// Retention window for the dedicated live-query event keyspace, which only
	/// the `Router` engine writes to and which is collected whichever engine
	/// runs, so entries captured before a switch of engine age out. Sized to
	/// cover subscriber reconnect and rolling-upgrade windows during which a
	/// subscriber may need to replay missed events. It also bounds how far
	/// behind the writes a node's router may fall: an event the router has yet
	/// to deliver when the window passes it is collected before delivery, and
	/// the router reports a lap found behind the window. Independent of any
	/// user-defined `CHANGEFEED` retention (default: 1h).
	pub live_query_retention: Duration,
	/// The feature level a new datastore is created at, and that an existing one
	/// is raised to when it is opened (default: none, which creates a new
	/// datastore at this build's release and leaves an existing one as it is
	/// unless its engine admits no other writer). The builder replaces it with
	/// the validated pin, or leaves the Legacy hold as it is.
	pub feature_level: FeatureLevelSetting,
	/// Whether a datastore a 3.x release last wrote is upgraded in place when it
	/// is opened (default: false, which refuses to open it). The upgrade is
	/// experimental and rewrites stored data, so it runs only when the operator
	/// opts in.
	pub experimental_upgrade_from_3x: bool,
}

impl Default for DatastoreConfig {
	fn default() -> Self {
		Self {
			datastore_cache_size: 1_000,
			export_batch_size: 1000,
			live_query_engine: LiveQueryEngine::Inline,
			live_query_retention: Duration::from_secs(3600),
			feature_level: FeatureLevelSetting::Unset,
			experimental_upgrade_from_3x: false,
		}
	}
}

impl cnf::Config for DatastoreConfig {
	fn parse(&mut self, map: &cnf::ConfigMap) {
		map.parse_key("datastore_cache_size", &mut self.datastore_cache_size)
			.parse_key("export_batch_size", &mut self.export_batch_size)
			.parse_key("live_query_engine", &mut self.live_query_engine)
			.parse_key("feature_level", &mut self.feature_level)
			.parse_key_bool("experimental_upgrade_from_3x", &mut self.experimental_upgrade_from_3x)
			.parse_key_with("live_query_retention", &mut self.live_query_retention, |x| {
				cnf::parse_duration(x).ok()
			});
	}
}
