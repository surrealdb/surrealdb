use crate::dbs::node::Timestamp;
use crate::sql::statements::LiveStatement;
use crate::sql::Uuid;
use crate::vs::Versionstamp;
use std::cmp::Ordering;

/// Used for cluster logic to move LQ data to LQ cleanup code
/// Not a stored struct; Used only in this module
///
/// This struct is public because it is used in Live Query errors for v1.
/// V1 is now deprecated and the struct can be made non-public
#[derive(Debug, Clone, Eq, PartialEq)]
#[non_exhaustive]
pub struct LqValue {
	pub nd: Uuid,
	pub ns: String,
	pub db: String,
	pub tb: String,
	pub lq: Uuid,
}

/// Used to track unreachable live queries in v1
#[derive(Debug)]
pub(crate) enum UnreachableLqType {
	Nd(LqValue),
	Tb(LqValue),
}

impl UnreachableLqType {
	pub(crate) fn get_inner(&self) -> &LqValue {
		match self {
			UnreachableLqType::Nd(lq) => lq,
			UnreachableLqType::Tb(lq) => lq,
		}
	}
}

impl PartialEq for UnreachableLqType {
	fn eq(&self, other: &Self) -> bool {
		self.get_inner().lq == other.get_inner().lq
	}
}

impl Eq for UnreachableLqType {}

impl PartialOrd for UnreachableLqType {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		Option::Some(self.get_inner().lq.cmp(&other.get_inner().lq))
	}
}

impl Ord for UnreachableLqType {
	fn cmp(&self, other: &Self) -> Ordering {
		self.get_inner().lq.cmp(&other.get_inner().lq)
	}
}

/// LqSelector is used for tracking change-feed backed queries in a common baseline
/// The intention is to have a collection of live queries that can have batch operations performed on them
/// This reduces the number of change feed queries
#[derive(Ord, PartialOrd, Eq, PartialEq, Clone, Debug)]
pub(crate) struct LqSelector {
	pub(crate) ns: String,
	pub(crate) db: String,
	pub(crate) tb: String,
}

/// This is an internal-only helper struct for organising the keys of how live queries are accessed
/// Because we want immutable keys, we cannot put mutable things in such as ts and vs
#[derive(Ord, PartialOrd, Eq, PartialEq, Clone, Debug)]
pub(crate) struct LqIndexKey {
	pub(crate) selector: LqSelector,
	pub(crate) lq: Uuid,
}

/// Internal only struct
/// This can be assumed to have a mutable reference
#[derive(Eq, PartialEq, Clone, Debug)]
pub(crate) struct LqIndexValue {
	pub(crate) stm: LiveStatement,
	pub(crate) vs: Versionstamp,
	// TODO(phughk, pre-2.0): unused? added because we have access to timestamp checkpoints but they arent used and this can be deleted
	pub(crate) ts: Timestamp,
}

/// Stores all data required for tracking a live query
/// Can be used to derive various in-memory map indexes and values
#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq, Clone))]
pub(crate) struct LqEntry {
	pub(crate) live_id: Uuid,
	pub(crate) ns: String,
	pub(crate) db: String,
	pub(crate) stm: LiveStatement,
}

/// This is a type representing information that is tracked outside of a datastore
/// For example, live query IDs need to be tracked by websockets so they are closed correctly on closing a connection
#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq, Clone))]
#[allow(dead_code)]
pub(crate) enum TrackedResult {
	LiveQuery(LqEntry),
	KillQuery(KillEntry),
}

/// KillEntry is a type that is used to hold the data necessary to kill a live query
/// It is not used for any indexing
#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq, Clone))]
pub(crate) struct KillEntry {
	pub(crate) live_id: Uuid,
	pub(crate) ns: String,
	pub(crate) db: String,
}

impl LqEntry {
	/// Treat like an into from a borrow
	pub(crate) fn as_key(&self) -> LqIndexKey {
		let tb = self.stm.what.to_string();
		LqIndexKey {
			selector: LqSelector {
				ns: self.ns.clone(),
				db: self.db.clone(),
				tb,
			},
			lq: self.live_id,
		}
	}

	pub(crate) fn as_value(&self, vs: Versionstamp, ts: Timestamp) -> LqIndexValue {
		LqIndexValue {
			stm: self.stm.clone(),
			vs,
			ts,
		}
	}
}
