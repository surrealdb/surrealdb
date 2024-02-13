use crate::dbs::node::Timestamp;
use crate::sql::statements::LiveStatement;
use crate::sql::Uuid;
use crate::vs::Versionstamp;
use std::cmp::Ordering;

/// Used for cluster logic to move LQ data to LQ cleanup code
/// Not a stored struct; Used only in this module
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct LqValue {
	pub nd: Uuid,
	pub ns: String,
	pub db: String,
	pub tb: String,
	pub lq: Uuid,
}

#[derive(Debug)]
pub(crate) enum LqType {
	Nd(LqValue),
	Tb(LqValue),
}

impl LqType {
	fn get_inner(&self) -> &LqValue {
		match self {
			LqType::Nd(lq) => lq,
			LqType::Tb(lq) => lq,
		}
	}
}

impl PartialEq for LqType {
	fn eq(&self, other: &Self) -> bool {
		self.get_inner().lq == other.get_inner().lq
	}
}

impl Eq for LqType {}

impl PartialOrd for LqType {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		Option::Some(self.get_inner().lq.cmp(&other.get_inner().lq))
	}
}

impl Ord for LqType {
	fn cmp(&self, other: &Self) -> Ordering {
		self.get_inner().lq.cmp(&other.get_inner().lq)
	}
}

#[derive(Ord, PartialOrd, Eq, PartialEq, Clone)]
pub(crate) struct LqSelector {
	ns: String,
	db: String,
	tb: String,
}

/// This is an internal-only helper struct for organising the keys of how live queries are accessed
/// Because we want immutable keys, we cannot put mutable things in such as ts and vs
#[derive(Ord, PartialOrd, Eq, PartialEq, Clone)]
pub(crate) struct LqIndexKey {
	selector: LqSelector,
	lq: Uuid,
}

/// Internal only struct
/// This can be assumed to have a mutable reference
#[derive(Eq, PartialEq, Clone)]
struct LqIndexValue {
	query: LiveStatement,
	vs: Versionstamp,
	ts: Timestamp,
}

pub(crate) struct LqEntry {
	pub(crate) live_id: Uuid,
	pub(crate) ns: String,
	pub(crate) db: String,
}

impl Into<LqIndexKey> for LqEntry {
	fn into(&self) -> LqIndexKey {
		LqIndexKey {
			selector: LqSelector {
				ns: self.ns.clone(),
				db: self.db.clone(),
				tb: "".to_string(),
			},
			lq: self.live_id,
		}
	}
}
