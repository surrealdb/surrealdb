use derive::Store;
use serde::{Deserialize, Serialize};

// NOTE: This is not a statement, but as per layering, keeping it here till we
// have a better structure.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize, PartialOrd, Hash, Store)]
pub struct ClusterMembership {
	pub name: String,
	// TiKV = TiKV TSO Timestamp as u64
	// not TiKV = local nanos as u64
	pub heartbeat: Timestamp,
}
// This struct is meant to represent a timestamp that can be used to partially order
// events in a cluster. It should be derived from a timestamp oracle, such as the
// one available in TiKV via the client `TimestampExt` implementation.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize, PartialOrd, Hash, Store)]
pub struct Timestamp {
	pub value: u64,
}
