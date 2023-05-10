use crate::sql::cluster_timestamp::Timestamp;
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
