use crate::sql::{Datetime, Strand};
use derive::Store;
use serde::{Deserialize, Serialize};
use time::Instant;

// NOTE: This is not a statement, but as per layering, keeping it here till we
// have a better structure.
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash, Store)]
#[serde(rename = "$surrealdb::private::sql::ClusterMembership")]
pub struct ClusterMembership {
	pub name: String,
	pub heartbeat: Instant, // We may just want timestamp
}
