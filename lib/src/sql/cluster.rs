use crate::sql::{Datetime, Strand};
use time::Instant;

// NOTE: This is not a statement, but as per layering, keeping it here till we
// have a better structure.
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[serde(rename = "$surrealdb::private::sql::ClusterMembership")]
pub struct ClusterMembership {
	pub name: Strand,
	pub heartbeat: Datetime, // We may just want timestamp
}
