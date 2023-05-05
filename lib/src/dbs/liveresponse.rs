use crate::sql::Value;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

// LiveQueryID is a unique identifier for a live query
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LiveQueryID(Uuid);

// LiveQueryResponse is a response sent to listeners of live queries
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Notification {
	pub lqid: LiveQueryID,
	pub result: Value,
	// ommit node id as unnecessary
	pub action: Action,
}

impl Notification {
	pub fn new(lqid: LiveQueryID, result: Value, action: Action) -> Notification {
		Notification {
			lqid,
			result,
			action,
		}
	}
}

// The type of update in the live query
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Action {
	Create,
	Update,
	Delete,
}
