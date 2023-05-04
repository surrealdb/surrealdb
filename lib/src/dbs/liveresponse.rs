use crate::sql::Value;
use uuid::Uuid;

// LiveQueryID is a unique identifier for a live query
pub type LiveQueryID = Uuid;

// LiveQueryResponse is a response sent to listeners of live queries
#[derive(Clone, Debug, PartialEq)]
pub struct LiveQueryResponse {
	pub lqid: LiveQueryID,
	pub result: Value,
	pub node_id: String,
	pub event_type: EventType,
}

// The type of update in the live query
#[derive(Clone, Debug, PartialEq)]
pub enum EventType {
	CREATE,
	UPDATE,
	DELETE,
}
