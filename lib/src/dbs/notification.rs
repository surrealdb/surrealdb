use crate::dbs::node::Timestamp;
use crate::sql::{Object, Uuid, Value};
use derive::Store;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Debug, Display};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Hash)]
#[serde(rename_all = "UPPERCASE")]
#[revisioned(revision = 1)]
pub enum Action {
	Create,
	Update,
	Delete,
}

impl Display for Action {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match *self {
			Action::Create => write!(f, "CREATE"),
			Action::Update => write!(f, "UPDATE"),
			Action::Delete => write!(f, "DELETE"),
		}
	}
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, Store, Hash)]
#[revisioned(revision = 1)]
pub struct Notification {
	// The Live Query ID used to differentiate between requests
	#[serde(rename = "id")]
	pub live_id: Uuid,
	// Node ID of the destined SurrealDB recipient
	pub node_id: Uuid,
	// Unique to avoid storage collisions
	pub notification_id: Uuid,
	// The type of change that happened
	pub action: Action,
	// The compute change that matches the user request
	pub result: Value,
	// The system-clock timestamp used for non-deterministic ordering
	pub timestamp: Timestamp,
}

impl Display for Notification {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		let obj: Object = map! {
			"live_id".to_string() => self.live_id.to_string().into(),
			"node_id".to_string() => self.node_id.to_string().into(),
			"notification_id".to_string() => self.notification_id.to_string().into(),
			"action".to_string() => self.action.to_string().into(),
			"result".to_string() => self.result.clone(),
			"timestamp".to_string() => self.timestamp.to_string().into(),
		}
		.into();
		write!(f, "{}", obj)
	}
}
