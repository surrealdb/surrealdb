use crate::dbs::node::Timestamp;
use crate::sql::{Object, Uuid, Value};
use derive::Store;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Debug, Display};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Hash)]
#[serde(rename_all = "UPPERCASE")]
#[revisioned(revision = 1)]
pub enum KvsAction {
	Create,
	Update,
	Delete,
}

impl Display for KvsAction {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match *self {
			KvsAction::Create => write!(f, "CREATE"),
			KvsAction::Update => write!(f, "UPDATE"),
			KvsAction::Delete => write!(f, "DELETE"),
		}
	}
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, Store, Hash)]
#[revisioned(revision = 1)]
pub struct KvsNotification {
	// The Live Query ID used to differentiate between requests
	#[serde(rename = "id")]
	pub live_id: Uuid,
	// Node ID of the destined SurrealDB recipient
	pub node_id: Uuid,
	// Unique to avoid storage collisions
	pub notification_id: Uuid,
	// The type of change that happened
	pub action: KvsAction,
	// The compute change that matches the user request
	pub result: Value,
	// The system-clock timestamp used for non-deterministic ordering
	pub timestamp: Timestamp,
}

impl Display for KvsNotification {
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

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
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

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct Notification {
	// Live query ID
	pub id: Uuid,
	pub action: Action,
	pub result: Value,
}

impl Display for Notification {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(
			f,
			"Notification {{id: {}, action: {}, result: {}}}",
			self.id, self.action, self.result
		)
	}
}

impl From<&KvsNotification> for Notification {
	fn from(n: &KvsNotification) -> Self {
		Self {
			id: n.live_id,
			action: Action::from(&n.action),
			result: n.result.clone(),
		}
	}
}

impl From<&KvsAction> for Action {
	fn from(value: &KvsAction) -> Self {
		match value {
			KvsAction::Create => Self::Create,
			KvsAction::Update => Self::Update,
			KvsAction::Delete => Self::Delete,
		}
	}
}
