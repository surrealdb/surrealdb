use crate::dbs::node::Timestamp;
use crate::sql::{Uuid, Value};
use derive::Store;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Hash)]
#[serde(rename_all = "UPPERCASE")]
#[revisioned(revision = 1)]
pub enum KvsAction {
	Create,
	Update,
	Delete,
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

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum Action {
	Create,
	Update,
	Delete,
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct Notification {
	// Live query ID
	pub id: Uuid,
	pub action: Action,
	pub result: Value,
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
