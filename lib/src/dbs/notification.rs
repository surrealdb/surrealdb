use crate::dbs::node::Timestamp;
use crate::sql::{Object, Uuid, Value};
use derive::Store;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Debug, Display};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Hash)]
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

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, Store, Hash)]
#[revisioned(revision = 1)]
pub struct Notification {
	pub live_id: Uuid,
	pub node_id: Uuid,
	pub notification_id: Uuid,
	pub action: Action,
	pub result: Value,
	pub timestamp: Timestamp,
}

impl Display for Notification {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		let obj: Object = map! {
			"id".to_string() => self.live_id.to_string().into(),
			"action".to_string() => self.action.to_string().into(),
			"result".to_string() => self.result.clone(),
		}
		.into();
		write!(f, "{}", obj)
	}
}
