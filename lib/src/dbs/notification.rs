use crate::sql::Value;
use serde::{Deserialize, Serialize};
use std::fmt;
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Notification {
	pub id: Uuid,
	pub action: Action,
	pub result: Value,
}

impl fmt::Display for Notification {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(
			f,
			"Notification {{ id: {}, action: {}, result: {} }}",
			self.id, self.action, self.result
		)
	}
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum Action {
	Create,
	Update,
	Delete,
}

impl fmt::Display for Action {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match *self {
			Action::Create => write!(f, "CREATE"),
			Action::Update => write!(f, "UPDATE"),
			Action::Delete => write!(f, "DELETE"),
		}
	}
}
