use crate::sql::{Uuid, Value};
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Notification {
	pub id: Uuid,
	pub action: Action,
	pub result: Value,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum Action {
	Create,
	Update,
	Delete,
}

impl fmt::Display for Notification {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(
			f,
			"Notification - ID: {}, Action: {}, Result: {}",
			self.id, self.action, self.result
		)
	}
}

impl fmt::Display for Action {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		let action_str = match self {
			Action::Create => "Create",
			Action::Update => "Update",
			Action::Delete => "Delete",
		};
		write!(f, "{}", action_str)
	}
}
