use crate::sql::{Object, Value};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::Debug;
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Notification {
	pub id: Uuid,
	pub action: Action,
	pub result: Value,
}

impl fmt::Display for Notification {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		let obj: Object = map! {
			"id".to_string() => self.id.to_string().into(),
			"action".to_string() => self.action.to_string().into(),
			"result".to_string() => self.result.clone(),
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

impl fmt::Display for Action {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match *self {
			Action::Create => write!(f, "CREATE"),
			Action::Update => write!(f, "UPDATE"),
			Action::Delete => write!(f, "DELETE"),
		}
	}
}
