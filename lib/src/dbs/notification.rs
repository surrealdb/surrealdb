use crate::sql::{Object, Uuid, Value};
use serde::{Deserialize, Serialize};
use std::fmt::{self, Debug, Display};

/// The action performed on a record
///
/// This is used in live query notifications.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq, Ord, PartialOrd, Hash)]
#[serde(rename_all = "UPPERCASE")]
#[non_exhaustive]
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
	pub id: Uuid,
	pub action: Action,
	pub result: Value,
}

impl Display for Notification {
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
