//! The model module describes public driver and protocol API structures
//! The reason is that we have internal representations of these objects that contain excessive
//! information that we do not want to expose or would like to version separately
use crate::sql::{Uuid, Value};
use serde::{Deserialize, Serialize};
use std::fmt::{self, Debug, Display};

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

impl From<crate::dbs::Notification> for Notification {
	fn from(n: crate::dbs::Notification) -> Self {
		Self {
			id: n.live_id,
			action: Action::from(n.action),
			result: n.result,
		}
	}
}

impl From<crate::dbs::Action> for Action {
	fn from(value: crate::dbs::Action) -> Self {
		match value {
			crate::dbs::Action::Create => Self::Create,
			crate::dbs::Action::Update => Self::Update,
			crate::dbs::Action::Delete => Self::Delete,
		}
	}
}
