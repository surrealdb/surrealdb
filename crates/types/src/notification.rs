use std::fmt::{self, Debug, Display};
use std::str::FromStr;

use serde::{Deserialize, Serialize};

// Needed because we use the SurrealValue derive macro inside the crate which exports it :)
use crate as surrealdb_types;
use crate::{SurrealValue, Uuid, Value};

/// The action that caused the notification

#[derive(
	Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, SurrealValue,
)]
#[surreal(untagged, uppercase)]
#[serde(rename_all = "UPPERCASE")]
pub enum Action {
	/// Record was created.
	Create,
	/// Record was updated.
	Update,
	/// Record was deleted.
	Delete,
	/// The live query was killed.
	Killed,
}

impl Display for Action {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match *self {
			Action::Create => write!(f, "CREATE"),
			Action::Update => write!(f, "UPDATE"),
			Action::Delete => write!(f, "DELETE"),
			Action::Killed => write!(f, "KILLED"),
		}
	}
}

impl FromStr for Action {
	type Err = anyhow::Error;

	fn from_str(s: &str) -> anyhow::Result<Self> {
		match s {
			"CREATE" => Ok(Action::Create),
			"UPDATE" => Ok(Action::Update),
			"DELETE" => Ok(Action::Delete),
			"KILLED" => Ok(Action::Killed),
			_ => Err(anyhow::anyhow!("Invalid action: {s}")),
		}
	}
}

/// A live query notification.

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, SurrealValue)]
#[non_exhaustive]
pub struct Notification {
	/// The id of the LIVE query to which this notification belongs
	pub id: Uuid,
	/// The CREATE / UPDATE / DELETE action which caused this notification
	pub action: Action,
	/// The id of the document to which this notification has been made
	pub record: Value,
	/// The resulting notification content, usually the altered record content
	pub result: Value,
}

impl Notification {
	/// Construct a new notification.
	pub fn new(id: Uuid, action: Action, record: Value, result: Value) -> Self {
		Self {
			id,
			action,
			record,
			result,
		}
	}
}
