use std::fmt::{self, Debug, Display};
use std::str::FromStr;

use revision::revisioned;
use serde::{Deserialize, Serialize};

use crate::{Kind, KindLiteral, SurrealValue, Uuid, Value};

/// The action that caused the notification
#[revisioned(revision = 1)]
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
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

impl SurrealValue for Action {
	fn kind_of() -> Kind {
		Kind::Either(vec![
			Kind::Literal(KindLiteral::String(String::from("CREATE"))),
			Kind::Literal(KindLiteral::String(String::from("UPDATE"))),
			Kind::Literal(KindLiteral::String(String::from("DELETE"))),
			Kind::Literal(KindLiteral::String(String::from("KILLED"))),
		])
	}

	fn is_value(value: &Value) -> bool {
		matches!(value, Value::String(_))
	}

	fn into_value(self) -> Value {
		Value::String(self.to_string())
	}

	fn from_value(value: Value) -> anyhow::Result<Self> {
		Self::from_str(&value.into_string()?)
	}
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
#[revisioned(revision = 1)]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
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
