#[cfg(test)]
use crate::dbs::fuzzy_eq::FuzzyEq;
use crate::sql::{Object, Uuid, Value};
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Debug, Display};

#[revisioned(revision = 1)]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
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

#[revisioned(revision = 2)]
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
#[non_exhaustive]
pub struct Notification {
	/// The id of the LIVE query to which this notification belongs
	pub id: Uuid,
	/// The CREATE / UPDATE / DELETE action which caused this notification
	pub action: Action,
	/// The resulting notification content, usually the altered record content
	pub result: Value,
	// session that started the query
	#[revision(start = 2)]
	pub session: Value,
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

impl Notification {
	/// Construct a new notification
	pub const fn new(id: Uuid, action: Action, result: Value, session: Value) -> Self {
		Self {
			id,
			action,
			result,
			session,
		}
	}
}

#[cfg(test)]
impl FuzzyEq for Notification {
	fn fuzzy_eq(&self, other: &Self) -> bool {
		self.action == other.action && self.result == other.result
	}
}
