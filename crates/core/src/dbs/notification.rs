use crate::expr::{Object, Uuid, Value};
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Debug, Display};
use surrealdb_protocol::proto::rpc::v1 as rpc_proto;

#[revisioned(revision = 2)]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
#[non_exhaustive]
pub enum Action {
	Create,
	Update,
	Delete,
	#[revision(start = 2)]
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

impl TryFrom<rpc_proto::Action> for Action {
	type Error = anyhow::Error;

	fn try_from(value: rpc_proto::Action) -> Result<Self, Self::Error> {
		match value {
			rpc_proto::Action::Create => Ok(Action::Create),
			rpc_proto::Action::Update => Ok(Action::Update),
			rpc_proto::Action::Delete => Ok(Action::Delete),
			rpc_proto::Action::Killed => Ok(Action::Killed),
			unexpected => Err(anyhow::anyhow!("Unknown Action type: {unexpected:?}")),
		}
	}
}

impl TryFrom<Action> for rpc_proto::Action {
	type Error = anyhow::Error;

	fn try_from(value: Action) -> Result<Self, Self::Error> {
		match value {
			Action::Create => Ok(rpc_proto::Action::Create),
			Action::Update => Ok(rpc_proto::Action::Update),
			Action::Delete => Ok(rpc_proto::Action::Delete),
			Action::Killed => Ok(rpc_proto::Action::Killed),
		}
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
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

impl Display for Notification {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		let obj: Object = map! {
			"id".to_string() => self.id.to_string().into(),
			"action".to_string() => self.action.to_string().into(),
			"record".to_string() => self.record.clone(),
			"result".to_string() => self.result.clone(),
		}
		.into();
		write!(f, "{}", obj)
	}
}

impl Notification {
	/// Construct a new notification
	pub const fn new(id: Uuid, action: Action, record: Value, result: Value) -> Self {
		Self {
			id,
			action,
			record,
			result,
		}
	}
}

impl TryFrom<rpc_proto::LiveResponse> for Notification {
	type Error = anyhow::Error;

	fn try_from(value: rpc_proto::LiveResponse) -> Result<Self, Self::Error> {
		todo!()
	}
}

impl TryFrom<Notification> for rpc_proto::LiveResponse {
	type Error = anyhow::Error;

	fn try_from(value: Notification) -> Result<Self, Self::Error> {
		todo!()
	}
}
