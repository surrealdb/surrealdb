use crate::expr::{Object, Thing as RecordId, Uuid, Value};
use anyhow::Context;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Debug, Display};
use surrealdb_protocol::proto::rpc::v1 as rpc_proto;

#[revisioned(revision = 2)]
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Hash)]
#[serde(rename_all = "UPPERCASE")]
#[non_exhaustive]
pub enum Action {
	Create,
	Update,
	Delete,
	#[revision(start = 2)]
	Killed,
}

// impl Ord for Action {
// 	fn cmp(&self, other: &Self) -> std::cmp::Ordering {
// 		self.partial_cmp(other).unwrap()
// 	}
// }

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
			rpc_proto::Action::Created => Ok(Action::Create),
			rpc_proto::Action::Updated => Ok(Action::Update),
			rpc_proto::Action::Deleted => Ok(Action::Delete),
			rpc_proto::Action::Killed => Ok(Action::Killed),
			unexpected => Err(anyhow::anyhow!("Unknown Action type: {unexpected:?}")),
		}
	}
}

impl TryFrom<Action> for rpc_proto::Action {
	type Error = anyhow::Error;

	fn try_from(value: Action) -> Result<Self, Self::Error> {
		match value {
			Action::Create => Ok(rpc_proto::Action::Created),
			Action::Update => Ok(rpc_proto::Action::Updated),
			Action::Delete => Ok(rpc_proto::Action::Deleted),
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
	/// The id of the document to which this notification has been made.
	///
	pub record: Option<RecordId>,
	/// The resulting notification content, usually the altered record content
	pub result: Value,
}

impl Display for Notification {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		let obj: Object = map! {
			"id".to_string() => self.id.to_string().into(),
			"action".to_string() => self.action.to_string().into(),
			"record".to_string() => match &self.record {
				Some(record) => record.to_string().into(),
				None => Value::None,
			},
			"result".to_string() => self.result.clone(),
		}
		.into();
		write!(f, "{}", obj)
	}
}

impl Notification {
	/// Construct a new notification
	pub const fn new(id: Uuid, action: Action, record: Option<RecordId>, result: Value) -> Self {
		Self {
			id,
			action,
			record,
			result,
		}
	}
}

impl TryFrom<rpc_proto::Notification> for Notification {
	type Error = anyhow::Error;

	#[inline]
	fn try_from(proto: rpc_proto::Notification) -> Result<Self, Self::Error> {
		let action = proto.action().try_into()?;
		Ok(Notification {
			id: proto.live_query_id.context("live_query_id is required")?.try_into()?,
			action,
			record: proto.record_id.map(TryInto::try_into).transpose()?,
			result: proto.value.context("value is required")?.try_into()?,
		})
	}
}

impl TryFrom<Notification> for rpc_proto::Notification {
	type Error = anyhow::Error;

	#[inline]
	fn try_from(notification: Notification) -> Result<Self, Self::Error> {
		Ok(rpc_proto::Notification {
			live_query_id: Some(notification.id.try_into()?),
			action: rpc_proto::Action::try_from(notification.action)? as i32,
			record_id: notification.record.map(TryInto::try_into).transpose()?,
			value: Some(notification.result.try_into()?),
		})
	}
}

impl TryFrom<rpc_proto::SubscribeResponse> for Notification {
	type Error = anyhow::Error;

	#[inline]
	fn try_from(proto: rpc_proto::SubscribeResponse) -> Result<Self, Self::Error> {
		let notification = proto.notification.ok_or(anyhow::anyhow!("No notification found"))?;
		notification.try_into()
	}
}

impl TryFrom<Notification> for rpc_proto::SubscribeResponse {
	type Error = anyhow::Error;

	#[inline]
	fn try_from(value: Notification) -> Result<Self, Self::Error> {
		Ok(rpc_proto::SubscribeResponse {
			notification: Some(value.try_into()?),
		})
	}
}
