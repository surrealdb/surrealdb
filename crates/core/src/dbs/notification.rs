use crate::expr::{Object, Uuid, Value};
use crate::protocol::flatbuffers::surreal_db::protocol::rpc as rpc_fb;
use crate::protocol::{FromFlatbuffers, ToFlatbuffers};
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Debug, Display};

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

impl ToFlatbuffers for Action {
	type Output<'bldr> = rpc_fb::Action;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> Self::Output<'bldr> {
		match self {
			Action::Create => rpc_fb::Action::Create,
			Action::Update => rpc_fb::Action::Update,
			Action::Delete => rpc_fb::Action::Delete,
			Action::Killed => rpc_fb::Action::Killed,
		}
	}
}

impl FromFlatbuffers for Action {
	type Input<'a> = rpc_fb::Action;

	#[inline]
	fn from_fb(reader: Self::Input<'_>) -> anyhow::Result<Self> {
		match reader {
			rpc_fb::Action::Create => Ok(Action::Create),
			rpc_fb::Action::Update => Ok(Action::Update),
			rpc_fb::Action::Delete => Ok(Action::Delete),
			rpc_fb::Action::Killed => Ok(Action::Killed),
			_ => Err(anyhow::anyhow!("Unknown action type in Notification")),
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

impl ToFlatbuffers for Notification {
	type Output<'bldr> = flatbuffers::WIPOffset<rpc_fb::LiveNotification<'bldr>>;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> Self::Output<'bldr> {
		let id = self.id.to_fb(builder);
		let action = self.action.to_fb(builder);
		let record = self.record.to_fb(builder);
		let result = self.result.to_fb(builder);

		rpc_fb::LiveNotification::create(
			builder,
			&rpc_fb::LiveNotificationArgs {
				id: Some(id),
				action,
				record: Some(record),
				result: Some(result),
			},
		)
	}
}

impl FromFlatbuffers for Notification {
	type Input<'a> = rpc_fb::LiveNotification<'a>;

	#[inline]
	fn from_fb(reader: Self::Input<'_>) -> anyhow::Result<Self> {
		let id = reader.id().ok_or_else(|| anyhow::anyhow!("Missing id in Notification"))?;
		let action = reader.action();
		let record =
			reader.record().ok_or_else(|| anyhow::anyhow!("Missing record in Notification"))?;
		let result =
			reader.result().ok_or_else(|| anyhow::anyhow!("Missing result in Notification"))?;

		Ok(Self {
			id: Uuid::from_fb(id)?,
			action: Action::from_fb(action)?,
			record: Value::from_fb(record)?,
			result: Value::from_fb(result)?,
		})
	}
}
