use revision::revisioned;
use serde::Deserialize;
use uuid::Uuid;

use crate::Value;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Deserialize)]
pub(crate) struct Failure {
	pub(crate) code: i64,
	pub(crate) message: String,
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, PartialEq, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
#[non_exhaustive]
pub enum ResponseAction {
	Create,
	Update,
	Delete,
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, PartialEq, Deserialize)]
pub struct ResponseNotification {
	pub id: Uuid,
	pub action: ResponseAction,
	pub result: Value,
}

#[revisioned(revision = 1)]
#[derive(Debug, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
#[doc(hidden)]
#[non_exhaustive]
pub enum Status {
	Ok,
	Err,
}

#[revisioned(revision = 1)]
#[derive(Debug, Deserialize)]
#[doc(hidden)]
#[non_exhaustive]
pub struct QueryMethodResponse {
	pub time: String,
	pub status: Status,
	pub result: Value,
}

#[revisioned(revision = 1)]
#[derive(Debug, Deserialize)]
pub(crate) enum Data {
	Other(Value),
	Query(Vec<QueryMethodResponse>),
	Live(ResponseNotification),
}
