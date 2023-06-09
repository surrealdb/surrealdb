use crate::sql::{Uuid, Value};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Notification {
	pub id: Uuid,
	pub action: Action,
	pub result: Value,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum Action {
	Create,
	Update,
	Delete,
}
