use crate::sql::Value;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

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
