use crate::sql::value::Value;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Operations(pub Vec<Operation>);

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Operation {
	pub op: String,
	pub prev: Option<String>,
	pub path: String,
	pub value: Value,
}
