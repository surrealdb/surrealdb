use anyhow::{Context, Result};
use serde::Deserialize;
use serde::Serialize;
use surrealdb_core::dbs::Variables;
use surrealdb_core::expr::Value;
use surrealdb_core::protocol::TryFromValue;
use surrealdb_protocol::TryIntoValue;
use surrealdb_protocol::proto::v1::Object as ObjectProto;
use surrealdb_protocol::proto::v1::Value as ValueProto;

use crate::opt::IntoVariables;

pub const USER: &str = "user";

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct User {
	pub id: String,
	pub name: String,
}

impl IntoVariables for User {
	fn into_variables(self) -> Variables {
		let mut vars = Variables::default();
		vars.insert("id".to_string(), self.id.into());
		vars.insert("name".to_string(), self.name.into());
		vars
	}
}

impl TryIntoValue for User {
	fn try_into_value(self) -> Result<ValueProto> {
		Ok(ValueProto::object(ObjectProto::new(
			[("id".to_string(), self.id.into()), ("name".to_string(), self.name.into())]
				.into_iter()
				.collect(),
		)))
	}
}
impl TryFromValue for User {
	fn try_from_value(value: ValueProto) -> Result<Self> {
		let id = String::try_from_value(value.get("id").context("id is required")?.clone())?;
		let name = String::try_from_value(value.get("name").context("name is required")?.clone())?;
		Ok(User {
			id,
			name,
		})
	}
}

impl TryFrom<ValueProto> for User {
	type Error = anyhow::Error;

	fn try_from(value: ValueProto) -> Result<Self> {
		Self::try_from_value(value)
	}
}

impl TryFrom<User> for Value {
	type Error = anyhow::Error;

	fn try_from(value: User) -> Result<Self> {
		Ok(Value::Object(
			[("id".to_string(), value.id.into()), ("name".to_string(), value.name.into())]
				.into_iter()
				.collect(),
		))
	}
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Root {
	user: String,
	pass: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AuthParams {}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct SigninData {
	pub token: String,
	pub refresh: Option<String>,
}
