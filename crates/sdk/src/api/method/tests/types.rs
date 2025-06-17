use serde::Deserialize;
use serde::Serialize;
use surrealdb_core::proto::surrealdb::value::{value, Value as ValueProto};

pub const USER: &str = "user";

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct User {
	pub id: String,
	pub name: String,
}

impl TryFrom<ValueProto> for User {
	type Error = anyhow::Error;

	fn try_from(value: ValueProto) -> Result<Self, Self::Error> {
		let Some(value::Inner::Object(obj)) = value.inner else {
			return Err(anyhow::anyhow!("Expected an object value, got {:?}", value.inner));
		};

		let id = obj.get("id").and_then(|v| v.downcast_str()).ok_or_else(|| anyhow::anyhow!("Missing or invalid 'id' field"))?.to_owned();
		let name = obj.get("name").and_then(|v| v.downcast_str()).ok_or_else(|| anyhow::anyhow!("Missing or invalid 'name' field"))?.to_owned();
		Ok(User { id, name })
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
