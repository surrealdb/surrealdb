use anyhow::Context;
use serde::Deserialize;
use serde::Serialize;
use surrealdb_core::expr::Object;
use surrealdb_core::protocol::flatbuffers::surreal_db::protocol::expr::Value as ValueFb;
use surrealdb_core::protocol::FromFlatbuffers;

pub const USER: &str = "user";

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct User {
	pub id: String,
	pub name: String,
}

impl<'rpc> TryFrom<ValueFb<'rpc>> for User {
	type Error = anyhow::Error;

	fn try_from(value: ValueFb<'rpc>) -> Result<Self, Self::Error> {
		
		let object = value.value_as_object()
			.ok_or_else(|| anyhow::anyhow!("Expected an object value, got {:?}", value.value_type()))?;

		let objet = Object::from_fb(object)
			.map_err(|e| anyhow::anyhow!("Failed to convert from flatbuffers object: {}", e))?;

		let id = objet.get("id")
			.context("Missing 'id' field in User object")?
			.as_string();

		let name = objet.get("name")
			.context("Missing 'name' field in User object")?
			.as_string();

		Ok(User {
			id,
			name,
		})
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
