use serde::Deserialize;
use serde::Serialize;
use surrealdb_core::dbs::Variables;
use surrealdb_core::expr::Object;

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
