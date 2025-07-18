use serde::{Deserialize, Serialize};

pub const USER: &str = "user";

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct User {
	pub id: String,
	pub name: String,
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
