use surrealdb_types::SurrealValue;

pub const USER: &str = "user";

#[derive(Debug, Default, SurrealValue)]
pub struct User {
	pub id: String,
	pub name: String,
}

#[derive(Debug, SurrealValue)]
pub struct Root {
	user: String,
	pass: String,
}

#[derive(Debug, Default, SurrealValue)]
pub struct SigninData {
	pub token: String,
	pub refresh: Option<String>,
}
