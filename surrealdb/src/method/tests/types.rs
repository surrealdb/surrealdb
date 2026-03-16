use crate::types::SurrealValue;

pub const USER: &str = "user";

#[derive(Debug, Default, SurrealValue)]
#[surreal(crate = "crate::types")]
pub struct User {
	pub id: String,
	pub name: String,
}
