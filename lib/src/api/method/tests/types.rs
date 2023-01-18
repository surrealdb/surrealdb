use serde::Deserialize;
use serde::Serialize;

pub const USER: &str = "user";

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct User {
	pub id: String,
	pub name: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Credentials {
	Database {
		ns: String,
		db: String,
		user: String,
		pass: String,
	},
	Namespace {
		ns: String,
		user: String,
		pass: String,
	},
	Root {
		user: String,
		pass: String,
	},
	Scope {
		ns: String,
		db: String,
		sc: String,
	},
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AuthParams {}
