use serde::Deserialize;
use serde::Serialize;
use std::fmt;
use surrealdb::sql::Value;

/// A JSON Web Token for authenticating with the server
#[derive(Clone, Serialize, Deserialize)]
pub struct Jwt(pub(crate) String);

impl From<String> for Jwt {
	fn from(jwt: String) -> Self {
		Jwt(jwt)
	}
}

impl<'a> From<&'a String> for Jwt {
	fn from(jwt: &'a String) -> Self {
		Jwt(jwt.to_owned())
	}
}

impl<'a> From<&'a str> for Jwt {
	fn from(jwt: &'a str) -> Self {
		Jwt(jwt.to_owned())
	}
}

impl From<Jwt> for Value {
	fn from(Jwt(jwt): Jwt) -> Self {
		jwt.into()
	}
}

impl fmt::Debug for Jwt {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "Jwt(REDUCTED)")
	}
}
