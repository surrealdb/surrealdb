//! Authentication types

use crate::sql::Value;
use serde::Deserialize;
use serde::Serialize;
use std::fmt;

/// A signup action
#[derive(Debug)]
pub struct Signup;

/// A signin action
#[derive(Debug)]
pub struct Signin;

/// Credentials for authenticating with the server
pub trait Credentials<Action, Response>: Serialize {}

/// Credentials for the root user
#[derive(Debug, Serialize)]
pub struct Root<'a> {
	/// The username of the root user
	#[serde(rename = "user")]
	pub username: &'a str,
	/// The password of the root user
	#[serde(rename = "pass")]
	pub password: &'a str,
}

impl Credentials<Signin, ()> for Root<'_> {}

/// Credentials for the namespace user
#[derive(Debug, Serialize)]
pub struct Namespace<'a> {
	/// The namespace the user has access to
	#[serde(rename = "ns")]
	pub namespace: &'a str,
	/// The username of the namespace user
	#[serde(rename = "user")]
	pub username: &'a str,
	/// The password of the namespace user
	#[serde(rename = "pass")]
	pub password: &'a str,
}

impl Credentials<Signin, Jwt> for Namespace<'_> {}

/// Credentials for the database user
#[derive(Debug, Serialize)]
pub struct Database<'a> {
	/// The namespace the user has access to
	#[serde(rename = "ns")]
	pub namespace: &'a str,
	/// The database the user has access to
	#[serde(rename = "db")]
	pub database: &'a str,
	/// The username of the database user
	#[serde(rename = "user")]
	pub username: &'a str,
	/// The password of the database user
	#[serde(rename = "pass")]
	pub password: &'a str,
}

impl Credentials<Signin, Jwt> for Database<'_> {}

/// Credentials for the scope user
#[derive(Debug, Serialize)]
pub struct Scope<'a, P> {
	/// The namespace the user has access to
	#[serde(rename = "ns")]
	pub namespace: &'a str,
	/// The database the user has access to
	#[serde(rename = "db")]
	pub database: &'a str,
	/// The scope to use for signin and signup
	#[serde(rename = "sc")]
	pub scope: &'a str,
	/// The additional params to use
	#[serde(flatten)]
	pub params: P,
}

impl<T, P> Credentials<T, Jwt> for Scope<'_, P> where P: Serialize {}

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
		write!(f, "Jwt(REDACTED)")
	}
}
