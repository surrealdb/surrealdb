use crate::param::Jwt;
use serde::Serialize;

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
pub struct NameSpace<'a> {
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

impl Credentials<Signin, Jwt> for NameSpace<'_> {}

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

impl<P> Credentials<Signup, Jwt> for Scope<'_, P> where P: Serialize {}
impl<P> Credentials<Signin, Jwt> for Scope<'_, P> where P: Serialize {}
