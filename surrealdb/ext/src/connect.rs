//! The connection flags `install` and `upgrade` take, and the credentials they
//! sign in with: the same flags, environment variables and auth levels as the
//! database CLI's client commands.

use clap::Args;
use clap::builder::{NonEmptyStringValueParser, PossibleValue, TypedValueParser};
use clap::error::{ContextKind, ContextValue, ErrorKind};
use surrealdb::opt::auth::{Database, Namespace, Root};
use thiserror::Error;

/// The level the authenticating user is defined on.
#[derive(Debug, Clone)]
pub enum CredentialsLevel {
	Root,
	Namespace,
	Database,
}

#[derive(Error, Debug)]
#[non_exhaustive]
pub enum Error {
	#[error("Username is needed for authentication but it was not provided")]
	Username,
	#[error("Password is needed for authentication but it was not provided")]
	Password,
	#[error("Namespace is needed for authentication but it was not provided")]
	Namespace,
	#[error("Database is needed for authentication but it was not provided")]
	Database,
}

/// Construct a Credentials instance for the given auth level
#[derive(Debug, Default)]
pub struct CredentialsBuilder {
	/// The auth username
	pub username: Option<String>,
	/// The auth password
	pub password: Option<String>,
	/// The auth namespace
	pub namespace: Option<String>,
	/// The auth database
	pub database: Option<String>,
}

impl CredentialsBuilder {
	// Builder methods
	pub fn with_username(mut self, username: impl Into<Option<String>>) -> Self {
		self.username = username.into();
		self
	}

	pub fn with_password(mut self, password: impl Into<Option<String>>) -> Self {
		self.password = password.into();
		self
	}

	pub fn with_namespace(mut self, namespace: impl Into<Option<String>>) -> Self {
		self.namespace = namespace.into();
		self
	}

	pub fn with_database(mut self, database: impl Into<Option<String>>) -> Self {
		self.database = database.into();
		self
	}

	pub fn root(self) -> Result<Root, Error> {
		Ok(Root {
			username: self.username.ok_or(Error::Username)?,
			password: self.password.ok_or(Error::Password)?,
		})
	}

	pub fn namespace(self) -> Result<Namespace, Error> {
		Ok(Namespace {
			username: self.username.ok_or(Error::Username)?,
			password: self.password.ok_or(Error::Password)?,
			namespace: self.namespace.ok_or(Error::Namespace)?,
		})
	}

	pub fn database(self) -> Result<Database, Error> {
		Ok(Database {
			username: self.username.ok_or(Error::Username)?,
			password: self.password.ok_or(Error::Password)?,
			namespace: self.namespace.ok_or(Error::Namespace)?,
			database: self.database.ok_or(Error::Database)?,
		})
	}
}

#[derive(Args, Debug)]
pub(crate) struct AuthArguments {
	#[arg(help = "Database authentication username to use when connecting")]
	#[arg(
		env = "SURREAL_USER",
		short = 'u',
		long = "username",
		visible_alias = "user",
		requires = "password"
	)]
	pub(crate) username: Option<String>,
	#[arg(help = "Database authentication password to use when connecting")]
	#[arg(
		env = "SURREAL_PASS",
		short = 'p',
		long = "password",
		visible_alias = "pass",
		requires = "username"
	)]
	pub(crate) password: Option<String>,
	#[arg(help = "Authentication token in JWT format to use when connecting")]
	#[arg(
		env = "SURREAL_TOKEN",
		short = 't',
		long = "token",
		conflicts_with_all = ["username", "password", "auth_level"],
	)]
	pub(crate) token: Option<String>,
	#[arg(help = "Level on which the authenticating user is defined")]
	#[arg(env = "SURREAL_AUTH_LEVEL", long = "auth-level", default_value = "root", requires_all = ["username", "password"])]
	#[arg(value_parser = CredentialsLevelParser)]
	pub(crate) auth_level: CredentialsLevel,
}

#[derive(Args, Debug)]
pub(crate) struct DatabaseSelectionArguments {
	#[arg(help = "The namespace selected for the operation")]
	#[arg(env = "SURREAL_NAMESPACE", long = "namespace", visible_alias = "ns")]
	pub(crate) namespace: String,
	#[arg(help = "The database selected for the operation")]
	#[arg(env = "SURREAL_DATABASE", long = "database", visible_alias = "db")]
	pub(crate) database: String,
}

#[derive(Args, Debug)]
pub(crate) struct DatabaseConnectionArguments {
	#[arg(help = "Remote database server url to connect to")]
	#[arg(short = 'e', long = "endpoint")]
	#[arg(default_value = "ws://localhost:8000")]
	#[arg(value_parser = endpoint_valid)]
	pub(crate) endpoint: String,
}

/// Accepts a remote endpoint: this tool connects to a running server and embeds
/// no storage engine.
fn endpoint_valid(v: &str) -> Result<String, String> {
	let without_query = v.split_once('?').map(|(s, _)| s).unwrap_or(v);
	match without_query.split_once("://").map(|(scheme, _)| scheme) {
		Some("http" | "https" | "ws" | "wss") => Ok(v.to_string()),
		_ => Err(String::from(
			"Provide the url of a running server: http://, https://, ws:// or wss://",
		)),
	}
}

/// Parses `--auth-level`.
#[derive(Clone)]
struct CredentialsLevelParser;

impl TypedValueParser for CredentialsLevelParser {
	type Value = CredentialsLevel;

	fn parse_ref(
		&self,
		cmd: &clap::Command,
		arg: Option<&clap::Arg>,
		value: &std::ffi::OsStr,
	) -> Result<Self::Value, clap::Error> {
		let inner = NonEmptyStringValueParser::new();
		let v = inner.parse_ref(cmd, arg, value)?;

		match v.as_str() {
			"root" => Ok(CredentialsLevel::Root),
			"namespace" | "ns" => Ok(CredentialsLevel::Namespace),
			"database" | "db" => Ok(CredentialsLevel::Database),
			v => {
				let mut err = clap::Error::new(ErrorKind::InvalidValue);
				err.insert(ContextKind::InvalidValue, ContextValue::String(v.to_string()));
				Err(err)
			}
		}
	}

	fn possible_values(&self) -> Option<Box<dyn Iterator<Item = PossibleValue> + '_>> {
		Some(Box::new(
			[
				PossibleValue::new("root"),
				PossibleValue::new("namespace"),
				PossibleValue::new("ns"),
				PossibleValue::new("database"),
				PossibleValue::new("db"),
			]
			.into_iter(),
		))
	}
}
