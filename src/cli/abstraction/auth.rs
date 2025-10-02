//! Authentication client types

use surrealdb::opt::auth::{Database, Namespace, Root};
use thiserror::Error;

/// Credentials level
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
