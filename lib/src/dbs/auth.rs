use std::fmt;

use once_cell::sync::OnceCell;

use crate::err::Error;

pub static AUTH_ENABLED: OnceCell<bool> = OnceCell::new();

/// The authentication level for a datastore execution context.
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
pub enum Level {
	No,
	Kv,
	Ns,
	Db,
	Sc,
}

impl fmt::Display for Level {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Level::No => write!(f, "unauthenticated"),
			Level::Kv => write!(f, "root"),
			Level::Ns => write!(f, "namespace"),
			Level::Db => write!(f, "database"),
			Level::Sc => write!(f, "scope"),
		}
	}
}

impl TryFrom<&str> for Level {
	type Error = Error;

	fn try_from(value: &str) -> Result<Self, Self::Error> {
		match value.to_lowercase().as_str() {
			"unauthenticated" | "no" => Ok(Level::No),
			"root" | "kv" => Ok(Level::Kv),
			"namespace" | "ns" => Ok(Level::Ns),
			"database" | "db" => Ok(Level::Db),
			"scope" | "sc" => Ok(Level::Sc),
			_ => Err(Error::InvalidLevel {
				level: value.to_string(),
			}),
		}
	}
}

impl TryFrom<String> for Level {
	type Error = Error;

	fn try_from(value: String) -> Result<Self, Self::Error> {
		Self::try_from(value.as_str())
	}
}

/// Specifies the current authentication for the datastore execution context.
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd)]
pub enum Auth {
	/// Specifies that the user is not authenticated
	#[default]
	No,
	/// Specifies that the user has full permissions for the KV level
	Kv,
	/// Specifies that the user has full permissions for a particular Namespace
	Ns(String),
	/// Specifies that the user has full permissions for a particular Namespace and Database
	Db(String, String),
	/// Specifies that the user has full permissions for a particular Namespace, Database, and Scope
	Sc(String, String, String),
}

impl Auth {
	// Is authentication enabled?
	pub fn is_enabled() -> bool {
		// If AUTH_ENABLED is not set, then authentication is enabled by default
		*AUTH_ENABLED.get().unwrap_or(&true)
	}
	/// Checks whether the current authentication has root level permissions
	pub fn is_kv(&self) -> bool {
		self.check(Level::Kv)
	}
	/// Checks whether the current authentication has namespace level permissions
	pub fn is_ns(&self) -> bool {
		self.check(Level::Ns)
	}
	/// Checks whether the current authentication has database level permissions
	pub fn is_db(&self) -> bool {
		self.check(Level::Db)
	}
	/// Checks whether the current authentication has scope level permissions
	pub fn is_sc(&self) -> bool {
		self.check(Level::Sc)
	}
	/// Checks whether the current authentication is unauthenticated
	pub fn is_no(&self) -> bool {
		self.check(Level::Sc)
	}
	/// Return current authentication level
	pub fn level(&self) -> Level {
		match self {
			Auth::No => Level::No,
			Auth::Sc(_, _, _) => Level::Sc,
			Auth::Db(_, _) => Level::Db,
			Auth::Ns(_) => Level::Ns,
			Auth::Kv => Level::Kv,
		}
	}
	/// Checks whether permissions clauses need to be processed
	pub(crate) fn perms(&self) -> bool {
		match self {
			Auth::No => true,
			Auth::Sc(_, _, _) => true,
			Auth::Db(_, _) => false,
			Auth::Ns(_) => false,
			Auth::Kv => false,
		}
	}
	/// Checks whether the current authentication matches the required level
	pub(crate) fn check(&self, level: Level) -> bool {
		// If authentication is disabled, return always true
		if !Self::is_enabled() {
			return true;
		}

		match self {
			Auth::No => matches!(level, Level::No),
			Auth::Sc(_, _, _) => matches!(level, Level::No | Level::Sc),
			Auth::Db(_, _) => matches!(level, Level::No | Level::Sc | Level::Db),
			Auth::Ns(_) => matches!(level, Level::No | Level::Sc | Level::Db | Level::Ns),
			Auth::Kv => true,
		}
	}
}
