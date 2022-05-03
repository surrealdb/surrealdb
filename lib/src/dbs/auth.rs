#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
pub enum Level {
	No,
	Kv,
	Ns,
	Db,
	Sc,
}

/// Specifies the authentication level for the datastore execution context.
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
pub enum Auth {
	/// Specifies that the user is not authenticated
	No,
	/// Specifies that the user is authenticated with full root permissions
	Kv,
	/// Specifies that the user is has full permissions for a particular Namespace
	Ns(String),
	/// Specifies that the user is has full permissions for a particular Namespace and Database
	Db(String, String),
	/// Specifies that the user is has full permissions for a particular Namespace, Database, and Scope
	Sc(String, String, String),
}

impl Default for Auth {
	fn default() -> Self {
		Auth::No
	}
}

impl Auth {
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
		match self {
			Auth::No => matches!(level, Level::No),
			Auth::Sc(_, _, _) => matches!(level, Level::No | Level::Sc),
			Auth::Db(_, _) => matches!(level, Level::No | Level::Sc | Level::Db),
			Auth::Ns(_) => matches!(level, Level::No | Level::Sc | Level::Db | Level::Ns),
			Auth::Kv => true,
		}
	}
}
