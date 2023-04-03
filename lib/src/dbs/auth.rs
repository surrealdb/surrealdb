/// The authentication level for a datastore execution context.
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
pub enum Level {
	No,
	Kv,
	Ns,
	Db,
	Sc,
}

/// Specifies the current authentication for the datastore execution context.
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd)]
pub enum Auth {
	/// Specifies that the user is not authenticated
	#[default]
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

impl Auth {
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
