use crate::cnf;
use crate::dbs::Auth;
use crate::dbs::Level;
use crate::err::Error;
use std::sync::Arc;

// An Options is passed around when processing a set of query
// statements. An Options contains specific information for how
// to process each particular statement, including the record
// version to retrieve, whether futures should be processed, and
// whether field/event/table queries should be processed (useful
// when importing data, where these queries might fail).

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Options {
	// Currently selected NS
	pub ns: Option<Arc<String>>,
	// Currently selected DB
	pub db: Option<Arc<String>>,
	// Connection authentication data
	pub auth: Arc<Auth>,
	// How many subqueries have we gone into?
	pub dive: usize,
	// Whether live queries are allowed?
	pub live: bool,
	// Should we debug query response SQL?
	pub debug: bool,
	// Should we force tables/events to re-run?
	pub force: bool,
	// Should we run permissions checks?
	pub perms: bool,
	// Should we error if tables don't exist?
	pub strict: bool,
	// Should we process field queries?
	pub fields: bool,
	// Should we process event queries?
	pub events: bool,
	// Should we process table queries?
	pub tables: bool,
	// Should we process index queries?
	pub indexes: bool,
	// Should we process function futures?
	pub futures: bool,
}

impl Default for Options {
	fn default() -> Self {
		Options::new(Auth::No)
	}
}

impl Options {
	// Create a new Options object
	pub fn new(auth: Auth) -> Options {
		Options {
			ns: None,
			db: None,
			dive: 0,
			live: false,
			perms: true,
			debug: false,
			force: false,
			strict: false,
			fields: true,
			events: true,
			tables: true,
			indexes: true,
			futures: false,
			auth: Arc::new(auth),
		}
	}

	// Get currently selected NS
	pub fn ns(&self) -> &str {
		self.ns.as_ref().unwrap()
	}

	// Get currently selected DB
	pub fn db(&self) -> &str {
		self.db.as_ref().unwrap()
	}

	// Create a new Options object for a subquery
	pub fn dive(&self) -> Result<Options, Error> {
		if self.dive < cnf::MAX_RECURSIVE_QUERIES {
			Ok(Options {
				auth: self.auth.clone(),
				ns: self.ns.clone(),
				db: self.db.clone(),
				dive: self.dive + 1,
				..*self
			})
		} else {
			Err(Error::TooManySubqueries)
		}
	}

	// Create a new Options object for a subquery
	pub fn debug(&self, v: bool) -> Options {
		Options {
			auth: self.auth.clone(),
			ns: self.ns.clone(),
			db: self.db.clone(),
			debug: v,
			..*self
		}
	}

	// Create a new Options object for a subquery
	pub fn force(&self, v: bool) -> Options {
		Options {
			auth: self.auth.clone(),
			ns: self.ns.clone(),
			db: self.db.clone(),
			force: v,
			..*self
		}
	}

	// Create a new Options object for a subquery
	pub fn perms(&self, v: bool) -> Options {
		Options {
			auth: self.auth.clone(),
			ns: self.ns.clone(),
			db: self.db.clone(),
			perms: v,
			..*self
		}
	}

	// Create a new Options object for a subquery
	pub fn fields(&self, v: bool) -> Options {
		Options {
			auth: self.auth.clone(),
			ns: self.ns.clone(),
			db: self.db.clone(),
			fields: v,
			..*self
		}
	}

	// Create a new Options object for a subquery
	pub fn events(&self, v: bool) -> Options {
		Options {
			auth: self.auth.clone(),
			ns: self.ns.clone(),
			db: self.db.clone(),
			events: v,
			..*self
		}
	}

	// Create a new Options object for a subquery
	pub fn tables(&self, v: bool) -> Options {
		Options {
			auth: self.auth.clone(),
			ns: self.ns.clone(),
			db: self.db.clone(),
			tables: v,
			..*self
		}
	}

	// Create a new Options object for a subquery
	pub fn indexes(&self, v: bool) -> Options {
		Options {
			auth: self.auth.clone(),
			ns: self.ns.clone(),
			db: self.db.clone(),
			indexes: v,
			..*self
		}
	}

	// Create a new Options object for a subquery
	pub fn import(&self, v: bool) -> Options {
		Options {
			auth: self.auth.clone(),
			ns: self.ns.clone(),
			db: self.db.clone(),
			fields: !v,
			events: !v,
			tables: !v,
			..*self
		}
	}

	// Create a new Options object for a subquery
	pub fn strict(&self, v: bool) -> Options {
		Options {
			auth: self.auth.clone(),
			ns: self.ns.clone(),
			db: self.db.clone(),
			strict: v,
			..*self
		}
	}

	// Create a new Options object for a subquery
	pub fn futures(&self, v: bool) -> Options {
		Options {
			auth: self.auth.clone(),
			ns: self.ns.clone(),
			db: self.db.clone(),
			futures: v,
			..*self
		}
	}

	// Check whether realtime queries are supported
	pub fn realtime(&self) -> Result<(), Error> {
		if !self.live {
			return Err(Error::RealtimeDisabled);
		}
		Ok(())
	}

	// Check whether the authentication permissions are ok
	pub fn check(&self, level: Level) -> Result<(), Error> {
		if !self.auth.check(level) {
			return Err(Error::QueryPermissions);
		}
		Ok(())
	}

	// Check whether the necessary NS / DB options have been set
	pub fn needs(&self, level: Level) -> Result<(), Error> {
		if self.ns.is_none() && matches!(level, Level::Ns | Level::Db) {
			return Err(Error::NsEmpty);
		}
		if self.db.is_none() && matches!(level, Level::Db) {
			return Err(Error::DbEmpty);
		}
		Ok(())
	}
}
