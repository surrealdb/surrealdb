use crate::cnf;
use crate::dbs::Auth;
use crate::err::Error;
use crate::sql::version::Version;

// An Options is passed around when processing a set of query
// statements. An Options contains specific information for how
// to process each particular statement, including the record
// version to retrieve, whether futures should be processed, and
// whether field/event/table queries should be processed (useful
// when importing data, where these queries might fail).

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Options<'a> {
	pub auth: &'a Auth,
	pub dive: usize,                  // How many subqueries have we gone into?
	pub debug: bool,                  // Should we debug query response SQL?
	pub force: bool,                  // Should we force tables/events to re-run?
	pub fields: bool,                 // Should we process field queries?
	pub events: bool,                 // Should we process event queries?
	pub tables: bool,                 // Should we process table queries?
	pub futures: bool,                // Should we process function futures?
	pub version: Option<&'a Version>, // Current
}

impl<'a> Default for Options<'a> {
	fn default() -> Self {
		Options::new(&Auth::No)
	}
}

impl<'a> Options<'a> {
	// Create a new Options object
	pub fn new(auth: &'a Auth) -> Options<'a> {
		Options {
			auth,
			dive: 0,
			debug: false,
			force: false,
			fields: true,
			events: true,
			tables: true,
			futures: false,
			version: None,
		}
	}

	// Create a new Options object for a subquery
	pub fn dive(&self) -> Result<Options<'a>, Error> {
		if self.dive < cnf::MAX_RECURSIVE_QUERIES {
			Ok(Options {
				dive: self.dive + 1,
				..*self
			})
		} else {
			Err(Error::RecursiveSubqueryError {
				limit: self.dive,
			})
		}
	}

	// Create a new Options object for a subquery
	pub fn debug(&self, v: bool) -> Options<'a> {
		Options {
			debug: v,
			..*self
		}
	}

	// Create a new Options object for a subquery
	pub fn force(&self, v: bool) -> Options<'a> {
		Options {
			force: v,
			..*self
		}
	}

	// Create a new Options object for a subquery
	pub fn fields(&self, v: bool) -> Options<'a> {
		Options {
			fields: v,
			..*self
		}
	}

	// Create a new Options object for a subquery
	pub fn events(&self, v: bool) -> Options<'a> {
		Options {
			events: v,
			..*self
		}
	}

	// Create a new Options object for a subquery
	pub fn tables(&self, v: bool) -> Options<'a> {
		Options {
			tables: v,
			..*self
		}
	}

	// Create a new Options object for a subquery
	pub fn import(&self, v: bool) -> Options<'a> {
		Options {
			fields: v,
			events: v,
			tables: v,
			..*self
		}
	}

	// Create a new Options object for a subquery
	pub fn futures(&self, v: bool) -> Options<'a> {
		Options {
			futures: v,
			..*self
		}
	}

	// Create a new Options object for a subquery
	pub fn version(&self, v: Option<&'a Version>) -> Options<'a> {
		Options {
			version: v,
			..*self
		}
	}
}
