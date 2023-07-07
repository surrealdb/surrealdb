use crate::cnf;
use crate::dbs::Auth;
use crate::dbs::Level;
use crate::dbs::Notification;
use crate::err::Error;
use channel::Sender;
use std::sync::Arc;
use uuid::Uuid;

/// An Options is passed around when processing a set of query
/// statements. An Options contains specific information for how
/// to process each particular statement, including the record
/// version to retrieve, whether futures should be processed, and
/// whether field/event/table queries should be processed (useful
/// when importing data, where these queries might fail).
#[derive(Clone, Debug)]
pub struct Options {
	/// Current Node ID
	id: Option<Uuid>,
	/// Currently selected NS
	ns: Option<Arc<str>>,
	/// Currently selected DB
	db: Option<Arc<str>>,
	/// Approximately how large is the current call stack?
	dive: u8,
	/// Connection authentication data
	pub auth: Arc<Auth>,
	/// Whether live queries are allowed?
	pub live: bool,
	/// Should we force tables/events to re-run?
	pub force: bool,
	/// Should we run permissions checks?
	pub perms: bool,
	/// Should we error if tables don't exist?
	pub strict: bool,
	/// Should we process field queries?
	pub fields: bool,
	/// Should we process event queries?
	pub events: bool,
	/// Should we process table queries?
	pub tables: bool,
	/// Should we process index queries?
	pub indexes: bool,
	/// Should we process function futures?
	pub futures: bool,
	/// The channel over which we send notifications
	pub sender: Option<Sender<Notification>>,
}

impl Default for Options {
	fn default() -> Self {
		Options::new()
	}
}

impl Options {
	/// Create a new Options object
	pub fn new() -> Options {
		Options {
			id: None,
			ns: None,
			db: None,
			dive: 0,
			live: false,
			perms: true,
			force: false,
			strict: false,
			fields: true,
			events: true,
			tables: true,
			indexes: true,
			futures: false,
			sender: None,
			auth: Arc::new(Auth::No),
		}
	}

	// --------------------------------------------------

	/// Specify which Namespace should be used for
	/// code which uses this `Options` object.
	pub fn set_ns(&mut self, ns: Option<Arc<str>>) {
		self.ns = ns
	}

	/// Specify which Database should be used for
	/// code which uses this `Options` object.
	pub fn set_db(&mut self, db: Option<Arc<str>>) {
		self.db = db
	}

	// --------------------------------------------------

	/// Set the Node ID for subsequent code which uses
	/// this `Options`, with support for chaining.
	pub fn with_id(mut self, id: Uuid) -> Self {
		self.id = Some(id);
		self
	}

	/// Specify which Namespace should be used for code which
	/// uses this `Options`, with support for chaining.
	pub fn with_ns(mut self, ns: Option<Arc<str>>) -> Self {
		self.ns = ns;
		self
	}

	/// Specify which Database should be used for code which
	/// uses this `Options`, with support for chaining.
	pub fn with_db(mut self, db: Option<Arc<str>>) -> Self {
		self.db = db;
		self
	}

	/// Specify the authentication options for subsequent
	/// code which uses this `Options`, with chaining.
	pub fn with_auth(mut self, auth: Arc<Auth>) -> Self {
		self.auth = auth;
		self
	}

	/// Specify whether live queries are supported for
	/// code which uses this `Options`, with chaining.
	pub fn with_live(mut self, live: bool) -> Self {
		self.live = live;
		self
	}

	/// Specify whether permissions should be run for
	/// code which uses this `Options`, with chaining.
	pub fn with_perms(mut self, perms: bool) -> Self {
		self.perms = perms;
		self
	}

	///
	pub fn with_force(mut self, force: bool) -> Self {
		self.force = force;
		self
	}

	///
	pub fn with_strict(mut self, strict: bool) -> Self {
		self.strict = strict;
		self
	}

	///
	pub fn with_fields(mut self, fields: bool) -> Self {
		self.fields = fields;
		self
	}

	///
	pub fn with_events(mut self, events: bool) -> Self {
		self.events = events;
		self
	}

	///
	pub fn with_tables(mut self, tables: bool) -> Self {
		self.tables = tables;
		self
	}

	///
	pub fn with_indexes(mut self, indexes: bool) -> Self {
		self.indexes = indexes;
		self
	}

	///
	pub fn with_futures(mut self, futures: bool) -> Self {
		self.futures = futures;
		self
	}

	/// Create a new Options object for a subquery
	pub fn with_import(mut self, import: bool) -> Self {
		self.fields = !import;
		self.events = !import;
		self.tables = !import;
		self
	}

	// --------------------------------------------------

	/// Create a new Options object for a subquery
	pub fn new_with_perms(&self, perms: bool) -> Self {
		Self {
			sender: self.sender.clone(),
			auth: self.auth.clone(),
			ns: self.ns.clone(),
			db: self.db.clone(),
			perms,
			..*self
		}
	}

	/// Create a new Options object for a subquery
	pub fn new_with_force(&self, force: bool) -> Self {
		Self {
			sender: self.sender.clone(),
			auth: self.auth.clone(),
			ns: self.ns.clone(),
			db: self.db.clone(),
			force,
			..*self
		}
	}

	/// Create a new Options object for a subquery
	pub fn new_with_strict(&self, strict: bool) -> Self {
		Self {
			sender: self.sender.clone(),
			auth: self.auth.clone(),
			ns: self.ns.clone(),
			db: self.db.clone(),
			strict,
			..*self
		}
	}

	/// Create a new Options object for a subquery
	pub fn new_with_fields(&self, fields: bool) -> Self {
		Self {
			sender: self.sender.clone(),
			auth: self.auth.clone(),
			ns: self.ns.clone(),
			db: self.db.clone(),
			fields,
			..*self
		}
	}

	/// Create a new Options object for a subquery
	pub fn new_with_events(&self, events: bool) -> Self {
		Self {
			sender: self.sender.clone(),
			auth: self.auth.clone(),
			ns: self.ns.clone(),
			db: self.db.clone(),
			events,
			..*self
		}
	}

	/// Create a new Options object for a subquery
	pub fn new_with_tables(&self, tables: bool) -> Self {
		Self {
			sender: self.sender.clone(),
			auth: self.auth.clone(),
			ns: self.ns.clone(),
			db: self.db.clone(),
			tables,
			..*self
		}
	}

	/// Create a new Options object for a subquery
	pub fn new_with_indexes(&self, indexes: bool) -> Self {
		Self {
			sender: self.sender.clone(),
			auth: self.auth.clone(),
			ns: self.ns.clone(),
			db: self.db.clone(),
			indexes,
			..*self
		}
	}

	/// Create a new Options object for a subquery
	pub fn new_with_futures(&self, futures: bool) -> Self {
		Self {
			sender: self.sender.clone(),
			auth: self.auth.clone(),
			ns: self.ns.clone(),
			db: self.db.clone(),
			futures,
			..*self
		}
	}

	/// Create a new Options object for a subquery
	pub fn new_with_import(&self, import: bool) -> Self {
		Self {
			sender: self.sender.clone(),
			auth: self.auth.clone(),
			ns: self.ns.clone(),
			db: self.db.clone(),
			fields: !import,
			events: !import,
			tables: !import,
			..*self
		}
	}

	/// Create a new Options object for a subquery
	pub fn new_with_sender(&self, sender: Sender<Notification>) -> Self {
		Self {
			auth: self.auth.clone(),
			ns: self.ns.clone(),
			db: self.db.clone(),
			sender: Some(sender),
			..*self
		}
	}

	/// Create a new Options object for a function/subquery/future/etc.
	///
	/// The parameter is the approximate cost of the operation (more concretely, the size of the
	/// stack frame it uses relative to a simple function call). When in doubt, use a value of 1.
	pub fn dive(&self, cost: u8) -> Result<Self, Error> {
		let dive = self.dive.saturating_add(cost);
		if dive <= *cnf::MAX_COMPUTATION_DEPTH {
			Ok(Self {
				sender: self.sender.clone(),
				auth: self.auth.clone(),
				ns: self.ns.clone(),
				db: self.db.clone(),
				dive,
				..*self
			})
		} else {
			Err(Error::ComputationDepthExceeded)
		}
	}

	// --------------------------------------------------

	/// Get current Node ID
	pub fn id(&self) -> Result<Uuid, Error> {
		self.id.ok_or(Error::Unreachable)
	}

	/// Get currently selected NS
	pub fn ns(&self) -> &str {
		self.ns.as_ref().map(AsRef::as_ref).unwrap()
		// self.ns.as_ref().map(AsRef::as_ref).ok_or(Error::Unreachable)
	}

	/// Get currently selected DB
	pub fn db(&self) -> &str {
		self.db.as_ref().map(AsRef::as_ref).unwrap()
		// self.db.as_ref().map(AsRef::as_ref).ok_or(Error::Unreachable)
	}

	/// Check whether this request supports realtime queries
	pub fn realtime(&self) -> Result<(), Error> {
		if !self.live {
			return Err(Error::RealtimeDisabled);
		}
		Ok(())
	}

	/// Check whether the authentication permissions are ok
	pub fn check(&self, level: Level) -> Result<(), Error> {
		if !self.auth.check(level) {
			return Err(Error::QueryPermissions);
		}
		Ok(())
	}

	/// Check whether the necessary NS / DB options have been set
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
