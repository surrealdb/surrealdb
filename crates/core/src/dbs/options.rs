use crate::cnf::MAX_COMPUTATION_DEPTH;
use crate::dbs::Notification;
use crate::err::Error;
use crate::iam::{Action, Auth, ResourceKind, Role};
use crate::sql::{
	statements::define::DefineIndexStatement, statements::define::DefineTableStatement, Base,
};
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
#[non_exhaustive]
pub struct Options {
	/// Current Node ID
	id: Option<Uuid>,
	/// Currently selected NS
	ns: Option<Arc<str>>,
	/// Currently selected DB
	db: Option<Arc<str>>,
	/// Approximately how large is the current call stack?
	dive: u32,
	/// Connection authentication data
	pub auth: Arc<Auth>,
	/// Is authentication enabled?
	pub auth_enabled: bool,
	/// Whether live queries are allowed?
	pub live: bool,
	/// Should we force tables/events to re-run?
	pub force: Force,
	/// Should we run permissions checks?
	pub perms: bool,
	/// Should we error if tables don't exist?
	pub strict: bool,
	/// Should we process field queries?
	pub import: bool,
	/// Should we process function futures?
	pub futures: Futures,
	/// Should we process variable field projections?
	pub projections: bool,
	/// The channel over which we send notifications
	pub sender: Option<Sender<Notification>>,
	/// Version as nanosecond timestamp passed down to Datastore
	pub version: Option<u64>,
}

#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum Force {
	All,
	None,
	Table(Arc<[DefineTableStatement]>),
	Index(Arc<[DefineIndexStatement]>),
}

#[derive(Clone, Debug)]
pub enum Futures {
	Disabled,
	Enabled,
	Never,
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
			dive: *MAX_COMPUTATION_DEPTH,
			live: false,
			perms: true,
			force: Force::None,
			strict: false,
			import: false,
			futures: Futures::Disabled,
			projections: false,
			auth_enabled: true,
			sender: None,
			auth: Arc::new(Auth::default()),
			version: None,
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

	/// Set all the required options from a single point.
	/// The system expects these values to always be set,
	/// so this should be called for all instances when
	/// there is doubt.
	pub fn with_required(
		mut self,
		node_id: Uuid,
		ns: Option<Arc<str>>,
		db: Option<Arc<str>>,
		auth: Arc<Auth>,
	) -> Self {
		self.id = Some(node_id);
		self.ns = ns;
		self.db = db;
		self.auth = auth;
		self
	}

	/// Set the maximum depth a computation can reach.
	pub fn with_max_computation_depth(mut self, depth: u32) -> Self {
		self.dive = depth;
		self
	}

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

	/// Specify wether tables/events should re-run
	pub fn with_force(mut self, force: Force) -> Self {
		self.force = force;
		self
	}

	/// Sepecify if we should error when a table does not exist
	pub fn with_strict(mut self, strict: bool) -> Self {
		self.strict = strict;
		self
	}

	/// Specify if we are currently importing data
	pub fn with_import(mut self, import: bool) -> Self {
		self.import = import;
		self
	}

	/// Specify if we should process futures
	pub fn with_futures(mut self, futures: bool) -> Self {
		if matches!(self.futures, Futures::Never) {
			return self;
		}

		self.futures = match futures {
			true => Futures::Enabled,
			false => Futures::Disabled,
		};
		self
	}

	/// Specify if we should never process futures
	pub fn with_futures_never(mut self) -> Self {
		self.futures = Futures::Never;
		self
	}

	/// Specify if we should process field projections
	pub fn with_projections(mut self, projections: bool) -> Self {
		self.projections = projections;
		self
	}

	/// Create a new Options object with auth enabled
	pub fn with_auth_enabled(mut self, auth_enabled: bool) -> Self {
		self.auth_enabled = auth_enabled;
		self
	}

	// Set the version
	pub fn with_version(mut self, version: Option<u64>) -> Self {
		self.version = version;
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
			force: self.force.clone(),
			futures: self.futures.clone(),
			perms,
			..*self
		}
	}

	/// Create a new Options object for a subquery
	pub fn new_with_force(&self, force: Force) -> Self {
		Self {
			sender: self.sender.clone(),
			auth: self.auth.clone(),
			ns: self.ns.clone(),
			db: self.db.clone(),
			futures: self.futures.clone(),
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
			force: self.force.clone(),
			futures: self.futures.clone(),
			strict,
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
			force: self.force.clone(),
			futures: self.futures.clone(),
			import,
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
			force: self.force.clone(),
			futures: match self.futures {
				Futures::Never => Futures::Never,
				_ => match futures {
					true => Futures::Enabled,
					false => Futures::Disabled,
				},
			},
			..*self
		}
	}

	/// Create a new Options object for a subquery
	pub fn new_with_projections(&self, projections: bool) -> Self {
		Self {
			sender: self.sender.clone(),
			auth: self.auth.clone(),
			ns: self.ns.clone(),
			db: self.db.clone(),
			force: self.force.clone(),
			futures: self.futures.clone(),
			projections,
			..*self
		}
	}

	/// Create a new Options object for a subquery
	pub fn new_with_sender(&self, sender: Sender<Notification>) -> Self {
		Self {
			auth: self.auth.clone(),
			ns: self.ns.clone(),
			db: self.db.clone(),
			force: self.force.clone(),
			futures: self.futures.clone(),
			sender: Some(sender),
			..*self
		}
	}

	// Get currently selected base
	pub fn selected_base(&self) -> Result<Base, Error> {
		match (self.ns.as_ref(), self.db.as_ref()) {
			(None, None) => Ok(Base::Root),
			(Some(_), None) => Ok(Base::Ns),
			(Some(_), Some(_)) => Ok(Base::Db),
			(None, Some(_)) => Err(Error::NsEmpty),
		}
	}

	/// Create a new Options object for a function/subquery/future/etc.
	///
	/// The parameter is the approximate cost of the operation (more concretely, the size of the
	/// stack frame it uses relative to a simple function call). When in doubt, use a value of 1.
	pub fn dive(&self, cost: u8) -> Result<Self, Error> {
		if self.dive < cost as u32 {
			return Err(Error::ComputationDepthExceeded);
		}
		Ok(Self {
			sender: self.sender.clone(),
			auth: self.auth.clone(),
			ns: self.ns.clone(),
			db: self.db.clone(),
			force: self.force.clone(),
			futures: self.futures.clone(),
			dive: self.dive - cost as u32,
			..*self
		})
	}

	// --------------------------------------------------

	/// Get current Node ID
	#[inline(always)]
	pub fn id(&self) -> Result<Uuid, Error> {
		self.id.ok_or_else(|| fail!("No Node ID is specified"))
	}

	/// Get currently selected NS
	#[inline(always)]
	pub fn ns(&self) -> Result<&str, Error> {
		self.ns.as_ref().map(AsRef::as_ref).ok_or(Error::NsEmpty)
	}

	/// Get currently selected DB
	#[inline(always)]
	pub fn db(&self) -> Result<&str, Error> {
		self.db.as_ref().map(AsRef::as_ref).ok_or(Error::DbEmpty)
	}

	/// Check whether this request supports realtime queries
	#[inline(always)]
	pub fn realtime(&self) -> Result<(), Error> {
		if !self.live {
			return Err(Error::RealtimeDisabled);
		}
		Ok(())
	}

	// Validate Options for Namespace
	#[inline(always)]
	pub fn valid_for_ns(&self) -> Result<(), Error> {
		if self.ns.is_none() {
			return Err(Error::NsEmpty);
		}
		Ok(())
	}

	// Validate Options for Database
	#[inline(always)]
	pub fn valid_for_db(&self) -> Result<(), Error> {
		if self.ns.is_none() {
			return Err(Error::NsEmpty);
		}
		if self.db.is_none() {
			return Err(Error::DbEmpty);
		}
		Ok(())
	}

	/// Check if the current auth is allowed to perform an action on a given resource
	pub fn is_allowed(&self, action: Action, res: ResourceKind, base: &Base) -> Result<(), Error> {
		// Validate the target resource and base
		let res = match base {
			Base::Root => res.on_root(),
			Base::Ns => res.on_ns(self.ns()?),
			Base::Db => res.on_db(self.ns()?, self.db()?),
			// TODO(gguillemas): This variant is kept in 2.0.0 for backward compatibility. Drop in 3.0.0.
			Base::Sc(_) => {
				// We should not get here, the scope base is only used in parsing for backward compatibility.
				return Err(Error::InvalidAuth);
			}
		};

		// If auth is disabled, allow all actions for anonymous users
		if !self.auth_enabled && self.auth.is_anon() {
			return Ok(());
		}

		self.auth.is_allowed(action, &res).map_err(Error::IamError)
	}

	/// Whether or not to check table permissions
	///
	/// TODO: This method is called a lot during data operations, so we decided to bypass the system's authorization mechanism.
	/// This is a temporary solution, until we optimize the new authorization system.
	pub fn check_perms(&self, action: Action) -> Result<bool, Error> {
		// If permissions are disabled, don't check permissions
		if !self.perms {
			return Ok(false);
		}

		// If auth is disabled and actor is anonymous, don't check permissions
		if !self.auth_enabled && self.auth.is_anon() {
			return Ok(false);
		}

		// Is the actor allowed to view?
		let can_view =
			[Role::Viewer, Role::Editor, Role::Owner].iter().any(|r| self.auth.has_role(r));
		// Is the actor allowed to edit?
		let can_edit = [Role::Editor, Role::Owner].iter().any(|r| self.auth.has_role(r));
		// Is the target database in the actor's level?
		let db_in_actor_level = self.auth.is_root()
			|| self.auth.is_ns() && self.auth.level().ns().unwrap() == self.ns()?
			|| self.auth.is_db()
				&& self.auth.level().ns().unwrap() == self.ns()?
				&& self.auth.level().db().unwrap() == self.db()?;

		// Is the actor allowed to do the action on the selected database?
		let is_allowed = match action {
			Action::View => {
				// Today all users have at least View permissions, so if the target database belongs to the user's level, don't check permissions
				can_view && db_in_actor_level
			}
			Action::Edit => {
				// Editor and Owner roles are allowed to edit, but only if the target database belongs to the user's level
				can_edit && db_in_actor_level
			}
		};

		// Check permissions if the author is not already allowed to do the action
		Ok(!is_allowed)
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn is_allowed() {
		// With auth disabled
		{
			let opts = Options::default().with_auth_enabled(false);

			// When no NS is provided and it targets the NS base, it should return an error
			opts.is_allowed(Action::View, ResourceKind::Any, &Base::Ns).unwrap_err();
			// When no DB is provided and it targets the DB base, it should return an error
			opts.is_allowed(Action::View, ResourceKind::Any, &Base::Db).unwrap_err();
			opts.clone()
				.with_db(Some("db".into()))
				.is_allowed(Action::View, ResourceKind::Any, &Base::Db)
				.unwrap_err();

			// When a root resource is targeted, it succeeds
			opts.is_allowed(Action::View, ResourceKind::Any, &Base::Root).unwrap();
			// When a NS resource is targeted and NS was provided, it succeeds
			opts.clone()
				.with_ns(Some("ns".into()))
				.is_allowed(Action::View, ResourceKind::Any, &Base::Ns)
				.unwrap();
			// When a DB resource is targeted and NS and DB was provided, it succeeds
			opts.clone()
				.with_ns(Some("ns".into()))
				.with_db(Some("db".into()))
				.is_allowed(Action::View, ResourceKind::Any, &Base::Db)
				.unwrap();
		}

		// With auth enabled
		{
			let opts = Options::default()
				.with_auth_enabled(true)
				.with_auth(Auth::for_root(Role::Owner).into());

			// When no NS is provided and it targets the NS base, it should return an error
			opts.is_allowed(Action::View, ResourceKind::Any, &Base::Ns).unwrap_err();
			// When no DB is provided and it targets the DB base, it should return an error
			opts.is_allowed(Action::View, ResourceKind::Any, &Base::Db).unwrap_err();
			opts.clone()
				.with_db(Some("db".into()))
				.is_allowed(Action::View, ResourceKind::Any, &Base::Db)
				.unwrap_err();

			// When a root resource is targeted, it succeeds
			opts.is_allowed(Action::View, ResourceKind::Any, &Base::Root).unwrap();
			// When a NS resource is targeted and NS was provided, it succeeds
			opts.clone()
				.with_ns(Some("ns".into()))
				.is_allowed(Action::View, ResourceKind::Any, &Base::Ns)
				.unwrap();
			// When a DB resource is targeted and NS and DB was provided, it succeeds
			opts.clone()
				.with_ns(Some("ns".into()))
				.with_db(Some("db".into()))
				.is_allowed(Action::View, ResourceKind::Any, &Base::Db)
				.unwrap();
		}
	}

	#[test]
	pub fn execute_futures() {
		let mut opts = Options::default().with_futures(false);

		// Futures should be disabled
		assert!(matches!(opts.futures, Futures::Disabled));

		// Allow setting to true
		opts = opts.with_futures(true);
		assert!(matches!(opts.futures, Futures::Enabled));

		// Set to never and disallow setting to true
		opts = opts.with_futures_never();
		opts = opts.with_futures(true);
		assert!(matches!(opts.futures, Futures::Never));
	}
}
