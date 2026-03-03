use std::fmt::Debug;
use std::pin::Pin;
use std::sync::Arc;

use anyhow::{Result, bail};
use uuid::Uuid;

use crate::catalog;
use crate::catalog::SubscriptionDefinition;
use crate::cnf::dynamic::DynamicConfiguration;
use crate::err::Error;
use crate::expr::Base;
use crate::iam::{Action, Auth, ResourceKind};
use crate::types::PublicNotification;

/// An Options is passed around when processing a set of query
/// statements.
///
/// An Options contains specific information for how
/// to process each particular statement, including the record
/// version to retrieve, whether computed values should be processed, and
/// whether field/event/table queries should be processed (useful
/// when importing data, where these queries might fail).
#[derive(Clone, Debug)]
pub struct Options {
	/// The current Node ID of the datastore instance
	id: Uuid,
	/// The currently selected Namespace
	pub(crate) ns: Option<Arc<str>>,
	/// The currently selected Database
	pub(crate) db: Option<Arc<str>>,
	/// Approximately how large is the current call stack?
	pub(crate) dive: u32,
	/// Connection authentication data
	pub(crate) auth: Arc<Auth>,
	/// Is authentication enabled on this datastore?
	pub(crate) auth_enabled: bool,
	/// Whether live queries can be used?
	pub(crate) live: bool,
	/// Should we force tables/events to re-run?
	pub(crate) force: Force,
	/// Should we run permissions checks?
	pub(crate) perms: bool,
	/// Should we process field queries?
	pub(crate) import: bool,
	/// The data version as nanosecond timestamp
	pub(crate) version: Option<u64>,
	/// Optional message broker for live notifications
	pub(crate) broker: Option<Arc<dyn MessageBroker>>,
	/// Configuration parameters that can be dynamically changed
	dynamic_configuration: DynamicConfiguration,
	/// Tracks async event nesting depth for enforcing event MAXDEPTH.
	async_event_depth: Option<u16>,
}

#[derive(Clone, Debug)]
pub enum Force {
	All,
	None,
	Table(Arc<[catalog::TableDefinition]>),
}

/// Trait for a pluggable message broker used to forward live query events across nodes.
/// Default implementation can be a no-op. Implementations should be cheap to clone behind Arc.
pub trait MessageBroker: Send + Sync + Debug {
	fn can_be_sent(&self, opt: &Options, subscription: &SubscriptionDefinition) -> Result<bool>;

	/// Forward a live query event for the given subscription to its owning node.
	/// The concrete implementation decides how to encode and route this request.
	fn send(
		&self,
		notification: PublicNotification,
	) -> Pin<Box<dyn Future<Output = ()> + Send + '_>>;
}

impl Options {
	pub(crate) fn new(
		id: Uuid,
		dynamic_configuration: DynamicConfiguration,
		max_computation_depth: u32,
	) -> Self {
		Self {
			id,
			ns: None,
			db: None,
			dive: max_computation_depth,
			live: false,
			perms: true,
			force: Force::None,
			import: false,
			auth_enabled: true,
			broker: None,
			auth: Arc::new(Auth::default()),
			version: None,
			async_event_depth: None,
			dynamic_configuration,
		}
	}

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

	/// Set the maximum depth a computation can reach.
	pub fn with_max_computation_depth(mut self, depth: u32) -> Self {
		self.dive = depth;
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

	/// Specify if we are currently importing data
	pub fn with_import(mut self, import: bool) -> Self {
		self.set_import(import);
		self
	}

	/// Specify if we are currently importing data
	pub fn set_import(&mut self, import: bool) {
		self.import = import;
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

	/// Set the current async event nesting depth (0 for top-level).
	/// Used to enforce MAXDEPTH when async events trigger async events.
	pub fn with_async_event_depth(mut self, depth: u16) -> Self {
		self.async_event_depth = Some(depth);
		self
	}

	// --------------------------------------------------

	/// Create a new Options object for a subquery
	pub fn new_with_auth(&self, auth: Arc<Auth>) -> Self {
		Self {
			broker: self.broker.clone(),
			auth,
			ns: self.ns.clone(),
			db: self.db.clone(),
			force: self.force.clone(),
			perms: self.perms,
			dynamic_configuration: self.dynamic_configuration.clone(),
			..*self
		}
	}

	/// Create a new Options object for a subquery
	pub fn new_with_perms(&self, perms: bool) -> Self {
		Self {
			broker: self.broker.clone(),
			auth: self.auth.clone(),
			ns: self.ns.clone(),
			db: self.db.clone(),
			force: self.force.clone(),
			dynamic_configuration: self.dynamic_configuration.clone(),
			perms,
			..*self
		}
	}

	/// Create a new Options object for a subquery
	pub fn new_with_force(&self, force: Force) -> Self {
		Self {
			broker: self.broker.clone(),
			auth: self.auth.clone(),
			ns: self.ns.clone(),
			db: self.db.clone(),
			force,
			dynamic_configuration: self.dynamic_configuration.clone(),
			..*self
		}
	}

	/// Create a new Options object for a subquery
	pub fn new_with_import(&self, import: bool) -> Self {
		Self {
			broker: self.broker.clone(),
			auth: self.auth.clone(),
			ns: self.ns.clone(),
			db: self.db.clone(),
			force: self.force.clone(),
			import,
			dynamic_configuration: self.dynamic_configuration.clone(),
			..*self
		}
	}

	/// Create a new Options object for a subquery
	pub fn new_with_broker(&self, sender: Arc<dyn MessageBroker>) -> Self {
		Self {
			auth: self.auth.clone(),
			ns: self.ns.clone(),
			db: self.db.clone(),
			force: self.force.clone(),
			broker: Some(sender),
			dynamic_configuration: self.dynamic_configuration.clone(),
			..*self
		}
	}

	// Get currently selected base
	pub(crate) fn selected_base(&self) -> Result<Base, Error> {
		match (self.ns.as_ref(), self.db.as_ref()) {
			(None, None) => Ok(Base::Root),
			(Some(_), None) => Ok(Base::Ns),
			(Some(_), Some(_)) => Ok(Base::Db),
			(None, Some(_)) => Err(Error::NsEmpty),
		}
	}

	/// Create a new Options object for a function/subquery/computed/etc.
	///
	/// The parameter is the approximate cost of the operation (more concretely, the size of the
	/// stack frame it uses relative to a simple function call). When in doubt, use a value of 1.
	pub(crate) fn dive(&self, cost: u8) -> Result<Self, Error> {
		if self.dive < cost as u32 {
			return Err(Error::ComputationDepthExceeded);
		}
		Ok(Self {
			broker: self.broker.clone(),
			auth: self.auth.clone(),
			ns: self.ns.clone(),
			db: self.db.clone(),
			force: self.force.clone(),
			dive: self.dive - cost as u32,
			dynamic_configuration: self.dynamic_configuration.clone(),
			..*self
		})
	}

	// --------------------------------------------------

	/// Get current Node ID
	#[inline]
	pub fn id(&self) -> Uuid {
		self.id
	}

	/// Get currently selected NS
	#[inline(always)]
	pub fn ns(&self) -> Result<&str> {
		self.ns.as_deref().ok_or_else(|| Error::NsEmpty).map_err(anyhow::Error::new)
	}

	pub(crate) fn arc_ns(&self) -> Result<Arc<str>> {
		self.ns.clone().ok_or_else(|| Error::NsEmpty).map_err(anyhow::Error::new)
	}

	/// Get currently selected DB
	#[inline(always)]
	pub fn db(&self) -> Result<&str> {
		self.db.as_deref().ok_or_else(|| Error::DbEmpty).map_err(anyhow::Error::new)
	}

	pub(crate) fn arc_db(&self) -> Result<Arc<str>> {
		self.db.clone().ok_or_else(|| Error::DbEmpty).map_err(anyhow::Error::new)
	}

	/// Get currently selected NS and DB
	#[inline(always)]
	pub fn ns_db(&self) -> Result<(&str, &str)> {
		Ok((self.ns()?, self.db()?))
	}

	pub(crate) fn arc_ns_db(&self) -> Result<(Arc<str>, Arc<str>)> {
		Ok((self.arc_ns()?, self.arc_db()?))
	}

	pub fn ns_db_arc(&self) -> Result<(&str, &str)> {
		Ok((self.ns()?, self.db()?))
	}

	/// Check whether this request supports realtime queries
	#[inline(always)]
	pub fn realtime(&self) -> Result<()> {
		if !self.live {
			bail!(Error::RealtimeDisabled);
		}
		Ok(())
	}

	// Validate Options for Namespace
	#[inline(always)]
	pub fn valid_for_ns(&self) -> Result<()> {
		if self.ns.is_none() {
			bail!(Error::NsEmpty);
		}
		Ok(())
	}

	// Validate Options for Database
	#[inline(always)]
	pub fn valid_for_db(&self) -> Result<()> {
		if self.ns.is_none() {
			bail!(Error::NsEmpty);
		}
		if self.db.is_none() {
			bail!(Error::DbEmpty);
		}
		Ok(())
	}

	/// Check if the current auth is allowed to perform an action on a given resource
	pub fn is_allowed(&self, action: Action, res: ResourceKind, base: &Base) -> Result<()> {
		// Validate the target resource and base
		let res = match base {
			Base::Root => res.on_root(),
			Base::Ns => res.on_ns(self.ns()?),
			Base::Db => {
				let (ns, db) = self.ns_db()?;
				res.on_db(ns, db)
			}
		};

		// If auth is disabled, allow all actions for anonymous users
		if !self.auth_enabled && self.auth.is_anon() {
			return Ok(());
		}

		self.auth.is_allowed(action, &res)
	}

	/// Checks the current server configuration, and
	/// user authentication information to determine
	/// whether we need to process table permissions
	/// on each document.
	///
	/// This method is repeatedly called during the
	/// document processing operations, and so the
	/// performance of this function is important.
	/// We decided to bypass the system cedar auth
	/// system as a temporary solution until the
	/// new authorization system is optimised.
	pub fn check_perms(&self, action: Action) -> Result<bool> {
		// Check if permissions are enabled for this sub-process
		if !self.perms {
			return Ok(false);
		}
		// Check if server auth is disabled
		if !self.auth_enabled && self.auth.is_anon() {
			return Ok(false);
		}
		// Check the action to determine if we need to check permissions
		match action {
			// This is a request to edit a resource
			Action::Edit => {
				// Check if the actor is allowed to edit
				let allowed = self.auth.has_editor_role();
				// Today all users have at least View
				// permissions, so if the target database
				// belongs to the user's level, we don't
				// need to check any table permissions.
				let (ns, db) = self.ns_db()?;
				let db_in_actor_level = self.auth.is_root()
					|| self.auth.is_ns_check(ns)
					|| self.auth.is_db_check(ns, db);
				// If either of the above checks are false
				// then we need to check table permissions
				Ok(!allowed || !db_in_actor_level)
			}
			// This is a request to view a resource
			Action::View => {
				// Check if the actor is allowed to view
				let allowed = self.auth.has_viewer_role();
				// Today, Owner and Editor roles have
				// Edit permissions, so if the target
				// database belongs to the user's level
				// we don't need to check table permissions.
				let (ns, db) = self.ns_db()?;
				let db_in_actor_level = self.auth.is_root()
					|| self.auth.is_ns_check(ns)
					|| self.auth.is_db_check(ns, db);
				// If either of the above checks are false
				// then we need to check table permissions
				Ok(!allowed || !db_in_actor_level)
			}
		}
	}

	/// Returns the handle to runtime‑adjustable configuration toggles.
	///
	/// Currently this includes the global query timeout, which can be modified
	/// via `ALTER SYSTEM QUERY_TIMEOUT ...`.
	pub(crate) fn dynamic_configuration(&self) -> &DynamicConfiguration {
		&self.dynamic_configuration
	}

	pub(crate) fn async_event_depth(&self) -> Option<u16> {
		self.async_event_depth
	}
}

#[cfg(test)]
mod tests {

	use super::*;
	use crate::iam::Role;

	#[test]
	fn is_allowed() {
		// With auth disabled
		{
			let opts = Options::new(Uuid::new_v4(), DynamicConfiguration::default(), 120)
				.with_auth_enabled(false);

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
			opts.with_ns(Some("ns".into()))
				.with_db(Some("db".into()))
				.is_allowed(Action::View, ResourceKind::Any, &Base::Db)
				.unwrap();
		}

		// With auth enabled
		{
			let opts = Options::new(Uuid::new_v4(), DynamicConfiguration::default(), 120)
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
			opts.with_ns(Some("ns".into()))
				.with_db(Some("db".into()))
				.is_allowed(Action::View, ResourceKind::Any, &Base::Db)
				.unwrap();
		}
	}
}
