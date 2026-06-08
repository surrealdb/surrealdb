use std::fmt::Debug;
use std::sync::Arc;

use anyhow::{Result, bail};

use crate::catalog;
use crate::cnf::CommonConfig;
use crate::err::Error;
use crate::expr::Base;
use crate::iam::Auth;

/// Per-call execution frame passed along with [`crate::ctx::Context`].
///
/// Clone this struct to shadow a single field for an inner computation (e.g.
/// `let opt = &opt.new_with_perms(false);`). It deliberately holds only
/// **statement-scoped** knobs: active NS/DB (after `USE`), auth (may be limited
/// for subqueries), recursion budget, import/force flags, and optional version
/// / async-event depth.
///
/// **Not** here: node identity, datastore auth toggle, live-query capability,
/// dynamic server config, or the live broker — those live on [`crate::ctx::Context`].
#[derive(Clone, Debug)]
pub struct Options {
	/// The currently selected Namespace
	pub(crate) ns: Option<Arc<str>>,
	/// The currently selected Database
	pub(crate) db: Option<Arc<str>>,
	/// Approximately how large is the current call stack?
	pub(crate) dive: u32,
	/// Connection authentication data
	pub(crate) auth: Arc<Auth>,
	/// Should we force tables/events to re-run?
	pub(crate) force: Force,
	/// Should we run permissions checks?
	pub(crate) perms: bool,
	/// Should we process field queries?
	pub(crate) import: bool,
	/// The data version as a timestamp
	pub(crate) version: Option<u64>,
	/// Tracks async event nesting depth for enforcing event MAXDEPTH.
	async_event_depth: Option<u16>,
}

#[derive(Clone, Debug)]
pub enum Force {
	All,
	None,
	Table(Arc<[catalog::TableDefinition]>),
}

impl Options {
	pub(crate) fn new(config: &CommonConfig) -> Self {
		Self {
			ns: None,
			db: None,
			dive: config.max_computation_depth,
			perms: true,
			force: Force::None,
			import: false,
			auth: Arc::new(Auth::default()),
			version: None,
			async_event_depth: None,
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

	/// Specify whether permissions should be run for
	/// code which uses this `Options`, with chaining.
	pub fn with_perms(mut self, perms: bool) -> Self {
		self.perms = perms;
		self
	}

	/// Specify whether tables/events should re-run
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
			auth,
			ns: self.ns.clone(),
			db: self.db.clone(),
			force: self.force.clone(),
			perms: self.perms,
			..self.clone()
		}
	}

	/// Create a new Options object for a subquery
	pub fn new_with_perms(&self, perms: bool) -> Self {
		Self {
			perms,
			..self.clone()
		}
	}

	/// Create a new Options object for a subquery
	pub fn new_with_force(&self, force: Force) -> Self {
		Self {
			force,
			..self.clone()
		}
	}

	/// Create a new Options object for a subquery
	pub fn new_with_import(&self, import: bool) -> Self {
		Self {
			import,
			..self.clone()
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
			dive: self.dive - cost as u32,
			..self.clone()
		})
	}

	// --------------------------------------------------

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

	pub(crate) fn async_event_depth(&self) -> Option<u16> {
		self.async_event_depth
	}
}

// Keep the execution frame small; add fields only with justification.
const _: () = assert!(std::mem::size_of::<Options>() <= 128);
