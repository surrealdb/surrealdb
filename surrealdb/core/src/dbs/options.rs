use std::fmt::Debug;
use std::sync::Arc;

use anyhow::{Result, bail};

use crate::err::Error;
use crate::exec::Error as ExecError;
use crate::exec::config::ExecConfig;
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
	/// Why a data-modifying statement is rejected in this frame, if it is.
	///
	/// Both reasons are stored expressions that a *read* evaluates, so a write
	/// reached from one is a side effect the reading statement never asked for.
	/// See [`NoWriteFrame`] and `SECURITY_GUIDE.md`.
	pub(crate) no_write: Option<NoWriteFrame>,
	/// Set while evaluating a `create`/`update`/`delete` `PERMISSIONS` predicate
	/// with the `mutable_permissions` capability enabled — the transitional
	/// frame that permits writes GHSA-66r2-5gwj-gxm2 would otherwise block.
	///
	/// Purely observational: it does not gate execution (that is `no_write`'s
	/// job — this frame leaves it unset). A data-modifying statement reached
	/// under it is recorded on the per-statement counters as usage of the
	/// capability. Propagates into the predicate's sub-evaluations (nested
	/// subqueries and called function bodies) so their writes count too;
	/// naturally scoped, because it is only ever set on the predicate frame and
	/// callers resume their own `Options` once the predicate returns.
	pub(crate) mutable_permission_predicate: bool,
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
}

/// The kind of stored expression being evaluated, for frames where a
/// data-modifying statement must be rejected.
///
/// Each variant is a body the database evaluates on behalf of a reader who did
/// not write it and cannot see it. Both have a definition-time check that
/// rejects a mutation in the body itself; neither check can see through a call
/// to a user-defined function, whose body is stored separately and may be
/// redefined afterwards. This frame is the runtime half of each pair.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum NoWriteFrame {
	/// A stored `PERMISSIONS` predicate. Evaluated with `perms` disabled so it
	/// does not recurse into its own table's gates, which is exactly why it
	/// must not be able to write (GHSA-66r2-5gwj-gxm2).
	PermissionPredicate,
	/// A `COMPUTED` field body. Evaluated on every read of the field, under the
	/// definer's auth rather than the reader's, so a write here would let a
	/// read mutate the database as somebody else.
	ComputedField,
}

impl Options {
	pub(crate) fn new(config: &ExecConfig) -> Self {
		Self {
			ns: None,
			db: None,
			dive: config.max_computation_depth,
			perms: true,
			no_write: None,
			mutable_permission_predicate: false,
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

	/// Resume the computation-depth count `depth` levels deeper, reducing the
	/// remaining budget by that much (saturating at 0).
	///
	/// The streaming executor tracks nesting depth as a count *up* toward
	/// `max_computation_depth`; the legacy `compute` path tracks the same limit
	/// as the count *down* remaining budget held here in [`Self::dive`]. When the
	/// streaming engine hands a sub-computation to the legacy path (or to
	/// embedded scripting, which re-enters via `compute`), this carries the depth
	/// across so one continuous count applies end-to-end. The configured maximum
	/// is deliberately *not* altered — only the remaining budget shrinks — so the
	/// limit a query actually hits is always `max_computation_depth`, never an
	/// arbitrary smaller number derived from how deep the caller already was.
	pub(crate) fn with_dive_consumed(&self, depth: u32) -> Self {
		Self {
			dive: self.dive.saturating_sub(depth),
			..self.clone()
		}
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

	/// Return a copy of these options with the auth narrowed by the given limit.
	pub fn limited_by(&self, limit: &crate::iam::AuthLimit) -> Options {
		self.clone().with_auth(Arc::new(self.auth.new_limited(limit)))
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

	/// Create a new Options object for evaluating a `PERMISSIONS` predicate.
	///
	/// Disables permission recursion (like `new_with_perms(false)`) and marks
	/// the frame as a permission-predicate evaluation, so any attempt to run a
	/// mutating statement within the predicate is rejected by `Expr::compute`.
	/// Use this instead of `new_with_perms(false)` at every site that computes
	/// a stored `Permission::Specific(..)` clause.
	pub fn new_for_permission_predicate(&self) -> Self {
		Self {
			perms: false,
			no_write: Some(NoWriteFrame::PermissionPredicate),
			..self.clone()
		}
	}

	/// Create a new Options object for evaluating a `PERMISSIONS FOR
	/// create/update/delete` predicate when the `mutable_permissions` capability
	/// is enabled.
	///
	/// Like [`Self::new_for_permission_predicate`] it disables permission
	/// recursion, but it leaves `no_write` unset so a data-modifying statement in
	/// the predicate runs instead of being rejected. This reverses part of the
	/// GHSA-66r2-5gwj-gxm2 block for those write-triggered clauses; `SELECT`
	/// predicates never use it. The write runs with `perms` disabled (like the
	/// blocking frame), so an enabled server accepts the pre-fix escalation
	/// surface for these clauses — the documented cost of the transitional
	/// capability. Callers gate this on the capability; when it is off they use
	/// [`Self::new_for_permission_predicate`] instead.
	pub fn new_for_permission_predicate_allow_writes(&self) -> Self {
		Self {
			perms: false,
			no_write: None,
			mutable_permission_predicate: true,
			..self.clone()
		}
	}

	/// Create a new Options object for evaluating a `COMPUTED` field body.
	///
	/// Unlike [`Self::new_for_permission_predicate`] this leaves `perms`
	/// untouched: a computed body reads the database as the field's definer and
	/// those reads stay permission-checked. Only the write rejection is shared.
	pub fn new_for_computed_field(&self) -> Self {
		Self {
			no_write: Some(NoWriteFrame::ComputedField),
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
			(None, Some(_)) => Err(ExecError::NsEmpty.into()),
		}
	}

	/// Create a new Options object for a function/subquery/computed/etc.
	///
	/// The parameter is the approximate cost of the operation (more concretely, the size of the
	/// stack frame it uses relative to a simple function call). When in doubt, use a value of 1.
	pub(crate) fn dive(&self, cost: u8) -> Result<Self, Error> {
		if self.dive < cost as u32 {
			return Err(ExecError::ComputationDepthExceeded.into());
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
		self.ns.as_deref().ok_or_else(|| ExecError::NsEmpty).map_err(anyhow::Error::new)
	}

	pub(crate) fn arc_ns(&self) -> Result<Arc<str>> {
		self.ns.clone().ok_or_else(|| ExecError::NsEmpty).map_err(anyhow::Error::new)
	}

	/// Get currently selected DB
	#[inline(always)]
	pub fn db(&self) -> Result<&str> {
		self.db.as_deref().ok_or_else(|| ExecError::DbEmpty).map_err(anyhow::Error::new)
	}

	pub(crate) fn arc_db(&self) -> Result<Arc<str>> {
		self.db.clone().ok_or_else(|| ExecError::DbEmpty).map_err(anyhow::Error::new)
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
			bail!(ExecError::NsEmpty);
		}
		Ok(())
	}

	// Validate Options for Database
	#[inline(always)]
	pub fn valid_for_db(&self) -> Result<()> {
		if self.ns.is_none() {
			bail!(ExecError::NsEmpty);
		}
		if self.db.is_none() {
			bail!(ExecError::DbEmpty);
		}
		Ok(())
	}

	pub(crate) fn async_event_depth(&self) -> Option<u16> {
		self.async_event_depth
	}
}

// Keep the execution frame small; add fields only with justification.
const _: () = assert!(std::mem::size_of::<Options>() <= 128);
