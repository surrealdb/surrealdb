use std::fmt::Debug;
use std::sync::Arc;

use anyhow::{Result, bail};
use tracing::warn;

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
	/// The [`Self::perms`] value the actor's own statement carried.
	///
	/// Equal to `perms` in every frame except one: a `REFERENCE ON DELETE
	/// CASCADE` disables `perms` so the referencing record's own gate cannot
	/// block referential integrity. `perms` is a single frame-wide flag, so
	/// that also disables it for everything the nested delete reaches —
	/// including the graph-edge cascade, whose contract is the opposite (it
	/// must run as the caller, so an edge table's `PERMISSIONS FOR delete`
	/// still applies to an endpoint delete). A frame that needs the actor's
	/// real permission state rather than the integrity bypass reads this.
	pub(crate) caller_perms: bool,
	/// Why a data-modifying statement is rejected in this frame, if it is.
	///
	/// Both reasons are stored expressions that a *read* evaluates, so a write
	/// reached from one is a side effect the reading statement never asked for.
	/// See [`NoWriteFrame`] and `SECURITY_GUIDE.md`.
	pub(crate) no_write: Option<NoWriteFrame>,
	/// Should we process field queries?
	pub(crate) import: bool,
	/// The data version as a timestamp
	pub(crate) version: Option<u64>,
	/// Whether record reads must lock the fetched records for the duration
	/// of the transaction (`SELECT ... FOR UPDATE`)
	pub(crate) for_update: bool,
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
/// not write it and cannot see it. This frame is where the rejection happens:
/// it fires at the point a write is actually reached, so it holds regardless of
/// how many function calls deep the write sits or which branch selected it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum NoWriteFrame {
	/// A stored `SELECT` `PERMISSIONS` predicate. Evaluated with `perms`
	/// disabled so it does not recurse into its own table's gates, which is
	/// exactly why it must not be able to write (GHSA-66r2-5gwj-gxm2). The
	/// create/update/delete clauses are reached from a write and use
	/// [`Options::new_for_mutable_permission_predicate`] instead.
	PermissionPredicate,
	/// A `COMPUTED` field body. Evaluated on every read of the field, under the
	/// definer's auth rather than the reader's, so a write here would let a
	/// read mutate the database as somebody else. `DEFINE FIELD` rejects a
	/// mutation written directly into the body; this frame covers the writes
	/// that only a call can reach.
	ComputedField,
	/// A Surrealism module function that declared read-only access.
	///
	/// The guest reaches the datastore through a `transaction` handle, and
	/// declares in its signature whether it needs write access. That declaration
	/// reaches the host as the `writeable` flag in the baked exports manifest,
	/// and the planner already uses it to pick an access mode. Nothing enforced
	/// it, because the guest composes its query as a string at runtime — so a
	/// read-only function could still issue `CREATE`. This frame is that
	/// enforcement, and it fires wherever the write is reached, however deep.
	///
	/// How the declaration is spelled is the guest language's business; the host
	/// sees only the flag.
	SurrealismReadOnly,
}

impl NoWriteFrame {
	/// Whether this frame refuses a *call* to the named builtin.
	///
	/// Asked at two points, because a builtin can be reached two ways that do
	/// not meet: as an `Expr::FunctionCall`, which `expr_compute` sees, and as a
	/// method on a value (`f'bucket:k'.put(..)`), which dispatches straight
	/// through `fnc::idiom` with only its arguments going back through the
	/// expression walk. Both ask this so the answer cannot differ between them.
	///
	/// Scoped to [`Self::SurrealismReadOnly`]: see `refuses_under_no_write` in
	/// `legacy/expr/expression.rs` for why the other two frames permit these
	/// calls, and `SECURITY_GUIDE.md` for the decision that keeps
	/// [`Self::PermissionPredicate`] out of it.
	pub(crate) fn refuses_builtin_call(self, name: &str) -> bool {
		matches!(self, Self::SurrealismReadOnly) && crate::expr::method::mutates_state_builtin(name)
	}

	/// The error a refused write raises under this frame, and any operator
	/// warning that goes with it.
	///
	/// Lives here for the same reason [`Self::refuses_builtin_call`] does: the
	/// two enforcement points must not answer differently. Deciding *whether*
	/// to refuse in one place and *what to say* in another is how a frame ends
	/// up refused correctly under the wrong error — and these are not
	/// interchangeable, because they carry different wire codes and kinds that
	/// a client may branch on.
	pub(crate) fn side_effect_error(self) -> ExecError {
		match self {
			Self::PermissionPredicate => {
				// Point the operator at the sanctioned mechanism for write side
				// effects driven by a read.
				warn!(
					"A SELECT PERMISSIONS clause attempted to modify data and was blocked \
					 (GHSA-66r2-5gwj-gxm2). Move audit-style side effects to a DEFINE EVENT."
				);
				ExecError::PermissionPredicateSideEffect
			}
			Self::ComputedField => ExecError::ComputedFieldSideEffect,
			Self::SurrealismReadOnly => ExecError::SurrealismReadOnlySideEffect,
		}
	}
}

impl Options {
	pub(crate) fn new(config: &ExecConfig) -> Self {
		Self {
			ns: None,
			db: None,
			dive: config.max_computation_depth,
			perms: true,
			caller_perms: true,
			no_write: None,
			force: Force::None,
			import: false,
			auth: Arc::new(Auth::default()),
			version: None,
			for_update: false,
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
	///
	/// Moves [`Self::caller_perms`] in step: this is a statement of the actor's
	/// permission state, not a referential-integrity bypass. Use
	/// [`Self::new_for_reference_cascade`] for the latter.
	pub fn with_perms(mut self, perms: bool) -> Self {
		self.perms = perms;
		self.caller_perms = perms;
		self
	}

	/// Disable permission checks for a `REFERENCE ON DELETE` cascade, keeping
	/// [`Self::caller_perms`] at the actor's real permission state.
	///
	/// The cascade must be able to modify or delete the referencing record
	/// whatever the actor may do to it, or referential integrity could not be
	/// maintained. That bypass is scoped to the referencing record itself:
	/// nested work with its own permission contract — the graph-edge cascade in
	/// [`crate::doc::Document::purge_edges`] — restores `caller_perms`.
	pub(crate) fn new_for_reference_cascade(&self) -> Self {
		Self {
			perms: false,
			..self.clone()
		}
	}

	/// Restore the actor's own permission state, undoing a
	/// [`Self::new_for_reference_cascade`] bypass.
	pub(crate) fn new_with_caller_perms(&self) -> Self {
		Self {
			perms: self.caller_perms,
			..self.clone()
		}
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

	// Set whether record reads lock the fetched records
	pub fn with_for_update(mut self, for_update: bool) -> Self {
		self.for_update = for_update;
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
	///
	/// Moves [`Self::caller_perms`] in step, for the reason given on
	/// [`Self::with_perms`].
	pub fn new_with_perms(&self, perms: bool) -> Self {
		Self {
			perms,
			caller_perms: perms,
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
			caller_perms: false,
			no_write: Some(NoWriteFrame::PermissionPredicate),
			..self.clone()
		}
	}

	/// Create a new Options object for evaluating a `PERMISSIONS FOR
	/// create/update/delete` predicate.
	///
	/// Like [`Self::new_for_permission_predicate`] it disables permission
	/// recursion, but it leaves `no_write` unset so a data-modifying statement
	/// in the predicate runs instead of being rejected. Only the clauses a
	/// *write* triggers use this frame; a `SELECT` predicate never does. The
	/// write runs with `perms` disabled, like the blocking frame.
	pub fn new_for_mutable_permission_predicate(&self) -> Self {
		Self {
			perms: false,
			caller_perms: false,
			no_write: None,
			..self.clone()
		}
	}

	/// Create a new Options object for a Surrealism module function that
	/// declared read-only access.
	///
	/// Leaves `perms` untouched: the guest's reads stay permission-checked
	/// against its own narrowed capabilities. Only the write rejection is added.
	///
	/// Tightens rather than replaces. A module call nested inside a `SELECT`
	/// PERMISSIONS predicate arrives already carrying
	/// [`NoWriteFrame::PermissionPredicate`], and overwriting it would refuse
	/// the write under this frame's error instead — losing the
	/// GHSA-66r2-5gwj-gxm2 warning that tells an operator a permissions clause
	/// tried to modify data, and pointing them at the wrong component. The
	/// outermost frame is the one that explains the refusal, and every frame
	/// forbids the same thing, so keeping the first is both safe and more
	/// informative.
	pub fn new_for_read_only_module(&self) -> Self {
		Self {
			no_write: self.no_write.or(Some(NoWriteFrame::SurrealismReadOnly)),
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

#[cfg(test)]
mod tests {
	use super::{ExecError, NoWriteFrame};

	/// Every frame raises its own error, and no two share one.
	///
	/// The errors are not interchangeable — `PermissionPredicateSideEffect` is
	/// `-32000` / `Internal` and carries the GHSA warning, while
	/// `SurrealismReadOnlySideEffect` is `-32600` / `Validation` — so a client
	/// branching on code or kind takes a different branch for each. That is
	/// what makes naming a variant at an enforcement point wrong even when the
	/// scoping happens to make it unreachable, and it is the trap waiting for
	/// whoever widens `refuses_builtin_call` to cover the predicate frame.
	///
	/// Exhaustive by construction: a new variant that forgot to be listed here
	/// fails the count assertion below.
	#[test]
	fn each_frame_raises_its_own_error() {
		let frames = [
			NoWriteFrame::PermissionPredicate,
			NoWriteFrame::ComputedField,
			NoWriteFrame::SurrealismReadOnly,
		];
		for frame in frames {
			let expected = match frame {
				NoWriteFrame::PermissionPredicate => ExecError::PermissionPredicateSideEffect,
				NoWriteFrame::ComputedField => ExecError::ComputedFieldSideEffect,
				NoWriteFrame::SurrealismReadOnly => ExecError::SurrealismReadOnlySideEffect,
			};
			assert_eq!(
				frame.side_effect_error().to_string(),
				expected.to_string(),
				"{frame:?} raised the wrong error"
			);
		}

		// Distinct messages, so a client can tell which frame refused.
		let mut seen: Vec<String> =
			frames.iter().map(|f| f.side_effect_error().to_string()).collect();
		seen.sort();
		seen.dedup();
		assert_eq!(seen.len(), frames.len(), "two frames report the same error");
	}
}
