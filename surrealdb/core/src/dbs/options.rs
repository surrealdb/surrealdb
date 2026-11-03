use std::fmt::Debug;
use std::sync::Arc;

use anyhow::{Result, bail};
use tracing::warn;

use crate::err::Error;
use crate::exec::Error as ExecError;
use crate::exec::config::ExecConfig;
use crate::expr::Base;
use crate::expr::logical::EffectSet;
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
	/// Whether the evaluation spends [`Self::dive`] per expression it enters
	/// rather than per re-entry: inside a table's event clauses, the body of a
	/// user-defined function whose text writes, and what either reaches
	/// ([`crate::exec::vm::depth`]).
	pub(crate) per_expression_depth: bool,
}

#[derive(Clone, Debug)]
pub enum Force {
	All,
	None,
}

/// The kind of stored expression being evaluated, for frames where an effect
/// must be refused.
///
/// Each variant but [`Self::SurrealismReadOnly`] is a schema clause that must
/// not have effects: a `PERMISSIONS` clause, a `COMPUTED`, `VALUE`, `DEFAULT`
/// or `ASSERT` clause, or a field migration's `MIGRATE` transform. A definition holding such an
/// effect is refused where it is defined; this frame is where a clause stored before that rule is
/// refused at the point the effect is reached, however many function calls deep it
/// sits or whichever branch selected it. It refuses a write and a schema
/// statement, a `SLEEP`, and a builtin [`Self::refuses_builtin_call`] names,
/// each with [`Self::side_effect_error`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum NoWriteFrame {
	/// A stored permission predicate a read judges: a table's or field's
	/// `PERMISSIONS FOR select`, a function's or parameter's `PERMISSIONS`.
	/// Evaluated with `perms` disabled so it does not recurse into its own
	/// table's gates, which is why a write reached from it would bypass every
	/// gate (GHSA-66r2-5gwj-gxm2).
	PermissionPredicate,
	/// A stored `PERMISSIONS FOR create/update/delete` predicate a write
	/// judges, a table's or a field's. Evaluated with `perms` disabled, as a
	/// read's predicate is.
	WritePermission,
	/// A `COMPUTED` field body. Evaluated on every read of the field, under the
	/// definer's auth rather than the reader's.
	ComputedField,
	/// A field's `VALUE` clause.
	FieldValue,
	/// A field's `DEFAULT` clause.
	FieldDefault,
	/// A field's `ASSERT` clause.
	FieldAssert,
	/// A Surrealism module function that declared read-only access.
	///
	/// The guest reaches the datastore through a `transaction` handle, and
	/// declares in its signature whether it needs write access. That declaration
	/// reaches the host as the `writeable` flag in the baked exports manifest,
	/// and the planner uses it to pick an access mode. The guest composes its
	/// query as a string at runtime, so a read-only function could still issue
	/// `CREATE`; this frame is what refuses it, wherever the write is reached,
	/// however deep.
	///
	/// How the declaration is spelled is the guest language's business; the host
	/// sees only the flag.
	SurrealismReadOnly,
	/// A field migration's transform. It depends on `$value` alone, so it may
	/// neither write nor dereference a record.
	Migration,
	/// A LIVE query's subscriber-authored projection, condition or FETCH clause.
	/// It is evaluated at notification time inside the triggering write's
	/// transaction, so a write reached from it would let a subscriber inject
	/// side effects into another user's write (GHSA-pvv9-h693-m8r4).
	LiveClause,
	/// An extension definition's `BINDINGS` expression. It runs with
	/// permissions off whenever a caller's invocation reads it, so a write or an
	/// external effect reached from it would be one any caller could trigger
	/// past the table and field permissions.
	ExtensionBinding,
}

impl NoWriteFrame {
	/// The schema clause this frame evaluates, as an error names it; `None`
	/// for [`Self::SurrealismReadOnly`], which is no schema clause.
	pub(crate) fn clause(self) -> Option<&'static str> {
		match self {
			Self::PermissionPredicate | Self::WritePermission => Some("PERMISSIONS"),
			Self::ComputedField => Some("COMPUTED"),
			Self::FieldValue => Some("VALUE"),
			Self::FieldDefault => Some("DEFAULT"),
			Self::FieldAssert => Some("ASSERT"),
			Self::Migration => Some("MIGRATE"),
			Self::LiveClause => Some("LIVE"),
			Self::ExtensionBinding => Some("BINDINGS"),
			Self::SurrealismReadOnly => None,
		}
	}

	/// Whether this frame refuses a *call* to the builtin `name` that declares
	/// `effects`.
	///
	/// Asked wherever a builtin is applied, by a function call and by a method
	/// on a value (`f'bucket:k'.put(..)`) alike, so the answer cannot differ
	/// between them.
	///
	/// A schema clause refuses a builtin that blocks, and one that advances
	/// state outside the transaction unless the clause is a `VALUE` or
	/// `DEFAULT` ([`crate::exec::clause_effects::refused_builtin_by`]); a
	/// `LIVE` clause also refuses one that writes to an external system, so a
	/// subscriber-authored clause cannot exfiltrate another user's write
	/// (GHSA-pvv9-h693-m8r4); a builtin that writes data through the transaction
	/// is refused where the write is made. A read-only Surrealism function
	/// refuses every builtin that changes state
	/// ([`crate::expr::method::mutates_state_builtin`]).
	pub(crate) fn refuses_builtin_call(self, name: &str, effects: EffectSet) -> bool {
		match self.clause() {
			Some(clause) => {
				effects.intersects(crate::exec::clause_effects::refused_builtin_by(clause))
			}
			None => crate::expr::method::mutates_state_builtin(name),
		}
	}

	/// Whether this frame refuses every call into an extension. An extension
	/// acts through its own grant, network included, which no frame narrows,
	/// so a frame whose clause refuses external writes refuses the call.
	#[cfg(feature = "surrealism")]
	pub(crate) fn refuses_extension_call(self) -> bool {
		matches!(self, Self::ExtensionBinding)
	}

	/// The error a refused effect raises under this frame. `construct` names
	/// the construct refused and what it does, as
	/// [`crate::exec::clause_effects`] phrases it.
	///
	/// Lives here for the same reason [`Self::refuses_builtin_call`] does: the
	/// enforcement points must not answer differently. A schema clause raises
	/// [`ExecError::SchemaClauseEffect`] naming itself; a read-only Surrealism
	/// function raises its own error, which carries a different meaning for
	/// the client.
	pub(crate) fn side_effect_error(self, construct: &str) -> ExecError {
		if self == Self::PermissionPredicate {
			// Point the operator at the sanctioned mechanism for side effects
			// driven by a read.
			warn!(
				"A PERMISSIONS clause attempted an effect and was blocked \
				 (GHSA-66r2-5gwj-gxm2). Move audit-style side effects to a DEFINE EVENT."
			);
		}
		match self.clause() {
			Some(clause) => ExecError::SchemaClauseEffect {
				clause: clause.to_owned(),
				construct: construct.to_owned(),
			},
			None => ExecError::SurrealismReadOnlySideEffect,
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
			per_expression_depth: false,
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
	/// The planner tracks nesting depth as a count *up* toward
	/// `max_computation_depth`; the budget held here in [`Self::dive`] counts
	/// *down*. Embedded scripting, which re-enters the engine from inside a
	/// computation, carries the depth across with this, so one continuous count
	/// applies end to end. The configured maximum is deliberately *not*
	/// altered — only the remaining budget shrinks — so the limit a query
	/// actually hits is always `max_computation_depth`, never an arbitrary
	/// smaller number derived from how deep the caller already was. A body
	/// entered to be evaluated one expression at a time from code that is not
	/// resumes the count the same way ([`crate::exec::vm::depth`]).
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
	/// the graph-edge purge — restores `caller_perms`.
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

	/// Create a new Options object whose reads are not permission-checked,
	/// with no write refusal of its own.
	///
	/// Like [`Self::new_for_permission_predicate`] it disables permission
	/// recursion, but it leaves `no_write` unset: what the frame refuses is
	/// the caller's to set. An event body runs here and may write; a
	/// permission predicate is given its [`NoWriteFrame`] on top.
	pub fn new_with_unchecked_reads(&self) -> Self {
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

	/// Create a new Options object for evaluating an extension definition's
	/// `BINDINGS` expression: reads unchecked, as the definition's auth limit
	/// allows, and no write or external effect.
	pub fn new_for_extension_binding(&self) -> Self {
		Self {
			perms: false,
			caller_perms: false,
			no_write: Some(NoWriteFrame::ExtensionBinding),
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
		self.check_dive(cost)?;
		Ok(Self {
			dive: self.dive - cost as u32,
			..self.clone()
		})
	}

	/// Spend `cost` of this object's computation-depth budget in place, with the
	/// failure [`Self::dive`] raises once the budget is spent, leaving the budget
	/// as it was.
	pub(crate) fn spend_dive(&mut self, cost: u8) -> Result<(), Error> {
		self.check_dive(cost)?;
		self.dive -= cost as u32;
		Ok(())
	}

	/// Whether a dive of `cost` would succeed, spending nothing.
	pub(crate) fn check_dive(&self, cost: u8) -> Result<(), Error> {
		if self.dive < cost as u32 {
			return Err(ExecError::ComputationDepthExceeded.into());
		}
		Ok(())
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
	use crate::expr::logical::EffectSet;

	/// Every schema-clause frame raises the schema-clause error naming its own
	/// clause and the construct refused; the read-only Surrealism frame raises
	/// its own error, which a client branches on apart.
	///
	/// Exhaustive by construction: a new variant must be placed in the match.
	#[test]
	fn each_frame_raises_its_clauses_error() {
		let frames = [
			NoWriteFrame::PermissionPredicate,
			NoWriteFrame::WritePermission,
			NoWriteFrame::ComputedField,
			NoWriteFrame::FieldValue,
			NoWriteFrame::FieldDefault,
			NoWriteFrame::FieldAssert,
			NoWriteFrame::SurrealismReadOnly,
			NoWriteFrame::Migration,
			NoWriteFrame::LiveClause,
			NoWriteFrame::ExtensionBinding,
		];
		for frame in frames {
			let expected = match frame {
				NoWriteFrame::PermissionPredicate | NoWriteFrame::WritePermission => {
					Some("PERMISSIONS")
				}
				NoWriteFrame::ComputedField => Some("COMPUTED"),
				NoWriteFrame::FieldValue => Some("VALUE"),
				NoWriteFrame::FieldDefault => Some("DEFAULT"),
				NoWriteFrame::FieldAssert => Some("ASSERT"),
				NoWriteFrame::Migration => Some("MIGRATE"),
				NoWriteFrame::LiveClause => Some("LIVE"),
				NoWriteFrame::ExtensionBinding => Some("BINDINGS"),
				NoWriteFrame::SurrealismReadOnly => None,
			};
			let raised = frame.side_effect_error("`CREATE` writes data");
			match expected {
				Some(clause) => assert_eq!(
					raised.to_string(),
					ExecError::SchemaClauseEffect {
						clause: clause.to_owned(),
						construct: "`CREATE` writes data".to_owned(),
					}
					.to_string(),
					"{frame:?} raised the wrong error"
				),
				None => assert_eq!(
					raised.to_string(),
					ExecError::SurrealismReadOnlySideEffect.to_string()
				),
			}
		}
	}

	/// A schema clause refuses a call that blocks, and one that advances state
	/// outside the transaction unless it is a `VALUE` or `DEFAULT`; it leaves
	/// randomness, the clock and a write through the transaction to be judged
	/// elsewhere. A `LIVE` clause alone also refuses a write to an external
	/// system, and no clause refuses a read of one.
	#[test]
	fn a_clause_frame_refuses_state_advancing_and_blocking_builtins() {
		let advances = [
			(NoWriteFrame::PermissionPredicate, true),
			(NoWriteFrame::WritePermission, true),
			(NoWriteFrame::ComputedField, true),
			(NoWriteFrame::FieldAssert, true),
			(NoWriteFrame::Migration, true),
			(NoWriteFrame::LiveClause, true),
			(NoWriteFrame::ExtensionBinding, true),
			(NoWriteFrame::FieldValue, false),
			(NoWriteFrame::FieldDefault, false),
		];
		for (frame, refused) in advances {
			assert_eq!(
				frame.refuses_builtin_call("sequence::nextval", EffectSet::ADVANCES_STATE),
				refused,
				"{frame:?}"
			);
			assert!(frame.refuses_builtin_call("sleep", EffectSet::BLOCKING), "{frame:?}");
			assert!(!frame.refuses_builtin_call("rand::uuid", EffectSet::ENTROPY), "{frame:?}");
			assert!(!frame.refuses_builtin_call("time::now", EffectSet::CLOCK), "{frame:?}");
			assert!(
				!frame.refuses_builtin_call(
					"eval::surql",
					EffectSet::OPAQUE_BODY.union(EffectSet::WRITES_DATA)
				),
				"{frame:?}"
			);
			// Only a LIVE clause and an extension's BINDINGS refuse a builtin
			// that writes to an external system: the first runs inside another
			// user's triggering write, so such a call would exfiltrate that
			// write (GHSA-pvv9-h693-m8r4); the second runs with its definer's
			// rights on every caller's invocation. Every
			// other clause, `VALUE` and `DEFAULT` included, leaves an external
			// write to be judged elsewhere, and no clause refuses a read of an
			// external system, which may enrich a value without a side effect.
			let external =
				matches!(frame, NoWriteFrame::LiveClause | NoWriteFrame::ExtensionBinding);
			assert_eq!(
				frame.refuses_builtin_call(
					"http::post",
					EffectSet::READS_EXTERNAL.union(EffectSet::WRITES_EXTERNAL)
				),
				external,
				"http::post under {frame:?}"
			);
			assert_eq!(
				frame.refuses_builtin_call("file::put", EffectSet::WRITES_EXTERNAL),
				external,
				"file::put under {frame:?}"
			);
			assert!(
				!frame.refuses_builtin_call("http::get", EffectSet::READS_EXTERNAL),
				"http::get under {frame:?}"
			);
		}
	}
}
