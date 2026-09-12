//! The streaming engine's one bridge into the legacy evaluator.
//!
//! The streaming engine plans no writes of its own. Five things reach the
//! legacy `compute()` path from inside a running plan, and all go through here:
//!
//! - the GQL mutation operators (`mutate.rs`), which run a synthetic write statement per binding
//!   row so that table/field permissions, events, indexes and live queries apply exactly as for a
//!   native mutation;
//! - a closure value whose body only the legacy evaluator can run — a `Closure::Expr` read back
//!   from a `DEFINE PARAM`, or a natively implemented API middleware closure — invoked from a
//!   streaming expression (`physical_expr/function/helpers.rs`);
//! - the statement `eval::surql` evaluates when it has no logical form
//!   (`function/builtin/eval.rs`). `eval` parses its text into a statement of its own, so it is a
//!   statement boundary, and the engine is chosen for that statement as the executor chooses for a
//!   top-level one;
//! - a stored catalog clause with no logical form — a table or field `PERMISSIONS FOR select`
//!   guard, or a `COMPUTED` body — evaluated by a scan enforcing permissions at runtime
//!   (`exec::permission::LegacyClause`). The analyzer leaves a source whose table declares such a
//!   clause unresolved, so the read is never declined; the clause runs here, in the frame its
//!   caller is in;
//! - a user-defined function body the streaming engine cannot run — one whose stored text has no
//!   logical form (a body that writes), or whose plan the planner declines
//!   (`physical_expr/function/user_defined.rs`). The body runs on the stored block, in the frame
//!   the call is in.
//!
//! Everything else the engine runs is a compiled [`crate::exec::PhysicalExpr`]
//! or [`crate::exec::ExecOperator`]; a statement the planner declines is decided
//! at a statement boundary — the executor's, or `eval`'s — never partway through
//! a plan.

use std::sync::Arc;

use reblessive::tree::TreeStack;

use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::err::Error;
use crate::exec::context::ExecutionContext;
use crate::exec::{Error as ExecError, FlowResult};
use crate::expr::Expr;
use crate::val::Value;

/// The `Options` and `FrozenContext` a legacy evaluation inside `exec_ctx` runs
/// with.
///
/// The `ExecutionContext`'s `FrozenContext` is the single source of truth for
/// parameters, transactions, capabilities, and all legacy context fields, so a
/// binding made on the planned path (`with_param` / `output_context`) is
/// visible to the legacy evaluator. The options carry the evaluation frame the
/// context is in; see [`legacy_options_for_frame`].
pub(crate) fn legacy_handles(
	exec_ctx: &ExecutionContext,
) -> Result<(Options, FrozenContext), Error> {
	let options = exec_ctx
		.options()
		.ok_or_else(|| ExecError::Thrown("Options not available for legacy evaluation".into()))?;
	let root = exec_ctx.root();
	let options = legacy_options_for_frame(
		options,
		root.skip_fetch_perms,
		root.computing_field,
		root.version_stamp,
	);
	Ok((options, Arc::clone(exec_ctx.ctx())))
}

/// The `Options` a legacy evaluation runs with, given the frame it runs in.
///
/// Every write in an expression reaches the legacy path through this module,
/// and two frames must not be allowed to take it:
///
/// - a `PERMISSIONS` predicate (signalled by `permission_predicate`), which is evaluated with
///   permission enforcement disabled (GHSA-66r2-5gwj-gxm2);
/// - a `COMPUTED` field body (signalled by `computing_field`), which is evaluated on every read of
///   the field and under the definer's auth.
///
/// The permission frame also disables `perms`; the computed frame does not,
/// because a computed body's own reads stay permission-checked. The two
/// signals are taken as arguments rather than read from a context because the
/// permission frame is tracked per `EvalContext` at some call sites and per
/// root at others. Naming them at each site keeps this the only place that
/// decides what they mean.
///
/// `version_stamp` is the `VERSION` the streaming context reads at
/// (`ExecutionContext::version_stamp`, set by `VersionScope`). The legacy
/// evaluator reads at `Options::version`, so the stamp is carried across:
/// a legacy evaluation reached from a versioned statement reads the same
/// point in time as the statement. Without a stamp the options keep the
/// version they already carry.
pub(crate) fn legacy_options_for_frame(
	options: &Options,
	permission_predicate: bool,
	computing_field: bool,
	version_stamp: Option<u64>,
) -> Options {
	let options = if permission_predicate {
		options.new_for_permission_predicate()
	} else if computing_field {
		options.new_for_computed_field()
	} else {
		options.clone()
	};
	match version_stamp {
		Some(stamp) => options.with_version(Some(stamp)),
		None => options,
	}
}

/// Evaluate `expr` with the legacy evaluator.
///
/// The single call site for `Expr::compute()` in the streaming engine; the
/// callers named in the module documentation are the only ones that may use it.
pub(crate) async fn legacy_compute(
	expr: &Expr,
	frozen: &FrozenContext,
	opt: &Options,
	doc: Option<&crate::doc::CursorDoc>,
) -> FlowResult<Value> {
	let mut stack = TreeStack::new();
	stack.enter(|stk| crate::legacy::expr_compute(expr, stk, frozen, opt, doc)).finish().await
}

#[cfg(test)]
mod tests {
	use super::legacy_options_for_frame;
	use crate::dbs::{NoWriteFrame, Options};
	use crate::exec::config::ExecConfig;

	fn options() -> Options {
		Options::new(&ExecConfig::default())
	}

	/// A legacy evaluation reached from a `PERMISSIONS` predicate runs in a
	/// frame that refuses every write (GHSA-66r2-5gwj-gxm2), whichever caller
	/// of the bridge reaches it.
	#[test]
	fn the_permission_frame_forbids_writes_on_the_legacy_path() {
		let opt = options();
		let frame = legacy_options_for_frame(&opt, true, false, None);
		assert_eq!(frame.no_write, Some(NoWriteFrame::PermissionPredicate));
		assert!(!frame.perms);
	}

	/// A computed body may not write either, but its reads stay
	/// permission-checked.
	#[test]
	fn the_computed_frame_forbids_writes_and_keeps_reads_checked() {
		let opt = options();
		let frame = legacy_options_for_frame(&opt, false, true, None);
		assert_eq!(frame.no_write, Some(NoWriteFrame::ComputedField));
		assert_eq!(frame.perms, opt.perms);
	}

	/// The permission frame wins when both signals are set: it is the stricter
	/// of the two.
	#[test]
	fn the_permission_frame_takes_precedence_over_the_computed_one() {
		let opt = options();
		let frame = legacy_options_for_frame(&opt, true, true, None);
		assert_eq!(frame.no_write, Some(NoWriteFrame::PermissionPredicate));
	}

	#[test]
	fn a_plain_frame_changes_nothing() {
		let opt = options();
		let frame = legacy_options_for_frame(&opt, false, false, None);
		assert_eq!(frame.no_write, None);
		assert_eq!(frame.perms, opt.perms);
		assert_eq!(frame.version, None);
	}

	/// A legacy evaluation reached from a versioned statement reads at the
	/// statement's `VERSION`, in every frame.
	#[test]
	fn the_version_stamp_is_carried_into_every_frame() {
		let opt = options();
		assert_eq!(legacy_options_for_frame(&opt, false, false, Some(7)).version, Some(7));
		assert_eq!(legacy_options_for_frame(&opt, true, false, Some(7)).version, Some(7));
		assert_eq!(legacy_options_for_frame(&opt, false, true, Some(7)).version, Some(7));
	}

	/// Without a stamp from the streaming context, the options keep the
	/// version they already carry: a legacy caller that entered the streaming
	/// engine at a version leaves it at that version.
	#[test]
	fn no_stamp_keeps_the_options_version() {
		let opt = options().with_version(Some(3));
		assert_eq!(legacy_options_for_frame(&opt, false, false, None).version, Some(3));
	}
}
