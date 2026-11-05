//! Entering the engine from outside a plan.
//!
//! Most expressions the engine evaluates arrive inside a plan, compiled by the
//! planner along with everything around them. A few do not: a schema
//! statement's name and field paths, an `ACCESS` statement's subject and
//! `WHERE`, a bucket's `PERMISSIONS` clause. Each of those is reached by a
//! caller that holds handles — a [`FrozenContext`] and an [`Options`] — and
//! needs one expression evaluated, not a statement planned. An access
//! method's `SIGNIN`, `SIGNUP`, `AUTHENTICATE` and `CONTEXT` clauses arrive
//! with no plan either, at the top of a request, and are planned as a
//! statement of their own.
//!
//! [`frame`] turns those handles into the [`ExecutionContext`] the engine runs
//! in, [`scalar`] evaluates one expression in it, and [`statement`] plans one
//! as a program of its own. These are the only routes in. A caller that compiles
//! its own expression decides for itself what a suppressed permission or a
//! refused write means, and two callers deciding separately could disagree about
//! the same clause.
//!
//! An expression reached this way is served from the compile cache by its text
//! ([`crate::exec::vm::serve::expr`]), compiled only on the cache's miss, and
//! its frame loads the namespace and database by name: both are seams of the
//! run-time seal ([`crate::exec::compiles`]), which a running program reaches
//! through a builtin that evaluates code the program did not compile — an API
//! handler, a module call — or a schema statement's clause. A caller outside
//! a running program — a statement's execution before its program starts —
//! reaches it at an entry boundary.

use std::sync::Arc;

use anyhow::Result;

use crate::catalog::providers::{DatabaseProvider, NamespaceProvider};
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::exe::FlowResultExt;
use crate::exec::context::{RootContext, SessionInfo};
use crate::exec::{Error as ExecError, EvalContext, ExecutionContext};
use crate::expr::Expr;
use crate::expr::logical::{LogicalEval, LogicalExpr, LogicalOp};
use crate::val::Value;

/// The execution context an expression reached from outside a plan evaluates
/// in, built from the frame its caller is running under.
///
/// `doc` is the enclosing row, which an expression in scalar position reads as
/// its document — `DEFINE PARAM $p VALUE a.b` inside a projection names the
/// row's `a.b`. A top-level caller has none.
///
/// The namespace and database are resolved when the frame names both, holds a
/// transaction, and both exist, so an expression that reads a table resolves
/// it. Anything else is a root frame: a statement that creates the namespace or
/// database it acts on has none to resolve, and an expression needing one fails
/// on its own rather than through the frame it was built in.
pub(crate) async fn frame(
	ctx: &FrozenContext,
	opt: &Options,
	doc: Option<&CursorDoc>,
) -> ExecutionContext {
	let root = ExecutionContext::Root(RootContext {
		ctx: Arc::clone(ctx),
		options: Some(opt.clone()),
		datastore: None,
		cancellation: Arc::new(ctx.cancel_token().map(|t| t.child_token()).unwrap_or_default()),
		auth: Arc::clone(&opt.auth),
		session: SessionInfo::from_context(ctx).map(Arc::new),
		current_value: None,
		current_row: doc.map(|doc| Arc::new(doc.doc.as_ref().clone())),
		forbidden_row: None,
		inherited_knn: None,
		// All three follow the caller's `perms`, as they do for any other
		// expression the engine evaluates inside a caller's frame:
		// enforcement is off in this frame whichever of the three a reader
		// asks for. `skip_fetch_perms` also refuses a write, which decides
		// nothing here — what the caller may write is `no_write`, read from
		// the same `Options`.
		skip_fetch_perms: !opt.perms,
		perms_suppressed: !opt.perms,
		caller_perms_suppressed: !opt.caller_perms,
		computing_record: None,
		no_write: opt.no_write,
		version_stamp: opt.version,
		guard_verdicts: Default::default(),
		writing: false,
		caller_view_checked: false,
		nested_runs: Default::default(),
		session_effects: None,
	});
	let (Ok(ns), Ok(db)) = (opt.ns(), opt.db()) else {
		return root;
	};
	let Some(txn) = ctx.try_tx() else {
		return root;
	};
	let (Ok(ns_def), Ok(db_def)) = crate::exec::vm::descriptor::object(async {
		(txn.expect_ns_by_name(ns).await, txn.expect_db_by_name(ns, db).await)
	})
	.await
	else {
		return root;
	};
	root.with_database(ns_def, db_def)
}

/// Evaluate one expression in `exec`.
///
/// Lowers, compiles and evaluates the expression. Every expression takes this route.
pub(crate) async fn scalar(exec: &ExecutionContext, expr: &Expr) -> Result<Value> {
	evaluate(expr, &eval_frame(exec)).await
}

/// The per-row frame an expression reached from outside a plan evaluates in.
///
/// An expression in scalar position reads the enclosing row as its document,
/// the way any other expression there does; at the top level there is none.
pub(crate) fn eval_frame(exec: &ExecutionContext) -> EvalContext<'_> {
	match exec.current_row() {
		Some(row) => EvalContext::from_exec_ctx(exec).with_value_and_doc(row),
		None => EvalContext::from_exec_ctx(exec),
	}
}

/// Lower, compile and evaluate one expression in `frame`.
///
/// Compiled against the transaction the caller already holds, so a nested
/// `SELECT` inside an expression resolves its access path here rather than at
/// execution time, and at the frame's own re-entry depth, so a body reached
/// through this keeps counting toward `max_computation_depth`.
///
/// An expression written with a construct that has no logical form
/// ([`crate::expr::logical::lower_expr`] names the set), or one holding a write
/// the engine has no plan for ([`crate::exec::logical::unplannable_write_in_expr`],
/// asked before the compile so the refusal is the walk's rather than whichever
/// branch the fold reached first), fails with `PlannerUnsupported`. A caller
/// reached through here accepts what an expression accepts and nothing wider.
pub(crate) async fn evaluate(expr: &Expr, frame: &EvalContext<'_>) -> Result<Value> {
	let exec = frame.exec_ctx;
	let logical = crate::expr::logical::lower_expr(expr.clone()).map_err(|e| {
		ExecError::PlannerUnsupported {
			reason: e.to_string(),
		}
	})?;
	if let Some(reason) = crate::exec::logical::unplannable_write_in_expr(&logical) {
		return Err(ExecError::PlannerUnsupported {
			reason,
		}
		.into());
	}
	let compiled = crate::exec::vm::serve::expr(
		exec,
		logical,
		Some(Arc::clone(&exec.root().auth)),
		frame.plan_depth,
	)
	.await?;
	compiled.evaluate(frame.clone()).await.catch_return()
}

/// Evaluate one expression in `exec` as a statement of its own: the
/// expression is planned as one program before it runs, the way a request's
/// statement is, rather than evaluated as a unit.
///
/// For a caller at the top of a request, outside any plan, with no enclosing
/// row: an access method's `SIGNIN`, `SIGNUP`, `AUTHENTICATE` or `CONTEXT`
/// clause. A subquery is planned as the statement it holds; any other
/// expression as a value statement. It refuses what [`evaluate`] refuses, and
/// a `RETURN` leaving it is its value.
pub(crate) async fn statement(exec: &ExecutionContext, expr: &Expr) -> Result<Value> {
	let logical = crate::expr::logical::lower_expr(expr.clone()).map_err(|e| {
		ExecError::PlannerUnsupported {
			reason: e.to_string(),
		}
	})?;
	if let Some(reason) = crate::exec::logical::unplannable_write_in_expr(&logical) {
		return Err(ExecError::PlannerUnsupported {
			reason,
		}
		.into());
	}
	let op = if let LogicalExpr::Subquery(op) = logical {
		*op
	} else {
		LogicalOp::Eval(LogicalEval {
			expr: logical,
		})
	};
	let plan = crate::exec::vm::serve::statement(
		exec,
		op,
		Some(Arc::clone(&exec.root().auth)),
		0,
		Vec::new(),
	)
	.await?;
	let run = async {
		let shape = plan.output_shape();
		let stream = plan.execute(exec)?;
		crate::exec::operators::collect_shaped_value(stream, shape).await
	};
	run.await.catch_return()
}
