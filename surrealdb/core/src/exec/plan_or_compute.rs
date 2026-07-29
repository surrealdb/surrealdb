//! Shared helpers for the plan-or-compute fallback pattern.
//!
//! Operators that use deferred planning (foreach, ifelse, sequence, block) share
//! a common pattern: try to plan an expression with the streaming engine, and if
//! the planner returns `PlannerUnsupported` or `PlannerUnimplemented`, fall back
//! to the legacy `Expr::compute()` path.
//!
//! This module centralises that logic so each operator does not need its own copy.

use std::sync::Arc;

use futures::StreamExt;
use reblessive::tree::TreeStack;

use crate::ctx::FrozenContext;
use crate::err::Error;
use crate::exec::context::{ContextLevel, ExecutionContext};
use crate::exec::planner::try_plan_expr;
use crate::exec::{Error as ExecError, FlowResult, ValueBatchStream};
use crate::expr::part::{Part, RecurseInstruction};
use crate::expr::statements::InfoStatement;
use crate::expr::{Base, Block, ControlFlow, ControlFlowExt, Expr, Literal};
use crate::val::Value;

// ============================================================================
// Legacy Context Helpers
// ============================================================================

/// Extract the `Options` and `FrozenContext` needed for legacy `Expr::compute()`.
///
/// The `ExecutionContext`'s `FrozenContext` is the single source of truth for
/// parameters, transactions, capabilities, and all legacy context fields.
pub(crate) fn get_legacy_context(
	exec_ctx: &ExecutionContext,
) -> Result<(crate::dbs::Options, FrozenContext), Error> {
	let options = exec_ctx.options().ok_or_else(|| {
		ExecError::Thrown("Options not available for legacy compute fallback".into())
	})?;
	let options = legacy_fallback_options(exec_ctx, options);
	Ok((options, Arc::clone(exec_ctx.ctx())))
}

/// Derive the `Options` to use for a legacy compute fallback.
///
/// When the streaming context is evaluating a `PERMISSIONS` predicate
/// (signalled by `skip_fetch_perms`), the fallback must block write side
/// effects so a predicate cannot mutate data through the legacy compute path
/// (GHSA-66r2-5gwj-gxm2).
fn legacy_fallback_options(
	exec_ctx: &ExecutionContext,
	options: &crate::dbs::Options,
) -> crate::dbs::Options {
	if exec_ctx.root().skip_fetch_perms {
		options.new_for_permission_predicate()
	} else {
		options.clone()
	}
}

/// Extract the `Options` and `FrozenContext` for legacy fallback, adding a loop
/// variable to the context.
///
/// Used by the `ForeachPlan` operator to inject the current iteration value.
pub(crate) fn get_legacy_context_with_param(
	exec_ctx: &ExecutionContext,
	param_name: &str,
	param_value: &Value,
) -> Result<(crate::dbs::Options, FrozenContext), Error> {
	let options = exec_ctx.options().ok_or_else(|| {
		ExecError::Thrown("Options not available for legacy compute fallback".into())
	})?;
	let options = legacy_fallback_options(exec_ctx, options);

	let mut ctx = crate::ctx::Context::new_child(exec_ctx.ctx());
	ctx.add_value(param_name.to_string(), std::sync::Arc::new(param_value.clone()));

	Ok((options, ctx.freeze()))
}

// ============================================================================
// Legacy Compute
// ============================================================================

/// Execute an expression using the legacy `Expr::compute()` path.
///
/// This is the **single call site** for `Expr::compute()` in the streaming
/// executor.  All operators that need legacy compute fallback should call this
/// function rather than invoking `.compute()` directly.
pub(crate) async fn legacy_compute(
	expr: &Expr,
	frozen: &FrozenContext,
	opt: &crate::dbs::Options,
	doc: Option<&crate::doc::CursorDoc>,
) -> crate::expr::FlowResult<Value> {
	let mut stack = TreeStack::new();
	stack.enter(|stk| expr.compute(stk, frozen, opt, doc)).finish().await
}

// ============================================================================
// Plan-or-Compute Evaluation
// ============================================================================

/// Plan and evaluate an expression, falling back to legacy compute if the
/// planner returns `PlannerUnsupported` or `PlannerUnimplemented`.
///
/// This is the simple variant used when no context mutation is needed
/// (e.g. evaluating a FOR range, an IF condition, or an IF/ELSE branch body).
/// Plan and evaluate an expression, falling back to legacy compute if the
/// planner returns `PlannerUnsupported` or `PlannerUnimplemented`, seeding the
/// planner's expression-nesting depth (see
/// [`crate::exec::planner::Planner::with_depth`]).
///
/// `depth` is 0 for an ordinary top-level evaluation. It is non-zero when
/// re-planning a nested query at runtime — chiefly `eval::*` re-planning its
/// query string, and the deferred control-flow operators (IfElse/Foreach/
/// Sequence) re-planning their bodies — where passing the depth recorded on the
/// triggering node continues the count toward `max_computation_depth` instead of
/// resetting it. That is what bounds nested-`eval`/UDF recursion the same way the
/// legacy `compute` path bounds it via `Options::dive`; the legacy fallback below
/// mirrors it by handing the remaining budget (`max - depth`) to `compute`.
pub(crate) async fn evaluate_expr_at_depth(
	expr: &Expr,
	ctx: &ExecutionContext,
	depth: u32,
) -> crate::expr::FlowResult<Value> {
	let auth = ctx.options().map(|o| Arc::clone(&o.auth));
	match try_plan_expr!(expr, ctx.ctx(), ctx.txn(), auth, depth) {
		Ok(plan) => {
			let stream = plan.execute(ctx)?;
			collect_single_value(stream).await
		}
		Err(Error::Exec(
			e @ (ExecError::PlannerUnsupported(_) | ExecError::PlannerUnimplemented(_)),
		)) => {
			match &e {
				ExecError::PlannerUnimplemented(msg) => {
					tracing::warn!("PlannerUnimplemented fallback in evaluate_expr: {msg}");
				}
				ExecError::PlannerUnsupported(msg) => {
					tracing::debug!("PlannerUnsupported fallback in evaluate_expr: {msg}",);
				}
				_ => {}
			}
			let (opt, frozen) =
				get_legacy_context(ctx).context("Legacy compute fallback context unavailable")?;
			// Continue the depth count into the legacy path: a DML/DDL body reached
			// via `eval` keeps counting from `depth` rather than a fresh budget.
			let opt = opt.with_dive_consumed(depth);
			legacy_compute(expr, &frozen, &opt, None).await
		}
		Err(e) => Err(ControlFlow::Err(e.into())),
	}
}

/// Plan and evaluate a body expression that may mutate the execution context
/// (e.g. a LET statement inside a FOR loop body).
///
/// When the planned operator has `mutates_context() == true`, the context is
/// updated via `output_context()`. The legacy fallback for loop bodies injects
/// the loop variable into the context before calling `Expr::compute()`.
pub(crate) async fn evaluate_body_expr(
	expr: &Expr,
	ctx: &mut ExecutionContext,
	param_name: &str,
	param_value: &Value,
	depth: u32,
) -> crate::expr::FlowResult<Value> {
	let frozen_ctx = Arc::clone(ctx.ctx());
	let auth = ctx.options().map(|o| Arc::clone(&o.auth));

	match try_plan_expr!(expr, &frozen_ctx, ctx.txn(), auth, depth) {
		Ok(plan) => {
			if plan.mutates_context() {
				*ctx = plan.output_context(ctx).await.map_err(ControlFlow::Err)?;
				Ok(Value::None)
			} else {
				let stream = plan.execute(ctx)?;
				collect_single_value(stream).await
			}
		}
		Err(Error::Exec(
			e @ (ExecError::PlannerUnsupported(_) | ExecError::PlannerUnimplemented(_)),
		)) => {
			match &e {
				ExecError::PlannerUnimplemented(msg) => {
					tracing::warn!("PlannerUnimplemented fallback in evaluate_body_expr: {msg}");
				}
				ExecError::PlannerUnsupported(msg) => {
					tracing::debug!("PlannerUnsupported fallback in evaluate_body_expr: {msg}",);
				}
				_ => {}
			}
			let (opt, frozen) = get_legacy_context_with_param(ctx, param_name, param_value)
				.context("Legacy compute fallback context unavailable")?;
			// Continue the depth count into the legacy path from `depth`.
			let opt = opt.with_dive_consumed(depth);

			if let Expr::Let(set_stmt) = expr {
				if set_stmt.is_protected_set() {
					return Err(ExecError::InvalidParam {
						name: set_stmt.name.to_string(),
					}
					.into());
				}

				let value = legacy_compute(&set_stmt.what, &frozen, &opt, None).await?;

				let value = if let Some(kind) = &set_stmt.kind {
					value.coerce_to_kind(kind).map_err(|e| ExecError::SetCoerce {
						name: set_stmt.name.to_string(),
						error: Box::new(e),
					})?
				} else {
					value
				};

				*ctx = ctx.with_param(set_stmt.name.clone(), value);
				Ok(Value::None)
			} else {
				legacy_compute(expr, &frozen, &opt, None).await
			}
		}
		Err(e) => Err(ControlFlow::Err(e.into())),
	}
}

// ============================================================================
// Stream Collection Helpers
// ============================================================================

/// Collect values from a stream into a single value.
///
/// - Empty stream → `Value::None`
/// - Single value → that value
/// - Multiple values → wrapped in an array (for query results like SELECT)
///
/// Propagates control flow signals (BREAK, CONTINUE, RETURN, errors).
pub(crate) async fn collect_single_value(
	mut stream: ValueBatchStream,
) -> crate::expr::FlowResult<Value> {
	let mut values = Vec::new();

	while let Some(batch_result) = stream.next().await {
		match batch_result {
			Ok(batch) => values.extend(batch.values),
			Err(ctrl) => return Err(ctrl),
		}
	}

	if values.is_empty() {
		Ok(Value::None)
	} else if values.len() == 1 {
		Ok(values.into_iter().next().expect("values verified non-empty"))
	} else {
		Ok(Value::Array(crate::val::Array(values)))
	}
}

/// Collect all values from a stream into a `Vec`.
///
/// Propagates control flow signals directly.
pub(crate) async fn collect_stream(mut stream: ValueBatchStream) -> FlowResult<Vec<Value>> {
	let mut results = Vec::new();

	while let Some(batch_result) = stream.next().await {
		match batch_result {
			Ok(batch) => results.extend(batch.values),
			Err(ctrl) => return Err(ctrl),
		}
	}

	Ok(results)
}

// ============================================================================
// Expression Context Level Analysis
// ============================================================================

/// Determine the minimum [`ContextLevel`] required to evaluate an [`Expr`].
///
/// This is used by deferred-planning operators ([`IfElsePlan`], [`SequencePlan`],
/// [`ForeachPlan`]) that store raw `Expr` values and plan them at runtime.
/// The function recursively inspects the expression tree to determine the
/// minimum context level needed, avoiding overly conservative hardcoded
/// `ContextLevel::Database` values.
pub(crate) fn expr_required_context(expr: &Expr) -> ContextLevel {
	match expr {
		// Pure values — no context needed
		Expr::Param(_) | Expr::Constant(_) | Expr::Mock(_) | Expr::Break | Expr::Continue => {
			ContextLevel::Root
		}

		// Literals may contain inner expressions (arrays, objects, record IDs)
		Expr::Literal(lit) => literal_required_context(lit),

		// Closure body may reference database-level constructs
		Expr::Closure(closure) => expr_required_context(&closure.body),

		// Sleep just does a time delay (duration is a concrete value, not an Expr)
		Expr::Sleep(_) => ContextLevel::Root,

		// Idiom parts may contain inner expressions (WHERE filters, method args,
		// graph lookups, etc.) that need their own context.
		Expr::Idiom(idiom) => idiom_required_context(idiom),

		// Table references are used in FROM clauses which need a database
		Expr::Table(_) => ContextLevel::Database,

		// Block: max of all contained expressions
		Expr::Block(block) => block_required_context(block),

		// Unary operators: delegate to inner expression
		Expr::Prefix {
			expr,
			..
		}
		| Expr::Postfix {
			expr,
			..
		}
		| Expr::Throw(expr) => expr_required_context(expr),

		// Binary: max of left and right
		Expr::Binary {
			left,
			right,
			..
		} => expr_required_context(left).max(expr_required_context(right)),

		// Function calls may be user-defined (stored in the database), so
		// conservatively require database context.
		Expr::FunctionCall(_) => ContextLevel::Database,

		// Return: delegate to inner expression
		Expr::Return(stmt) => expr_required_context(&stmt.what),

		// IfElse: max of all conditions and branches
		Expr::IfElse(stmt) => {
			let branches_ctx = stmt
				.exprs
				.iter()
				.flat_map(|(cond, body)| [expr_required_context(cond), expr_required_context(body)])
				.max()
				.unwrap_or(ContextLevel::Root);
			let else_ctx =
				stmt.close.as_ref().map(expr_required_context).unwrap_or(ContextLevel::Root);
			branches_ctx.max(else_ctx)
		}

		// DML statements need a database
		Expr::Select(_)
		| Expr::Create(_)
		| Expr::Update(_)
		| Expr::Upsert(_)
		| Expr::Delete(_)
		| Expr::Relate(_)
		| Expr::Insert(_) => ContextLevel::Database,

		// DDL statements need a database
		Expr::Define(_) | Expr::Remove(_) | Expr::Alter(_) | Expr::Rebuild(_) => {
			ContextLevel::Database
		}

		// Info: depends on the level
		Expr::Info(info) => info_stmt_required_context(info),

		// Foreach: max of range expression and body block
		Expr::Foreach(stmt) => {
			expr_required_context(&stmt.range).max(block_required_context(&stmt.block))
		}

		// Let: delegate to value expression
		Expr::Let(stmt) => expr_required_context(&stmt.what),

		// Explain: delegate to inner statement
		Expr::Explain {
			statement,
			..
		} => expr_required_context(statement),

		// GQL MATCH queries the datastore and needs a database.
		#[cfg(feature = "gql")]
		Expr::Match(_) => ContextLevel::Database,
	}
}

/// Determine the minimum [`ContextLevel`] required for a [`Block`].
pub(crate) fn block_required_context(block: &Block) -> ContextLevel {
	block.0.iter().map(expr_required_context).max().unwrap_or(ContextLevel::Root)
}

/// Determine the minimum [`ContextLevel`] required by an [`InfoStatement`].
fn info_stmt_required_context(info: &InfoStatement) -> ContextLevel {
	match info {
		InfoStatement::Root(_, _) => ContextLevel::Root,
		InfoStatement::Ns(_, _) => ContextLevel::Namespace,
		InfoStatement::Db(_, _) | InfoStatement::Tb(_, _, _) | InfoStatement::Index(_, _, _) => {
			ContextLevel::Database
		}
		InfoStatement::User(user_expr, base, _) => {
			let base_ctx = match base {
				Some(Base::Root) | None => ContextLevel::Root,
				Some(Base::Ns) => ContextLevel::Namespace,
				Some(Base::Db) => ContextLevel::Database,
			};
			base_ctx.max(expr_required_context(user_expr))
		}
	}
}

/// Determine the minimum [`ContextLevel`] required by a [`Literal`].
///
/// Most literals are pure values, but compound literals (arrays, objects,
/// record IDs) can contain inner [`Expr`]s that may reference the database.
fn literal_required_context(lit: &Literal) -> ContextLevel {
	match lit {
		Literal::Array(exprs) | Literal::Set(exprs) => {
			exprs.iter().map(expr_required_context).max().unwrap_or(ContextLevel::Root)
		}
		Literal::Object(entries) => entries
			.iter()
			.map(|e| expr_required_context(&e.value))
			.max()
			.unwrap_or(ContextLevel::Root),
		Literal::RecordId(rid) => record_id_key_required_context(&rid.key),
		_ => ContextLevel::Root,
	}
}

/// Determine the minimum [`ContextLevel`] required by a [`RecordIdKeyLit`].
fn record_id_key_required_context(key: &crate::expr::RecordIdKeyLit) -> ContextLevel {
	use crate::expr::RecordIdKeyLit;
	match key {
		RecordIdKeyLit::Array(exprs) => {
			exprs.iter().map(expr_required_context).max().unwrap_or(ContextLevel::Root)
		}
		RecordIdKeyLit::Object(entries) => entries
			.iter()
			.map(|e| expr_required_context(&e.value))
			.max()
			.unwrap_or(ContextLevel::Root),
		RecordIdKeyLit::Range(range) => {
			let start = match &range.start {
				std::ops::Bound::Included(k) | std::ops::Bound::Excluded(k) => {
					record_id_key_required_context(k)
				}
				std::ops::Bound::Unbounded => ContextLevel::Root,
			};
			let end = match &range.end {
				std::ops::Bound::Included(k) | std::ops::Bound::Excluded(k) => {
					record_id_key_required_context(k)
				}
				std::ops::Bound::Unbounded => ContextLevel::Root,
			};
			start.max(end)
		}
		_ => ContextLevel::Root,
	}
}

/// Determine the minimum [`ContextLevel`] required by an [`Idiom`].
///
/// An idiom's parts may contain inner expressions (WHERE filters, index
/// expressions, method arguments) and graph lookups that need database access.
fn idiom_required_context(idiom: &crate::expr::Idiom) -> ContextLevel {
	idiom.0.iter().map(part_required_context).max().unwrap_or(ContextLevel::Root)
}

/// Determine the minimum [`ContextLevel`] required by a single [`Part`].
fn part_required_context(part: &Part) -> ContextLevel {
	match part {
		// These parts contain inner expressions
		Part::Where(expr) | Part::Value(expr) | Part::Start(expr) => expr_required_context(expr),
		Part::Method(_, args) => {
			args.iter().map(expr_required_context).max().unwrap_or(ContextLevel::Root)
		}
		// Graph/reference lookups need database access
		Part::Lookup(_) => ContextLevel::Database,
		// Recurse: check the instruction for inner expressions
		Part::Recurse(_, _, instruction) => match instruction {
			Some(RecurseInstruction::Shortest {
				expects,
				..
			}) => expr_required_context(expects),
			_ => ContextLevel::Root,
		},
		// Pure structural parts — no context needed
		Part::All
		| Part::Flatten
		| Part::Last
		| Part::First
		| Part::Field(_)
		| Part::Destructure(_)
		| Part::Optional
		| Part::Doc
		| Part::RepeatRecurse => ContextLevel::Root,
	}
}
