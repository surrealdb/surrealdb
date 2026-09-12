//! User-defined function expression - fn::my_function(), etc.

use std::collections::HashMap;
use std::sync::{Arc, OnceLock, Weak};

use surrealdb_strand::Strand;
use surrealdb_types::{SqlFormat, ToSql};

use super::helpers::{
	args_access_mode, args_required_context, check_permission, evaluate_args, fmt_call_args,
	validate_arg_count, validate_return,
};
use crate::catalog::FunctionDefinition;
use crate::catalog::providers::DatabaseProvider;
use crate::dbs::capabilities::Error as CapabilitiesError;
use crate::err::Error;
use crate::exec::operators::legacy_bridge::{legacy_compute, legacy_handles};
use crate::exec::permission::CompiledGuard;
use crate::exec::physical_expr::{EvalContext, PhysicalExpr, ScalarSubquery};
use crate::exec::{AccessMode, BoxFut, Error as ExecError, ExecOperator, ExecutionContext};
use crate::expr::logical::LogicalOp;
use crate::expr::{Block, ControlFlow, Error as ExprError, FlowResult};
use crate::kvs::cache::ds::FunctionSnapshot;
use crate::val::Value;

/// A function body bound and compiled with the plan that calls it, for the
/// stored definition under `definition`
/// ([`Planner::plan_function_body`](crate::exec::planner::Planner)).
#[derive(Debug, Clone)]
pub(crate) struct PlannedBody {
	/// The content hash of the definition the body was compiled from
	/// ([`FunctionSnapshot::hash_of`]). A call reads the definition afresh
	/// and evaluates this plan only while the hash still matches.
	pub(crate) definition: u64,
	/// The definition the body was compiled from. A call that reads this very
	/// allocation back — the transaction cache hands out one `Arc` per
	/// definition — is known to match without hashing it.
	pub(crate) compiled_from: Arc<FunctionDefinition>,
	/// The body, as [`UserDefinedFunctionExec::run_body`] would plan it for a
	/// call in the caller's frame.
	pub(crate) plan: BodyPlan,
}

/// Where a call finds the plan of its body.
#[derive(Debug, Clone)]
pub(crate) enum BodyPlan {
	/// A plan compiled beneath the call.
	Compiled(Arc<dyn ExecOperator>),
	/// The plan of the body this call is inside — a recursive edge. Every
	/// level of the recursion evaluates the one plan compiled for the body,
	/// bounded by the computation-depth budget the call consumes
	/// ([`ExecutionContext::dive`]).
	Recursive(Arc<RecursiveBody>),
}

impl BodyPlan {
	/// The plan to evaluate; `None` for a recursive edge whose body did not
	/// finish compiling, which the call then binds and plans for itself.
	fn resolve(&self) -> Option<Arc<dyn ExecOperator>> {
		match self {
			Self::Compiled(plan) => Some(Arc::clone(plan)),
			Self::Recursive(body) => body.plan(),
		}
	}
}

/// The plan of a function body, published once the body has compiled, so the
/// recursive edges inside the body evaluate the plan they are part of.
///
/// Held weakly: the compiled plan owns the edge that refers back to it, and a
/// strong reference here would keep both alive forever. The plan outlives
/// every evaluation of its edges because an edge is only ever reached by
/// evaluating the plan it sits in.
#[derive(Debug, Default)]
pub(crate) struct RecursiveBody(OnceLock<Weak<dyn ExecOperator>>);

impl RecursiveBody {
	pub(crate) fn new() -> Arc<Self> {
		Arc::new(Self::default())
	}

	/// Publish the compiled plan. A second call is ignored: a body compiles
	/// once.
	pub(crate) fn resolve(&self, plan: &Arc<dyn ExecOperator>) {
		let _ = self.0.set(Arc::downgrade(plan));
	}

	fn plan(&self) -> Option<Arc<dyn ExecOperator>> {
		self.0.get()?.upgrade()
	}
}

/// User-defined function expression - fn::my_function(), etc.
///
/// These functions are stored in the database and retrieved at runtime.
#[derive(Debug, Clone)]
pub struct UserDefinedFunctionExec {
	/// Function name without the "fn::" prefix
	pub(crate) name: String,
	pub(crate) arguments: Vec<Arc<dyn PhysicalExpr>>,
	/// Expression-nesting depth recorded when this call was planned. A body
	/// compiled on call is seeded with it so nested `eval`/UDF recursion
	/// keeps counting toward `max_computation_depth` instead of resetting.
	pub(crate) plan_depth: u32,
	/// Access mode of the callee's body, resolved at plan time against the call
	/// graph in the plan-time snapshot ([`crate::fnc::mutability`], conservative
	/// polarity: opaque bodies and undefined callees count as `ReadWrite`).
	/// `ReadWrite` when the planner had no transaction to resolve against — the
	/// safe over-approximation, matching `resolve_module_writeable`. Combined
	/// with the arguments' access modes in [`Self::access_mode`].
	pub(crate) body_access_mode: AccessMode,
	/// The body compiled with the plan, when the planner could compile it;
	/// see [`Self::planned_body`] for when a call evaluates it. `None` leaves
	/// every call to bind and compile the body itself.
	pub(crate) body: Option<PlannedBody>,
	/// The `PERMISSIONS` clause of the definition this call last found,
	/// compiled. A call in a projection is made once per row, and compiling
	/// its guard is plan-time work; see [`CompiledGuard`] for what is kept
	/// and what is not.
	pub(crate) permission: CompiledGuard<FunctionDefinition>,
}
impl PhysicalExpr for UserDefinedFunctionExec {
	fn name(&self) -> &'static str {
		"UserDefinedFunction"
	}

	fn as_any(&self) -> &dyn std::any::Any {
		self
	}

	fn required_context(&self) -> crate::exec::ContextLevel {
		// User-defined functions are stored in the database, and arguments
		// may have their own context requirements
		args_required_context(&self.arguments).max(crate::exec::ContextLevel::Database)
	}

	fn evaluate<'a>(&'a self, ctx: EvalContext<'a>) -> BoxFut<'a, FlowResult<Value>> {
		Box::pin(async move {
			let func_name = format!("fn::{}", self.name);

			// 1. Require database context (propagate core Error so it downcasts to Validation)
			let db_ctx =
				ctx.exec_ctx.database().map_err(|e| ControlFlow::Err(anyhow::Error::new(e)))?;

			// 2. Check if function is allowed by capabilities
			if !ctx.capabilities().allows_function_name(&func_name) {
				return Err(
					anyhow::Error::new(CapabilitiesError::FunctionNotAllowed(func_name)).into()
				);
			}

			// 3. Retrieve function definition
			let ns_id = db_ctx.ns_ctx.ns.namespace_id;
			let db_id = db_ctx.db.database_id;
			let func_def = ctx
				.txn()
				.get_db_function(ns_id, db_id, &self.name, ctx.exec_ctx.version_stamp())
				.await
				.map_err(|e| anyhow::anyhow!("Function '{}' not found: {}", func_name, e))?;

			// 4. Apply auth limiting — cap the caller's privileges to the definer's auth level.
			let auth_limit =
				crate::iam::AuthLimit::try_from(&func_def.auth_limit).map_err(|e| {
					anyhow::anyhow!("Invalid auth limit on function '{}': {}", func_name, e)
				})?;
			let limited_ctx = ctx.exec_ctx.with_limited_auth(&auth_limit);
			let ctx = EvalContext {
				exec_ctx: &limited_ctx,
				current_value: ctx.current_value,
				local_params: ctx.local_params,
				recursion_ctx: ctx.recursion_ctx,
				document_root: ctx.document_root,
				skip_fetch_perms: ctx.skip_fetch_perms,
				computing_record: ctx.computing_record,
				plan_depth: ctx.plan_depth,
			};

			// 5. Check permissions (with limited auth). Inside a permission
			// predicate the call is the definer's and is not checked.
			if !ctx.skip_fetch_perms && ctx.exec_ctx.should_check_perms(crate::iam::Action::View)? {
				check_permission(
					&self.permission,
					func_def.permissions_body.logical_ref()?,
					&func_def,
					&func_name,
					&ctx,
				)
				.await?;
			}

			// 6. Evaluate all arguments
			let evaluated_args = evaluate_args(&self.arguments, ctx.clone()).await?;

			// 7. Validate argument count
			validate_arg_count(&func_name, evaluated_args.len(), &func_def.args)?;

			// 8. Create isolated context with function parameters bound
			let mut local_params: HashMap<Strand, Value> = HashMap::new();
			for ((param_name, kind), arg_value) in func_def.args.iter().zip(evaluated_args) {
				let coerced = arg_value.coerce_to_kind(kind).map_err(|e| {
					ExprError::InvalidFunctionArguments {
						name: func_name.clone(),
						message: format!("Failed to coerce argument `${param_name}`: {e}"),
					}
				})?;
				local_params.insert(param_name.as_str().into(), coerced);
			}

			// 9. Create a new execution context with the parameters, one re-entry deeper: the
			//    body's plan is shared by every level of a recursion, so the budget that bounds the
			//    recursion is the context's, consumed per call, not the plan's static depth. The
			//    call's evaluation frame moves onto it: a call reached from a `PERMISSIONS`
			//    predicate is marked on the `EvalContext` (a record dereference sets it there and
			//    nowhere else), and the body — bound, compiled and evaluated from this context
			//    alone — has to run in that frame.
			let mut isolated_ctx = limited_ctx.dive().map_err(ControlFlow::Err)?;
			for (name, value) in &local_params {
				isolated_ctx = isolated_ctx.with_param(name.clone(), value.clone());
			}
			if ctx.skip_fetch_perms {
				isolated_ctx = isolated_ctx.with_skip_fetch_perms(true);
			}

			// 10. Run the body: the plan compiled with the statement where it stands for this call,
			//     otherwise a plan compiled here, in its own scope. The definition is stored apart
			//     from any caller, so a body inherits no MATCHES registrations from the statement
			//     that called it, and the arguments are bound as `$name` in the call's context, so
			//     binding sees them the way it sees a `LET`. The body's logical form and its legacy
			//     block compile from the same stored text, so the one way the logical form can be
			//     missing is that the text has none — a body that writes — and that body runs on
			//     the legacy engine.
			let result = match self.planned_body(&func_def, &ctx) {
				Some(plan) => {
					evaluate_body(plan, &isolated_ctx, &ctx, &local_params, self.plan_depth + 1)
						.await
				}
				None => match func_def.logical_body() {
					Ok(body) => {
						self.run_body(
							body.clone(),
							&func_def.block,
							&isolated_ctx,
							&ctx,
							&local_params,
						)
						.await
					}
					Err(reason) => {
						tracing::debug!("fn::{} body has no logical form: {reason:#}", self.name);
						run_legacy_body(&func_def.block, &isolated_ctx, self.plan_depth + 1).await
					}
				},
			};
			let result = match result {
				Ok(v) => v,
				Err(ControlFlow::Return(v)) => v,
				Err(ControlFlow::Break) | Err(ControlFlow::Continue) => {
					// BREAK/CONTINUE inside a function (outside of loop) is an error
					return Err(ExecError::InvalidControlFlow.into());
				}
				Err(e) => return Err(e),
			};

			// 11. Validate and coerce return type
			Ok(validate_return(&func_name, func_def.returns.as_ref(), result)?)
		})
	}

	fn access_mode(&self) -> AccessMode {
		// The body's access mode was resolved at plan time through the call
		// graph; combine it with the arguments, which are evaluated at the call
		// site. A body that provably only reads lets the caller keep its
		// read-only fast paths (fan-out overlap, buffered read streams).
		self.body_access_mode.combine(args_access_mode(&self.arguments))
	}
}

impl UserDefinedFunctionExec {
	/// The plan compiled with the statement, when it stands for this call.
	///
	/// It was bound for the statement's principal in the caller's frame, from
	/// the definition it hashes to. A call in a permission frame binds without
	/// a principal, a `VERSION`ed call reads the definition at the stamp, and
	/// a definition redefined since hashes differently: each of those binds
	/// and compiles the body for itself ([`Self::run_body`]).
	fn planned_body(
		&self,
		definition: &Arc<FunctionDefinition>,
		call: &EvalContext<'_>,
	) -> Option<Arc<dyn ExecOperator>> {
		let body = self.body.as_ref()?;
		if call.skip_fetch_perms || call.exec_ctx.version_stamp().is_some() {
			return None;
		}
		// The transaction cache hands the same `Arc` back for as long as the
		// definition stands, so a pointer match settles it; the hash is for a
		// definition read through another cache or re-read after eviction.
		if !Arc::ptr_eq(definition, &body.compiled_from)
			&& FunctionSnapshot::hash_of(definition) != body.definition
		{
			return None;
		}
		body.plan.resolve()
	}

	/// Compile the body's logical plan for this call and evaluate it.
	///
	/// The body is one re-entry deeper than the call, so the planner continues
	/// the depth count at `plan_depth + 1`. The compiled plan is evaluated as a
	/// scalar subquery of the call: `$this` and `$parent` denote the caller's
	/// row, and a `RETURN` in the body surfaces as `ControlFlow::Return` for
	/// the caller to unwrap. A plan the planner declines falls back to the
	/// legacy engine on the stored block.
	///
	/// A body called from a `PERMISSIONS` predicate is bound without a
	/// principal. Inside a predicate no read is permission-checked — the
	/// frame on `isolated_ctx` disables every runtime gate, as the legacy
	/// engine's `perms = false` does — so binding for the caller would apply
	/// table and field guards the frame is meant to waive and judge the
	/// predicate on a reduced view. Without a principal the analyzer inserts
	/// no guard and leaves every source under runtime enforcement, which the
	/// frame then waives.
	async fn run_body(
		&self,
		body: LogicalOp,
		block: &Block,
		isolated_ctx: &ExecutionContext,
		call: &EvalContext<'_>,
		local_params: &HashMap<Strand, Value>,
	) -> FlowResult<Value> {
		let depth = self.plan_depth + 1;
		let principal = if isolated_ctx.root().skip_fetch_perms {
			None
		} else {
			Some(Arc::clone(&isolated_ctx.root().auth))
		};
		let planned = crate::exec::planner::plan_statement_inner(
			body,
			isolated_ctx.ctx(),
			isolated_ctx.function_registry(),
			call.txn(),
			principal,
			depth,
		)
		.await;
		match planned {
			Ok(plan) => evaluate_body(plan, isolated_ctx, call, local_params, depth).await,
			Err(Error::Exec(ExecError::PlannerUnsupported(_))) => {
				run_legacy_body(block, isolated_ctx, depth).await
			}
			Err(e) => Err(ControlFlow::Err(anyhow::Error::new(e))),
		}
	}
}

/// Evaluate a compiled body as a scalar subquery of the call: `$this` and
/// `$parent` denote the caller's row, the arguments are the call's local
/// parameters, and a `RETURN` in the body surfaces as `ControlFlow::Return`
/// for the caller to unwrap. `depth` is the body's re-entry depth, one deeper
/// than the call.
async fn evaluate_body(
	plan: Arc<dyn ExecOperator>,
	isolated_ctx: &ExecutionContext,
	call: &EvalContext<'_>,
	local_params: &HashMap<Strand, Value>,
	depth: u32,
) -> FlowResult<Value> {
	let body = ScalarSubquery {
		plan,
		inherits_row: true,
	};
	let eval_ctx = EvalContext {
		exec_ctx: isolated_ctx,
		current_value: call.current_value,
		local_params: Some(local_params),
		recursion_ctx: None,
		document_root: None,
		skip_fetch_perms: call.skip_fetch_perms,
		computing_record: call.computing_record.clone(),
		plan_depth: depth,
	};
	body.evaluate(eval_ctx).await
}

/// Evaluate a function body on the legacy engine.
///
/// The stored block is the legacy engine's input. The handles come from the
/// engine's one legacy bridge, which derives the options from the frame
/// `isolated_ctx` is in — a permission predicate refuses writes and disables
/// permission checks, a computed field refuses writes, a `VERSION` stamp is
/// read at — with `depth` already consumed from the dive budget so the count
/// continues across the engine boundary.
async fn run_legacy_body(
	block: &Block,
	isolated_ctx: &ExecutionContext,
	depth: u32,
) -> FlowResult<Value> {
	let (options, frozen) =
		legacy_handles(isolated_ctx).map_err(|e| ControlFlow::Err(anyhow::Error::new(e)))?;
	let options = options.with_dive_consumed(depth);
	let body = crate::expr::Expr::Block(Box::new(block.clone()));
	legacy_compute(&body, &frozen, &options, None).await
}

impl ToSql for UserDefinedFunctionExec {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		f.push_str("fn::");
		f.push_str(&self.name);
		fmt_call_args(f, fmt, &self.arguments);
	}
}

#[cfg(all(test, feature = "kv-mem"))]
#[allow(clippy::unwrap_used)]
mod tests {
	use std::sync::Arc;

	use super::{BodyPlan, UserDefinedFunctionExec};
	use crate::exec::operators::test_util::{TestDb, val};
	use crate::exec::planner::Planner;
	use crate::exec::{AccessMode, EvalContext, ExecutionContext, PhysicalExpr};
	use crate::expr::logical::lower_expr;
	use crate::val::Value;

	/// Plan `expression` against a txn-backed planner over `ctx`, as the
	/// statement's principal.
	async fn physical_of(ctx: &ExecutionContext, expression: &str) -> Arc<dyn PhysicalExpr> {
		let crate::exec::ExecutionContext::Database(db_ctx) = ctx else {
			panic!("exec_ctx builds a Database context");
		};
		let txn = ctx.txn();
		let planner = Planner::with_txn(
			ctx.ctx(),
			&db_ctx.ns_ctx.root.function_registry,
			txn,
			Some("test".to_owned()),
			Some("test".to_owned()),
		)
		.with_auth(Arc::clone(&ctx.root().auth));
		let expr = lower_expr(crate::syn::expr(expression).unwrap()).unwrap();
		planner.physical_expr(expr).await.unwrap()
	}

	/// Plan `expression` against a txn-backed planner over `db` and return the
	/// resolved access mode of the resulting physical expression tree.
	async fn access_mode_of(db: &TestDb, expression: &str) -> AccessMode {
		let ctx = db.exec_ctx().await;
		physical_of(&ctx, expression).await.access_mode()
	}

	/// Whether the call `expression` compiles to carries its body's plan.
	async fn embeds_body(db: &TestDb, expression: &str) -> bool {
		let ctx = db.exec_ctx().await;
		let expr = physical_of(&ctx, expression).await;
		let call = expr
			.as_any()
			.downcast_ref::<UserDefinedFunctionExec>()
			.expect("a user-defined function call");
		call.body.is_some()
	}

	async fn evaluate(expr: &Arc<dyn PhysicalExpr>, ctx: &ExecutionContext) -> Value {
		expr.evaluate(EvalContext::from_exec_ctx(ctx)).await.expect("the call evaluates")
	}

	#[tokio::test]
	async fn a_body_is_planned_with_the_statement_and_evaluated_by_every_call() {
		let db = TestDb::new(
			"DEFINE FUNCTION fn::double($x: int) { RETURN $x * 2; };
			 DEFINE FUNCTION fn::twice($x: int) { LET $d = fn::double($x); RETURN $d + fn::double(1); };",
		)
		.await;
		assert!(embeds_body(&db, "fn::double(3)").await);
		assert!(embeds_body(&db, "fn::twice(3)").await);
		let ctx = db.exec_ctx().await;
		let call = physical_of(&ctx, "fn::twice(3)").await;
		assert_eq!(evaluate(&call, &ctx).await, Value::from(8));
		assert_eq!(evaluate(&call, &ctx).await, Value::from(8));
	}

	#[tokio::test]
	async fn a_recursive_edge_evaluates_the_plan_it_is_inside() {
		// The body of `down` is compiled with the call site, and the call to
		// `down` inside it is on the path being compiled, so that one is a
		// recursive edge: it evaluates the very plan it sits in, bounded by
		// the depth budget each call consumes; the recursion still terminates
		// on its own condition.
		let db = TestDb::new(
			"DEFINE FUNCTION fn::down($n: int) { IF $n <= 0 { RETURN 0 }; RETURN fn::down($n - 1) + 1; };
			 DEFINE FUNCTION fn::tail($n: int) { IF $n <= 0 { RETURN 0 }; RETURN fn::tail($n - 1); };
			 DEFINE FUNCTION fn::ping($n: int) { IF $n <= 0 { RETURN 'ping' }; RETURN fn::pong($n - 1); };
			 DEFINE FUNCTION fn::pong($n: int) { IF $n <= 0 { RETURN 'pong' }; RETURN fn::ping($n - 1); };",
		)
		.await;
		assert!(embeds_body(&db, "fn::down(3)").await);
		let ctx = db.exec_ctx().await;
		// `tail`'s recursive call is the whole returned expression, where
		// `calls_in` can see it; `down`'s sits inside a `+`.
		let call = physical_of(&ctx, "fn::tail(3)").await;
		let outer = call
			.as_any()
			.downcast_ref::<UserDefinedFunctionExec>()
			.expect("a user-defined function call");
		let outer_plan = match &outer.body.as_ref().expect("the outer call carries its body").plan {
			BodyPlan::Compiled(plan) => Arc::clone(plan),
			BodyPlan::Recursive(_) => panic!("the outer call is not a recursive edge"),
		};
		let mut inner = Vec::new();
		calls_in(&outer_plan, &mut inner);
		let inner: Vec<_> = inner.into_iter().filter(|c| c.name == "tail").collect();
		assert_eq!(inner.len(), 1, "one recursive call inside the body");
		let edge = inner[0].body.as_ref().expect("the recursive edge carries the body's plan");
		let BodyPlan::Recursive(shared) = &edge.plan else {
			panic!("the recursive edge is a recursive edge");
		};
		let shared = shared.plan().expect("the body's plan is published once compiled");
		assert!(Arc::ptr_eq(&shared, &outer_plan), "the edge evaluates the plan it is inside");
		assert_eq!(evaluate(&call, &ctx).await, Value::from(0));
		assert_eq!(evaluate(&physical_of(&ctx, "fn::down(3)").await, &ctx).await, Value::from(3));
		assert_eq!(
			evaluate(&physical_of(&ctx, "fn::ping(3)").await, &ctx).await,
			Value::from("pong")
		);
	}

	/// Every user-defined call `op` holds: in its operators' expressions, in
	/// the plans embedded in those expressions, and in its children.
	fn calls_in(op: &Arc<dyn crate::exec::ExecOperator>, out: &mut Vec<UserDefinedFunctionExec>) {
		for (_, expr) in op.expressions() {
			if let Some(call) = expr.as_any().downcast_ref::<UserDefinedFunctionExec>() {
				out.push(call.clone());
			}
			for (_, plan) in expr.embedded_operators() {
				calls_in(plan, out);
			}
		}
		for child in op.children() {
			calls_in(child, out);
		}
	}

	#[tokio::test]
	async fn a_recursive_call_inside_a_nested_select_is_the_recursive_edge() {
		// The body of `nest` is compiled with the call site; the nested
		// `SELECT` inside it is compiled by a planner derived from the same
		// scope, so the call to `nest` in its projection is on the path being
		// compiled and is left to the runtime rather than planned again a
		// level deeper.
		let db = TestDb::new(
			"DEFINE FUNCTION fn::nest($n: int) {
				IF $n <= 0 { RETURN 0 };
				LET $r = (SELECT VALUE fn::nest($n - 1) FROM [1]);
				RETURN $r[0] + 1;
			};",
		)
		.await;
		let ctx = db.exec_ctx().await;
		let call = physical_of(&ctx, "fn::nest(2)").await;
		let call = call
			.as_any()
			.downcast_ref::<UserDefinedFunctionExec>()
			.expect("a user-defined function call");
		let body = call.body.as_ref().expect("the outer call carries its body's plan");
		let plan = body.plan.resolve().expect("the outer call's body is compiled");
		let mut inner = Vec::new();
		calls_in(&plan, &mut inner);
		let inner: Vec<_> = inner.into_iter().filter(|c| c.name == "nest").collect();
		assert_eq!(inner.len(), 1, "one recursive call inside the nested SELECT");
		let edge = inner[0].body.as_ref().expect("the recursive edge carries the body's plan");
		assert!(
			matches!(&edge.plan, BodyPlan::Recursive(shared) if shared.plan().is_some_and(|p| Arc::ptr_eq(&p, &plan))),
			"the recursive edge evaluates the plan it is inside"
		);
		assert_eq!(evaluate(&physical_of(&ctx, "fn::nest(2)").await, &ctx).await, Value::from(2));
	}

	#[tokio::test]
	async fn an_argument_read_in_a_predicate_or_a_source_leaves_the_body_to_the_call() {
		// The call binds the argument as a value, which the access path
		// answers; a plan compiled with the statement would carry a name
		// there. An argument read anywhere else keeps the body's plan.
		let db = TestDb::new(
			"DEFINE TABLE t SCHEMAFULL;
			 DEFINE FIELD name ON t TYPE string;
			 DEFINE INDEX t_name ON t FIELDS name;
			 CREATE t:1 SET name = 'a';
			 DEFINE FUNCTION fn::by_name($n: string) { RETURN SELECT VALUE id FROM t WHERE name = $n; };
			 DEFINE FUNCTION fn::from($r: record) { RETURN SELECT VALUE name FROM $r; };
			 DEFINE FUNCTION fn::names($n: int) { RETURN (SELECT VALUE name FROM t LIMIT $n); };",
		)
		.await;
		assert!(!embeds_body(&db, "fn::by_name('a')").await);
		assert!(!embeds_body(&db, "fn::from(t:1)").await);
		assert!(embeds_body(&db, "fn::names(1)").await);
		let ctx = db.exec_ctx().await;
		assert_eq!(
			evaluate(&physical_of(&ctx, "fn::by_name('a')").await, &ctx).await,
			val("[t:1]").await
		);
		assert_eq!(
			evaluate(&physical_of(&ctx, "fn::names(1)").await, &ctx).await,
			Value::from(vec![Value::from("a")])
		);
	}

	#[tokio::test]
	async fn a_redefined_function_is_bound_afresh_by_the_call() {
		// The plan carries the definition's content hash; a call that reads a
		// definition hashing differently binds and plans the body it read.
		let db = TestDb::new("DEFINE FUNCTION fn::answer() { RETURN 1; };").await;
		let planned_ctx = db.exec_ctx().await;
		let call = physical_of(&planned_ctx, "fn::answer()").await;
		assert_eq!(evaluate(&call, &planned_ctx).await, Value::from(1));
		db.run("DEFINE FUNCTION OVERWRITE fn::answer() { RETURN 2; };").await;
		let ctx = db.exec_ctx().await;
		assert_eq!(evaluate(&call, &ctx).await, Value::from(2));
	}

	#[tokio::test]
	async fn a_name_the_statement_binds_is_read_by_the_call() {
		// `$x` is bound nowhere the planner can see, so the body keeps it a
		// runtime lookup rather than resolving it to `NONE`, and the call
		// reads what the statement bound.
		let db = TestDb::new("DEFINE FUNCTION fn::outer() { RETURN $x; };").await;
		assert!(embeds_body(&db, "fn::outer()").await);
		let ctx = db.exec_ctx().await;
		let call = physical_of(&ctx, "fn::outer()").await;
		let bound = ctx.with_param("x", Value::from(7));
		assert_eq!(evaluate(&call, &bound).await, Value::from(7));
		assert_eq!(evaluate(&call, &ctx).await, Value::None);
	}

	#[tokio::test]
	async fn a_read_only_udf_resolves_read_only() {
		let db = TestDb::new(
			"DEFINE FUNCTION fn::pure() { RETURN 1; };
			 DEFINE FUNCTION fn::relay() { RETURN fn::pure(); };",
		)
		.await;
		assert_eq!(access_mode_of(&db, "fn::pure()").await, AccessMode::ReadOnly);
		// Read-only through a call hop stays read-only.
		assert_eq!(access_mode_of(&db, "fn::relay()").await, AccessMode::ReadOnly);
	}

	#[tokio::test]
	async fn a_writing_udf_resolves_read_write() {
		let db = TestDb::new(
			"DEFINE TABLE log SCHEMALESS;
			 DEFINE FUNCTION fn::sink() { CREATE log; RETURN 1; };",
		)
		.await;
		assert_eq!(access_mode_of(&db, "fn::sink()").await, AccessMode::ReadWrite);
	}

	#[tokio::test]
	async fn an_undefined_callee_resolves_read_write() {
		// Late binding: the callee may be defined to write later, so the
		// conservative resolution must not license a read-only fast path now.
		let db = TestDb::new("").await;
		assert_eq!(access_mode_of(&db, "fn::ghost()").await, AccessMode::ReadWrite);
	}

	#[tokio::test]
	async fn a_body_invoking_a_closure_field_resolves_read_write() {
		// `$o.w()` is not a registered method, so it invokes a closure stored
		// on the receiver — a body the facts walk cannot see. The body must
		// count as opaque and force a conservative read-write resolution.
		let db =
			TestDb::new("DEFINE FUNCTION fn::call_field($o: object) { RETURN $o.w(); };").await;
		assert_eq!(access_mode_of(&db, "fn::call_field({ w: || 1 })").await, AccessMode::ReadWrite);
	}

	#[tokio::test]
	async fn a_read_only_body_with_a_writing_argument_is_read_write() {
		// The body only reads, but a writing function passed as the argument is
		// evaluated at the call site, so the call as a whole is read-write —
		// `access_mode` combines the resolved body mode with the arguments'.
		let db = TestDb::new(
			"DEFINE TABLE log SCHEMALESS;
			 DEFINE FUNCTION fn::id($x: any) { RETURN $x; };
			 DEFINE FUNCTION fn::sink() { CREATE log; RETURN 1; };",
		)
		.await;
		assert_eq!(access_mode_of(&db, "fn::id(fn::sink())").await, AccessMode::ReadWrite);
	}
}
