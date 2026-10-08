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
use crate::exec::permission::CompiledGuard;
use crate::exec::physical_expr::{EvalContext, PhysicalExpr, PlannedGuard, ScalarSubquery};
use crate::exec::{AccessMode, BoxFut, Error as ExecError, ExecOperator, ExecutionContext};
use crate::expr::logical::LogicalOp;
use crate::expr::{ControlFlow, Error as ExprError, FlowResult};
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
	/// The body, as [`FunctionCall::run_body`] would plan it for a call in
	/// the caller's frame.
	pub(crate) plan: BodyPlan,
	/// Whether the body was planned for a principal whose reads are
	/// permission-checked, so its binding applies table and field guards. A
	/// body planned for one whose reads are not checked binds no guard, and
	/// stands for a call in a permission frame as well.
	pub(crate) checked: bool,
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
	pub(crate) fn resolve(&self) -> Option<Arc<dyn ExecOperator>> {
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
	/// What the call does apart from evaluating its arguments.
	pub(crate) call: FunctionCall,
	pub(crate) arguments: Vec<Arc<dyn PhysicalExpr>>,
}

/// A call of a user-defined function, apart from its arguments: what happens
/// before they are evaluated ([`Self::enter`]) and after ([`Self::call`]).
/// The arguments are evaluated between the two, in the frame the entry opens.
#[derive(Debug, Clone)]
pub(crate) struct FunctionCall {
	/// Function name without the "fn::" prefix
	pub(crate) name: String,
	/// Expression-nesting depth recorded when this call was planned. A body
	/// compiled on call is seeded with it so nested `eval`/UDF recursion
	/// keeps counting toward `max_computation_depth` instead of resetting.
	pub(crate) plan_depth: u32,
	/// Access mode of the callee's body, resolved at plan time against the call
	/// graph in the plan-time snapshot ([`crate::fnc::mutability`], conservative
	/// polarity: opaque bodies and undefined callees count as `ReadWrite`).
	/// `ReadWrite` when the planner had no transaction to resolve against — the
	/// safe over-approximation, matching `resolve_module_writeable`. Combined
	/// with the arguments' access modes in [`UserDefinedFunctionExec::access_mode`].
	pub(crate) body_access_mode: AccessMode,
	/// The body compiled with the plan, when the planner could compile it;
	/// see [`Self::planned_body`] for when a call evaluates it. `None` leaves
	/// every call to bind and compile the body itself.
	pub(crate) body: Option<PlannedBody>,
	/// The body compiled with the plan without a principal, for a call the
	/// plan evaluates where reads are not permission-checked — a permission
	/// predicate — when [`Self::body`] binds guards that frame waives.
	pub(crate) unchecked_body: Option<PlannedBody>,
	/// The `PERMISSIONS` clause of the definition this call last found,
	/// compiled. A call in a projection is made once per row, and compiling
	/// its guard is plan-time work; see [`CompiledGuard`] for what is kept
	/// and what is not.
	pub(crate) permission: CompiledGuard<FunctionDefinition>,
	/// The `PERMISSIONS` clause compiled with the plan, for the definition
	/// the planner read; see [`PlannedGuard`].
	pub(crate) planned_permission: PlannedGuard<FunctionDefinition>,
}

/// The function a call has entered: its definition, and the caller's context
/// capped by the definition's `AUTH LIMIT`, which the arguments are evaluated
/// in and the body runs under.
pub(crate) struct EnteredCall {
	pub(crate) definition: Arc<FunctionDefinition>,
	pub(crate) limited: ExecutionContext,
}

/// `ctx`, narrowed to `limited`: the same evaluation under the definer's auth.
pub(crate) fn in_limited<'a>(
	ctx: &EvalContext<'a>,
	limited: &'a ExecutionContext,
) -> EvalContext<'a> {
	EvalContext {
		exec_ctx: limited,
		current_value: ctx.current_value,
		local_params: ctx.local_params,
		recursion_ctx: ctx.recursion_ctx.clone(),
		document_root: ctx.document_root,
		// The same evaluation, narrowed to the definer's auth: what the
		// caller could see it can still see.
		write_before: ctx.write_before,
		write_after: ctx.write_after,
		write_value: ctx.write_value,
		write_input: ctx.write_input,
		write_event: ctx.write_event,
		write_reference: ctx.write_reference,
		skip_fetch_perms: ctx.skip_fetch_perms,
		computing_record: ctx.computing_record.clone(),
		plan_depth: ctx.plan_depth,
	}
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
			let entered = self.call.enter(&ctx).await?;
			let args = evaluate_args(&self.arguments, in_limited(&ctx, &entered.limited)).await?;
			self.call.call(&entered, &ctx, args).await
		})
	}

	fn access_mode(&self) -> AccessMode {
		// The body's access mode was resolved at plan time through the call
		// graph; combine it with the arguments, which are evaluated at the call
		// site. A body that provably only reads lets the caller keep its
		// read-only fast paths (fan-out overlap, buffered read streams).
		self.call.body_access_mode.combine(args_access_mode(&self.arguments))
	}
}

impl FunctionCall {
	/// Enter the call, as far as its arguments: the database it is defined
	/// in, the capability to call it, its definition, the definer's `AUTH
	/// LIMIT` and its `PERMISSIONS`, checked under that limit. Inside a
	/// permission predicate the call is the definer's and is not checked.
	pub(crate) async fn enter(&self, ctx: &EvalContext<'_>) -> FlowResult<EnteredCall> {
		let func_name = format!("fn::{}", self.name);

		// Require database context (propagate core Error so it downcasts to Validation)
		let db_ctx =
			ctx.exec_ctx.database().map_err(|e| ControlFlow::Err(anyhow::Error::new(e)))?;

		// Check if function is allowed by capabilities
		if !ctx.capabilities().allows_function_name(&func_name) {
			return Err(anyhow::Error::new(CapabilitiesError::FunctionNotAllowed(func_name)).into());
		}

		// Retrieve function definition. A failed read is reported as
		// `Function '<name>' not found: <reason>`, the wording clients match on,
		// and in code a write reaches as the reason alone.
		let writing = ctx.exec_ctx.root().writing;
		let ns_id = db_ctx.ns_ctx.ns.namespace_id;
		let db_id = db_ctx.db.database_id;
		let txn = ctx.txn();
		let definition = crate::exec::vm::descriptor::object(txn.get_db_function(
			ns_id,
			db_id,
			&self.name,
			ctx.exec_ctx.version_stamp(),
		))
		.await
		.map_err(|e| {
			if writing {
				e
			} else {
				anyhow::anyhow!("Function '{}' not found: {}", func_name, e)
			}
		})?;

		// Apply auth limiting — cap the caller's privileges to the definer's auth level.
		let auth_limit = crate::iam::AuthLimit::try_from(&definition.auth_limit).map_err(|e| {
			anyhow::anyhow!("Invalid auth limit on function '{}': {}", func_name, e)
		})?;
		let limited = ctx.exec_ctx.with_limited_auth(&auth_limit);
		let narrowed = in_limited(ctx, &limited);

		// Check permissions (with limited auth).
		if !narrowed.skip_fetch_perms
			&& narrowed.exec_ctx.should_check_perms(crate::iam::Action::View)?
		{
			check_permission(
				crate::exec::guard_verdicts::GuardKind::Function,
				(&self.permission, &self.planned_permission),
				definition.permissions_body.logical_ref()?,
				Some(&definition.auth_limit),
				&definition,
				&func_name,
				&narrowed,
			)
			.await?
		}
		Ok(EnteredCall {
			definition,
			limited,
		})
	}

	/// Complete the call entered as `entered`, from `ctx`, the caller's
	/// evaluation, with its arguments evaluated: bind them as the definition
	/// declares, run the body one re-entry deeper under the definer's auth,
	/// and coerce what it returns.
	pub(crate) async fn call(
		&self,
		entered: &EnteredCall,
		ctx: &EvalContext<'_>,
		evaluated_args: Vec<Value>,
	) -> FlowResult<Value> {
		let func_def = &entered.definition;
		let limited_ctx = &entered.limited;
		let ctx = in_limited(ctx, limited_ctx);

		let local_params: HashMap<Strand, Value> = func_def
			.args
			.iter()
			.map(|(name, _)| Strand::from(name.as_str()))
			.zip(self.bind_args(entered, evaluated_args)?)
			.collect();

		// Create a new execution context with the parameters, one re-entry
		// deeper: the body's plan is shared by every level of a recursion, so
		// the budget that bounds the recursion is the context's, consumed per
		// call, not the plan's static depth. The body reads the caller's
		// bindings unless the call runs in a privileged frame, whose context
		// holds none of them. The call's evaluation frame moves onto it: a
		// call reached from a `PERMISSIONS` predicate is marked on the
		// `EvalContext` (a record dereference sets it there and nowhere
		// else), and the body — bound, compiled and evaluated from this
		// context alone — has to run in that frame.
		let mut isolated_ctx = limited_ctx.dive().map_err(ControlFlow::Err)?;
		let mut scope = crate::ctx::Context::new_child(isolated_ctx.ctx());
		for (name, value) in &local_params {
			scope.add_value(name.clone(), std::sync::Arc::new(value.clone()));
		}
		isolated_ctx = isolated_ctx.with_new_ctx(scope.freeze());
		if ctx.skip_fetch_perms {
			isolated_ctx = isolated_ctx.with_skip_fetch_perms(true);
		}

		// Run the body: the plan compiled with the statement where it stands
		// for this call, otherwise the plan the compile cache serves, in its
		// own scope. The definition is stored apart from any caller, so a body
		// inherits no MATCHES registrations from the statement that called it,
		// and the arguments are bound as `$name` in the call's context, which
		// the body reads when it runs. A body whose stored text has no logical
		// form fails the call with `PlannerUnsupported`.
		let result = match self.planned_body(func_def, &ctx) {
			Some(plan) => {
				evaluate_body(plan, &isolated_ctx, &ctx, &local_params, self.plan_depth + 1).await
			}
			None => match func_def.logical_body() {
				Ok(body) => self.run_body(body.clone(), &isolated_ctx, &ctx, &local_params).await,
				Err(reason) => {
					Err(ControlFlow::Err(anyhow::Error::new(ExecError::PlannerUnsupported {
						reason: format!("fn::{} body: {reason:#}", self.name),
					})))
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

		self.returned(entered, result)
	}

	/// The arguments `evaluated_args` bound as the entered definition
	/// declares them: counted, then each coerced to its parameter's kind.
	pub(crate) fn bind_args(
		&self,
		entered: &EnteredCall,
		evaluated_args: Vec<Value>,
	) -> FlowResult<Vec<Value>> {
		let func_name = format!("fn::{}", self.name);
		let func_def = &entered.definition;
		validate_arg_count(&func_name, evaluated_args.len(), &func_def.args)?;
		let mut bound = Vec::with_capacity(evaluated_args.len());
		for ((param_name, kind), arg_value) in func_def.args.iter().zip(evaluated_args) {
			let coerced = arg_value.coerce_to_kind(kind).map_err(|e| {
				ExprError::InvalidFunctionArguments {
					name: func_name.clone(),
					message: format!("Failed to coerce argument `${param_name}`: {e}"),
				}
			})?;
			bound.push(coerced);
		}
		Ok(bound)
	}

	/// `result`, what the body returned, validated and coerced to the entered
	/// definition's return kind.
	pub(crate) fn returned(&self, entered: &EnteredCall, result: Value) -> FlowResult<Value> {
		let func_name = format!("fn::{}", self.name);
		Ok(validate_return(&func_name, entered.definition.returns.as_ref(), result)?)
	}

	/// The plan compiled with the statement, when it stands for this call.
	///
	/// [`Self::body`] was bound for the statement's principal in the caller's
	/// frame, from the definition it hashes to. A call in a permission frame
	/// takes [`Self::unchecked_body`], bound without a principal, where the
	/// principal's binding applies guards; a `VERSION`ed call reads the
	/// definition at the stamp, and a definition redefined since hashes
	/// differently. A call no planned body stands for binds and compiles the
	/// body for itself ([`Self::run_body`]).
	fn planned_body(
		&self,
		definition: &Arc<FunctionDefinition>,
		call: &EvalContext<'_>,
	) -> Option<Arc<dyn ExecOperator>> {
		if call.exec_ctx.version_stamp().is_some() {
			return None;
		}
		let body = match self.body.as_ref()? {
			body if call.skip_fetch_perms && body.checked => self.unchecked_body.as_ref()?,
			body => body,
		};
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

	/// The body's plan for this call, served from the compile cache by the
	/// body as stored ([`crate::exec::vm::serve::statement`]), evaluated.
	///
	/// The body is one re-entry deeper than the call, so the planner continues
	/// the depth count at `plan_depth + 1`. The arguments are read when the
	/// plan runs, never folded into it, so one plan serves every call. The compiled plan is
	/// evaluated as a scalar subquery of the call: `$this` and `$parent` denote the caller's
	/// row, and a `RETURN` in the body surfaces as `ControlFlow::Return` for
	/// the caller to unwrap. A plan the planner declines fails the call with
	/// `PlannerUnsupported`.
	///
	/// A body called from a `PERMISSIONS` predicate is bound without a
	/// principal. Inside a predicate no read is permission-checked — the
	/// frame on `isolated_ctx` disables every runtime gate, as
	/// `perms = false` does — so binding for the caller would apply
	/// table and field guards the frame is meant to waive and judge the
	/// predicate on a reduced view. Without a principal the analyzer inserts
	/// no guard and leaves every source under runtime enforcement, which the
	/// frame then waives.
	async fn run_body(
		&self,
		body: LogicalOp,
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
		// Served from the compile cache by the body as stored, the arguments
		// read when it runs rather than folded into its plan, so one plan
		// serves every call of the definition in the frame
		// ([`crate::exec::vm::serve`]).
		let args: Vec<String> = local_params.keys().map(|name| name.as_str().to_owned()).collect();
		let planned =
			crate::exec::vm::serve::statement(isolated_ctx, body, principal, depth, args).await;
		match planned {
			Ok(plan) => evaluate_body(plan, isolated_ctx, call, local_params, depth).await,
			Err(e) => Err(ControlFlow::Err(e)),
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
		// Cleared: a slot belongs to the stage that bound it, and the body is
		// evaluated in a context of its own. A binding that crossed the call
		// would give the body a `$value` it must not see.
		write_before: None,
		write_after: None,
		write_value: None,
		write_input: None,
		write_event: None,
		write_reference: None,
		skip_fetch_perms: call.skip_fetch_perms,
		computing_record: call.computing_record.clone(),
		plan_depth: depth,
	};
	body.evaluate(eval_ctx).await
}

impl ToSql for UserDefinedFunctionExec {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		f.push_str("fn::");
		f.push_str(&self.call.name);
		fmt_call_args(f, fmt, &self.arguments);
	}
}

#[cfg(all(test, feature = "kv-mem"))]
#[allow(clippy::unwrap_used)]
mod tests {
	use std::sync::Arc;

	use super::UserDefinedFunctionExec;
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
			db_ctx.ns_ctx.root.ctx.function_registry(),
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
		call.call.body.is_some()
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
	async fn a_recursive_function_ends_on_its_own_condition() {
		// A call inside the body of `down` names `down` itself: each call
		// consumes the depth budget, and the recursion stops on the
		// function's own condition.
		let db = TestDb::new(
			"DEFINE FUNCTION fn::down($n: int) { IF $n <= 0 { RETURN 0 }; RETURN fn::down($n - 1) + 1; };
			 DEFINE FUNCTION fn::tail($n: int) { IF $n <= 0 { RETURN 0 }; RETURN fn::tail($n - 1); };
			 DEFINE FUNCTION fn::ping($n: int) { IF $n <= 0 { RETURN 'ping' }; RETURN fn::pong($n - 1); };
			 DEFINE FUNCTION fn::pong($n: int) { IF $n <= 0 { RETURN 'pong' }; RETURN fn::ping($n - 1); };",
		)
		.await;
		let ctx = db.exec_ctx().await;
		assert_eq!(evaluate(&physical_of(&ctx, "fn::tail(3)").await, &ctx).await, Value::from(0));
		assert_eq!(evaluate(&physical_of(&ctx, "fn::down(3)").await, &ctx).await, Value::from(3));
		assert_eq!(
			evaluate(&physical_of(&ctx, "fn::ping(3)").await, &ctx).await,
			Value::from("pong")
		);
	}

	#[tokio::test]
	async fn a_recursive_call_inside_a_nested_select_ends_on_its_own_condition() {
		// The call to `nest` in the projection of the nested `SELECT` inside
		// its own body is a recursive call like any other.
		let db = TestDb::new(
			"DEFINE FUNCTION fn::nest($n: int) {
				IF $n <= 0 { RETURN 0 };
				LET $r = (SELECT VALUE fn::nest($n - 1) FROM [1]);
				RETURN $r[0] + 1;
			};",
		)
		.await;
		let ctx = db.exec_ctx().await;
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
	async fn a_name_the_body_reads_is_the_databases_parameter() {
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

	/// A body that writes is compiled with the call site like any other body:
	/// a mutation in it is no reason to leave it uncompiled. What the embedded
	/// plan buys is that the compile happened once, with the statement — a
	/// call that finds none binds and compiles the body again on every
	/// evaluation.
	#[tokio::test]
	async fn a_writing_body_is_planned_with_the_statement() {
		let db = TestDb::new(
			"DEFINE TABLE log SCHEMALESS;
			 DEFINE FUNCTION fn::sink() { CREATE log; RETURN 1; };",
		)
		.await;
		assert!(embeds_body(&db, "fn::sink()").await);
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
