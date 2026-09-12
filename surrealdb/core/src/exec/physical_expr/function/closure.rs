//! Closure expressions - closure creation and invocation.
//!
//! A closure literal compiles its body once, when the statement holding it is
//! compiled ([`ClosureExec`]); creating the value captures the parameters the
//! body references. The value is a [`Closure::Builtin`] carrying a
//! [`CompiledClosure`], whose body is the compiled expression: nothing turns a
//! compiled body back into a surface expression. A call binds the arguments as
//! `$param`s in an isolated child context and evaluates the compiled body
//! there ([`CompiledClosure::invoke`]), one unit deeper into the
//! computation-depth budget the context carries ([`ExecutionContext::dive`]),
//! so a closure that reaches itself through a captured or passed value fails
//! with the depth error instead of growing the native stack.
//!
//! The body carries nothing of the statement that created it beyond its
//! captures. It is invoked by later statements, each in a transaction of its
//! own, so it is compiled without the creating statement's catalog: binding
//! leaves it as lowered, and the physical planner compiles it with no
//! transaction, namespace, database, principal or `VERSION`. It resolves
//! tables, permissions, indexes, functions and parameters when it runs, in the
//! invoking statement's transaction and frame — a field or index defined
//! between creation and call is seen by the call.
//!
//! A closure value can also reach the legacy evaluator — `array::map` run by a
//! legacy statement, or a `Closure::Expr` stored by `DEFINE PARAM` — so both
//! evaluators can invoke both closure forms: [`CompiledClosure::invoke_legacy`]
//! rebuilds a streaming context from the legacy handles, and
//! [`super::helpers::invoke_closure`] hands a legacy-only closure to the
//! legacy evaluator through the engine's one bridge.

use std::sync::Arc;

use surrealdb_cnf::PROTECTED_PARAM_NAMES;
use surrealdb_strand::Strand;
use surrealdb_types::{SqlFormat, ToSql};

use super::helpers::{
	args_access_mode, args_required_context, evaluate_args, fmt_call_args, validate_return,
};
use crate::catalog::providers::{DatabaseProvider, NamespaceProvider};
use crate::ctx::{Context, FrozenContext};
use crate::dbs::{NoWriteFrame, Options, Variables};
use crate::doc::CursorDoc;
use crate::exec::context::{RootContext, SessionInfo};
use crate::exec::function::FunctionRegistry;
use crate::exec::physical_expr::{EvalContext, PhysicalExpr};
use crate::exec::{AccessMode, BoxFut, Error as ExecError, ExecutionContext};
use crate::expr::logical::{LogicalClosure, LogicalExpr};
use crate::expr::{ControlFlow, Error as ExprError, FlowResult, Kind, Param};
use crate::val::closure::BuiltinClosureObj;
use crate::val::{BoxInvokeFut, Closure, ClosureEvaluator, Value};

// =============================================================================
// ClosureExec - closure value creation
// =============================================================================

/// Closure expression - |$x| $x * 2
///
/// Holds the closure as written, for rendering and for the capture walk, and
/// its body compiled once in its own scope.
#[derive(Debug, Clone)]
pub struct ClosureExec {
	pub(crate) closure: LogicalClosure,
	pub(crate) body: Arc<dyn PhysicalExpr>,
}

impl ClosureExec {
	pub(crate) fn new(closure: LogicalClosure, body: Arc<dyn PhysicalExpr>) -> Self {
		Self {
			closure,
			body,
		}
	}

	/// The parameters the body references that are bound where the closure is
	/// created. The walk enters the statements a body holds — a block body is
	/// a subquery — so `|| { $a }` captures `$a`. Protected names (`$auth`,
	/// `$session`, …) are never captured: they describe the caller of the
	/// closure, not its creator.
	fn capture(&self, ctx: &EvalContext<'_>) -> Variables {
		let mut captures = Variables::new();
		let _ = self.closure.body.visit_deep(&mut |e| {
			if let LogicalExpr::Param(param) = e {
				let name = param.as_str();
				if !PROTECTED_PARAM_NAMES.contains(&name) && !captures.0.contains_key(name) {
					let bound = ctx
						.local_params
						.and_then(|params| params.get(name))
						.or_else(|| ctx.exec_ctx.value(name));
					if let Some(v) = bound {
						captures.insert(name, v.clone());
					}
				}
			}
			std::ops::ControlFlow::<()>::Continue(())
		});
		captures
	}
}

impl PhysicalExpr for ClosureExec {
	fn name(&self) -> &'static str {
		"Closure"
	}

	fn as_any(&self) -> &dyn std::any::Any {
		self
	}

	fn required_context(&self) -> crate::exec::ContextLevel {
		// A closure literal is a value; its body's requirements apply where it
		// is called.
		crate::exec::ContextLevel::Root
	}

	fn evaluate<'a>(&'a self, ctx: EvalContext<'a>) -> BoxFut<'a, FlowResult<Value>> {
		Box::pin(async move {
			let captures = self.capture(&ctx);
			Ok(Value::Closure(Box::new(Closure::Builtin(Arc::new(CompiledClosure {
				args: self.closure.args.clone(),
				returns: self.closure.returns.clone(),
				body: Arc::clone(&self.body),
				captures,
				registry: Arc::clone(ctx.exec_ctx.function_registry()),
				sql: self.closure.to_sql(),
			})))))
		})
	}

	fn access_mode(&self) -> AccessMode {
		// A closure literal is inert as a value, but wherever it can flow it can
		// also be invoked — the call operator, or a closure-taking builtin such
		// as `array::map` that runs the body per element under this statement's
		// options. A writing body therefore makes the expression writable, so
		// anything combining this access mode (e.g. `args_access_mode` on the
		// builtin holding it) is conservative.
		self.body.access_mode()
	}
}

impl ToSql for ClosureExec {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		self.closure.fmt_sql(f, fmt);
	}
}

// =============================================================================
// CompiledClosure - the closure value the streaming engine creates
// =============================================================================

/// A closure value whose body is a compiled expression.
///
/// The only [`BuiltinClosureObj`] the streaming engine constructs. Carried in a
/// [`Closure::Builtin`] so the value layer holds it without naming the
/// execution environment; both evaluators downcast to it to invoke.
pub struct CompiledClosure {
	/// The declared parameters, in order.
	args: Vec<(Param, Kind)>,
	/// The declared return type, coerced to on the way out.
	returns: Option<Kind>,
	/// The body, compiled in its own scope.
	body: Arc<dyn PhysicalExpr>,
	/// The parameters captured where the closure was created.
	captures: Variables,
	/// The function registry the body was compiled against, so a legacy caller
	/// can rebuild the streaming context the body needs.
	registry: Arc<FunctionRegistry>,
	/// The closure as written, for rendering.
	sql: String,
}

impl std::fmt::Debug for CompiledClosure {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("CompiledClosure")
			.field("args", &self.args)
			.field("returns", &self.returns)
			.field("captures", &self.captures)
			.field("sql", &self.sql)
			.finish_non_exhaustive()
	}
}

impl BuiltinClosureObj for CompiledClosure {
	fn as_any(&self) -> &dyn std::any::Any {
		self
	}

	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		ToSql::fmt_sql(self, f, fmt)
	}

	/// Two compiled closures are equal when they were written the same way —
	/// parameters, return kind and body, all of which the rendered source
	/// carries — and captured the same values, which is what makes two
	/// `Closure::Expr` values equal. Any other builtin closure is a different
	/// closure.
	fn dyn_eq(&self, other: &dyn BuiltinClosureObj) -> bool {
		match other.as_any().downcast_ref::<CompiledClosure>() {
			Some(other) => self.sql == other.sql && self.captures == other.captures,
			None => false,
		}
	}
}

impl ToSql for CompiledClosure {
	fn fmt_sql(&self, f: &mut String, _fmt: SqlFormat) {
		f.push_str(&self.sql);
	}
}

impl CompiledClosure {
	/// Bind `args` to the declared parameters, coercing each to its kind.
	fn bind_args(&self, args: Vec<Value>) -> anyhow::Result<Vec<(Strand, Value)>> {
		// A missing argument is only an error when its parameter cannot be NONE.
		if self.args.len() > args.len()
			&& let Some((param, kind)) =
				self.args[args.len()..].iter().find(|(_, k)| !k.can_be_none())
		{
			return Err(ExprError::InvalidFunctionArguments {
				name: "ANONYMOUS".to_string(),
				message: format!(
					"Expected a value of type '{}' for argument {}",
					kind.to_sql(),
					param.to_sql()
				),
			}
			.into());
		}
		let mut bound = Vec::with_capacity(self.args.len());
		for ((param, kind), value) in self.args.iter().zip(args) {
			let coerced =
				value.coerce_to_kind(kind).map_err(|_| ExprError::InvalidFunctionArguments {
					name: "ANONYMOUS".to_string(),
					message: format!(
						"Expected a value of type '{}' for argument {}",
						kind.to_sql(),
						param.to_sql()
					),
				})?;
			bound.push((param.clone().into_strand(), coerced));
		}
		Ok(bound)
	}

	/// Invoke the closure from a streaming expression.
	///
	/// The body runs in an isolated child of the caller's context — it sees
	/// the transaction, the capabilities, its captures and its arguments, and
	/// nothing else of the caller's scope — one re-entry deeper than the call:
	/// the call consumes one unit of the context's computation-depth budget
	/// ([`ExecutionContext::dive`]), so a body that invokes its own closure
	/// again, directly, through a block or subquery of its own, through a
	/// closure-taking builtin such as `array::map`, or through the legacy
	/// evaluator, fails with [`ExecError::ComputationDepthExceeded`] once the
	/// budget is spent.
	///
	/// The body evaluates against the caller's document root, and against the
	/// caller's current value only when there is no root. A method inside an
	/// idiom chain (`tags.map(|$x| $x + n)`) invokes the closure while the
	/// chain stands at the receiver, and the body's bare fields and
	/// `type::field` must still read the row the statement is evaluating, as a
	/// direct call's would. This is the document the legacy evaluator hands a
	/// closure it runs (see [`super::helpers::invoke_closure`]). A `RETURN` in
	/// the body is its value; `BREAK` and `CONTINUE` have no loop to reach and
	/// are errors.
	pub(crate) async fn invoke(
		&self,
		ctx: &EvalContext<'_>,
		args: Vec<Value>,
	) -> FlowResult<Value> {
		let bound = self.bind_args(args)?;

		let mut isolated = Context::new_isolated(ctx.exec_ctx.ctx());
		isolated.attach_variables(self.captures.clone())?;
		for (name, value) in bound {
			isolated.add_value(name, Arc::new(value));
		}
		let isolated_ctx = ctx.exec_ctx.dive()?.with_new_ctx(isolated.freeze());

		let eval_ctx = EvalContext {
			exec_ctx: &isolated_ctx,
			current_value: ctx.document_root.or(ctx.current_value),
			local_params: None,
			recursion_ctx: None,
			document_root: ctx.document_root,
			skip_fetch_perms: ctx.skip_fetch_perms,
			computing_record: ctx.computing_record.clone(),
			plan_depth: ctx.plan_depth + 1,
		};

		let result = match self.body.evaluate(eval_ctx).await {
			Ok(v) => v,
			Err(ControlFlow::Return(v)) => v,
			Err(ControlFlow::Break) | Err(ControlFlow::Continue) => {
				return Err(ExecError::InvalidControlFlow.into());
			}
			Err(e) => return Err(e),
		};

		Ok(validate_return("ANONYMOUS", self.returns.as_ref(), result)?)
	}

	/// Invoke the closure from the legacy evaluator.
	///
	/// Rebuilds the streaming context the compiled body needs from the legacy
	/// handles: the options' auth, permission and computed-field frames and
	/// remaining computation-depth budget, the session the context carries,
	/// the namespace and database the options select, and `doc` as the
	/// current value. The context is built per call and never stored on the
	/// value, so a closure bound by one statement and called by a later one
	/// runs against the later statement's transaction.
	pub(crate) async fn invoke_legacy(
		&self,
		ctx: &FrozenContext,
		opt: &Options,
		doc: Option<&CursorDoc>,
		args: Vec<Value>,
	) -> anyhow::Result<Value> {
		let exec_ctx = self.legacy_exec_ctx(ctx, opt).await?;
		let current = doc.map(|d| d.doc.as_ref().clone());
		let eval_ctx = EvalContext {
			exec_ctx: &exec_ctx,
			current_value: current.as_ref(),
			local_params: None,
			recursion_ctx: None,
			document_root: current.as_ref(),
			skip_fetch_perms: !opt.perms,
			computing_record: None,
			// The legacy caller's depth is the remaining dive of `opt`, which
			// the rebuilt context carries as its options; the body's own
			// nesting was counted when its statement was compiled.
			plan_depth: 0,
		};
		self.invoke(&eval_ctx, args).await.map_err(|ctrl| match ctrl {
			ControlFlow::Err(e) => e,
			other => anyhow::Error::new(crate::err::EngineError::Internal(format!(
				"unexpected control flow from a closure body: {other}"
			))),
		})
	}

	/// The streaming context a legacy call site's handles denote.
	async fn legacy_exec_ctx(
		&self,
		ctx: &FrozenContext,
		opt: &Options,
	) -> anyhow::Result<ExecutionContext> {
		let root = ExecutionContext::Root(RootContext {
			ctx: Arc::clone(ctx),
			function_registry: Arc::clone(&self.registry),
			options: Some(opt.clone()),
			datastore: None,
			cancellation: ctx.cancel_token().map(|t| t.child_token()).unwrap_or_default(),
			auth: Arc::clone(&opt.auth),
			session: SessionInfo::from_context(ctx).map(Arc::new),
			current_value: None,
			current_row: None,
			skip_fetch_perms: !opt.perms,
			computing_record: None,
			computing_field: opt.no_write == Some(NoWriteFrame::ComputedField),
			version_stamp: opt.version,
		});
		let (Ok(ns), Ok(db)) = (opt.ns(), opt.db()) else {
			return Ok(root);
		};
		let Some(txn) = ctx.try_tx() else {
			return Ok(root);
		};
		let ns_def = txn.expect_ns_by_name(ns).await?;
		let db_def = txn.expect_db_by_name(ns, db).await?;
		Ok(root.with_database(ns_def, db_def))
	}
}

// =============================================================================
// StreamingClosureEvaluator - the value layer's seam, for streaming callers
// =============================================================================

/// The streaming engine's [`ClosureEvaluator`]: invokes a closure with the
/// evaluation context of the expression that holds it, so a closure-taking
/// builtin (`array::map`, `value::chain`, …) run by the streaming engine
/// evaluates a compiled body without leaving the engine.
pub(crate) struct StreamingClosureEvaluator<'a> {
	ctx: &'a EvalContext<'a>,
}

impl<'a> StreamingClosureEvaluator<'a> {
	pub(crate) fn new(ctx: &'a EvalContext<'a>) -> Self {
		Self {
			ctx,
		}
	}
}

impl ClosureEvaluator for StreamingClosureEvaluator<'_> {
	fn invoke<'a>(
		&'a self,
		_stk: &'a mut reblessive::tree::Stk,
		closure: &'a Closure,
		args: Vec<Value>,
	) -> BoxInvokeFut<'a> {
		Box::pin(async move {
			super::helpers::invoke_closure(closure, args, self.ctx).await.map_err(|ctrl| match ctrl
			{
				ControlFlow::Err(e) => e,
				other => anyhow::Error::new(crate::err::EngineError::Internal(format!(
					"unexpected control flow from a closure body: {other}"
				))),
			})
		})
	}
}

// =============================================================================
// ClosureCallExec - for invoking closures stored in parameters
// =============================================================================

/// Closure call expression - $closure(args...)
///
/// Invokes a closure value with the provided arguments. The target expression
/// must evaluate to a `Value::Closure`.
#[derive(Debug, Clone)]
pub struct ClosureCallExec {
	/// The expression that evaluates to a closure value
	pub(crate) target: Arc<dyn PhysicalExpr>,
	/// The argument expressions to pass to the closure
	pub(crate) arguments: Vec<Arc<dyn PhysicalExpr>>,
}

impl PhysicalExpr for ClosureCallExec {
	fn name(&self) -> &'static str {
		"ClosureCall"
	}

	fn as_any(&self) -> &dyn std::any::Any {
		self
	}

	fn required_context(&self) -> crate::exec::ContextLevel {
		// The closure body is a runtime value, invisible at plan time, so
		// this can only declare what the target and argument expressions
		// need. The executor materializes the highest context level the
		// session's ns/db selection supports rather than just this minimum,
		// which is what lets a database-touching body run. Declaring
		// `Database` here instead would wrongly reject pure closures in
		// sessions with no database selected.
		let target_ctx = self.target.required_context();
		let args_ctx = args_required_context(&self.arguments);
		target_ctx.max(args_ctx)
	}

	fn evaluate<'a>(&'a self, ctx: EvalContext<'a>) -> BoxFut<'a, FlowResult<Value>> {
		Box::pin(async move {
			let target_value = self.target.evaluate(ctx.clone()).await?;

			let closure = match target_value {
				Value::Closure(c) => c,
				other => {
					return Err(ExecError::InvalidFunction {
						name: "ANONYMOUS".to_string(),
						message: format!("'{}' is not a function", other.kind_of()),
					}
					.into());
				}
			};

			let evaluated_args = evaluate_args(&self.arguments, ctx.clone()).await?;

			super::helpers::invoke_closure(&closure, evaluated_args, &ctx).await
		})
	}

	fn access_mode(&self) -> AccessMode {
		// The closure is a runtime value, so its body's access mode is unknown
		// here; be conservative.
		AccessMode::ReadWrite
			.combine(self.target.access_mode())
			.combine(args_access_mode(&self.arguments))
	}
}

impl ToSql for ClosureCallExec {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		self.target.fmt_sql(f, fmt);
		fmt_call_args(f, fmt, &self.arguments);
	}
}

#[cfg(all(test, feature = "kv-mem"))]
#[allow(clippy::unwrap_used)]
mod tests {
	use super::*;
	use crate::exec::operators::test_util::{TestDb, eval, eval_on, physical_expr, val};

	/// Plan `expression` against a txn-backed planner over `db` and return the
	/// resolved access mode of the resulting physical expression tree.
	async fn access_mode_of(db: &TestDb, expression: &str) -> AccessMode {
		let ctx = db.exec_ctx().await;
		physical_expr(expression, &ctx).await.access_mode()
	}

	#[tokio::test]
	async fn a_pure_closure_literal_is_read_only() {
		let db = TestDb::new("").await;
		assert_eq!(access_mode_of(&db, "|$x: any| { RETURN $x; }").await, AccessMode::ReadOnly);
	}

	#[tokio::test]
	async fn a_writing_closure_literal_is_read_write() {
		// A closure does not write when it is created, but it can be invoked
		// wherever the value flows, so a writing body makes the expression
		// writable. A write statement has no logical form, so the body writes
		// through a function whose body writes.
		let db = TestDb::new(
			"DEFINE TABLE log SCHEMALESS;
			 DEFINE FUNCTION fn::log($v: any) { CREATE log SET v = $v; };",
		)
		.await;
		assert_eq!(access_mode_of(&db, "|$x: any| { fn::log($x) }").await, AccessMode::ReadWrite);
	}

	#[tokio::test]
	async fn a_writing_closure_passed_to_a_builtin_is_read_write() {
		// `array::map` runs the closure per element under this statement's
		// options, so a writing closure argument makes the whole call writable.
		// Feeding the closure body into the builtin's access mode is what closes
		// the gap: otherwise the call would resolve read-only and could be
		// overlapped or spawned as if it never wrote.
		let db = TestDb::new(
			"DEFINE TABLE log SCHEMALESS;
			 DEFINE FUNCTION fn::log($v: any) { CREATE log SET v = $v; };",
		)
		.await;
		assert_eq!(
			access_mode_of(&db, "array::map([1, 2], |$x: any| { fn::log($x) })").await,
			AccessMode::ReadWrite
		);
		// A pure closure argument keeps the call read-only — no regression for
		// the common case.
		assert_eq!(
			access_mode_of(&db, "array::map([1, 2], |$x: any| { RETURN $x; })").await,
			AccessMode::ReadOnly
		);
	}

	#[tokio::test]
	async fn a_closure_is_created_as_a_compiled_value_and_called_with_its_arguments() {
		let db = TestDb::new("").await;
		let ctx = db.exec_ctx().await;
		let value = eval("|$x: int| $x * 2", &ctx).await.unwrap();
		let Value::Closure(closure) = &value else {
			panic!("expected a closure, got {value:?}");
		};
		let Closure::Builtin(obj) = closure.as_ref() else {
			panic!("the streaming engine creates compiled closures");
		};
		assert!(obj.as_any().downcast_ref::<CompiledClosure>().is_some());

		assert_eq!(eval("(|$x: int| $x * 2)(21)", &ctx).await.unwrap(), Value::from(42));
	}

	#[tokio::test]
	async fn a_closure_captures_the_bindings_its_body_references() {
		let db = TestDb::new("").await;
		let ctx = db.exec_ctx().await.with_param("factor", Value::from(3));
		// `$factor` is bound where the closure is created and read by the body.
		assert_eq!(eval("(|$x: int| $x * $factor)(5)", &ctx).await.unwrap(), Value::from(15));
	}

	#[tokio::test]
	async fn a_body_reads_the_row_the_call_is_made_from_not_the_chain_position() {
		let db = TestDb::new("").await;
		let ctx = db.exec_ctx().await;
		let row = val("{ n: 7, tags: [1, 2] }").await;
		// `.map` runs the closure while the idiom stands at `tags`; a bare field
		// in the body still names a field of the row.
		assert_eq!(
			eval_on("tags.map(|$x| $x + n)", &row, &ctx).await.unwrap(),
			val("[8, 9]").await
		);
		// So does a projection function, which reads the current value.
		assert_eq!(
			eval_on("['n'].map(|$x| type::field($x))", &row, &ctx).await.unwrap(),
			val("[7]").await
		);
		// A direct call is unchanged: the row is the current value already.
		assert_eq!(
			eval_on("array::map(tags, |$x| $x + n)", &row, &ctx).await.unwrap(),
			val("[8, 9]").await
		);
	}

	#[tokio::test]
	async fn arguments_are_coerced_and_a_missing_required_one_is_an_error() {
		let db = TestDb::new("").await;
		let ctx = db.exec_ctx().await;
		assert_eq!(eval("(|$x: float| $x)(2)", &ctx).await.unwrap(), Value::from(2.0f64));
		assert!(eval("(|$x: int| $x)()", &ctx).await.is_err());
		// An optional parameter may be omitted.
		assert_eq!(eval("(|$x: option<int>| $x)()", &ctx).await.unwrap(), Value::None);
	}

	#[tokio::test]
	async fn the_return_type_is_enforced() {
		let db = TestDb::new("").await;
		let ctx = db.exec_ctx().await;
		assert!(eval("(|$x: any| -> int { $x })('nope')", &ctx).await.is_err());
		assert_eq!(eval("(|$x: any| -> int { $x })(7)", &ctx).await.unwrap(), Value::from(7));
	}

	#[tokio::test]
	async fn a_loop_signal_escaping_the_body_is_an_error() {
		let db = TestDb::new("").await;
		let ctx = db.exec_ctx().await;
		let err = eval("(|| { BREAK })()", &ctx).await.expect_err("BREAK has no loop to reach");
		let ControlFlow::Err(err) = err else {
			panic!("expected an error, got {err}");
		};
		assert!(matches!(err.downcast_ref::<ExecError>(), Some(ExecError::InvalidControlFlow)));
	}

	#[tokio::test]
	async fn calling_a_non_closure_is_an_error() {
		let db = TestDb::new("").await;
		let ctx = db.exec_ctx().await.with_param("f", Value::from(1));
		assert!(eval("$f(1)", &ctx).await.is_err());
	}

	/// Whether `err` is the computation-depth error, however it is wrapped.
	fn is_depth_error(err: &ControlFlow) -> bool {
		let ControlFlow::Err(err) = err else {
			return false;
		};
		matches!(crate::err::exec_error(err), Some(ExecError::ComputationDepthExceeded))
	}

	#[tokio::test]
	async fn a_closure_that_reaches_itself_fails_with_the_depth_error() {
		let db = TestDb::new("").await;
		let ctx = db.exec_ctx().await;
		// Each closure is passed itself and calls itself again, so the body
		// re-enters until the budget the context carries is spent. The
		// recursion runs through a direct call, through a block of the body's
		// own (a plan whose operators rebuild their evaluation context), and
		// through a closure-taking builtin whose call node was planned at a
		// fixed depth.
		for closure in
			["|$s| $s($s)", "|$s| { RETURN $s($s); }", "|$s| array::map([0], |$i| $s($s))"]
		{
			let value = eval(closure, &ctx).await.unwrap();
			let ctx = ctx.with_param("f", value);
			let err = eval("$f($f)", &ctx).await.expect_err("the recursion has no end");
			assert!(is_depth_error(&err), "{closure}: {err}");
		}
	}

	#[tokio::test]
	async fn a_recursion_that_ends_within_the_budget_returns_its_value() {
		let db = TestDb::new("").await;
		let ctx = db.exec_ctx().await;
		let value = eval("|$s, $n| IF $n = 0 { 0 } ELSE { $s($s, $n - 1) }", &ctx).await.unwrap();
		let ctx = ctx.with_param("f", value);
		assert_eq!(eval("$f($f, 3)", &ctx).await.unwrap(), Value::from(0));
	}

	#[tokio::test]
	async fn compiled_closures_compare_by_source_and_captures() {
		let db = TestDb::new("").await;
		let ctx = db.exec_ctx().await;
		let f = eval("|$x| $x", &ctx).await.unwrap();
		// A closure value is equal to itself, and to another value written the
		// same way; a differently written one is a different closure.
		assert_eq!(f, f.clone());
		assert_eq!(f, eval("|$x| $x", &ctx).await.unwrap());
		assert_ne!(f, eval("|$y| $y", &ctx).await.unwrap());
		// The same source over different captures is a different closure.
		let one = eval("|$x| $x + $n", &ctx.with_param("n", Value::from(1))).await.unwrap();
		let two = eval("|$x| $x + $n", &ctx.with_param("n", Value::from(2))).await.unwrap();
		assert_eq!(one, eval("|$x| $x + $n", &ctx.with_param("n", Value::from(1))).await.unwrap());
		assert_ne!(one, two);
	}
}
