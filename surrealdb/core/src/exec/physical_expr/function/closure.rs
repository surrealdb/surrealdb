//! Closure expressions - closure creation and invocation.
//!
//! A closure literal makes a value ([`ClosureExec`]); creating it captures
//! the parameters the body references. The value is a [`Closure::Builtin`]
//! carrying a [`CompiledClosure`]: the closure as written, its digest and its
//! captures. It holds no compiled body. A call binds the arguments in an
//! isolated child context, one unit deeper into the computation-depth budget
//! the context carries ([`ExecutionContext::dive`]), so a closure that reaches
//! itself through a captured or passed value fails with the depth error
//! instead of growing the native stack.
//!
//! Every invocation runs the body as a VM unit ([`crate::exec::vm::closure`]):
//! inside a running program the unit the run's table holds for the closure in
//! the invoking frame, which the instruction handing the value over linked
//! before the call, and outside one the unit loaded for the invoking frame
//! from the compile cache. No closure body is evaluated as an operator tree.
//! Every invocation path — a call, a closure-taking builtin, a stored
//! closure — goes through [`CompiledClosure::invoke`].
//!
//! A literal's body is compiled by the planner that compiles the literal
//! only to answer what the body does to the transaction and to report the
//! error compiling it raises; the compilation is never evaluated.
//!
//! A closure stored as written (`Closure::Expr`, as `DEFINE PARAM` keeps one)
//! is invoked as a value of this type ([`CompiledClosure::of_stored`]);
//! [`crate::exec::closure`] is the route in for every form of closure value.

use std::sync::Arc;

use surrealdb_cnf::PROTECTED_PARAM_NAMES;
use surrealdb_strand::Strand;
use surrealdb_types::{SqlFormat, ToSql};

use super::helpers::{
	args_access_mode, args_required_context, evaluate_args, fmt_call_args, validate_return,
};
use crate::ctx::Context;
use crate::dbs::Variables;
use crate::exec::physical_expr::{EvalContext, PhysicalExpr};
use crate::exec::vm::ClosureDigest;
use crate::exec::vm::closure::Invoked;
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
/// Holds the closure as written, for rendering, for the capture walk and for
/// the values it makes, and what its body does to the transaction, which the
/// planner answered compiling the body.
#[derive(Debug, Clone)]
pub struct ClosureExec {
	pub(crate) closure: Arc<LogicalClosure>,
	/// What the body does to the transaction.
	body_access: AccessMode,
	/// [`ClosureDigest::of`] `closure`.
	digest: ClosureDigest,
}

impl ClosureExec {
	/// The literal `closure` whose body, compiled, does `body_access` to the
	/// transaction.
	pub(crate) fn new(closure: LogicalClosure, body_access: AccessMode) -> Self {
		Self {
			digest: ClosureDigest::of(&closure),
			closure: Arc::new(closure),
			body_access,
		}
	}

	/// The parameters the body references that are bound where the closure is
	/// created. The walk enters the statements a body holds — a block body is
	/// a subquery — so `|| { $a }` captures `$a`. Protected names (`$auth`,
	/// `$session`, …) are never captured: they describe the caller of the
	/// closure, not its creator.
	///
	/// The lookup resolves a name the way [`super::super::literal::Param`]
	/// does — block-local parameters, then the write-path bindings, then the
	/// context chain — because a captured name must hold what the body would
	/// have read had it run where the closure was written. The write bindings
	/// are not context values, and [`CompiledClosure::invoke`] clears them, so
	/// a body naming `$value` reaches its creator's binding through the
	/// capture or not at all.
	fn capture(&self, ctx: &EvalContext<'_>) -> Variables {
		let mut captures = Variables::new();
		let _ = self.closure.body.visit_deep(&mut |e| {
			if let LogicalExpr::Param(param) = e {
				let name = param.as_str();
				if !PROTECTED_PARAM_NAMES.contains(&name) && !captures.0.contains_key(name) {
					let bound = ctx
						.local_params
						.and_then(|params| params.get(name))
						.or_else(|| ctx.write_binding(name))
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
			Ok(Value::Closure(Box::new(Closure::Builtin(Arc::new(CompiledClosure::planned(
				&self.closure,
				self.digest,
				captures,
			))))))
		})
	}

	fn access_mode(&self) -> AccessMode {
		// A closure literal is inert as a value, but wherever it can flow it can
		// also be invoked — the call operator, or a closure-taking builtin such
		// as `array::map` that runs the body per element under this statement's
		// options. A writing body therefore makes the expression writable, so
		// anything combining this access mode (e.g. `args_access_mode` on the
		// builtin holding it) is conservative.
		self.body_access
	}
}

impl ToSql for ClosureExec {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		self.closure.fmt_sql(f, fmt);
	}
}

/// `args` bound to a closure's declared parameters `declared`, each coerced
/// to its kind, in order: a missing argument is an error unless its
/// parameter can be NONE, and an argument past the declared ones is dropped.
pub(crate) fn bind_closure_args(
	declared: &[(Param, Kind)],
	args: Vec<Value>,
) -> anyhow::Result<Vec<Value>> {
	if declared.len() > args.len()
		&& let Some((param, kind)) = declared[args.len()..].iter().find(|(_, k)| !k.can_be_none())
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
	let mut bound = Vec::with_capacity(declared.len());
	for ((param, kind), value) in declared.iter().zip(args) {
		let coerced =
			value.coerce_to_kind(kind).map_err(|_| ExprError::InvalidFunctionArguments {
				name: "ANONYMOUS".to_string(),
				message: format!(
					"Expected a value of type '{}' for argument {}",
					kind.to_sql(),
					param.to_sql()
				),
			})?;
		bound.push(coerced);
	}
	Ok(bound)
}

// =============================================================================
// CompiledClosure - the closure value the engine creates
// =============================================================================

/// A closure value: the closure as written and the values it captured.
///
/// The only [`BuiltinClosureObj`] the engine constructs for a closure
/// literal. Carried in a [`Closure::Builtin`] so the value layer holds it
/// without naming the execution environment; both evaluators downcast to it
/// to invoke.
pub struct CompiledClosure {
	/// The declared parameters, in order.
	args: Vec<(Param, Kind)>,
	/// The declared return type, coerced to on the way out.
	returns: Option<Kind>,
	/// The parameters captured where the closure was created.
	captures: Variables,
	/// The closure as written, for rendering.
	sql: String,
	/// The closure as written, which a unit is lowered from on a compile
	/// cache's miss, and which compares a value with the closure a unit is
	/// the body of.
	logical: Arc<LogicalClosure>,
	/// [`ClosureDigest::of`] `logical`: the code reference a unit is found
	/// by.
	digest: ClosureDigest,
	/// Whether an invocation spends a unit of the computation-depth budget:
	/// a call does; a stored clause evaluated as a unit
	/// ([`crate::exec::scalar_unit`]) does not, as no call is made.
	dives: bool,
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

	fn captured_values(&self) -> Vec<&Value> {
		self.captures.0.values().collect()
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
	/// The value `closure`, a closure literal, makes where `captures` are
	/// what the parameters its body reads are bound to there.
	pub(crate) fn planned(
		closure: &Arc<LogicalClosure>,
		digest: ClosureDigest,
		captures: Variables,
	) -> Self {
		Self {
			args: closure.args.clone(),
			returns: closure.returns.clone(),
			captures,
			sql: closure.to_sql(),
			logical: Arc::clone(closure),
			digest,
			dives: true,
		}
	}

	/// This value invoked with no call made: an invocation spends nothing of
	/// the computation-depth budget.
	pub(crate) fn without_call(mut self) -> Self {
		self.dives = false;
		self
	}

	/// Whether an invocation spends a unit of the computation-depth budget.
	pub(crate) fn dives(&self) -> bool {
		self.dives
	}

	/// The value a stored closure — a `Closure::Expr`, as `DEFINE PARAM`
	/// keeps one — invokes as: the closure lowered.
	pub(crate) fn of_stored(
		args: &[(Param, Kind)],
		returns: Option<&Kind>,
		body: &crate::expr::Expr,
		captures: &Variables,
	) -> anyhow::Result<Self> {
		use crate::expr::logical::lower_expr;
		let body_sql: Arc<str> = Arc::from(body.to_sql());
		let logical = Arc::new(LogicalClosure {
			args: args.to_vec(),
			returns: returns.cloned(),
			body: lower_expr(body.clone().into())?,
			body_sql,
		});
		let digest = ClosureDigest::of(&logical);
		Ok(Self {
			args: logical.args.clone(),
			returns: logical.returns.clone(),
			captures: captures.clone(),
			sql: logical.to_sql(),
			logical,
			digest,
			dives: true,
		})
	}

	/// The closure as written.
	pub(crate) fn logical(&self) -> &Arc<LogicalClosure> {
		&self.logical
	}

	/// [`ClosureDigest::of`] the closure as written.
	pub(crate) fn digest(&self) -> ClosureDigest {
		self.digest
	}

	/// Bind `args` to the declared parameters, coercing each to its kind.
	fn bind_args(&self, args: Vec<Value>) -> anyhow::Result<Vec<(Strand, Value)>> {
		let values = bind_closure_args(&self.args, args)?;
		Ok(self.args.iter().map(|(param, _)| param.clone().into_strand()).zip(values).collect())
	}

	/// Invoke the closure from a streaming expression.
	///
	/// The body runs as a unit ([`crate::exec::vm::closure::run_unit`]) in an
	/// isolated child of the caller's context — it sees the transaction, the
	/// capabilities, its captures and its arguments, and nothing else of the
	/// caller's scope — one re-entry deeper than the call: the call consumes
	/// one unit of the context's computation-depth budget
	/// ([`ExecutionContext::dive`]), so a body that invokes its own closure
	/// again, directly, through a block or subquery of its own, through a
	/// closure-taking builtin such as `array::map`, fails with
	/// [`ExecError::ComputationDepthExceeded`] once the budget is spent.
	///
	/// The body evaluates against the caller's document root, and against the
	/// caller's current value only when there is no root. A method inside an
	/// idiom chain (`tags.map(|$x| $x + n)`) invokes the closure while the
	/// chain stands at the receiver, and the body's bare fields and
	/// `type::field` must still read the row the statement is evaluating, as a
	/// direct call's would. A `RETURN` in the body is its value; `BREAK` and
	/// `CONTINUE` have no loop to reach and are errors.
	pub(crate) async fn invoke(
		&self,
		ctx: &EvalContext<'_>,
		args: Vec<Value>,
	) -> FlowResult<Value> {
		let evaluated = self.evaluate_body(ctx, args).await;
		self.returned(evaluated)
	}

	/// The body's own outcome for an invocation from `ctx` with `args`, as
	/// [`Self::invoke`] runs it: a `RETURN`, `BREAK` or `CONTINUE` the body
	/// signals is passed on as signalled, and the value is not checked against
	/// the declared return kind.
	pub(crate) async fn evaluate_body(
		&self,
		ctx: &EvalContext<'_>,
		args: Vec<Value>,
	) -> FlowResult<Value> {
		let bound = self.bind_args(args)?;
		match crate::exec::vm::closure::run_unit(self, ctx, bound).await {
			Invoked::Ran(ran) => ran,
			// The unit the instruction handing the value over linked stopped
			// standing during the call it made — a builtin invoking the value
			// per element, whose earlier invocation changed what the unit was
			// lowered against. The call cannot hand the step back, so the
			// invocation links the unit as the instruction would have, and runs
			// what it links.
			Invoked::Unlinked(bound) => Box::pin(self.relinked(ctx, bound)).await,
		}
	}

	/// The body's value for an invocation from `ctx` whose linked unit
	/// stopped standing: it links one again, and runs what it linked. Boxed
	/// by the caller, so that the invocation a body nests to the depth budget
	/// carries none of it.
	async fn relinked(
		&self,
		ctx: &EvalContext<'_>,
		bound: Vec<(Strand, Value)>,
	) -> FlowResult<Value> {
		crate::exec::vm::closure::link(self, ctx).await?;
		match crate::exec::vm::closure::run_unit(self, ctx, bound).await {
			Invoked::Ran(ran) => ran,
			Invoked::Unlinked(_) => {
				Err(ControlFlow::Err(anyhow::Error::new(crate::err::EngineError::Internal(
					"a closure unit linked for an invocation stopped standing before it ran"
						.to_owned(),
				))))
			}
		}
	}

	/// [`Self::invoke`] for a program's instruction that linked the
	/// closure's unit into the run's table before invoking it: `None` where
	/// the table holds no unit standing for it in the invoking frame and the
	/// instruction has not just linked one (`relinked`), for the instruction
	/// to link one again. Where it has, the invocation links the unit itself.
	pub(crate) async fn invoke_linked(
		&self,
		ctx: &EvalContext<'_>,
		args: Vec<Value>,
		relinked: bool,
	) -> Option<FlowResult<Value>> {
		let bound = match self.bind_args(args) {
			Ok(bound) => bound,
			Err(e) => return Some(Err(e.into())),
		};
		let evaluated = match crate::exec::vm::closure::run_unit(self, ctx, bound).await {
			Invoked::Ran(ran) => ran,
			Invoked::Unlinked(bound) if relinked => Box::pin(self.relinked(ctx, bound)).await,
			Invoked::Unlinked(_) => return None,
		};
		Some(self.returned(evaluated))
	}

	/// The value of an invocation whose body evaluated to `evaluated`: a
	/// `RETURN` is the value, a loop signal an error, and the value is
	/// checked against the declared return kind.
	fn returned(&self, evaluated: FlowResult<Value>) -> FlowResult<Value> {
		let result = match evaluated {
			Ok(v) => v,
			Err(ControlFlow::Return(v)) => v,
			Err(ControlFlow::Break) | Err(ControlFlow::Continue) => {
				return Err(ExecError::InvalidControlFlow.into());
			}
			Err(e) => return Err(e),
		};

		Ok(validate_return("ANONYMOUS", self.returns.as_ref(), result)?)
	}

	/// The frame an invocation from `ctx` runs the body's unit in: an
	/// isolated child of the caller's context binding the captures and then
	/// the arguments `bound`, one re-entry deeper than the call, holding
	/// `units` where the invocation brings a table of closure units of its
	/// own, computing the record the caller computes, where it computes one,
	/// and waiving the reads' checks where the caller's reads waive them.
	pub(crate) fn frame(
		&self,
		ctx: &EvalContext<'_>,
		bound: Vec<(Strand, Value)>,
		units: Option<Arc<crate::exec::vm::closure::ClosureUnits>>,
	) -> FlowResult<ExecutionContext> {
		let mut isolated = Context::new_isolated(ctx.exec_ctx.ctx());
		isolated.attach_variables(self.captures.clone())?;
		for (name, value) in bound {
			isolated.add_value(name, Arc::new(value));
		}
		if let Some(units) = units {
			isolated.set_vm_closures(units);
		}
		let frame = if self.dives {
			ctx.exec_ctx.dive()?
		} else {
			ctx.exec_ctx.clone()
		};
		let frame = frame.with_new_ctx(isolated.freeze());
		let frame = match &ctx.computing_record {
			Some(record) => frame.with_computing_record(Some(Arc::clone(record))),
			None => frame,
		};
		// A caller whose reads waive the checks — a permission predicate's,
		// a gate's — runs the body with its reads waived too.
		Ok(if ctx.skip_fetch_perms && !frame.root().skip_fetch_perms {
			frame.with_skip_fetch_perms(true)
		} else {
			frame
		})
	}

	/// The parameters captured where the closure was created.
	pub(crate) fn captures(&self) -> &Variables {
		&self.captures
	}
}

// =============================================================================
// StreamingClosureEvaluator - the value layer's seam, for streaming callers
// =============================================================================

/// The engine's [`ClosureEvaluator`]: invokes a closure with the
/// evaluation context of the expression that holds it, so a closure-taking
/// builtin (`array::map`, `value::chain`, …) run by the engine
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
	fn invoke<'a>(&'a self, closure: &'a Closure, args: Vec<Value>) -> BoxInvokeFut<'a> {
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
		// A closure literal is the body this call runs, and `ClosureExec`
		// reports that body's mode, so the target's own mode is exact. Any
		// other target is a runtime value — a parameter, a field, the result
		// of a call — whose body cannot be read here, so such a call is
		// charged `ReadWrite` whatever it turns out to hold.
		let unknown_body = if self.target.as_any().is::<ClosureExec>() {
			AccessMode::ReadOnly
		} else {
			AccessMode::ReadWrite
		};
		unknown_body.combine(self.target.access_mode()).combine(args_access_mode(&self.arguments))
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
	async fn invoking_a_closure_literal_takes_the_mode_of_the_body_that_runs() {
		// The target is the body the call runs, so charging the call
		// `ReadWrite` regardless would make a pure immediately-invoked closure
		// demand a writeable transaction.
		let db = TestDb::new(
			"DEFINE TABLE log SCHEMALESS;
			 DEFINE FUNCTION fn::log($v: any) { CREATE log SET v = $v; };",
		)
		.await;
		assert_eq!(access_mode_of(&db, "(|| [1, 2])()").await, AccessMode::ReadOnly);
		assert_eq!(access_mode_of(&db, "(|| { fn::log(1) })()").await, AccessMode::ReadWrite);
	}

	#[tokio::test]
	async fn invoking_a_closure_a_runtime_value_holds_stays_read_write() {
		// Nothing here reads the body that will run, so the call is charged
		// `ReadWrite` whatever the value turns out to hold — including when it
		// holds a body that only reads.
		let db = TestDb::new("").await;
		assert_eq!(access_mode_of(&db, "$f()").await, AccessMode::ReadWrite);
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
			panic!("the engine creates compiled closures");
		};
		assert!(obj.as_any().downcast_ref::<CompiledClosure>().is_some());

		assert_eq!(eval("(|$x: int| $x * 2)(21)", &ctx).await.unwrap(), Value::from(42));
	}

	#[tokio::test]
	async fn a_closure_captures_the_write_binding_live_where_it_was_written() {
		let db = TestDb::new("").await;
		let ctx = db.exec_ctx().await;
		let bound = val("'bound'").await;

		// Created where `$value` is bound, the way a write stage evaluates a
		// field or permission clause.
		let expr = physical_expr("|| $value", &ctx).await;
		let mut creating = EvalContext::from_exec_ctx(&ctx);
		creating.write_value = Some(&bound);
		let value = expr.evaluate(creating).await.unwrap();
		let Value::Closure(closure) = &value else {
			panic!("expected a closure, got {value:?}");
		};
		let Closure::Builtin(obj) = closure.as_ref() else {
			panic!("the engine creates compiled closures");
		};
		let compiled = obj.as_any().downcast_ref::<CompiledClosure>().unwrap();

		// Invoked where nothing binds `$value`. The binding is not a context
		// value and the invocation clears the slots, so the capture is the only
		// way the body can still read it.
		let called = compiled.invoke(&EvalContext::from_exec_ctx(&ctx), vec![]).await.unwrap();
		assert_eq!(called, bound, "the body must read the binding its creator held");

		// A name nothing bound stays unbound rather than picking up a slot.
		let absent = physical_expr("|| $nothing", &ctx).await;
		let mut creating = EvalContext::from_exec_ctx(&ctx);
		creating.write_value = Some(&bound);
		let value = absent.evaluate(creating).await.unwrap();
		let Value::Closure(closure) = &value else {
			panic!("expected a closure, got {value:?}");
		};
		let Closure::Builtin(obj) = closure.as_ref() else {
			panic!("the engine creates compiled closures");
		};
		let compiled = obj.as_any().downcast_ref::<CompiledClosure>().unwrap();
		let called = compiled.invoke(&EvalContext::from_exec_ctx(&ctx), vec![]).await.unwrap();
		assert_eq!(called, Value::None);
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

	/// A closure invoked with a current value and no document runs its unit
	/// over the value as no document: a statement in an element body of the
	/// body takes the element as its `$parent`, as one evaluated where there
	/// is no row does, while one invoked with the value as its document takes
	/// the document.
	#[tokio::test]
	async fn a_body_invoked_with_a_value_and_no_document_reads_no_document() {
		let db = TestDb::new("").await;
		let ctx = db.exec_ctx().await;
		let closure =
			eval("|| [1, 2, 3][WHERE (SELECT VALUE $parent FROM ONLY 0) > 1]", &ctx).await.unwrap();
		let ctx = ctx.with_param("f", closure);
		let call = physical_expr("$f()", &ctx).await;
		let row = Value::from(0);
		let undocumented =
			call.evaluate(EvalContext::from_exec_ctx(&ctx).with_value(&row)).await.unwrap();
		assert_eq!(undocumented, val("[2, 3]").await);
		let documented =
			call.evaluate(EvalContext::from_exec_ctx(&ctx).with_value_and_doc(&row)).await.unwrap();
		assert_eq!(documented, val("[]").await);
		let rowless = call.evaluate(EvalContext::from_exec_ctx(&ctx)).await.unwrap();
		assert_eq!(rowless, val("[2, 3]").await);
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
