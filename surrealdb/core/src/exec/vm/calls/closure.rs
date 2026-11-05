//! Closure expressions - closure creation and invocation.
//!
//! A closure literal makes a value ([`ClosureExec`]); creating it captures
//! the parameters the body references and reads no row. The value is a [`Closure::Builtin`]
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
//! from the compile cache. Every invocation path — a call, a closure-taking
//! builtin, a stored closure — goes through [`CompiledClosure::invoke`].
//!
//! A literal's body is compiled by the planner that compiles the literal
//! only to answer what the body does to the transaction and to report the
//! error compiling it raises; the compilation is never evaluated.
//!
//! A closure stored as written (`Closure::Expr`, as `DEFINE PARAM` keeps one)
//! is invoked as a value of this type ([`CompiledClosure::of_stored`]);
//! [`crate::exec::closure`] is the route in for every form of closure value.

use std::sync::Arc;

use surrealdb_strand::Strand;
use surrealdb_types::{SqlFormat, ToSql};

use super::helpers::validate_return;
use crate::ctx::Context;
use crate::dbs::Variables;
use crate::exec::vm::ClosureDigest;
use crate::exec::vm::closure::Invoked;
use crate::exec::{Error as ExecError, EvalContext, ExecutionContext};
use crate::expr::logical::LogicalClosure;
use crate::expr::{ControlFlow, Error as ExprError, FlowResult, Kind, Param};
use crate::val::closure::BuiltinClosureObj;
use crate::val::{BoxInvokeFut, Closure, ClosureEvaluator, Value};

// =============================================================================
// ClosureExec - closure value creation
// =============================================================================

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
	/// Whether the body reads the row of the frame that invokes it, which its
	/// unit is then lowered to take.
	reads_row: bool,
	/// Set where a parameter its body reads was bound, where the closure was
	/// made, to a KNN query vector's row being scanned
	/// ([`Binding::Forbidden`](crate::ctx::Binding::Forbidden)): that binding
	/// holds no value `captures` could carry, and the body reads it from
	/// `captures` alone, so the closure carries the refusal instead, which an
	/// invocation raises. Making the closure, and passing it along, reads
	/// nothing.
	forbidden: Option<Arc<crate::exec::context::ForbiddenRow>>,
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
	/// carries — and captured the same values.
	/// This is what makes two `Closure::Expr` values equal. Any other builtin
	/// closure is a different closure.
	/// Equal closures behave alike when invoked: the same source over the same
	/// captures, and both or neither carrying a refusal of a KNN query vector's
	/// row being scanned, which `captures` cannot hold.
	fn dyn_eq(&self, other: &dyn BuiltinClosureObj) -> bool {
		match other.as_any().downcast_ref::<CompiledClosure>() {
			Some(other) => {
				self.sql == other.sql
					&& self.captures == other.captures
					&& self.forbidden.is_some() == other.forbidden.is_some()
			}
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
			reads_row: false,
			forbidden: None,
		}
	}

	/// The rows a unit run for the value is lowered to take: the invoking row
	/// where the body reads one.
	pub(crate) fn rows(&self) -> crate::exec::vm::program::RowBits {
		crate::exec::vm::program::RowBits {
			row: self.reads_row,
		}
	}

	/// This value, having captured a KNN query vector's row being scanned
	/// (`forbidden`) as a parameter its body reads.
	pub(crate) fn capturing_forbidden_row(
		mut self,
		forbidden: Arc<crate::exec::context::ForbiddenRow>,
	) -> Self {
		self.forbidden = Some(forbidden);
		self
	}

	/// The KNN query vector state whose row being scanned the closure
	/// captured, which an invocation is refused with.
	pub(crate) fn captured_forbidden_row(&self) -> Option<&crate::exec::context::ForbiddenRow> {
		self.forbidden.as_deref()
	}

	/// This value invoked with no call made: an invocation spends nothing of
	/// the computation-depth budget.
	pub(crate) fn without_call(mut self) -> Self {
		self.dives = false;
		self
	}

	/// This value with a body that reads the row of the frame that invokes it
	/// where `reads_row` holds.
	pub(crate) fn reading_row(mut self, reads_row: bool) -> Self {
		self.reads_row = reads_row;
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
			body: lower_expr(body.clone())?,
			body_sql,
		});
		let digest = ClosureDigest::of(&logical);
		let reads_row = crate::exec::ir::build::logical::closure_reads_row(&logical);
		Ok(Self {
			args: logical.args.clone(),
			returns: logical.returns.clone(),
			captures: captures.clone(),
			sql: logical.to_sql(),
			logical,
			digest,
			dives: true,
			reads_row,
			forbidden: None,
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

	/// Bind `args` to the declared parameters, coercing each to its kind and judging it against
	/// the named types the kind holds where `ctx` reaches the catalog. A parameter the
	/// invocation leaves out is judged as NONE, so it may be left out only where its kind, with
	/// the named types it holds, admits NONE.
	async fn bind_args(
		&self,
		ctx: &EvalContext<'_>,
		args: Vec<Value>,
	) -> anyhow::Result<Vec<(Strand, Value)>> {
		let values = bind_closure_args(&self.args, args)?;
		let refused = |param: &Param, e| ExprError::InvalidFunctionArguments {
			name: "ANONYMOUS".to_string(),
			message: format!("Failed to coerce argument {}: {e}", param.to_sql()),
		};
		let mut bound = Vec::with_capacity(values.len());
		let mut declared = self.args.iter();
		for value in values {
			let Some((param, kind)) = declared.next() else {
				break;
			};
			let value = crate::exec::vm::schema_host::check_types_held(ctx.exec_ctx, kind, value)
				.await
				.map_err(|e| refused(param, e))?;
			bound.push((param.clone().into_strand(), value));
		}
		for (param, kind) in declared {
			crate::exec::vm::schema_host::check_types_held(ctx.exec_ctx, kind, Value::None)
				.await
				.map_err(|e| refused(param, e))?;
		}
		Ok(bound)
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
	/// The body evaluates over the invoking frame's row: its document root,
	/// or its current value where it has no document root, which its fields,
	/// `$this` and `@` read. Nothing of the caller's names is read; the
	/// invocation supplies its arguments. A `RETURN` in the body is its value; `BREAK` and
	/// `CONTINUE` have no loop to reach and are errors.
	pub(crate) async fn invoke(
		&self,
		ctx: &EvalContext<'_>,
		args: Vec<Value>,
	) -> FlowResult<Value> {
		let evaluated = self.evaluate_body(ctx, args).await;
		self.returned(ctx, evaluated).await
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
		let bound = self.bind_args(ctx, args).await?;
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

	/// The body's value for an invocation from `ctx` whose linked unit no
	/// longer stands: it links the unit again and runs what it linked.
	/// The caller boxes this future, which keeps the native stack small when
	/// a body nests invocations up to the depth budget.
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
		let bound = match self.bind_args(ctx, args).await {
			Ok(bound) => bound,
			Err(e) => return Some(Err(e.into())),
		};
		let evaluated = match crate::exec::vm::closure::run_unit(self, ctx, bound).await {
			Invoked::Ran(ran) => ran,
			Invoked::Unlinked(bound) if relinked => Box::pin(self.relinked(ctx, bound)).await,
			Invoked::Unlinked(_) => return None,
		};
		Some(self.returned(ctx, evaluated).await)
	}

	/// The value of an invocation whose body evaluated to `evaluated`: a
	/// `RETURN` is the value, a loop signal an error, and the value is
	/// checked against the declared return kind and the named types it holds where `ctx` reaches
	/// the catalog.
	async fn returned(
		&self,
		ctx: &EvalContext<'_>,
		evaluated: FlowResult<Value>,
	) -> FlowResult<Value> {
		let result = match evaluated {
			Ok(v) => v,
			Err(ControlFlow::Return(v)) => v,
			Err(ControlFlow::Break) | Err(ControlFlow::Continue) => {
				return Err(ExecError::InvalidControlFlow.into());
			}
			Err(e) => return Err(e),
		};

		let value = validate_return("ANONYMOUS", self.returns.as_ref(), result)?;
		let Some(kind) = self.returns.as_ref() else {
			return Ok(value);
		};
		crate::exec::vm::schema_host::check_types_held(ctx.exec_ctx, kind, value).await.map_err(
			|e| {
				ControlFlow::from(anyhow::Error::new(ExecError::ReturnCoerce {
					name: "ANONYMOUS".to_string(),
					error: Box::new(e),
				}))
			},
		)
	}

	/// The frame an invocation from `ctx` runs the body's unit in. It is an
	/// isolated child of the caller's context, one budget unit deeper where
	/// the call spends one. It carries the request's parameters for the
	/// functions the body calls, binds the captures, then binds the arguments
	/// `bound`. The invoking row is not bound here; the unit reads it as
	/// its row. `units` is the closure units the invocation brings,
	/// if any. The frame computes the record the caller computes, where it does,
	/// and waives the reads' checks where the caller's reads waive them.
	pub(crate) fn frame(
		&self,
		ctx: &EvalContext<'_>,
		bound: Vec<(Strand, Value)>,
		units: Option<Arc<crate::exec::vm::closure::ClosureUnits>>,
	) -> FlowResult<ExecutionContext> {
		let mut isolated = Context::new_isolated(ctx.exec_ctx.ctx());
		isolated.inherit_request_params(ctx.exec_ctx.ctx());
		isolated.attach_captures(self.captures.clone())?;
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
				other @ (ControlFlow::Break | ControlFlow::Continue | ControlFlow::Return(_)) => {
					anyhow::Error::new(crate::err::EngineError::Internal(format!(
						"unexpected control flow from a closure body: {other}"
					)))
				}
			})
		})
	}
}

// =============================================================================
// ClosureCallExec - for invoking closures stored in parameters
// =============================================================================
