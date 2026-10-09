//! Running a call of a user-defined function and a read of a parameter.
//!
//! A call enters the function by reading its definition and comparing its hash
//! with the one the program was lowered from: the body, the unit it runs, and
//! the predicate that judges its `PERMISSIONS` clause were all lowered when
//! the program was planned, so a definition that hashes otherwise is an
//! internal error and the call fails closed. Nothing here binds, plans or
//! lowers.
//!
//! A read of a parameter resolves a name the way the operator tree's
//! [`Param`](crate::exec::physical_expr::Param) does — the row's `$this`, the
//! frame's bindings, the write bindings, the context, `$parent` — and then
//! reads `DEFINE PARAM`, judging the definition's `PERMISSIONS` clause with
//! the predicate the program holds for it, or with the parameter's unit where
//! the definition holds another clause ([`super::param_unit`]).

use std::sync::Arc;

use surrealdb_strand::Strand;

use super::UnitTable;
use super::host::{admitted_unit_as, gather_program, host_scalar};
use super::interp::FunctionMemo;
use super::program::{DefHash, FunctionSpec, HostScalar, ParamSpec, Program};
use super::services::{Admission, EngineServices, Services};
use crate::catalog::FunctionDefinition;
use crate::dbs::capabilities::Error as CapabilitiesError;
use crate::exec::operators::permission_frame;
use crate::exec::physical_expr::function::helpers::{validate_arg_count, validate_return};
use crate::exec::physical_expr::{EnteredCall, EvalContext};
use crate::exec::{Error as ExecError, ExecutionContext, FlowResult};
use crate::expr::logical::{GuardKind, LogicalPermissionRef};
use crate::expr::{ControlFlow, Error as ExprError};
use crate::iam::Action;
use crate::val::Value;

thread_local! {
	/// The hashes of the definitions this thread entered most recently, by
	/// allocation: the catalog hands out one allocation per definition, which
	/// is immutable, so its hash is hashed once rather than on every run of
	/// every program that calls it. An entry holds its definition, so an
	/// address is never reused for another.
	static HASHES: std::cell::RefCell<Vec<(Arc<FunctionDefinition>, DefHash)>> =
		const { std::cell::RefCell::new(Vec::new()) };
}

/// How many definitions [`HASHES`] holds.
const HASHED: usize = 16;

/// The hash of `definition`, answered from [`HASHES`] where this thread has
/// hashed that allocation before.
fn hash_of(definition: &Arc<FunctionDefinition>) -> DefHash {
	HASHES.with(|hashes| {
		let mut hashes = hashes.borrow_mut();
		if let Some((_, hash)) = hashes.iter().find(|(held, _)| Arc::ptr_eq(held, definition)) {
			return *hash;
		}
		let hash = DefHash::of_function(definition);
		if hashes.len() == HASHED {
			hashes.remove(0);
		}
		hashes.push((Arc::clone(definition), hash));
		hash
	})
}

thread_local! {
	/// The principals this thread ran function bodies under most recently, by
	/// the caller's principal and the definition whose `AUTH LIMIT` narrowed
	/// it, each held so that no other can take its address.
	static NARROWED: std::cell::RefCell<Vec<(Arc<FunctionDefinition>, Narrowed)>> =
		const { std::cell::RefCell::new(Vec::new()) };
}

/// The principal a definition's body runs under for one caller.
///
/// The principal is the caller's narrowed by the definition's `AUTH LIMIT`,
/// which those two alone decide, and an allocation of [`crate::iam::Auth`] is
/// never changed: an answer for one caller allocation and one definition
/// allocation is the answer for every call that pairs them again. A caller the
/// limit leaves as it is runs the body as itself, the same allocation, so
/// the body's own calls pair it again.
#[derive(Clone)]
pub(super) struct Narrowed {
	/// The caller's principal, which the answer is for.
	caller: Arc<crate::iam::Auth>,
	/// The principal the body runs under.
	limited: Arc<crate::iam::Auth>,
}

/// How many principals [`NARROWED`] holds.
const NARROWINGS: usize = 8;

/// The principal `caller`'s call of `definition` runs its body under: answered
/// from [`NARROWED`] where this thread derived it before, and derived (and
/// refused, where the limit does not parse) otherwise.
fn narrowed_for(
	caller: &Arc<crate::iam::Auth>,
	definition: &Arc<FunctionDefinition>,
	func_name: &str,
) -> FlowResult<Narrowed> {
	let held = NARROWED.with(|held| {
		held.borrow()
			.iter()
			.find(|(def, narrowed)| {
				Arc::ptr_eq(def, definition) && Arc::ptr_eq(&narrowed.caller, caller)
			})
			.map(|(_, narrowed)| narrowed.clone())
	});
	if let Some(held) = held {
		return Ok(held);
	}
	let limit = crate::iam::AuthLimit::try_from(&definition.auth_limit)
		.map_err(|e| anyhow::anyhow!("Invalid auth limit on function '{}': {}", func_name, e))?;
	let narrowed = caller.new_limited(&limit);
	let limited = if narrowed == **caller {
		Arc::clone(caller)
	} else {
		Arc::new(narrowed)
	};
	let narrowed = Narrowed {
		caller: Arc::clone(caller),
		limited,
	};
	NARROWED.with(|held| {
		let mut held = held.borrow_mut();
		if held.len() == NARROWINGS {
			held.remove(0);
		}
		held.push((Arc::clone(definition), narrowed.clone()));
	});
	Ok(narrowed)
}

fn internal(why: String) -> ControlFlow {
	ControlFlow::Err(anyhow::Error::new(crate::err::EngineError::Internal(why)))
}

/// Whether `ctx` is a frame `frame` admits; see [`super::host::frame_admits`].
fn admits(frame: &super::program::UnitFrame, ctx: &ExecutionContext) -> bool {
	super::host::frame_admits(frame, ctx)
}

/// Enter the function `spec` calls, from `ctx`, the caller's evaluation:
/// the database it is defined in, the capability to call it, its definition,
/// the definer's `AUTH LIMIT` and its `PERMISSIONS`, checked under that limit.
/// A call in a frame that skips permissions is the definer's and is not
/// checked.
///
/// `known` is what an earlier entry of the run learned of the function: the
/// definition it found to hash as the program expects, so a definition read
/// back as the same allocation is not hashed again, and the principal the body
/// ran under for the caller's auth. Answers the call entered and the digest of
/// the principal its body runs under.
pub(super) async fn enter(
	program: &Program,
	spec: &FunctionSpec,
	known: &mut FunctionMemo,
	ctx: &EvalContext<'_>,
) -> FlowResult<(EnteredCall, u64)> {
	let func_name = spec.qualified.as_str();
	// Require database context (propagate core Error so it downcasts to Validation).
	ctx.exec_ctx.database().map_err(|e| ControlFlow::Err(anyhow::Error::new(e)))?;
	if !admits(&spec.frame, ctx.exec_ctx) {
		return Err(internal(format!(
			"fn::{} was lowered for a frame other than the one it is called in",
			spec.name
		)));
	}
	if !ctx.capabilities().allows_function_name(func_name) {
		return Err(anyhow::Error::new(CapabilitiesError::FunctionNotAllowed(
			func_name.to_owned(),
		))
		.into());
	}
	// A failed read is reported as `Function '<name>' not found: <reason>`,
	// the wording clients match on, and in code a write reaches as the
	// reason alone ([`crate::exec::context::RootContext::writing`]).
	let writing = ctx.exec_ctx.root().writing;
	let definition = EngineServices::new(ctx.exec_ctx)
		.read_function(&spec.name, ctx.exec_ctx.version_stamp())
		.await
		.map_err(|e| match e {
			ControlFlow::Err(e) if !writing => {
				ControlFlow::Err(anyhow::anyhow!("Function '{func_name}' not found: {e}"))
			}
			other => other,
		})?;
	let stands = known.definition.as_ref().is_some_and(|known| Arc::ptr_eq(known, &definition));
	if !stands {
		if !spec.defined || hash_of(&definition) != spec.def_hash {
			return Err(internal(format!(
				"fn::{} was redefined after the program that calls it was lowered",
				spec.name
			)));
		}
		known.definition = Some(Arc::clone(&definition));
		known.principal = None;
	}
	// Apply auth limiting: cap the caller's privileges to the definer's auth level.
	let caller = &ctx.exec_ctx.root().auth;
	if !matches!(&known.principal, Some(held) if Arc::ptr_eq(&held.caller, caller)) {
		known.principal = Some(narrowed_for(caller, &definition, func_name)?);
	}
	let held = known.principal.as_ref().expect("a principal derived above");
	let limited = ctx.exec_ctx.with_principal(&held.limited);
	// A caller evaluated one expression at a time spends the way to the call,
	// its arguments evaluated beneath it ([`super::depth`]).
	let limited = match limited.per_expression_depth() {
		true => limited.dive_by(spec.depth.site).map_err(ControlFlow::Err)?,
		false => limited,
	};
	// The body's units admit the class of the narrowed principal in the
	// database the call runs in.
	let principal = super::host::class_digest_here(&limited);
	if !ctx.skip_fetch_perms && limited.should_check_perms(Action::View)? {
		let denied = || ExecError::FunctionPermissions {
			name: func_name.to_owned(),
		};
		match definition.permissions_body.logical_ref()? {
			LogicalPermissionRef::Full => {}
			LogicalPermissionRef::None => return Err(denied().into()),
			LogicalPermissionRef::Specific(_) => {
				let Some((hash, pipeline, scope)) = spec.guard else {
					return Err(internal(format!(
						"fn::{} has a permission predicate the program did not lower",
						spec.name
					)));
				};
				if hash != spec.def_hash {
					return Err(internal(format!(
						"the permission predicate of fn::{} was lowered from another definition",
						spec.name
					)));
				}
				crate::exec::guard_verdicts::judged(
					&limited,
					scope,
					crate::exec::guard_verdicts::GuardKind::Function,
					&spec.name,
					hash.0,
					|| judge(program, pipeline, &limited, ctx.current_value, denied),
				)
				.await?;
			}
		}
	}
	Ok((
		EnteredCall {
			definition,
			limited,
		},
		principal,
	))
}

/// Judge a permission predicate: pipeline `pipeline` of the program's host
/// code, evaluated over `row` in the permission frame of `ctx`, in which
/// reads are not themselves permission-checked and writes are refused. A
/// predicate that raises anything but an error denies; an error keeps its
/// type.
pub(super) async fn judge(
	program: &Program,
	pipeline: u16,
	ctx: &ExecutionContext,
	row: Option<&Value>,
	denied: impl Fn() -> ExecError,
) -> FlowResult<()> {
	let frame = permission_frame(ctx, GuardKind::Read, None);
	match host_scalar(program, &HostScalar::Code(pipeline), &frame, row).await {
		Ok(verdict) if verdict.is_truthy() => Ok(()),
		Ok(_) => Err(denied().into()),
		Err(ControlFlow::Err(e)) => Err(ControlFlow::Err(e)),
		Err(_) => Err(denied().into()),
	}
}

/// The arguments `evaluated` bound as the entered definition declares them:
/// counted, then each coerced to its parameter's kind.
pub(super) fn bind_args(
	spec: &FunctionSpec,
	entered: &EnteredCall,
	evaluated: Vec<Value>,
) -> FlowResult<Vec<Value>> {
	let func_name = spec.qualified.as_str();
	let definition = &entered.definition;
	validate_arg_count(func_name, evaluated.len(), &definition.args)?;
	let mut bound = Vec::with_capacity(evaluated.len());
	for ((param_name, kind), arg_value) in definition.args.iter().zip(evaluated) {
		let coerced =
			arg_value.coerce_to_kind(kind).map_err(|e| ExprError::InvalidFunctionArguments {
				name: func_name.to_owned(),
				message: format!("Failed to coerce argument `${param_name}`: {e}"),
			})?;
		bound.push(coerced);
	}
	Ok(bound)
}

/// `result`, what the body returned, validated and coerced to the entered
/// definition's return kind.
pub(super) fn returned(
	spec: &FunctionSpec,
	entered: &EnteredCall,
	result: Value,
) -> FlowResult<Value> {
	Ok(validate_return(&spec.qualified, entered.definition.returns.as_ref(), result)?)
}

/// Complete the call entered as `entered`, from `ctx`, the caller's
/// evaluation, with its arguments evaluated: bind them as the definition
/// declares, run the body's unit one re-entry deeper under the definer's
/// auth, and coerce what it returns.
///
/// The unit is `spec.unchecked_body` where the call is evaluated with
/// permissions skipped and the body was bound for a checked principal, and
/// `spec.body` otherwise; a call whose program lowered neither was lowered
/// past the computation-depth budget and raises the error that names it. `units` is the table the
/// unit ids of the program that holds `spec` index.
pub(super) async fn call(
	units: UnitTable,
	spec: &FunctionSpec,
	entered: &EnteredCall,
	principal: u64,
	ctx: &EvalContext<'_>,
	evaluated: Vec<Value>,
) -> FlowResult<Value> {
	let bound = bind_args(spec, entered, evaluated)?;
	// A body evaluated one expression at a time spends the way to its first
	// write as it is entered, and needs what it spends at its deepest left
	// inside the re-entry the call spends; one entered that way from code
	// that is not spends the call's planner depth, and one more for the body
	// as a block ([`super::depth`]).
	let entering = |limited: &ExecutionContext| {
		limited.check_dive_by(spec.depth.deepest.saturating_add(1))?;
		limited.dive_by(spec.depth.body)
	};
	let limited_ctx = &if entered.limited.per_expression_depth() {
		entering(&entered.limited).map_err(ControlFlow::Err)?
	} else if spec.depth.writes {
		let block = entered
			.limited
			.entering_stored_code(spec.plan_depth + 1, true)
			.dive_by(1)
			.map_err(ControlFlow::Err)?;
		entering(&block).map_err(ControlFlow::Err)?
	} else {
		entered.limited.clone()
	};
	let skips = ctx.skip_fetch_perms;
	let unit = match (skips && spec.checked, spec.unchecked_body, spec.body) {
		// A call lowered past the computation-depth budget holds no body.
		(_, None, None) => return Err(ExecError::ComputationDepthExceeded.into()),
		(true, Some(unit), _) | (false, _, Some(unit)) => unit,
		(true, None, _) => {
			return Err(internal(format!(
				"fn::{} runs where permissions are skipped and has no body bound for a frame that waives them",
				spec.name
			)));
		}
		(false, Some(_), None) => {
			return Err(internal(format!("fn::{} is called and has no body to run", spec.name)));
		}
	};
	let (body, table) = entry_of(
		&units,
		unit,
		limited_ctx,
		skips || limited_ctx.root().skip_fetch_perms,
		principal,
	)?;
	if spec.light {
		let value = call_light(&body, limited_ctx, ctx, bound).await?;
		return returned(spec, entered, value);
	}
	// The body runs one re-entry deeper, the budget being the context's,
	// consumed per call, in the frame the call was reached in, as a scalar
	// subquery of the call: `$this` and `$parent` denote the caller's row,
	// which is also the document its statements read, and a `RETURN` in the
	// body surfaces as a signal for the caller to unwrap. The arguments are
	// the parameters of the context it runs in, which also holds the table its
	// nested programs find their units in. The body's evaluation is built from
	// that context alone, so none of the caller's block-local parameters or
	// write bindings reach it.
	let row = ctx.current_value.map(|row| shared_row(limited_ctx, row));
	let running = limited_ctx.entering_call_body(
		entered
			.definition
			.args
			.iter()
			.zip(bound)
			.map(|((name, _), value)| (Strand::from(name.as_str()), value)),
		table.unwrap_or(units),
		row.as_ref(),
		skips,
		ctx.computing_record.as_ref(),
	)?;
	let result = crate::exec::trampoline::nest(gather_program(body, running)).await;
	let result = match result {
		Ok(v) => v,
		Err(ControlFlow::Return(v)) => v,
		Err(ControlFlow::Break) | Err(ControlFlow::Continue) => {
			// BREAK/CONTINUE inside a function (outside of loop) is an error.
			return Err(ExecError::InvalidControlFlow.into());
		}
		Err(e) => return Err(e),
	};
	returned(spec, entered, result)
}

/// The value `body`, a light body ([`FunctionSpec::light`]), computes for
/// arguments `bound`: run in `limited`, the frame the call entered, one
/// re-entry deeper, over the caller's row where `ctx` holds one, with the
/// arguments in its slots, on a state of its own. The body reads nothing of
/// the frame but what those slots and the row hold, so no frame is built for
/// it, and it runs in the caller's task — in place where it hands the host no
/// step. The call spends the re-entry a called body spends, so what the body
/// re-enters — the query an `eval` in it plans and runs — spends the
/// computation-depth budget from there, and a `RETURN` in the body is its
/// value.
async fn call_light(
	body: &Arc<Program>,
	limited: &ExecutionContext,
	ctx: &EvalContext<'_>,
	bound: Vec<Value>,
) -> FlowResult<Value> {
	use std::borrow::Cow;
	let deeper = limited.dive_by(1).map_err(ControlFlow::Err)?;
	let limited = &deeper;
	let state = |slots: Vec<Value>| -> FlowResult<_> {
		let mut state = super::host::unit_state(body, limited)?;
		state.permissions_bypassed = ctx.skip_fetch_perms;
		state.rowless = ctx.current_value.is_none();
		state.slots = slots;
		Ok(state)
	};
	let row = ctx.current_value.map_or(Cow::Owned(Value::None), Cow::Borrowed);
	let pipeline = &body.pipelines[0];
	let mut inline = state(bound)?;
	let (row, start, mut state) =
		match super::host::run_unit_inline(body, pipeline, row, &mut inline) {
			Ok(_) => return Ok(inline.output.pop().unwrap_or(Value::None)),
			Err(unfinished) => match unfinished.resume {
				Some(start) => (unfinished.row, start, inline),
				None => {
					let slots = std::mem::take(&mut inline.slots);
					(unfinished.row, 0, state(slots)?)
				}
			},
		};
	super::host::host_frames(body, &mut state, limited);
	match super::host::out_of_line(|| {
		super::host::run_cow_row_from(body, pipeline, row, &mut state, limited, start)
	})
	.await
	{
		Ok(_) => Ok(state.output.pop().unwrap_or(Value::None)),
		Err(ControlFlow::Return(value)) => Ok(value),
		Err(ControlFlow::Break | ControlFlow::Continue) => {
			Err(ExecError::InvalidControlFlow.into())
		}
		Err(error) => Err(error),
	}
}

/// `row`, the caller's row, as the allocation a body binds as `$parent` and
/// `$this` and reads as its document: the one the context already holds for
/// that row where it does, so a call made from within a body that runs over
/// the row does not copy it again, and a copy otherwise.
fn shared_row(ctx: &ExecutionContext, row: &Value) -> Arc<Value> {
	match ctx.current_row_shared() {
		Some(held) if std::ptr::eq(&**held, row) => Arc::clone(held),
		_ => Arc::new(row.clone()),
	}
}

/// The program that runs body unit `unit` of `units` for `ctx`, and the unit
/// table its ids index when that is not `units`: a unit that holds an arena
/// of its own is a bundle, run through the body it names.
fn entry_of(
	units: &UnitTable,
	unit: super::program::UnitId,
	ctx: &ExecutionContext,
	skips: bool,
	principal: u64,
) -> FlowResult<(Arc<Program>, Option<UnitTable>)> {
	let held = admitted_unit_as(units, unit, ctx, skips, |_| principal)?;
	match held.unit.as_ref().and_then(|head| head.entry) {
		Some(entry) => {
			let body = held
				.units
				.get(entry.0 as usize)
				.ok_or_else(|| internal(format!("a bundle that holds no body at {}", entry.0)))?;
			Ok((Arc::clone(body), Some(Arc::clone(&held.units))))
		}
		None => Ok((Arc::clone(held), None)),
	}
}

/// The value of parameter `spec` in `ctx` where the frame answers it, resolved
/// in the order the module documents; `None` where only `DEFINE PARAM` can, for
/// [`stored_param`] to read.
pub(super) fn frame_param(spec: &ParamSpec, ctx: &EvalContext<'_>) -> Option<Value> {
	let name = spec.name.as_str();
	// Handle special params $this and $self. When the current value is
	// available (per-row context), use it directly. When it is not (scalar
	// context, e.g. a subquery's FROM clause), a `$this` the subquery bound as
	// a parameter is read before defaulting to NONE.
	if matches!(name, "this" | "self") {
		if let Some(v) = ctx.current_value {
			return Some(v.clone());
		}
		if let Some(local_params) = ctx.local_params
			&& let Some(v) = local_params.get(name)
		{
			return Some(v.clone());
		}
		if let Some(v) = ctx.exec_ctx.value(name) {
			return Some(v.clone());
		}
		return Some(Value::None);
	}
	// Block-local parameters shadow global ones.
	if let Some(local_params) = ctx.local_params
		&& let Some(value) = local_params.get(name)
	{
		return Some(value.clone());
	}
	// A write-path binding sits between the two: a `LET $value = ...` in the
	// body being evaluated is an inner scope and wins, while the binding
	// shadows a session or database parameter of the same name.
	if let Some(value) = ctx.write_binding(name) {
		return Some(value.clone());
	}
	// The context's parameter chain, including the protected `$auth`,
	// `$access`, `$token` and `$session`.
	if let Some(v) = ctx.exec_ctx.value(name) {
		return Some(v.clone());
	}
	// `$parent` falls back to the document root when not explicitly bound,
	// except in code a write reaches, where an unbound `$parent` is `NONE`
	// ([`crate::exec::context::RootContext::writing`]).
	if name == "parent" {
		if ctx.exec_ctx.root().writing {
			return Some(Value::None);
		}
		if let Some(v) = ctx.document_root {
			return Some(v.clone());
		}
	}
	None
}

/// The value of `DEFINE PARAM` for `spec` in `ctx`, which no frame answers.
pub(super) async fn stored_param(
	program: &Program,
	spec: &ParamSpec,
	ctx: &EvalContext<'_>,
) -> FlowResult<Value> {
	// Read it from the database.
	if ctx.exec_ctx.database().is_ok() {
		return Ok(read_db_param(program, spec, ctx).await?);
	}
	// Without a database context but with options naming one, look it up by
	// name.
	if let Some(opts) = ctx.exec_ctx.options() {
		let ns_name = match opts.ns() {
			Ok(ns) => ns,
			Err(_) => return Err(ExecError::NsEmpty.into()),
		};
		let db_name = match opts.db() {
			Ok(db) => db,
			Err(_) => return Err(ExecError::DbEmpty.into()),
		};
		// A database-defined param's predicate may dereference records,
		// whose fetch needs a database-level context. Root and namespace
		// level execution carry none, so the param is read under one built
		// from its own namespace and database.
		if let Ok(Some((ns_def, db_def))) = crate::exec::vm::descriptor::database(
			ctx.exec_ctx.ctx(),
			ns_name,
			db_name,
			ctx.exec_ctx.version_stamp(),
		)
		.await
		{
			let db_level = ctx.exec_ctx.with_database(ns_def, db_def);
			let db_ctx = EvalContext {
				exec_ctx: &db_level,
				..ctx.clone()
			};
			return Ok(read_db_param(program, spec, &db_ctx).await?);
		}
		// The database does not exist yet: the parameter cannot be found.
		return Ok(Value::None);
	}
	// No options available and the parameter not found locally.
	Err(anyhow::anyhow!("Parameter not found: ${}", spec.name).into())
}

/// Read `DEFINE PARAM` for `spec` in the database `ctx` runs in, and judge the
/// definition's `PERMISSIONS` clause.
///
/// A predicate is judged on every read — it reads the row the parameter is
/// read on — with the pipeline the program lowered for the clause the
/// definition had then, or, for a clause hashing otherwise, with the
/// parameter's unit ([`super::param_unit`]).
async fn read_db_param(
	program: &Program,
	spec: &ParamSpec,
	ctx: &EvalContext<'_>,
) -> anyhow::Result<Value> {
	let db_ctx = ctx.exec_ctx.database()?;
	let found = EngineServices::new(ctx.exec_ctx)
		.read_db_param(
			db_ctx.ns_ctx.ns.namespace_id,
			db_ctx.db.database_id,
			spec.name.as_str(),
			ctx.exec_ctx.version_stamp(),
		)
		.await?;
	let Some(found) = found else {
		return Ok(Value::None);
	};
	if !ctx.skip_fetch_perms && ctx.exec_ctx.should_check_perms(Action::View)? {
		let denied = || ExecError::ParamPermissions {
			name: spec.name.to_string(),
		};
		match found.admission {
			Admission::Full => {}
			Admission::None => return Err(denied().into()),
			Admission::Predicate => {
				// A clause other than the one the program lowered is judged by
				// the parameter's unit, compiled from the clause read now.
				let unit;
				let (program, pipeline, scope) = match spec.guard {
					Some((hash, pipeline, scope)) if hash == found.permission => {
						(program, pipeline, scope)
					}
					_ => {
						let Some(loaded) =
							super::param_unit::guard(spec.name.as_str(), ctx.exec_ctx)
								.await
								.map_err(|flow| match flow {
									ControlFlow::Err(e) => e,
									_ => anyhow::Error::new(denied()),
								})?
						else {
							return Err(anyhow::Error::new(denied()));
						};
						unit = loaded;
						(&*unit, 0, crate::exec::guard_verdicts::GuardScope::PerCall)
					}
				};
				crate::exec::guard_verdicts::judged(
					ctx.exec_ctx,
					scope,
					crate::exec::guard_verdicts::GuardKind::Param,
					spec.name.as_str(),
					found.permission.0,
					|| judge(program, pipeline, ctx.exec_ctx, ctx.current_value, denied),
				)
				.await
				.map_err(|flow| match flow {
					ControlFlow::Err(e) => e,
					// A guard raises nothing else: `judge` turns every other
					// signal into a denial before it returns.
					_ => anyhow::Error::new(denied()),
				})?;
			}
		}
	}
	Ok(found.value)
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::dbs::Session;
	use crate::exec::operators::test_util::TestDb;
	use crate::iam::{Auth, Role};

	const DEFINED: &str = "DEFINE FUNCTION OVERWRITE fn::f() { RETURN 1; };";

	/// The definition of `fn::f` as `session` defined it.
	async fn defined_by(db: &TestDb, session: &Session) -> Arc<FunctionDefinition> {
		db.run_as(session, DEFINED).await;
		let ctx = db.exec_ctx().await;
		let Ok(definition) = EngineServices::new(&ctx).read_function("f", None).await else {
			panic!("the function was defined");
		};
		definition
	}

	fn narrowed(caller: &Arc<Auth>, definition: &Arc<FunctionDefinition>) -> Narrowed {
		let Ok(narrowed) = narrowed_for(caller, definition, "fn::f") else {
			panic!("the limit parses");
		};
		narrowed
	}

	#[tokio::test]
	async fn a_caller_the_limit_leaves_alone_runs_the_body_as_itself() {
		let db = TestDb::new_with_auth("").await;
		let definition = defined_by(&db, &TestDb::owner()).await;
		let caller = Arc::new(Auth::for_root(Role::Owner));
		let first = narrowed(&caller, &definition);
		assert!(Arc::ptr_eq(&first.limited, &caller));
		let again = narrowed(&caller, &definition);
		assert!(Arc::ptr_eq(&again.limited, &caller));
	}

	#[tokio::test]
	async fn the_limit_of_the_definition_read_narrows_the_caller() {
		let db = TestDb::new_with_auth("").await;
		let editor = Session::editor().with_ns("test").with_db("test");
		let limited_def = defined_by(&db, &editor).await;
		let caller = Arc::new(Auth::for_root(Role::Owner));

		let first = narrowed(&caller, &limited_def);
		assert!(!Arc::ptr_eq(&first.limited, &caller));
		assert!(first.limited.has_role(Role::Editor));
		assert!(!first.limited.has_role(Role::Owner));
		// Narrowed within its class, the body runs units of the caller's class.
		let class = |auth: &Auth| {
			super::super::program::UnitFrame::digest(true, Some(auth), Some("test"), Some("test"))
		};
		assert_eq!(class(&first.limited), class(&caller));

		// The same pairing is answered by the allocation it answered before.
		let again = narrowed(&caller, &limited_def);
		assert!(Arc::ptr_eq(&again.limited, &first.limited));

		// A redefinition is another allocation, and is not narrowed as the
		// definition it replaced was.
		let owner_def = defined_by(&db, &TestDb::owner()).await;
		assert!(!Arc::ptr_eq(&owner_def, &limited_def));
		let redefined = narrowed(&caller, &owner_def);
		assert!(Arc::ptr_eq(&redefined.limited, &caller));
		let still = narrowed(&caller, &limited_def);
		assert!(Arc::ptr_eq(&still.limited, &first.limited));

		// A caller below the limit is never raised to it, and another caller
		// of the same definition is narrowed from its own principal.
		let viewer = Arc::new(Auth::for_root(Role::Viewer));
		let kept = narrowed(&viewer, &limited_def);
		assert!(Arc::ptr_eq(&kept.limited, &viewer));
		assert!(!kept.limited.has_role(Role::Editor));
		let other = Arc::new(Auth::for_root(Role::Owner));
		let alike = narrowed(&other, &limited_def);
		assert_eq!(*alike.limited, *first.limited);
	}

	#[tokio::test]
	async fn a_limit_that_does_not_parse_is_refused_on_every_call() {
		let db = TestDb::new_with_auth("").await;
		let definition = defined_by(&db, &TestDb::owner()).await;
		let mut broken = (*definition).clone();
		broken.auth_limit.role = Some("no such role".to_owned());
		let broken = Arc::new(broken);
		let caller = Arc::new(Auth::for_root(Role::Owner));
		for _ in 0..3 {
			assert!(narrowed_for(&caller, &broken, "fn::f").is_err());
		}
	}
}
