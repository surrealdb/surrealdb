//! Running a call of code the host runs: an embedded script, a model or a
//! module export ([`ExternalSpec`]).
//!
//! [`enter`] makes the checks a call makes before it evaluates its arguments —
//! the capability, the definition, its `PERMISSIONS` clause — and [`call`] runs
//! the callee with them, both through [`Services`]. The permission predicate
//! was lowered when the program was planned; a definition whose clause hashes
//! otherwise than the one it was lowered from is an internal error and the
//! call fails closed. Nothing here binds, plans or lowers.

#[cfg(feature = "surrealism")]
use super::program::DefHash;
use super::program::{ExternalCallee, ExternalSpec, Program};
use super::services::{EngineServices, ModelDefinition, ModuleExport, Services};
use crate::exec::{EvalContext, FlowResult};
use crate::expr::ControlFlow;
use crate::val::Value;

/// A call entered by [`enter`], held until [`call`] makes it.
pub(super) enum Entered {
	Script,
	Model(ModelDefinition),
	Module(ModuleExport),
	Function,
}

fn internal(why: String) -> ControlFlow {
	ControlFlow::Err(anyhow::Error::new(crate::err::EngineError::Internal(why)))
}

/// Enter the call `spec` describes from `ctx`, before its arguments are
/// evaluated.
pub(super) async fn enter(
	program: &Program,
	spec: &ExternalSpec,
	ctx: &EvalContext<'_>,
) -> FlowResult<Entered> {
	let services = EngineServices::new(ctx.exec_ctx);
	// Only a module call judges its permissions against the program, and that needs Surrealism.
	#[cfg(not(feature = "surrealism"))]
	let _ = program;
	match &spec.callee {
		ExternalCallee::Script {
			..
		} => {
			services.check_script(ctx)?;
			Ok(Entered::Script)
		}
		ExternalCallee::Model {
			name,
			version,
		} => {
			let definition = services.read_model(name, version, ctx).await?;
			Ok(Entered::Model(definition))
		}
		ExternalCallee::Module {
			storage,
			sub,
			silo,
		} => {
			let definition = services.read_module(storage, sub.as_deref(), *silo, ctx).await?;
			#[cfg(feature = "surrealism")]
			judge(
				program,
				spec,
				&definition.permissions,
				&definition.permissions_body,
				storage,
				ctx,
			)
			.await?;
			let export = services.module_export(&definition, sub.as_deref(), ctx).await?;
			Ok(Entered::Module(export))
		}
		ExternalCallee::Function {
			..
		} => Ok(Entered::Function),
	}
}

/// Make the call `spec` describes, entered as `entered`, with `args` from
/// `ctx`.
/// `held` is the call unit the site linked at its last call, for a call of a
/// user-defined function ([`super::call_unit::Linked`]).
pub(super) async fn call(
	spec: &ExternalSpec,
	entered: Entered,
	args: Vec<Value>,
	ctx: &EvalContext<'_>,
	held: &mut Option<super::call_unit::Linked>,
) -> FlowResult<Value> {
	let services = EngineServices::new(ctx.exec_ctx);
	match (&spec.callee, entered) {
		(
			ExternalCallee::Script {
				source,
			},
			Entered::Script,
		) => services.call_script_dynamic(source, spec.plan_depth, args, ctx).await,
		(
			ExternalCallee::Model {
				name,
				version,
			},
			Entered::Model(definition),
		) => services.compute_model(name, version, &definition, args, ctx).await,
		(
			ExternalCallee::Module {
				storage,
				sub,
				..
			},
			Entered::Module(export),
		) => services.call_module(storage, &export, sub.as_deref(), args, ctx).await,
		(
			ExternalCallee::Function {
				name,
			},
			Entered::Function,
		) => super::call_unit::call(name, spec.plan_depth, args, ctx, held).await,
		_ => Err(internal("an external call made with another call's entry".to_owned())),
	}
}

/// Judge the `PERMISSIONS` clause of the definition a call of `name` read —
/// `permission` as the catalog stores it, `body` in logical form — for
/// `ctx`, unless the call is the definer's: in a frame that skips
/// permissions, or for a session whose reads are not checked.
#[cfg(feature = "surrealism")]
async fn judge(
	program: &Program,
	spec: &ExternalSpec,
	permission: &crate::catalog::Permission,
	body: &crate::catalog::LogicalPermissionBody,
	name: &str,
	ctx: &EvalContext<'_>,
) -> FlowResult<()> {
	if ctx.skip_fetch_perms || !ctx.exec_ctx.should_check_perms(crate::iam::Action::View)? {
		return Ok(());
	}
	use crate::expr::logical::LogicalPermissionRef;
	let denied = || crate::exec::Error::FunctionPermissions {
		name: crate::catalog::extension_name(name).into_owned(),
	};
	match body.logical_ref()? {
		LogicalPermissionRef::Full => Ok(()),
		LogicalPermissionRef::None => Err(denied().into()),
		LogicalPermissionRef::Specific(_) => {
			let Some((hash, pipeline, scope)) = spec.guard else {
				return Err(internal(format!(
					"{name} has a permission predicate the program did not lower"
				)));
			};
			if hash != DefHash::of_permission(permission) {
				return Err(internal(format!(
					"the permission predicate of {name} was lowered from another definition"
				)));
			}
			let kind = match &spec.callee {
				super::program::ExternalCallee::Model {
					..
				} => crate::exec::guard_verdicts::GuardKind::Model,
				super::program::ExternalCallee::Script {
					..
				}
				| super::program::ExternalCallee::Module {
					..
				}
				| super::program::ExternalCallee::Function {
					..
				} => crate::exec::guard_verdicts::GuardKind::Module,
			};
			crate::exec::guard_verdicts::judged(ctx.exec_ctx, scope, kind, name, hash.0, || {
				super::function::judge(
					program,
					pipeline,
					ctx.exec_ctx,
					ctx.value_in_scope().within_context(),
					denied,
				)
			})
			.await
		}
	}
}
