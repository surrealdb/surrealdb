//! Lowering calls of code the host runs: embedded scripts, machine-learning
//! models and module exports.
//!
//! A call is [`Instr::ExternalEnter`], its arguments, then
//! [`Instr::ExternalCall`]: the operator tree's call checks what it may call,
//! reads a definition and judges its permission before it evaluates its
//! arguments, and the two instructions keep that order. A model or module
//! definition's `PERMISSIONS` predicate is lowered here, from the definition
//! the catalog holds when the program is planned; nothing is compiled when the
//! call runs. A script's source is data the host's script runtime runs.

use super::{Lowered, Lowering, NotCovered};
use crate::exec::vm::program::{DefHash, ExternalCallee, ExternalSpec, Instr, Operand};
use crate::expr::Function;
use crate::expr::logical::{LogicalCall, LogicalExpr, LogicalPermissionRef};

impl Lowering<'_, '_> {
	/// What `call` calls, when it is a call of code the host runs.
	pub(super) fn external_callee(call: &LogicalCall) -> Option<ExternalCallee> {
		match &call.receiver {
			Function::Script(script) => Some(ExternalCallee::Script {
				source: script.0.clone(),
			}),
			Function::Model(model) => Some(ExternalCallee::Model {
				name: model.name.to_string(),
				version: model.version.to_string(),
			}),
			Function::Module(_, sub) => Some(ExternalCallee::Module {
				storage: crate::exec::vm::plan_catalog::module_storage(&call.receiver)?,
				sub: sub.clone(),
				silo: false,
			}),
			Function::Silo {
				sub,
				..
			} => Some(ExternalCallee::Module {
				storage: crate::exec::vm::plan_catalog::module_storage(&call.receiver)?,
				sub: sub.clone(),
				silo: true,
			}),
			Function::Normal(_) | Function::Custom(_) => None,
		}
	}

	/// `expr`, a call of code the host runs, as [`Instr::ExternalEnter`], its
	/// arguments in turn, and [`Instr::ExternalCall`]: its value's register.
	pub(super) async fn external_call(
		&mut self,
		expr: &LogicalExpr,
		call: &LogicalCall,
	) -> Lowered<Operand> {
		// A user-defined function a late-bound statement, or one running code
		// that may redefine it, calls is called through the definition the
		// transaction reads when the call runs.
		let late = match &call.receiver {
			Function::Custom(name)
				if self.calls_current_definitions() || self.version_unknown() =>
			{
				Some(ExternalCallee::Function {
					name: name.clone(),
				})
			}
			_ => None,
		};
		let Some(callee) = late.or_else(|| Self::external_callee(call)) else {
			return Err(NotCovered::internal("a call of code the host does not run"));
		};
		let guard = crate::exec::vm::host::out_of_line(|| self.external_guard(&callee)).await?;
		let needs = crate::exec::vm::host::out_of_line(|| self.needs_of(expr)).await;
		self.call_needs.push(needs);
		let external = u32::try_from(self.externals.len())
			.map_err(|_| NotCovered::internal("more external calls than a program holds"))?;
		self.externals.push(ExternalSpec {
			callee,
			plan_depth: self.planner.current_depth(),
			guard,
		});
		let frame = self.eval_frame;
		self.emit(Instr::ExternalEnter {
			external,
			frame,
		});
		let mut args = Vec::with_capacity(call.args.len());
		for arg in &call.args {
			args.push(crate::exec::vm::host::out_of_line(|| self.value(arg)).await?);
		}
		// Inside an element body the current value is the element; elsewhere
		// it is the row the host hands the call.
		let this = self.element.map(Operand::Reg);
		let dst = self.register()?;
		self.emit(Instr::ExternalCall {
			external,
			args,
			this,
			dst,
			frame,
		});
		self.uses_eval = true;
		self.may_fail = true;
		Ok(Operand::Reg(dst))
	}

	/// The body of `fn::name`, which reads an argument where only a value
	/// answers, as the one statement of its unit: run through the host, which
	/// runs the body compiled for the values the call's arguments hold
	/// ([`ExternalCallee::Specialised`]).
	pub(super) fn specialised_body(&mut self, name: &str) -> Lowered<()> {
		let external = u32::try_from(self.externals.len())
			.map_err(|_| NotCovered::internal("more external calls than a program holds"))?;
		self.externals.push(ExternalSpec {
			callee: ExternalCallee::Specialised {
				name: name.to_owned(),
			},
			plan_depth: self.planner.current_depth(),
			guard: None,
		});
		// The body may write anything the catalog lets it, in its database.
		self.call_needs.push(crate::exec::logical::Needs {
			access_mode: crate::exec::AccessMode::ReadWrite,
			context: crate::exec::ContextLevel::Database,
		});
		let frame = self.eval_frame;
		self.emit(Instr::ExternalEnter {
			external,
			frame,
		});
		let dst = self.register()?;
		self.emit(Instr::ExternalCall {
			external,
			args: Vec::new(),
			this: None,
			dst,
			frame,
		});
		self.uses_eval = true;
		self.may_fail = true;
		self.emit(Instr::Emit {
			src: dst,
		});
		self.emit(Instr::EndRow);
		self.push_write_pipeline(crate::exec::vm::program::Source::Once)?;
		self.scalar = true;
		Ok(())
	}

	/// The `PERMISSIONS` predicate of the model or module `callee` names, as
	/// the catalog holds it when the program is planned, lowered as a host
	/// pipeline in the frame the operator tree's guard is compiled in: the
	/// caller's principal, one re-entry deeper. `None` for a script, for a
	/// build without the callee's runtime, for a clause that is no predicate,
	/// for a definition the catalog does not hold, and where the call's frame
	/// judges no permission.
	async fn external_guard(
		&mut self,
		callee: &ExternalCallee,
	) -> Lowered<Option<(DefHash, u16, crate::exec::guard_verdicts::GuardScope)>> {
		let Some(permission) =
			crate::exec::vm::host::out_of_line(|| self.external_permission(callee)).await?
		else {
			return Ok(None);
		};
		let logical = permission.logical_ref().map_err(|e| {
			NotCovered::internal(format!("a definition's permission with no logical form: {e}"))
		})?;
		let LogicalPermissionRef::Specific(guard) = logical else {
			return Ok(None);
		};
		if self.reads_unchecked() {
			return Ok(None);
		}
		let guard = guard.clone();
		let hash = DefHash::of_permission(&permission.permission);
		let planner =
			self.planner.shadowing(std::iter::empty()).with_depth(self.planner.current_depth() + 1);
		let pipeline =
			crate::exec::vm::host::out_of_line(|| self.guard_pipeline_in(&planner, &guard)).await?;
		let scope =
			crate::exec::guard_verdicts::GuardScope::of(&guard, self.planner.function_registry());
		Ok(Some((hash, pipeline, scope)))
	}

	/// The `PERMISSIONS` clause of the definition `callee` names, as the
	/// program's plan read it, where the build runs the callee.
	async fn external_permission(
		&mut self,
		callee: &ExternalCallee,
	) -> Lowered<Option<ExternalPermission>> {
		match callee {
			ExternalCallee::Script {
				..
			} => Ok(None),
			#[cfg(feature = "ml")]
			ExternalCallee::Model {
				name,
				version,
			} => {
				let Some(txn) = self.planner.txn().cloned() else {
					return Ok(None);
				};
				let definition = self.catalog.model(&txn, name, version)?;
				Ok(definition.map(|definition| ExternalPermission {
					permission: definition.permissions.clone(),
					body: definition.permissions_body.clone(),
				}))
			}
			#[cfg(feature = "surrealism")]
			ExternalCallee::Module {
				storage,
				..
			} => {
				let Some(txn) = self.planner.txn().cloned() else {
					return Ok(None);
				};
				Ok(self.catalog.module(&txn, storage)?.map(|definition| ExternalPermission {
					permission: definition.permissions.clone(),
					body: definition.permissions_body.clone(),
				}))
			}
			#[cfg(not(feature = "ml"))]
			ExternalCallee::Model {
				..
			} => Ok(None),
			#[cfg(not(feature = "surrealism"))]
			ExternalCallee::Module {
				..
			} => Ok(None),
			// The call unit judges the function's own clause, and a body is
			// run by a call that judged it.
			ExternalCallee::Function {
				..
			}
			| ExternalCallee::Specialised {
				..
			} => Ok(None),
		}
	}
}

/// A definition's `PERMISSIONS` clause: as the catalog stores it, which its
/// hash is taken over, and in logical form.
struct ExternalPermission {
	permission: crate::catalog::Permission,
	body: crate::catalog::LogicalPermissionBody,
}

impl ExternalPermission {
	fn logical_ref(&self) -> anyhow::Result<LogicalPermissionRef<'_>> {
		self.body.logical_ref()
	}
}
