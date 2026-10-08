//! Module function expressions - Surrealism WASM and Silo packages.

use std::sync::Arc;

use surrealdb_types::{SqlFormat, ToSql};

use super::helpers::{args_access_mode, args_required_context, fmt_call_args};
#[cfg(feature = "surrealism")]
use super::helpers::{check_permission, evaluate_args, validate_return};
#[cfg(feature = "surrealism")]
use crate::catalog::ModuleDefinition;
#[cfg(feature = "surrealism")]
use crate::catalog::providers::DatabaseProvider;
#[cfg(feature = "surrealism")]
use crate::exec::frame::options_for_frame;
#[cfg(feature = "surrealism")]
use crate::exec::permission::CompiledGuard;
use crate::exec::physical_expr::{EvalContext, PhysicalExpr};
use crate::exec::{AccessMode, BoxFut};
#[cfg(feature = "surrealism")]
use crate::expr::Error;
use crate::expr::FlowResult;
use crate::val::Value;

/// Look up a module by its storage name, then call one of its exports.
///
/// Both executable forms reach a module the same way — the storage name is the
/// only thing that differs — so `mod::` and `silo::` calls share this body and
/// cannot drift apart in permission checking, argument coercion or return
/// validation. `mod_name` is the storage name
/// ([`ModuleName::get_storage_name`](crate::expr::module::ModuleName)), and
/// `sub` names the export, or the default one when `None`.
#[cfg(feature = "surrealism")]
async fn call_module_export<'a>(
	mod_name: String,
	sub: Option<&'a str>,
	arguments: &'a [Arc<dyn PhysicalExpr>],
	permission: &'a CompiledGuard<ModuleDefinition>,
	planned: &'a crate::exec::physical_expr::PlannedGuard<ModuleDefinition>,
	ctx: EvalContext<'a>,
) -> FlowResult<Value> {
	let val = read_module(&mod_name, sub, &ctx).await?;
	// Check permissions. Inside a permission predicate the call is the
	// definer's and is not checked.
	if !ctx.skip_fetch_perms && ctx.exec_ctx.should_check_perms(crate::iam::Action::View)? {
		check_permission(
			crate::exec::guard_verdicts::GuardKind::Module,
			(permission, planned),
			val.permissions_body.logical_ref()?,
			None,
			&val,
			&mod_name,
			&ctx,
		)
		.await?;
	}
	let export = module_export(&val, sub, &ctx).await?;
	let args = evaluate_args(arguments, ctx.clone()).await?;
	run_module_export(&mod_name, &export, sub, args, &ctx).await
}

/// The name a call of export `sub` of the module stored as `mod_name`
/// reports itself by.
#[cfg(feature = "surrealism")]
fn export_name(mod_name: &str, sub: Option<&str>) -> String {
	match sub {
		Some(sub) => format!("{mod_name}::{sub}"),
		None => mod_name.to_owned(),
	}
}

/// The definition of the module stored as `mod_name` that a call of its
/// export `sub` from `ctx` reads, before its arguments are evaluated: the
/// session may call the export, the context holds a database, and the catalog
/// holds the module as of the context's `VERSION`. Its `PERMISSIONS` clause
/// is the caller's to judge.
#[cfg(feature = "surrealism")]
pub(crate) async fn read_module(
	mod_name: &str,
	sub: Option<&str>,
	ctx: &EvalContext<'_>,
) -> FlowResult<Arc<ModuleDefinition>> {
	let fnc_name = export_name(mod_name, sub);
	ctx.check_allowed_function(&fnc_name)?;
	let db_ctx = ctx
		.exec_ctx
		.database()
		.map_err(|_| anyhow::anyhow!("Module function '{}' requires database context", fnc_name))?;
	let ns_id = db_ctx.ns_ctx.ns.namespace_id;
	let db_id = db_ctx.db.database_id;
	let txn = ctx.txn();
	let stamp = ctx.exec_ctx.version_stamp();
	Ok(crate::exec::vm::descriptor::object(txn.get_db_module(ns_id, db_id, mod_name, stamp))
		.await?)
}

/// An export of a module, resolved against the context a call runs in: the
/// module's executable and the export's signature.
#[cfg(feature = "surrealism")]
pub(crate) struct ModuleExport {
	executable: crate::expr::module::ModuleExecutable,
	signature: crate::expr::module::Signature,
}

/// Export `sub` of module `definition`, resolved for a call from `ctx`: the
/// executable's signature for it, read before the call's arguments are
/// evaluated.
#[cfg(feature = "surrealism")]
pub(crate) async fn module_export(
	definition: &ModuleDefinition,
	sub: Option<&str>,
	ctx: &EvalContext<'_>,
) -> FlowResult<ModuleExport> {
	let db_ctx = ctx.exec_ctx.database().map_err(|e| crate::expr::ControlFlow::Err(e.into()))?;
	let ns_id = db_ctx.ns_ctx.ns.namespace_id;
	let db_id = db_ctx.db.database_id;
	let executable: crate::expr::module::ModuleExecutable = definition.executable.clone().into();
	let signature = crate::fnc::module::module_executable_signature(
		&executable,
		ctx.exec_ctx.ctx(),
		&ns_id,
		&db_id,
		sub,
	)
	.await?;
	Ok(ModuleExport {
		executable,
		signature,
	})
}

/// Call `export`, export `sub` of the module stored as `mod_name`, with
/// `args` from `ctx`: counted and coerced to the signature's kinds, run in
/// the evaluation frame the call is in with the current value as its
/// document, and its value checked against the signature's return kind.
#[cfg(feature = "surrealism")]
pub(crate) async fn run_module_export(
	mod_name: &str,
	export: &ModuleExport,
	sub: Option<&str>,
	args: Vec<Value>,
	ctx: &EvalContext<'_>,
) -> FlowResult<Value> {
	use crate::doc::CursorDoc;

	let fnc_name = export_name(mod_name, sub);
	let signature = &export.signature;
	// Validate argument count against signature
	if args.len() != signature.args.len() {
		return Err(Error::InvalidFunctionArguments {
			name: fnc_name,
			message: format!(
				"The function expects {} arguments, but {} were provided.",
				signature.args.len(),
				args.len()
			),
		}
		.into());
	}

	// Validate and coerce arguments to their expected types
	let mut coerced_args = Vec::with_capacity(args.len());
	for (arg, kind) in args.into_iter().zip(signature.args.iter()) {
		let coerced = arg.coerce_to_kind(kind).map_err(|e| Error::InvalidFunctionArguments {
			name: fnc_name.clone(),
			message: format!("Failed to coerce argument: {e}"),
		})?;
		coerced_args.push(coerced);
	}

	// The body runs in the evaluation frame the call is in: a write it reaches
	// is refused wherever the caller refuses one, its own reads are not gated
	// by the clause it is called from, and it reads at the caller's `VERSION`.
	let opt = options_for_frame(
		ctx.exec_ctx
			.options()
			.ok_or_else(|| anyhow::anyhow!("Module functions require Options context"))?,
		ctx.skip_fetch_perms,
		ctx.exec_ctx.root().no_write,
		ctx.exec_ctx.version_stamp(),
	);

	// Build CursorDoc from current value
	let doc = ctx.current_value.map(|v| CursorDoc::new(None, v.clone()));

	let result = crate::fnc::module::module_executable_run(
		&export.executable,
		ctx.exec_ctx.ctx(),
		&opt,
		doc.as_ref(),
		coerced_args,
		sub,
	)
	.await?;

	// Validate return value if signature specifies a return type
	validate_return(&fnc_name, signature.returns.as_ref(), result).map_err(Into::into)
}

/// The error a call of a module export raises in a build without the
/// Surrealism runtime: `name` is the module's storage name followed by the
/// export for a `mod::` call, and the package's storage name for a `silo::`
/// call.
#[cfg(not(feature = "surrealism"))]
pub(crate) fn module_runtime_missing(name: &str, silo: bool) -> crate::expr::ControlFlow {
	let kind = if silo {
		"Silo"
	} else {
		"Module"
	};
	anyhow::anyhow!("{kind} function '{name}' requires the 'surrealism' feature to be enabled")
		.into()
}

// =============================================================================
// SurrealismModuleExec - for Function::Module
// =============================================================================

/// Surrealism WASM module function expression.
#[derive(Debug, Clone)]
pub struct SurrealismModuleExec {
	pub(crate) module: String,
	pub(crate) sub: Option<String>,
	pub(crate) arguments: Vec<Arc<dyn PhysicalExpr>>,
	/// Whether this function is declared writeable by the module author.
	///
	/// When `false` the call reports [`AccessMode::ReadOnly`], which is what
	/// lets a parent buffer its rows ahead of demand and evaluate them side by
	/// side. It does not choose the transaction: the transaction type is taken
	/// from the surface statement before planning, and a module or silo call
	/// answers `read_only()` with `false` whatever this flag says, so the
	/// statement runs in a write transaction either way.
	pub(crate) writeable: bool,
	/// The `PERMISSIONS` clause of the definition this call last found,
	/// compiled. A call in a projection is made once per row, and compiling
	/// its guard is plan-time work; see [`CompiledGuard`] for what is kept
	/// and what is not.
	#[cfg(feature = "surrealism")]
	pub(crate) permission: CompiledGuard<ModuleDefinition>,
	/// The `PERMISSIONS` clause compiled with the plan, for the definition
	/// the planner read; see [`crate::exec::physical_expr::PlannedGuard`].
	#[cfg(feature = "surrealism")]
	pub(crate) planned_permission: crate::exec::physical_expr::PlannedGuard<ModuleDefinition>,
}
impl PhysicalExpr for SurrealismModuleExec {
	fn name(&self) -> &'static str {
		"SurrealismModule"
	}

	fn as_any(&self) -> &dyn std::any::Any {
		self
	}

	fn required_context(&self) -> crate::exec::ContextLevel {
		// Module functions require database context, and arguments
		// may have their own context requirements
		args_required_context(&self.arguments).max(crate::exec::ContextLevel::Database)
	}

	#[cfg(feature = "surrealism")]
	fn evaluate<'a>(&'a self, ctx: EvalContext<'a>) -> BoxFut<'a, FlowResult<Value>> {
		Box::pin(call_module_export(
			format!("mod::{}", self.module),
			self.sub.as_deref(),
			&self.arguments,
			&self.permission,
			&self.planned_permission,
			ctx,
		))
	}

	#[cfg(not(feature = "surrealism"))]
	fn evaluate<'a>(&'a self, _ctx: EvalContext<'a>) -> BoxFut<'a, FlowResult<Value>> {
		Box::pin(async move {
			let name = match &self.sub {
				Some(s) => format!("mod::{}::{}", self.module, s),
				None => format!("mod::{}", self.module),
			};
			Err(module_runtime_missing(&name, false))
		})
	}

	fn access_mode(&self) -> AccessMode {
		let func_mode = if self.writeable {
			AccessMode::ReadWrite
		} else {
			AccessMode::ReadOnly
		};
		func_mode.combine(args_access_mode(&self.arguments))
	}
}

impl ToSql for SurrealismModuleExec {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		f.push_str("mod::");
		f.push_str(&self.module);
		if let Some(sub) = &self.sub {
			f.push_str("::");
			f.push_str(sub);
		}
		fmt_call_args(f, fmt, &self.arguments);
	}
}

// =============================================================================
// SiloModuleExec - for Function::Silo
// =============================================================================

/// Silo versioned package function expression.
#[derive(Debug, Clone)]
pub struct SiloModuleExec {
	pub(crate) org: String,
	pub(crate) pkg: String,
	pub(crate) major: u32,
	pub(crate) minor: u32,
	pub(crate) patch: u32,
	pub(crate) sub: Option<String>,
	pub(crate) arguments: Vec<Arc<dyn PhysicalExpr>>,
	/// Whether this function is declared writeable by the module author.
	pub(crate) writeable: bool,
	/// The `PERMISSIONS` clause of the definition this call last found,
	/// compiled. A call in a projection is made once per row, and compiling
	/// its guard is plan-time work; see [`CompiledGuard`] for what is kept
	/// and what is not.
	#[cfg(feature = "surrealism")]
	pub(crate) permission: CompiledGuard<ModuleDefinition>,
	/// The `PERMISSIONS` clause compiled with the plan, for the definition
	/// the planner read; see [`crate::exec::physical_expr::PlannedGuard`].
	#[cfg(feature = "surrealism")]
	pub(crate) planned_permission: crate::exec::physical_expr::PlannedGuard<ModuleDefinition>,
}
impl PhysicalExpr for SiloModuleExec {
	fn name(&self) -> &'static str {
		"SiloModule"
	}

	fn as_any(&self) -> &dyn std::any::Any {
		self
	}

	fn required_context(&self) -> crate::exec::ContextLevel {
		// Silo package functions require database context, and arguments
		// may have their own context requirements
		args_required_context(&self.arguments).max(crate::exec::ContextLevel::Database)
	}

	#[cfg(feature = "surrealism")]
	fn evaluate<'a>(&'a self, ctx: EvalContext<'a>) -> BoxFut<'a, FlowResult<Value>> {
		Box::pin(call_module_export(
			format!(
				"silo::{}::{}::<{}.{}.{}>",
				self.org, self.pkg, self.major, self.minor, self.patch
			),
			self.sub.as_deref(),
			&self.arguments,
			&self.permission,
			&self.planned_permission,
			ctx,
		))
	}

	#[cfg(not(feature = "surrealism"))]
	fn evaluate<'a>(&'a self, _ctx: EvalContext<'a>) -> BoxFut<'a, FlowResult<Value>> {
		Box::pin(async move {
			let name = format!(
				"silo::{}::{}::<{}.{}.{}>",
				self.org, self.pkg, self.major, self.minor, self.patch
			);
			Err(module_runtime_missing(&name, true))
		})
	}

	fn access_mode(&self) -> AccessMode {
		let func_mode = if self.writeable {
			AccessMode::ReadWrite
		} else {
			AccessMode::ReadOnly
		};
		func_mode.combine(args_access_mode(&self.arguments))
	}
}

impl ToSql for SiloModuleExec {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		f.push_str("silo::");
		f.push_str(&self.org);
		f.push_str("::");
		f.push_str(&self.pkg);
		f.push_str("::");
		f.push('<');
		f.push_str(&self.major.to_string());
		f.push('.');
		f.push_str(&self.minor.to_string());
		f.push('.');
		f.push_str(&self.patch.to_string());
		f.push('>');
		if let Some(sub) = &self.sub {
			f.push_str("::");
			f.push_str(sub);
		}
		fmt_call_args(f, fmt, &self.arguments);
	}
}
