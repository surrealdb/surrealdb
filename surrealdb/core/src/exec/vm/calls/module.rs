//! Module function expressions - Surrealism WASM and Silo packages.

#[cfg(feature = "surrealism")]
use std::sync::Arc;

#[cfg(feature = "surrealism")]
use super::helpers::validate_return;
#[cfg(feature = "surrealism")]
use crate::catalog::ModuleDefinition;
#[cfg(feature = "surrealism")]
use crate::catalog::providers::DatabaseProvider;
#[cfg(feature = "surrealism")]
use crate::exec::EvalContext;
#[cfg(feature = "surrealism")]
use crate::exec::frame::options_for_frame;
#[cfg(feature = "surrealism")]
use crate::expr::Error;
#[cfg(feature = "surrealism")]
use crate::expr::FlowResult;
#[cfg(feature = "surrealism")]
use crate::val::Value;

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

	// The module receives the value in scope as its document; where that is a
	// KNN query vector's row being scanned, the call is refused before it
	// runs, whether or not the module reads it.
	let doc = ctx.value_in_scope().read()?.map(|v| CursorDoc::new(None, v.clone()));

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
