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

/// The storage name of export `sub` of the module stored as `mod_name`, which
/// the capability lists are matched against. Errors name the export by
/// [`crate::catalog::extension_name`] of it.
#[cfg(feature = "surrealism")]
fn export_name(mod_name: &str, sub: Option<&str>) -> String {
	match sub {
		Some(sub) => format!("{mod_name}::{sub}"),
		None => mod_name.to_owned(),
	}
}

/// Checks the server's function capabilities for a call of `fnc_name`, an
/// export named under its module's storage name.
///
/// A named extension is stored under `mod::` and called as `ext::` or, as an
/// alias, `mod::`, so its export is matched under both spellings: an allow
/// entry in either admits the call and a deny entry in either refuses it.
/// Which spelling the query or the capability list used never changes the
/// outcome. An unnamed silo export has the one spelling it is stored under.
#[cfg(feature = "surrealism")]
fn check_allowed_export(ctx: &EvalContext<'_>, fnc_name: &str) -> anyhow::Result<()> {
	use crate::dbs::capabilities::{Error as CapabilitiesError, FuncTarget};

	let Some(rest) = fnc_name.strip_prefix("mod::") else {
		return ctx.check_allowed_function(fnc_name);
	};
	let target = FuncTarget("ext".to_string(), Some(rest.to_string()));
	if !ctx.capabilities().allows_extension_function_target(&target) {
		return Err(CapabilitiesError::FunctionNotAllowed(format!("ext::{rest}")).into());
	}
	Ok(())
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
	check_allowed_export(ctx, &fnc_name)?;
	let db_ctx = ctx.exec_ctx.database().map_err(|_| {
		anyhow::anyhow!(
			"Extension function '{}' requires database context",
			crate::catalog::extension_name(&fnc_name)
		)
	})?;
	let ns_id = db_ctx.ns_ctx.ns.namespace_id;
	let db_id = db_ctx.db.database_id;
	let txn = ctx.txn();
	let stamp = ctx.exec_ctx.version_stamp();
	Ok(crate::exec::vm::descriptor::object(txn.get_db_module(ns_id, db_id, mod_name, stamp))
		.await?)
}

/// An export of a module, resolved against the context a call runs in: the
/// module's executable, the `DIGEST` and `SIGNER` its archive is checked
/// against, its `CAPABILITIES`, and the export's signature.
#[cfg(feature = "surrealism")]
pub(crate) struct ModuleExport {
	executable: crate::expr::module::ModuleExecutable,
	/// The definition the call resolved to, shared rather than copied: its
	/// `SIGNER` can carry KBs of certificates.
	definition: Arc<ModuleDefinition>,
	/// The [`crate::extension::trust::signer_fingerprint`] of the definition's
	/// `SIGNER`, computed once for every check the call makes.
	fingerprint: Option<[u8; 32]>,
	signature: crate::expr::module::Signature,
}

#[cfg(feature = "surrealism")]
impl ModuleExport {
	fn trust(&self) -> crate::extension::trust::Trust<'_> {
		crate::extension::trust::Trust {
			digest: self.definition.digest.as_ref(),
			signer: self.definition.signer.as_ref().zip(self.fingerprint),
			unsigned: self.definition.unsigned,
		}
	}
}

/// Export `sub` of module `definition`, resolved for a call from `ctx`: the
/// executable's signature for it, read before the call's arguments are
/// evaluated.
///
/// A definition without `DIGEST` (see [`ModuleDefinition::needs_reinstall`])
/// is refused before its archive is touched: it was built for an earlier ABI
/// and pins no code.
#[cfg(feature = "surrealism")]
pub(crate) async fn module_export(
	definition: &Arc<ModuleDefinition>,
	sub: Option<&str>,
	ctx: &EvalContext<'_>,
) -> FlowResult<ModuleExport> {
	if definition.needs_reinstall() {
		let name = definition.get_name().map_err(crate::expr::ControlFlow::Err)?;
		return Err(crate::expr::ControlFlow::Err(anyhow::anyhow!(
			"The extension `{name}` cannot run: {}",
			crate::catalog::REINSTALL_WARNING
		)));
	}
	let db_ctx = ctx.exec_ctx.database().map_err(|e| crate::expr::ControlFlow::Err(e.into()))?;
	let ns_id = db_ctx.ns_ctx.ns.namespace_id;
	let db_id = db_ctx.db.database_id;
	let executable: crate::expr::module::ModuleExecutable = definition.executable.clone().into();
	let trust = crate::extension::trust::Trust::from(&**definition);
	let signature = crate::fnc::module::module_executable_signature(
		&executable,
		trust,
		ctx.exec_ctx.ctx(),
		&ns_id,
		&db_id,
		sub,
	)
	.await?;
	Ok(ModuleExport {
		executable,
		definition: Arc::clone(definition),
		fingerprint: trust.signer.map(|(_, fingerprint)| fingerprint),
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

	let fnc_name = crate::catalog::extension_name(&export_name(mod_name, sub)).into_owned();
	// An extension reaches outside the transaction through its own grant,
	// which no frame narrows, so a frame that refuses external effects refuses
	// the call. Judged here, where the call is made, it holds however the call
	// was reached: through a stored function redefined after the clause was
	// written, or one an import restored unchecked.
	if let Some(frame) = ctx.exec_ctx.root().no_write
		&& frame.refuses_extension_call()
	{
		let construct = format!(
			"it calls `{fnc_name}`, {}",
			crate::exec::clause_effects::effect_text(crate::expr::logical::EffectSet::OPAQUE_BODY)
		);
		return Err(anyhow::Error::from(frame.side_effect_error(&construct)).into());
	}
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

	// Each argument is coerced to its declared kind and judged against the named types it holds.
	let refused = |e| Error::InvalidFunctionArguments {
		name: fnc_name.clone(),
		message: format!("Failed to coerce argument: {e}"),
	};
	let mut coerced_args = Vec::with_capacity(args.len());
	for (arg, kind) in args.into_iter().zip(signature.args.iter()) {
		let coerced = arg.coerce_to_kind(kind).map_err(refused)?;
		let coerced = crate::exec::vm::schema_host::check_types_held(ctx.exec_ctx, kind, coerced)
			.await
			.map_err(refused)?;
		coerced_args.push(coerced);
	}

	// The body runs in the evaluation frame the call is in: a write it reaches
	// is refused wherever the caller refuses one, its own reads are not gated
	// by the clause it is called from, and it reads at the caller's `VERSION`.
	let opt = options_for_frame(
		ctx.exec_ctx
			.options()
			.ok_or_else(|| anyhow::anyhow!("Extension functions require Options context"))?,
		ctx.skip_fetch_perms,
		ctx.exec_ctx.root().no_write,
		ctx.exec_ctx.version_stamp(),
	);

	// The module receives the value in scope as its document; where that is a
	// KNN query vector's row being scanned, the call is refused before it
	// runs, whether or not the module reads it.
	let doc = ctx.value_in_scope().read()?.map(|v| CursorDoc::new(None, v.clone()));

	// `module_export` refuses a definition without CAPABILITIES.
	let capabilities = export
		.definition
		.capabilities
		.as_ref()
		.ok_or_else(|| anyhow::anyhow!("The extension `{fnc_name}` records no CAPABILITIES"))?;
	// A module may reach outside the transaction through its capabilities, so the statement is
	// not run again after a lost conflict.
	ctx.exec_ctx.txn().note_external_effect();
	let result = crate::fnc::module::module_executable_run(
		&export.executable,
		crate::fnc::module::CalledDefinition {
			trust: export.trust(),
			capabilities,
			definition: &export.definition,
		},
		ctx.exec_ctx.ctx(),
		&opt,
		doc.as_ref(),
		coerced_args,
		sub,
	)
	.await?;

	// The result is coerced to the declared return kind and judged against the named types it
	// holds.
	let result = validate_return(&fnc_name, signature.returns.as_ref(), result)?;
	let Some(kind) = signature.returns.as_ref() else {
		return Ok(result);
	};
	crate::exec::vm::schema_host::check_types_held(ctx.exec_ctx, kind, result).await.map_err(|e| {
		anyhow::Error::new(crate::exec::Error::ReturnCoerce {
			name: fnc_name,
			error: Box::new(e),
		})
		.into()
	})
}

/// The error a call of an extension export raises in a build without the
/// Surrealism runtime: `name` is the extension's storage name followed by the
/// export for an `ext::` call, and the package's storage name for a `silo::`
/// call. A named extension is reported by its `ext::` name.
#[cfg(not(feature = "surrealism"))]
pub(crate) fn module_runtime_missing(name: &str, silo: bool) -> crate::expr::ControlFlow {
	let name = crate::catalog::extension_name(name);
	let kind = if silo {
		"Silo"
	} else {
		"Extension"
	};
	anyhow::anyhow!("{kind} function '{name}' requires the 'surrealism' feature to be enabled")
		.into()
}
