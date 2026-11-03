//! Calling an extension's exported function.
//!
//! A `DEFINE EXTENSION` names an executable — a Surrealism package in a bucket,
//! or a Silo package by version — and `ext::<name>()` calls one of its exports.
//! Resolving a signature and running a call both need the execution
//! environment: the context's runtime cache, the session's capabilities, and
//! the namespace and database the options select. That is why this lives here
//! rather than beside [`ModuleExecutable`](crate::expr::module::ModuleExecutable),
//! which the expression layer holds and evaluates nothing of.
//!
//! The `doc` a call carries is the row in scope, which the host binds for a
//! `sql()` the module makes.
//!
//! The entry points that load and run a module exist only with the
//! `surrealism` feature, which a caller gates on.

use std::sync::Arc;

#[cfg(feature = "surrealism")]
use anyhow::{Result, bail};
#[cfg(feature = "surrealism")]
use surrealism_runtime::host::InvocationContext as _;

#[cfg(feature = "surrealism")]
use crate::catalog::{DatabaseId, NamespaceId};
use crate::ctx::FrozenContext;
#[cfg(feature = "surrealism")]
use crate::dbs::Options;
#[cfg(feature = "surrealism")]
use crate::dbs::capabilities::ExperimentalTarget;
#[cfg(feature = "surrealism")]
use crate::doc::CursorDoc;
#[cfg(feature = "surrealism")]
use crate::expr::ExtensionCapabilities;
#[cfg(feature = "surrealism")]
use crate::expr::module::{ModuleExecutable, Signature, SiloExecutable, SurrealismExecutable};
use crate::expr::statements::info::InfoStructure;
#[cfg(feature = "surrealism")]
use crate::extension::trust::Trust;
#[cfg(feature = "surrealism")]
use crate::surrealism::cache::SurrealismCacheLookup;
#[cfg(feature = "surrealism")]
use crate::surrealism::cache::SurrealismCachedModule;
#[cfg(feature = "surrealism")]
use crate::surrealism::host::Host;
#[cfg(feature = "surrealism")]
use crate::surrealism::runner::{
	Invocation, Limits, MAX_EXTENSION_DEPTH, RunnerCapabilities, Stdio, TransactionHandle,
};
#[cfg(feature = "surrealism")]
use crate::val::Object;
use crate::val::Value;

/// The definition a call runs under: the code it may run and what that code
/// may do.
#[cfg(feature = "surrealism")]
#[derive(Clone, Copy)]
pub(crate) struct CalledDefinition<'a> {
	/// `DIGEST` and `SIGNER`, which the archive is checked against.
	pub trust: Trust<'a>,
	/// `CAPABILITIES`, intersected with the datastore's on every invocation.
	pub capabilities: &'a ExtensionCapabilities,
	/// The definition itself, whose `BINDINGS` the invocation reads.
	pub definition: &'a Arc<crate::catalog::ModuleDefinition>,
}

#[cfg(feature = "surrealism")]
pub(crate) async fn module_executable_signature(
	this: &ModuleExecutable,
	trust: Trust<'_>,
	ctx: &FrozenContext,
	ns: &NamespaceId,
	db: &DatabaseId,
	sub: Option<&str>,
) -> Result<Signature> {
	match this {
		ModuleExecutable::Surrealism(surrealism) => {
			surrealism_executable_signature(surrealism, trust, ctx, ns, db, sub).await
		}
		ModuleExecutable::Silo(silo) => {
			silo_executable_signature(silo, trust, ctx, ns, db, sub).await
		}
	}
}

#[cfg(feature = "surrealism")]
pub(crate) async fn module_executable_run(
	this: &ModuleExecutable,
	definition: CalledDefinition<'_>,
	ctx: &FrozenContext,
	opt: &Options,
	doc: Option<&CursorDoc>,
	args: Vec<Value>,
	sub: Option<&str>,
) -> Result<Value> {
	match this {
		ModuleExecutable::Surrealism(surrealism) => {
			surrealism_executable_run(surrealism, definition, ctx, opt, doc, args, sub).await
		}
		ModuleExecutable::Silo(silo) => {
			silo_executable_run(silo, definition, ctx, opt, doc, args, sub).await
		}
	}
}

#[cfg(feature = "surrealism")]
pub(crate) async fn surrealism_executable_signature(
	this: &SurrealismExecutable,
	trust: Trust<'_>,
	ctx: &FrozenContext,
	ns: &NamespaceId,
	db: &DatabaseId,
	sub: Option<&str>,
) -> Result<Signature> {
	check_surrealism_enabled(ctx)?;
	let lookup = SurrealismCacheLookup::File(ns, db, &this.0.bucket, &this.0.key);
	let cached =
		crate::exec::vm::descriptor::object(ctx.get_surrealism_module(lookup, trust)).await?;
	signature_from_exports(cached.prepared.exports(), sub)
}

#[cfg(feature = "surrealism")]
pub(crate) async fn surrealism_executable_run(
	this: &SurrealismExecutable,
	definition: CalledDefinition<'_>,
	ctx: &FrozenContext,
	opt: &Options,
	doc: Option<&CursorDoc>,
	args: Vec<Value>,
	sub: Option<&str>,
) -> Result<Value> {
	check_surrealism_enabled(ctx)?;
	let (ns, db) = ctx.get_ns_db_ids(opt).await?;
	let lookup = SurrealismCacheLookup::File(&ns, &db, &this.0.bucket, &this.0.key);
	let cached =
		crate::exec::vm::descriptor::object(ctx.get_surrealism_module(lookup, definition.trust))
			.await?;
	run_on_runtime(cached, definition, ctx, opt, doc, args, sub).await
}

#[cfg(feature = "surrealism")]
pub(crate) async fn silo_executable_signature(
	this: &SiloExecutable,
	trust: Trust<'_>,
	ctx: &FrozenContext,
	ns: &NamespaceId,
	db: &DatabaseId,
	sub: Option<&str>,
) -> Result<Signature> {
	check_surrealism_enabled(ctx)?;
	let lookup = SurrealismCacheLookup::Silo(
		ns,
		db,
		&this.organisation,
		&this.package,
		this.major,
		this.minor,
		this.patch,
	);
	let cached =
		crate::exec::vm::descriptor::object(ctx.get_surrealism_module(lookup, trust)).await?;
	signature_from_exports(cached.prepared.exports(), sub)
}

#[cfg(feature = "surrealism")]
pub(crate) async fn silo_executable_run(
	this: &SiloExecutable,
	definition: CalledDefinition<'_>,
	ctx: &FrozenContext,
	opt: &Options,
	doc: Option<&CursorDoc>,
	args: Vec<Value>,
	sub: Option<&str>,
) -> Result<Value> {
	check_surrealism_enabled(ctx)?;
	let (ns, db) = ctx.get_ns_db_ids(opt).await?;
	let lookup = SurrealismCacheLookup::Silo(
		&ns,
		&db,
		&this.organisation,
		&this.package,
		this.major,
		this.minor,
		this.patch,
	);
	let cached =
		crate::exec::vm::descriptor::object(ctx.get_surrealism_module(lookup, definition.trust))
			.await?;
	run_on_runtime(cached, definition, ctx, opt, doc, args, sub).await
}

#[cfg(feature = "surrealism")]
pub(crate) fn check_surrealism_enabled(ctx: &FrozenContext) -> Result<()> {
	if !ctx.get_capabilities().allows_experimental(&ExperimentalTarget::Surrealism) {
		bail!("Experimental capability `surrealism` is not enabled");
	}
	Ok(())
}

/// What an invocation of a definition whose `CAPABILITIES` are
/// `capabilities` may do, from the cache when it was built before.
///
/// The grant is `capabilities` intersected with the datastore's capabilities,
/// never with the context's: inside a host call of another extension the
/// context holds that caller's narrowed grant, and a callee runs with its own
/// definition's grant even where that is more than its caller's. Whether the
/// caller may call it at all is checked against the caller's grant before the
/// definition is read. The grant is therefore the same whichever call builds
/// it, and is cached by the two.
#[cfg(feature = "surrealism")]
pub(crate) async fn extension_grant(
	ctx: &crate::ctx::Context,
	capabilities: &ExtensionCapabilities,
) -> Result<Arc<RunnerCapabilities>> {
	let cache = ctx
		.get_surrealism_cache()
		.ok_or_else(|| anyhow::anyhow!("Surrealism cache is not available"))?;
	let server = ctx.server_capabilities();
	cache
		.grant(capabilities, server, || {
			crate::surrealism::host::runner_capabilities(
				crate::extension::capabilities::effective_capabilities(capabilities, server)?,
				server,
				&ctx.config,
			)
		})
		.await
}

/// Invoke one export of a loaded definition, in fresh memory.
///
/// The invocation is lent a transaction only when the export declared one,
/// and runs with the definition's `CAPABILITIES` intersected with the
/// datastore's. It reads the definition's `BINDINGS` whether or not it was lent
/// a transaction. A call made from inside another extension (through
/// `tx.run`) counts one level deeper; it fails past [`MAX_EXTENSION_DEPTH`]
/// and never waits for capacity.
#[cfg(feature = "surrealism")]
pub(crate) async fn run_on_runtime(
	cached: SurrealismCachedModule,
	definition: CalledDefinition<'_>,
	ctx: &FrozenContext,
	opt: &Options,
	doc: Option<&CursorDoc>,
	args: Vec<Value>,
	sub: Option<&str>,
) -> Result<Value> {
	let display_name = sub.unwrap_or("<default>");
	tracing::debug!(name = %display_name, arg_count = args.len(), "run_on_runtime: starting");

	let depth = ctx.extension_depth();
	if depth >= MAX_EXTENSION_DEPTH {
		bail!(
			"Extension call depth exceeded: `{display_name}` would run {} extension invocations \
			 deep, and the limit is {MAX_EXTENSION_DEPTH}",
			depth + 1
		);
	}

	let args: Result<Vec<crate::types::PublicValue>, _> =
		args.into_iter().map(|x| x.try_into()).collect();
	let args = args?;

	let SurrealismCachedModule {
		prepared,
		module_display_name,
		..
	} = cached;
	let capabilities = extension_grant(ctx, definition.capabilities).await?;
	// The declared mode decides whether writes are permitted for this call, and
	// whether a handle is lent at all. Read both before building the host, from
	// the same manifest entry the planner used to pick the statement's access
	// mode. A writing export of a definition that does not grant `write` gets
	// the read-only frame.
	//
	// An export the manifest does not name fails here rather than in the guest,
	// so an unknown name is reported by the host with the manifest's own error.
	let signature = prepared.exports().get_signature(sub).ok_or_else(|| {
		anyhow::anyhow!("function '{display_name}' not found in exports manifest")
	})?;
	let writeable = signature.writeable && capabilities.grant.write;
	let declares_transaction = signature.declares_transaction;
	// `Arc`, not `Box`: the guest may hold several host calls in flight, and each
	// one resolves the shared context out of the store's resource table.
	let bindings = Arc::new(crate::surrealism::bindings::DefinitionBindings::new(
		ctx,
		opt,
		Arc::clone(definition.definition),
		Arc::clone(&prepared),
		Arc::clone(&module_display_name),
	));
	let host = Arc::new(Host::new(
		ctx,
		opt,
		doc,
		writeable,
		module_display_name,
		Arc::clone(&capabilities),
	));
	let stdio = Stdio {
		stdout: host.stdout_callback(),
		stderr: host.stderr_callback(),
	};

	// A function that declared no transaction parameter is lent nothing, which
	// is what makes "provably cannot query" true of the host rather than of the
	// generated dispatch. The manifest is user-supplied and says nothing about
	// the component beside it; what the property rests on is that this is the
	// only thing consulted, so a component whose body calls `transaction.sql`
	// must declare the parameter to be given a handle, and a manifest that
	// hides the parameter withholds it.
	let tx = declares_transaction.then_some(host as TransactionHandle);
	let deadline = ctx.timeout().map(|t| web_time::Instant::now() + t);
	let mut values = prepared
		.invoke(Invocation {
			function: sub,
			rows: vec![args],
			tx,
			bindings: Some(bindings),
			stdio,
			capabilities: &capabilities,
			limits: Limits {
				deadline,
				depth,
			},
		})
		.await?;
	let value = values.pop().ok_or_else(|| anyhow::anyhow!("the extension returned no value"))?;
	Ok(value.into())
}

#[cfg(feature = "surrealism")]
pub(crate) fn signature_from_exports(
	exports: &surrealism_runtime::exports::ExportsManifest,
	sub: Option<&str>,
) -> Result<Signature> {
	let export = exports.get_signature(sub).ok_or_else(|| {
		let name = sub.unwrap_or("<default>");
		anyhow::anyhow!("function '{name}' not found in exports manifest")
	})?;
	Ok(Signature {
		args: export.args.iter().map(|(_, k)| k.clone().into()).collect(),
		returns: Some(export.returns.clone().into()),
		writeable: export.writeable,
	})
}

/// The exports of `definition`, for `INFO`. They are read from the compiled
/// module, so only from an archive that passed the definition's `DIGEST` and
/// `SIGNER` check; `None` when it did not, or when it cannot be loaded.
#[cfg(feature = "surrealism")]
pub(crate) async fn get_module_exports(
	ctx: &FrozenContext,
	ns: &crate::catalog::NamespaceId,
	db: &crate::catalog::DatabaseId,
	definition: &crate::catalog::ModuleDefinition,
) -> Option<Value> {
	use crate::catalog::ModuleExecutable;
	use crate::surrealism::cache::SurrealismCacheLookup;

	let trust = Trust::from(definition);
	match &definition.executable {
		ModuleExecutable::Surrealism(s) => {
			let lookup = SurrealismCacheLookup::File(ns, db, &s.bucket, &s.key);
			let cached = match ctx.get_surrealism_module(lookup, trust).await {
				Ok(r) => r,
				Err(e) => {
					tracing::trace!("Could not load module runtime for exports: {e}");
					return None;
				}
			};
			Some(surrealism_exports_manifest_to_value(cached.prepared.exports()))
		}
		ModuleExecutable::Silo(s) => {
			let lookup = SurrealismCacheLookup::Silo(
				ns,
				db,
				&s.organisation,
				&s.package,
				s.major,
				s.minor,
				s.patch,
			);
			let cached = match ctx.get_surrealism_module(lookup, trust).await {
				Ok(r) => r,
				Err(e) => {
					tracing::trace!("Could not load module runtime for exports: {e}");
					return None;
				}
			};
			Some(surrealism_exports_manifest_to_value(cached.prepared.exports()))
		}
	}
}

/// Process module definitions into structured Values, enriching each with
/// export signatures from the cached surrealism runtime when available.
pub(crate) async fn process_modules(
	ctx: &FrozenContext,
	ns: crate::catalog::NamespaceId,
	db: crate::catalog::DatabaseId,
	modules: Arc<[crate::catalog::ModuleDefinition]>,
) -> Value {
	let mut values = Vec::with_capacity(modules.len());
	for module in modules.iter() {
		#[allow(unused_mut)]
		let mut val = module.clone().structure();
		#[cfg(feature = "surrealism")]
		if let Value::Object(ref mut obj) = val
			&& let Some(exports) = get_module_exports(ctx, &ns, &db, module).await
		{
			obj.insert("exports", exports);
		}
		values.push(val);
	}
	#[cfg(not(feature = "surrealism"))]
	let _ = (ctx, ns, db);
	Value::Array(values.into())
}

#[cfg(feature = "surrealism")]
pub(crate) fn surrealism_exports_manifest_to_value(
	exports: &surrealism_runtime::exports::ExportsManifest,
) -> Value {
	let values: Vec<Value> = exports
		.functions
		.iter()
		.map(|f| {
			let mut obj = Object::default();
			if let Some(name) = &f.name {
				obj.insert("name", Value::from(name.clone()));
			}
			obj.insert(
				"args",
				Value::Array(
					f.args
						.iter()
						.map(|(arg_name, kind)| {
							let mut arg_obj = Object::default();
							arg_obj.insert("name", Value::from(arg_name.clone()));
							arg_obj.insert("kind", Value::from(format!("{kind}")));
							Value::Object(arg_obj)
						})
						.collect(),
				),
			);
			obj.insert("returns", Value::from(format!("{}", f.returns)));
			obj.insert("writeable", Value::from(f.writeable));
			if let Some(comment) = &f.comment {
				obj.insert("comment", Value::from(comment.clone()));
			}
			Value::Object(obj)
		})
		.collect();

	Value::Array(values.into())
}
