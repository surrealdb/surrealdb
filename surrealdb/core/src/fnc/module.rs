//! Calling a module's exported function.
//!
//! A `DEFINE MODULE` names an executable — a Surrealism package in a bucket, or
//! a Silo package by version — and `mod::<name>()` calls one of its exports.
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
use crate::catalog::{DatabaseId, NamespaceId};
use crate::ctx::FrozenContext;
#[cfg(feature = "surrealism")]
use crate::dbs::Options;
#[cfg(feature = "surrealism")]
use crate::dbs::capabilities::ExperimentalTarget;
#[cfg(feature = "surrealism")]
use crate::doc::CursorDoc;
#[cfg(feature = "surrealism")]
use crate::expr::module::{ModuleExecutable, Signature, SiloExecutable, SurrealismExecutable};
use crate::expr::statements::info::InfoStructure;
#[cfg(feature = "surrealism")]
use crate::surrealism::cache::SurrealismCacheLookup;
#[cfg(feature = "surrealism")]
use crate::surrealism::cache::SurrealismCachedModule;
#[cfg(feature = "surrealism")]
use crate::surrealism::host::Host;
#[cfg(feature = "surrealism")]
use crate::val::Object;
use crate::val::Value;

#[cfg(feature = "surrealism")]
pub(crate) async fn module_executable_signature(
	this: &ModuleExecutable,
	ctx: &FrozenContext,
	ns: &NamespaceId,
	db: &DatabaseId,
	sub: Option<&str>,
) -> Result<Signature> {
	match this {
		ModuleExecutable::Surrealism(surrealism) => {
			surrealism_executable_signature(surrealism, ctx, ns, db, sub).await
		}
		ModuleExecutable::Silo(silo) => silo_executable_signature(silo, ctx, ns, db, sub).await,
	}
}

#[cfg(feature = "surrealism")]
pub(crate) async fn module_executable_run(
	this: &ModuleExecutable,
	ctx: &FrozenContext,
	opt: &Options,
	doc: Option<&CursorDoc>,
	args: Vec<Value>,
	sub: Option<&str>,
) -> Result<Value> {
	match this {
		ModuleExecutable::Surrealism(surrealism) => {
			surrealism_executable_run(surrealism, ctx, opt, doc, args, sub).await
		}
		ModuleExecutable::Silo(silo) => silo_executable_run(silo, ctx, opt, doc, args, sub).await,
	}
}

#[cfg(feature = "surrealism")]
pub(crate) async fn surrealism_executable_signature(
	this: &SurrealismExecutable,
	ctx: &FrozenContext,
	ns: &NamespaceId,
	db: &DatabaseId,
	sub: Option<&str>,
) -> Result<Signature> {
	check_surrealism_enabled(ctx)?;
	let lookup = SurrealismCacheLookup::File(ns, db, &this.0.bucket, &this.0.key);
	let runtime = crate::exec::vm::descriptor::object(ctx.get_surrealism_runtime(lookup)).await?;
	signature_from_runtime(&runtime, sub)
}

#[cfg(feature = "surrealism")]
pub(crate) async fn surrealism_executable_run(
	this: &SurrealismExecutable,
	ctx: &FrozenContext,
	opt: &Options,
	doc: Option<&CursorDoc>,
	args: Vec<Value>,
	sub: Option<&str>,
) -> Result<Value> {
	check_surrealism_enabled(ctx)?;
	let (ns, db) = ctx.get_ns_db_ids(opt).await?;
	let lookup = SurrealismCacheLookup::File(&ns, &db, &this.0.bucket, &this.0.key);
	let cached = crate::exec::vm::descriptor::object(ctx.get_surrealism_module(lookup)).await?;
	run_on_runtime(cached, ctx, opt, doc, args, sub).await
}

#[cfg(feature = "surrealism")]
pub(crate) async fn silo_executable_signature(
	this: &SiloExecutable,
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
	let runtime = crate::exec::vm::descriptor::object(ctx.get_surrealism_runtime(lookup)).await?;
	signature_from_runtime(&runtime, sub)
}

#[cfg(feature = "surrealism")]
pub(crate) async fn silo_executable_run(
	this: &SiloExecutable,
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
	let cached = crate::exec::vm::descriptor::object(ctx.get_surrealism_module(lookup)).await?;
	run_on_runtime(cached, ctx, opt, doc, args, sub).await
}

#[cfg(feature = "surrealism")]
pub(crate) fn check_surrealism_enabled(ctx: &FrozenContext) -> Result<()> {
	if !ctx.get_capabilities().allows_experimental(&ExperimentalTarget::Surrealism) {
		bail!("Experimental capability `surrealism` is not enabled");
	}
	Ok(())
}

#[cfg(feature = "surrealism")]
pub(crate) async fn run_on_runtime(
	cached: SurrealismCachedModule,
	ctx: &FrozenContext,
	opt: &Options,
	doc: Option<&CursorDoc>,
	args: Vec<Value>,
	sub: Option<&str>,
) -> Result<Value> {
	let display_name = sub.unwrap_or("<default>");
	tracing::debug!(name = %display_name, arg_count = args.len(), "run_on_runtime: starting");

	let args: Result<Vec<crate::types::PublicValue>, _> =
		args.into_iter().map(|x| x.try_into()).collect();
	let args = args?;

	let SurrealismCachedModule {
		runtime,
		module_display_name,
		#[cfg(feature = "http")]
		client,
	} = cached;
	// The declared mode decides whether writes are permitted for this call, and
	// whether a handle is lent at all. Read both before building the host, from
	// the same manifest entry the planner used to pick the statement's access
	// mode.
	//
	// An export the manifest does not name fails here rather than in the guest,
	// so an unknown name is reported by the host with the manifest's own error.
	let signature = runtime.get_signature(sub)?;
	let writeable = signature.writeable;
	let declares_transaction = signature.declares_transaction;
	let module_name = module_display_name.as_ref().to_string();
	// `Arc`, not `Box`: the guest may hold several host calls in flight, and each
	// one resolves the shared context out of the store's resource table.
	let host: Arc<dyn surrealism_runtime::host::InvocationContext> = Arc::new(Host::new(
		ctx,
		opt,
		doc,
		writeable,
		module_name,
		#[cfg(feature = "http")]
		client,
	));

	// Read before waiting for a controller, so a `TIMEOUT 1s` query is not held
	// in the pool queue for the pool's own, much longer, patience — with the
	// caller's transaction open the whole time.
	let ctx_timeout = ctx.timeout();
	let mut controller = runtime.acquire_controller(host.as_ref(), ctx_timeout).await?;

	// A function that declared no transaction parameter is lent nothing, which
	// is what makes "provably cannot query" true of the host rather than of the
	// generated dispatch. The manifest is user-supplied and says nothing about
	// the component beside it; what the property rests on is that this is the
	// only thing consulted, so a component whose body calls `transaction.sql`
	// must declare the parameter to be given a handle, and a manifest that
	// hides the parameter withholds it.
	let lent = declares_transaction.then(|| Arc::clone(&host));
	let result =
		controller.invoke_with_timeout(lent, sub.map(String::from), args, ctx_timeout).await;

	if result.as_ref().is_err_and(|e| e.is_trap()) {
		tracing::error!(
			name = %display_name,
			error = ?result.as_ref().err(),
			"run_on_runtime: WASM TRAP, dropping controller"
		);
		drop(controller);
	} else {
		runtime.release_controller(controller);
	}

	Ok(result?.into())
}

#[cfg(feature = "surrealism")]
pub(crate) fn signature_from_runtime(
	runtime: &surrealism_runtime::runtime::Runtime,
	sub: Option<&str>,
) -> Result<Signature> {
	let export = runtime.get_signature(sub)?;
	Ok(Signature {
		args: export.args.iter().map(|(_, k)| k.clone().into()).collect(),
		returns: Some(export.returns.clone().into()),
		writeable: export.writeable,
	})
}

#[cfg(feature = "surrealism")]
pub(crate) async fn get_module_exports(
	ctx: &FrozenContext,
	ns: &crate::catalog::NamespaceId,
	db: &crate::catalog::DatabaseId,
	executable: &crate::catalog::ModuleExecutable,
) -> Option<Value> {
	use crate::buc::store::ObjectKey;
	use crate::catalog::ModuleExecutable;
	use crate::surrealism::cache::SurrealismCacheLookup;

	match executable {
		ModuleExecutable::Surrealism(s) => {
			if let Ok(bucket) = ctx.get_bucket_store(*ns, *db, &s.bucket).await {
				let key = ObjectKey::new(s.key.clone());
				if let Ok(Some(bytes)) = bucket.get(&key).await
					&& let Ok(manifest) = surrealism_runtime::package::exports_manifest_from_reader(
						std::io::Cursor::new(bytes),
					) {
					return Some(surrealism_exports_manifest_to_value(&manifest));
				}
			}

			let lookup = SurrealismCacheLookup::File(ns, db, &s.bucket, &s.key);
			let runtime = match ctx.get_surrealism_runtime(lookup).await {
				Ok(r) => r,
				Err(e) => {
					tracing::trace!("Could not load module runtime for exports: {e}");
					return None;
				}
			};
			Some(surrealism_exports_manifest_to_value(runtime.exports()))
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
			let runtime = match ctx.get_surrealism_runtime(lookup).await {
				Ok(r) => r,
				Err(e) => {
					tracing::trace!("Could not load module runtime for exports: {e}");
					return None;
				}
			};
			Some(surrealism_exports_manifest_to_value(runtime.exports()))
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
			&& let Some(exports) = get_module_exports(ctx, &ns, &db, &module.executable).await
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
