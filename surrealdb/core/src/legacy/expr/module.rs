#[cfg(feature = "surrealism")]
use std::sync::Arc;

use anyhow::{Result, bail};
use reblessive::tree::Stk;

use crate::catalog::{DatabaseId, NamespaceId};
use crate::ctx::FrozenContext;
use crate::dbs::Options;
#[cfg(feature = "surrealism")]
use crate::dbs::capabilities::ExperimentalTarget;
use crate::doc::CursorDoc;
use crate::expr::module::{ModuleExecutable, Signature, SiloExecutable, SurrealismExecutable};
#[cfg(feature = "surrealism")]
use crate::surrealism::cache::SurrealismCacheLookup;
#[cfg(feature = "surrealism")]
use crate::surrealism::cache::SurrealismCachedModule;
#[cfg(feature = "surrealism")]
use crate::surrealism::host::Host;
use crate::val::Value;

pub(crate) async fn module_executable_signature(
	this: &ModuleExecutable,
	ctx: &FrozenContext,
	ns: &NamespaceId,
	db: &DatabaseId,
	sub: Option<&str>,
) -> Result<Signature> {
	match this {
		ModuleExecutable::Surrealism(surrealism) => {
			crate::legacy::surrealism_executable_signature(surrealism, ctx, ns, db, sub).await
		}
		ModuleExecutable::Silo(silo) => {
			crate::legacy::silo_executable_signature(silo, ctx, ns, db, sub).await
		}
	}
}

pub(crate) async fn module_executable_run(
	this: &ModuleExecutable,
	stk: &mut Stk,
	ctx: &FrozenContext,
	opt: &Options,
	doc: Option<&CursorDoc>,
	args: Vec<Value>,
	sub: Option<&str>,
) -> Result<Value> {
	match this {
		ModuleExecutable::Surrealism(surrealism) => {
			crate::legacy::surrealism_executable_run(surrealism, stk, ctx, opt, doc, args, sub)
				.await
		}
		ModuleExecutable::Silo(silo) => {
			crate::legacy::silo_executable_run(silo, stk, ctx, opt, doc, args, sub).await
		}
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
	let runtime = ctx.get_surrealism_runtime(lookup).await?;
	signature_from_runtime(&runtime, sub)
}

#[cfg(feature = "surrealism")]
pub(crate) async fn surrealism_executable_run(
	this: &SurrealismExecutable,
	_stk: &mut Stk,
	ctx: &FrozenContext,
	opt: &Options,
	doc: Option<&CursorDoc>,
	args: Vec<Value>,
	sub: Option<&str>,
) -> Result<Value> {
	check_surrealism_enabled(ctx)?;
	let (ns, db) = ctx.get_ns_db_ids(opt).await?;
	let lookup = SurrealismCacheLookup::File(&ns, &db, &this.0.bucket, &this.0.key);
	let cached = ctx.get_surrealism_module(lookup).await?;
	run_on_runtime(cached, ctx, opt, doc, args, sub).await
}

#[cfg(not(feature = "surrealism"))]
pub(crate) async fn surrealism_executable_signature(
	_this: &SurrealismExecutable,
	_ctx: &FrozenContext,
	_ns: &NamespaceId,
	_db: &DatabaseId,
	_sub: Option<&str>,
) -> Result<Signature> {
	bail!("Surrealism modules are not supported in WASM environments")
}

#[cfg(not(feature = "surrealism"))]
pub(crate) async fn surrealism_executable_run(
	_this: &SurrealismExecutable,
	_stk: &mut Stk,
	_ctx: &FrozenContext,
	_opt: &Options,
	_doc: Option<&CursorDoc>,
	_args: Vec<Value>,
	_sub: Option<&str>,
) -> Result<Value> {
	bail!("Surrealism functions are not supported in WASM environments")
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
	let runtime = ctx.get_surrealism_runtime(lookup).await?;
	signature_from_runtime(&runtime, sub)
}

#[cfg(feature = "surrealism")]
pub(crate) async fn silo_executable_run(
	this: &SiloExecutable,
	_stk: &mut Stk,
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
	let cached = ctx.get_surrealism_module(lookup).await?;
	run_on_runtime(cached, ctx, opt, doc, args, sub).await
}

#[cfg(not(feature = "surrealism"))]
pub(crate) async fn silo_executable_signature(
	_this: &SiloExecutable,
	_ctx: &FrozenContext,
	_ns: &NamespaceId,
	_db: &DatabaseId,
	_sub: Option<&str>,
) -> Result<Signature> {
	bail!("Surrealism functions are not supported in WASM environments")
}

#[cfg(not(feature = "surrealism"))]
pub(crate) async fn silo_executable_run(
	_this: &SiloExecutable,
	_stk: &mut Stk,
	_ctx: &FrozenContext,
	_opt: &Options,
	_doc: Option<&CursorDoc>,
	_args: Vec<Value>,
	_sub: Option<&str>,
) -> Result<Value> {
	bail!("Surrealism functions are not supported in WASM environments")
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
