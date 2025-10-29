use std::{fmt, thread};

use anyhow::{Result, bail};
use reblessive::tree::Stk;

use crate::catalog;
#[cfg(not(target_arch = "wasm32"))]
use crate::catalog::{DatabaseId, NamespaceId};
use crate::ctx::Context;
use crate::dbs::Options;
#[cfg(not(target_arch = "wasm32"))]
use crate::dbs::capabilities::ExperimentalTarget;
use crate::doc::CursorDoc;
use crate::expr::{Kind, Value};
#[cfg(not(target_arch = "wasm32"))]
use crate::surrealism::cache::SurrealismCacheLookup;
#[cfg(not(target_arch = "wasm32"))]
use crate::surrealism::host::Host;
#[cfg(not(target_arch = "wasm32"))]
use crate::surrealism::host::SignatureHost;
use crate::val::File;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) enum ModuleExecutable {
	Surrealism(SurrealismExecutable),
	Silo(SiloExecutable),
}

impl From<catalog::ModuleExecutable> for ModuleExecutable {
	fn from(executable: catalog::ModuleExecutable) -> Self {
		match executable {
			catalog::ModuleExecutable::Surrealism(surrealism) => {
				ModuleExecutable::Surrealism(surrealism.into())
			}
			catalog::ModuleExecutable::Silo(silo) => ModuleExecutable::Silo(silo.into()),
		}
	}
}

impl From<ModuleExecutable> for catalog::ModuleExecutable {
	fn from(executable: ModuleExecutable) -> Self {
		match executable {
			ModuleExecutable::Surrealism(surrealism) => {
				catalog::ModuleExecutable::Surrealism(surrealism.into())
			}
			ModuleExecutable::Silo(silo) => catalog::ModuleExecutable::Silo(silo.into()),
		}
	}
}

impl ModuleExecutable {
	pub(crate) async fn signature(
		&self,
		ctx: &Context,
		ns: &NamespaceId,
		db: &DatabaseId,
		sub: Option<&str>,
	) -> Result<Signature> {
		match self {
			ModuleExecutable::Surrealism(surrealism) => {
				surrealism.signature(ctx, ns, db, sub).await
			}
			ModuleExecutable::Silo(silo) => silo.signature(ctx, sub).await,
		}
	}

	pub(crate) async fn run(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
		args: Vec<Value>,
		sub: Option<&str>,
	) -> Result<Value> {
		match self {
			ModuleExecutable::Surrealism(surrealism) => {
				surrealism.run(stk, ctx, opt, doc, args, sub).await
			}
			ModuleExecutable::Silo(silo) => silo.run(stk, ctx, opt, doc, args, sub).await,
		}
	}
}

impl fmt::Display for ModuleExecutable {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			ModuleExecutable::Surrealism(surrealism) => surrealism.fmt(f),
			ModuleExecutable::Silo(silo) => silo.fmt(f),
		}
	}
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) struct Signature {
	pub(crate) args: Vec<Kind>,
	pub(crate) returns: Option<Kind>,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) struct SurrealismExecutable(pub File);

impl From<catalog::SurrealismExecutable> for SurrealismExecutable {
	fn from(executable: catalog::SurrealismExecutable) -> Self {
		Self(File::new(executable.bucket, executable.key))
	}
}

impl From<SurrealismExecutable> for catalog::SurrealismExecutable {
	fn from(executable: SurrealismExecutable) -> Self {
		Self {
			bucket: executable.0.bucket,
			key: executable.0.key,
		}
	}
}

impl fmt::Display for SurrealismExecutable {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		self.0.fmt(f)
	}
}

#[cfg(not(target_arch = "wasm32"))]
impl SurrealismExecutable {
	pub(crate) async fn signature(
		&self,
		ctx: &Context,
		ns: &NamespaceId,
		db: &DatabaseId,
		sub: Option<&str>,
	) -> Result<Signature> {
		if !ctx.get_capabilities().allows_experimental(&ExperimentalTarget::Surrealism) {
			bail!(
				"Failed to get surrealism function signature: Experimental capability `surrealism` is not enabled"
			);
		}

		let lookup = SurrealismCacheLookup::File(&ns, &db, &self.0);
		let runtime = ctx.get_surrealism_runtime(lookup).await?;

		spawn_thread(move || async move {
			let host = Box::new(SignatureHost::new());
			let mut controller = runtime.new_controller(host).await?;

			let args = controller
				.args(sub.map(String::from))
				.await?
				.into_iter()
				.map(|x| x.into())
				.collect();

			let returns =
				controller.returns(sub.map(String::from)).await.map(|x| Some(x.into()))?;

			Ok(Signature {
				args,
				returns,
			})
		})
	}

	pub(crate) async fn run(
		&self,
		_stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
		args: Vec<Value>,
		sub: Option<&str>,
	) -> Result<Value> {
		if !ctx.get_capabilities().allows_experimental(&ExperimentalTarget::Surrealism) {
			bail!(
				"Failed to run surrealism function: Experimental capability `surrealism` is not enabled"
			);
		}

		let (ns, db) = ctx.get_ns_db_ids(opt).await?;
		let lookup = SurrealismCacheLookup::File(&ns, &db, &self.0);
		let runtime = ctx.get_surrealism_runtime(lookup).await?;

		let ctx = ctx.clone();
		let opt = opt.clone();
		let doc = doc.cloned();
		spawn_thread(move || async move {
			let host = Box::new(Host::new(&ctx, &opt, doc.as_ref()));
			let mut controller = runtime.new_controller(host).await?;

			let args: Result<Vec<crate::types::PublicValue>, _> =
				args.into_iter().map(|x| x.try_into()).collect();
			let args = args?;
			controller.invoke(sub.map(String::from), args).await.map(|x| x.into())
		})
	}
}

#[cfg(target_arch = "wasm32")]
impl SurrealismExecutable {
	pub(crate) async fn signature(
		&self,
		_stk: &mut Stk,
		_ctx: &Context,
		_opt: &Options,
		_doc: Option<&CursorDoc>,
		_sub: Option<&str>,
	) -> Result<Signature> {
		bail!("Surrealism functions are not supported in WASM environments")
	}

	pub(crate) async fn run(
		&self,
		_stk: &mut Stk,
		_ctx: &Context,
		_opt: &Options,
		_doc: Option<&CursorDoc>,
		_args: Vec<Value>,
		_sub: Option<&str>,
	) -> Result<Value> {
		bail!("Surrealism functions are not supported in WASM environments")
	}
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) struct SiloExecutable {
	pub organisation: String,
	pub package: String,
	pub major: u32,
	pub minor: u32,
	pub patch: u32,
}

impl From<catalog::SiloExecutable> for SiloExecutable {
	fn from(executable: catalog::SiloExecutable) -> Self {
		Self {
			organisation: executable.organisation,
			package: executable.package,
			major: executable.major,
			minor: executable.minor,
			patch: executable.patch,
		}
	}
}

impl From<SiloExecutable> for catalog::SiloExecutable {
	fn from(executable: SiloExecutable) -> Self {
		Self {
			organisation: executable.organisation,
			package: executable.package,
			major: executable.major,
			minor: executable.minor,
			patch: executable.patch,
		}
	}
}

impl fmt::Display for SiloExecutable {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(
			f,
			"silo::{}::{}<{}.{}.{}>",
			self.organisation, self.package, self.major, self.minor, self.patch
		)
	}
}

#[cfg(not(target_arch = "wasm32"))]
impl SiloExecutable {
	pub(crate) async fn signature(&self, ctx: &Context, sub: Option<&str>) -> Result<Signature> {
		if !ctx.get_capabilities().allows_experimental(&ExperimentalTarget::Surrealism) {
			bail!(
				"Failed to get silo function signature: Experimental capability `surrealism` is not enabled"
			);
		}

		let lookup = SurrealismCacheLookup::Silo(
			&self.organisation,
			&self.package,
			self.major,
			self.minor,
			self.patch,
		);
		let runtime = ctx.get_surrealism_runtime(lookup).await?;

		spawn_thread(move || async move {
			let host = Box::new(SignatureHost::new());
			let mut controller = runtime.new_controller(host).await?;

			let args = controller
				.args(sub.map(String::from))
				.await?
				.into_iter()
				.map(|x| x.into())
				.collect();

			let returns =
				controller.returns(sub.map(String::from)).await.map(|x| Some(x.into()))?;

			Ok(Signature {
				args,
				returns,
			})
		})
	}

	pub(crate) async fn run(
		&self,
		_stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
		args: Vec<Value>,
		sub: Option<&str>,
	) -> Result<Value> {
		if !ctx.get_capabilities().allows_experimental(&ExperimentalTarget::Surrealism) {
			bail!(
				"Failed to run silo function: Experimental capability `surrealism` is not enabled"
			);
		}

		let lookup = SurrealismCacheLookup::Silo(
			&self.organisation,
			&self.package,
			self.major,
			self.minor,
			self.patch,
		);
		let runtime = ctx.get_surrealism_runtime(lookup).await?;

		let ctx = ctx.clone();
		let opt = opt.clone();
		let doc = doc.cloned();
		spawn_thread(move || async move {
			let host = Box::new(Host::new(&ctx, &opt, doc.as_ref()));
			let mut controller = runtime.new_controller(host).await?;

			let args: Result<Vec<crate::types::PublicValue>, _> =
				args.into_iter().map(|x| x.try_into()).collect();
			let args = args?;
			controller.invoke(sub.map(String::from), args).await.map(|x| x.into())
		})
	}
}

#[cfg(target_arch = "wasm32")]
impl SiloExecutable {
	pub(crate) async fn signature(
		&self,
		_stk: &mut Stk,
		_ctx: &Context,
		_opt: &Options,
		_doc: Option<&CursorDoc>,
		_sub: Option<&str>,
	) -> Result<Signature> {
		bail!("Surrealism functions are not supported in WASM environments")
	}

	pub(crate) async fn run(
		&self,
		_stk: &mut Stk,
		_ctx: &Context,
		_opt: &Options,
		_doc: Option<&CursorDoc>,
		_args: Vec<Value>,
		_sub: Option<&str>,
	) -> Result<Value> {
		bail!("Surrealism functions are not supported in WASM environments")
	}
}

/// Spawn a dedicated thread to run async operations.
///
/// Uses scoped threads to allow safe borrowing from the current scope without requiring
/// 'static lifetime bounds. Creates a single-threaded tokio runtime in the thread to
/// handle async operations. The function blocks until the spawned thread completes.
fn spawn_thread<F, Fut, R>(f: F) -> Result<R>
where
	F: FnOnce() -> Fut + Send,
	Fut: std::future::Future<Output = Result<R>> + Send,
	R: Send,
{
	thread::scope(|s| {
		let handle = s.spawn(|| {
			// Create a single-threaded tokio runtime for async operations
			let rt = tokio::runtime::Builder::new_current_thread()
				.enable_all()
				.build()
				.map_err(|e| anyhow::anyhow!("Failed to create runtime: {e}"))?;
			rt.block_on(f())
		});
		handle.join().map_err(|_| anyhow::anyhow!("Thread panicked"))?
	})
}
