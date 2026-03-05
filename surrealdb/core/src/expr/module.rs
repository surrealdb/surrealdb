#[cfg(feature = "surrealism")]
use std::sync::Arc;

use anyhow::{Result, bail};
use reblessive::tree::Stk;
use surrealdb_types::{SqlFormat, ToSql};

use crate::catalog;
use crate::catalog::{DatabaseId, NamespaceId};
use crate::ctx::FrozenContext;
use crate::dbs::Options;
#[cfg(feature = "surrealism")]
use crate::dbs::capabilities::ExperimentalTarget;
use crate::doc::CursorDoc;
use crate::expr::{Kind, Value};
#[cfg(feature = "surrealism")]
use crate::surrealism::cache::SurrealismCacheLookup;
#[cfg(feature = "surrealism")]
use crate::surrealism::host::Host;
#[cfg(feature = "surrealism")]
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
		ctx: &FrozenContext,
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
		ctx: &FrozenContext,
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

impl ToSql for ModuleExecutable {
	fn fmt_sql(&self, f: &mut String, sql_fmt: SqlFormat) {
		let module_executable: crate::sql::ModuleExecutable = self.clone().into();
		module_executable.fmt_sql(f, sql_fmt);
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

impl ToSql for SurrealismExecutable {
	fn fmt_sql(&self, f: &mut String, sql_fmt: SqlFormat) {
		let surrealism_executable: crate::sql::SurrealismExecutable = self.clone().into();
		surrealism_executable.fmt_sql(f, sql_fmt);
	}
}

#[cfg(feature = "surrealism")]
impl SurrealismExecutable {
	pub(crate) async fn signature(
		&self,
		ctx: &FrozenContext,
		ns: &NamespaceId,
		db: &DatabaseId,
		sub: Option<&str>,
	) -> Result<Signature> {
		check_surrealism_enabled(ctx)?;
		let lookup = SurrealismCacheLookup::File(ns, db, &self.0.bucket, &self.0.key);
		let runtime = ctx.get_surrealism_runtime(lookup).await?;
		signature_from_runtime(runtime, sub).await
	}

	pub(crate) async fn run(
		&self,
		_stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
		doc: Option<&CursorDoc>,
		args: Vec<Value>,
		sub: Option<&str>,
	) -> Result<Value> {
		check_surrealism_enabled(ctx)?;
		let (ns, db) = ctx.get_ns_db_ids(opt).await?;
		let lookup = SurrealismCacheLookup::File(&ns, &db, &self.0.bucket, &self.0.key);
		let runtime = ctx.get_surrealism_runtime(lookup).await?;
		run_on_runtime(runtime, ctx, opt, doc, args, sub).await
	}
}

#[cfg(not(feature = "surrealism"))]
impl SurrealismExecutable {
	pub(crate) async fn signature(
		&self,
		_ctx: &FrozenContext,
		_ns: &NamespaceId,
		_db: &DatabaseId,
		_sub: Option<&str>,
	) -> Result<Signature> {
		bail!("Surrealism modules are not supported in WASM environments")
	}

	pub(crate) async fn run(
		&self,
		_stk: &mut Stk,
		_ctx: &FrozenContext,
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

impl ToSql for SiloExecutable {
	fn fmt_sql(&self, f: &mut String, sql_fmt: SqlFormat) {
		let silo_executable: crate::sql::SiloExecutable = self.clone().into();
		silo_executable.fmt_sql(f, sql_fmt);
	}
}

#[cfg(feature = "surrealism")]
impl SiloExecutable {
	pub(crate) async fn signature(
		&self,
		ctx: &FrozenContext,
		sub: Option<&str>,
	) -> Result<Signature> {
		check_surrealism_enabled(ctx)?;
		let lookup = SurrealismCacheLookup::Silo(
			&self.organisation,
			&self.package,
			self.major,
			self.minor,
			self.patch,
		);
		let runtime = ctx.get_surrealism_runtime(lookup).await?;
		signature_from_runtime(runtime, sub).await
	}

	pub(crate) async fn run(
		&self,
		_stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
		doc: Option<&CursorDoc>,
		args: Vec<Value>,
		sub: Option<&str>,
	) -> Result<Value> {
		check_surrealism_enabled(ctx)?;
		let lookup = SurrealismCacheLookup::Silo(
			&self.organisation,
			&self.package,
			self.major,
			self.minor,
			self.patch,
		);
		let runtime = ctx.get_surrealism_runtime(lookup).await?;
		run_on_runtime(runtime, ctx, opt, doc, args, sub).await
	}
}

#[cfg(not(feature = "surrealism"))]
impl SiloExecutable {
	pub(crate) async fn signature(
		&self,
		_ctx: &FrozenContext,
		_sub: Option<&str>,
	) -> Result<Signature> {
		bail!("Surrealism functions are not supported in WASM environments")
	}

	pub(crate) async fn run(
		&self,
		_stk: &mut Stk,
		_ctx: &FrozenContext,
		_opt: &Options,
		_doc: Option<&CursorDoc>,
		_args: Vec<Value>,
		_sub: Option<&str>,
	) -> Result<Value> {
		bail!("Surrealism functions are not supported in WASM environments")
	}
}

#[cfg(feature = "surrealism")]
fn check_surrealism_enabled(ctx: &FrozenContext) -> Result<()> {
	if !ctx.get_capabilities().allows_experimental(&ExperimentalTarget::Surrealism) {
		bail!("Experimental capability `surrealism` is not enabled");
	}
	Ok(())
}

#[cfg(feature = "surrealism")]
async fn signature_from_runtime(
	runtime: Arc<surrealism_runtime::controller::Runtime>,
	sub: Option<&str>,
) -> Result<Signature> {
	let host = Box::new(SignatureHost::new());
	let mut controller = runtime.new_controller(host).await?;

	let args =
		controller.args(sub.map(String::from)).await?.into_iter().map(|x| x.into()).collect();

	let returns = controller.returns(sub.map(String::from)).await.map(|x| Some(x.into()))?;

	Ok(Signature {
		args,
		returns,
	})
}

#[cfg(feature = "surrealism")]
async fn run_on_runtime(
	runtime: Arc<surrealism_runtime::controller::Runtime>,
	ctx: &FrozenContext,
	opt: &Options,
	doc: Option<&CursorDoc>,
	args: Vec<Value>,
	sub: Option<&str>,
) -> Result<Value> {
	let host = Box::new(Host::new(ctx, opt, doc));
	let mut controller = runtime.new_controller(host).await?;

	let args: Result<Vec<crate::types::PublicValue>, _> =
		args.into_iter().map(|x| x.try_into()).collect();
	let args = args?;
	Ok(controller.invoke(sub.map(String::from), args).await.map(|x| x.into())?)
}
