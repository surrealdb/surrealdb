use std::fmt::{self, Display};
use std::thread;

use anyhow::{Result, bail};
use reblessive::tree::Stk;

use crate::catalog;
use crate::ctx::{Context, MutableContext};
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::expr::expression::VisitExpression;
use crate::expr::{Block, Expr, FlowResultExt, Kind, Value};
use crate::fmt::EscapeKwFreeIdent;
use crate::surrealism::cache::SurrealismCacheLookup;
use crate::surrealism::host::Host;
use crate::val::File;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) enum Executable {
	Block(BlockExecutable),
	Surrealism(SurrealismExecutable),
	Silo(SiloExecutable),
}

impl From<catalog::Executable> for Executable {
	fn from(executable: catalog::Executable) -> Self {
		match executable {
			catalog::Executable::Block(block) => Executable::Block(block.into()),
			catalog::Executable::Surrealism(surrealism) => {
				Executable::Surrealism(surrealism.into())
			}
			catalog::Executable::Silo(silo) => Executable::Silo(silo.into()),
		}
	}
}

impl From<Executable> for catalog::Executable {
	fn from(executable: Executable) -> Self {
		match executable {
			Executable::Block(block) => catalog::Executable::Block(block.into()),
			Executable::Surrealism(surrealism) => {
				catalog::Executable::Surrealism(surrealism.into())
			}
			Executable::Silo(silo) => catalog::Executable::Silo(silo.into()),
		}
	}
}
impl Executable {
	pub(crate) async fn signature(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
		sub: Option<&str>,
	) -> Result<Signature> {
		match self {
			Executable::Block(block) => block.signature(stk, ctx, opt, doc, sub).await,
			Executable::Surrealism(surrealism) => {
				surrealism.signature(stk, ctx, opt, doc, sub).await
			}
			Executable::Silo(silo) => silo.signature(stk, ctx, opt, doc, sub).await,
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
			Executable::Block(block) => block.run(stk, ctx, opt, doc, args, sub).await,
			Executable::Surrealism(surrealism) => {
				surrealism.run(stk, ctx, opt, doc, args, sub).await
			}
			Executable::Silo(silo) => silo.run(stk, ctx, opt, doc, args, sub).await,
		}
	}
}

impl fmt::Display for Executable {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			Executable::Block(block) => block.fmt(f),
			Executable::Surrealism(surrealism) => surrealism.fmt(f),
			Executable::Silo(silo) => silo.fmt(f),
		}
	}
}

impl VisitExpression for Executable {
	fn visit<F>(&self, visitor: &mut F)
	where
		F: FnMut(&Expr),
	{
		match self {
			Executable::Block(block) => block.visit(visitor),
			Executable::Surrealism(surrealism) => surrealism.visit(visitor),
			Executable::Silo(silo) => silo.visit(visitor),
		}
	}
}

pub(crate) struct Signature {
	pub(crate) args: Vec<Kind>,
	pub(crate) returns: Option<Kind>,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) struct BlockExecutable {
	pub args: Vec<(String, Kind)>,
	pub returns: Option<Kind>,
	pub block: Block,
}

impl From<catalog::BlockExecutable> for BlockExecutable {
	fn from(executable: catalog::BlockExecutable) -> Self {
		Self {
			args: executable.args,
			returns: executable.returns,
			block: executable.block,
		}
	}
}

impl From<BlockExecutable> for catalog::BlockExecutable {
	fn from(executable: BlockExecutable) -> Self {
		Self {
			args: executable.args,
			returns: executable.returns,
			block: executable.block,
		}
	}
}

impl BlockExecutable {
	pub(crate) async fn signature(
		&self,
		_stk: &mut Stk,
		_ctx: &Context,
		_opt: &Options,
		_doc: Option<&CursorDoc>,
		sub: Option<&str>,
	) -> Result<Signature> {
		if sub.is_some() {
			bail!("Sub-functions are not supported for block functions");
		}

		let args = self.args.iter().map(|(_, kind)| kind.clone()).collect();
		let returns = self.returns.clone();

		Ok(Signature {
			args,
			returns,
		})
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
		if sub.is_some() {
			bail!("Sub-functions are not supported for block functions");
		}

		// Compute the function arguments
		// Duplicate context
		let mut ctx = MutableContext::new_isolated(ctx);
		// Process the function arguments
		for (val, (name, kind)) in args.into_iter().zip(&self.args) {
			ctx.add_value(
				name.clone(),
				val.coerce_to_kind(kind).map_err(Error::from).map_err(anyhow::Error::new)?.into(),
			);
		}
		// Freeze the context
		let ctx = ctx.freeze();
		// Run the block
		self.block.compute(stk, &ctx, opt, doc).await.catch_return()
	}
}

impl fmt::Display for BlockExecutable {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "(")?;
		for (i, (name, kind)) in self.args.iter().enumerate() {
			if i > 0 {
				f.write_str(", ")?;
			}
			write!(f, "${}: {kind}", EscapeKwFreeIdent(name))?;
		}
		f.write_str(") ")?;
		if let Some(ref v) = self.returns {
			write!(f, "-> {v} ")?;
		}
		Display::fmt(&self.block, f)?;
		Ok(())
	}
}

impl VisitExpression for BlockExecutable {
	fn visit<F>(&self, visitor: &mut F)
	where
		F: FnMut(&Expr),
	{
		self.block.visit(visitor);
	}
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
		write!(f, " AS {}", self.0)
	}
}

// Nothing to visit, but required by the trait
impl VisitExpression for SurrealismExecutable {
	fn visit<F>(&self, _visitor: &mut F)
	where
		F: FnMut(&Expr),
	{
	}
}

impl SurrealismExecutable {
	pub(crate) async fn signature(
		&self,
		_stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
		sub: Option<&str>,
	) -> Result<Signature> {
		let (ns, db) = ctx.get_ns_db_ids(opt).await?;
		let lookup = SurrealismCacheLookup::File(&ns, &db, &self.0);
		let runtime = ctx.get_surrealism_runtime(lookup).await?;

		let ctx = ctx.clone();
		let opt = opt.clone();
		let doc = doc.cloned();
		spawn_thread(move || async move {
			let host = Box::new(Host::new(&ctx, &opt, doc.as_ref()));
			let mut controller = runtime.new_controller(host).await?;

			let args = controller.args(sub.map(String::from)).await?
				.into_iter()
				.map(|x| x.into())
				.collect();
			let returns = controller.returns(sub.map(String::from)).await
				.map(|x| Some(x.into()))?;

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
			" AS silo::{}::{}<{}.{}.{}>",
			self.organisation, self.package, self.major, self.minor, self.patch
		)
	}
}

// Nothing to visit, but required by the trait
impl VisitExpression for SiloExecutable {
	fn visit<F>(&self, _visitor: &mut F)
	where
		F: FnMut(&Expr),
	{
	}
}

impl SiloExecutable {
	pub(crate) async fn signature(
		&self,
		_stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
		sub: Option<&str>,
	) -> Result<Signature> {
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

			let args = controller.args(sub.map(String::from)).await?
				.into_iter()
				.map(|x| x.into())
				.collect();
			let returns = controller.returns(sub.map(String::from)).await
				.map(|x| Some(x.into()))?;

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