//! Extension runners: how an extension is executed.
//!
//! Core owns everything that does not depend on an execution engine — the
//! definitions, fetching, the prepared cache, capabilities and the host
//! interface. A runner only prepares an archive and invokes it. It is installed
//! by the embedder through [`Builder::with_extension_runner`](crate::kvs::Builder),
//! or by the composer; the `surreal` binary and [`CommunityComposer`](crate::CommunityComposer)
//! install [`SurrealismRunner`], which runs Surrealism archives on wasmtime.
//!
//! A datastore with no runner still accepts definitions. Invoking one fails
//! with [`NO_RUNNER`].
//!
//! # Isolation
//!
//! A prepared extension is shared by every definition and tenant whose archive
//! has the same [`Digest`], so it must hold no state of theirs. Each
//! [`PreparedExtension::invoke`] runs in fresh memory initialised from the
//! extension's image, and nothing it writes survives it. What the invocation
//! may do arrives with it, in [`Invocation::capabilities`]: the definition's
//! `CAPABILITIES` intersected with the server's, never the archive's request.

use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context as _, Result, bail};
use bytes::Bytes;
use surrealism_runtime::NetPolicy;
use surrealism_runtime::config::SurrealismConfig as PackageConfig;
use surrealism_runtime::epoch::{EngineSettings, Engines};
use surrealism_runtime::exports::ExportsManifest;
use surrealism_runtime::host::InvocationContext;
use surrealism_runtime::package::{SurrealismPackage, UnpackOptions};
use surrealism_runtime::runtime::{Call, InstanceSettings, Runtime};
use web_time::Instant;

use crate::dbs::Capabilities;
use crate::expr::ManifestDigest;
use crate::extension::capabilities::EffectiveCapabilities;
#[cfg(feature = "http")]
use crate::http::HttpClient;
use crate::types::PublicValue;

/// The error an invocation fails with when the datastore has no runner.
pub const NO_RUNNER: &str =
	"This datastore has no extension runner installed, so extensions cannot be invoked";

/// The deepest an extension may call into another, counting the outermost
/// invocation as one. A call that would go deeper fails.
pub const MAX_EXTENSION_DEPTH: u8 = 8;

/// A boxed future, as the runner traits return.
pub type BoxFut<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Core's side of the host interface, lent to the guest for one invocation.
pub type TransactionHandle = Arc<dyn InvocationContext>;

/// An archive that may run, ready to prepare.
///
/// Core has read the archive's `surrealism/MANIFEST`, found that its digest is
/// the definition's `DIGEST`, and proved the definition's `SIGNER`. Whether the
/// other files match the manifest is not yet known: the runner learns that
/// when it unpacks the archive, and must refuse one whose files do not.
pub struct VerifiedSource {
	/// `sha256(surrealism/MANIFEST)`, the definition's `DIGEST`.
	pub digest: ManifestDigest,
	/// The `.surli` archive.
	pub archive: Bytes,
	/// Where an attached filesystem is unpacked; the system's temporary
	/// directory when `None`.
	pub temp_base: Option<PathBuf>,
	/// The most an attached filesystem may unpack to, in bytes.
	pub max_fs_bytes: u64,
	/// The archive's `SIGNATURE`, as its source delivered it. Every load of a
	/// definition checks it against that definition's `SIGNER`.
	pub signature: Option<Arc<[u8]>>,
}

/// What one definition of an extension may do, in the form its invocations
/// check: the network policy the guest's sockets are held to, the narrowed
/// server capabilities installed on every host call, and the limits.
///
/// Built from the definition's `CAPABILITIES` and the server's capabilities,
/// once per distinct `CAPABILITIES` value, and shared by every invocation of
/// a definition that grants it. The runner reads
/// the network policy and the limits; core reads the rest on every host call.
pub struct RunnerCapabilities {
	/// Where its own sockets may connect.
	pub net: Arc<NetPolicy>,
	/// The largest linear memory one invocation may grow to.
	pub memory: Option<usize>,
	/// The longest one invocation may run.
	pub timeout: Option<Duration>,
	/// Whether the timeout is enforced inside the guest. `false` trades that
	/// for speed: the guest can then run for ever.
	pub strict_timeout: bool,
	/// The definition's `CAPABILITIES` intersected with the server's, which
	/// every host call checks.
	pub(crate) grant: EffectiveCapabilities,
	/// The server's capabilities narrowed to [`Self::grant`], installed on the
	/// context of every host call.
	pub(crate) scoped: Arc<Capabilities>,
	/// The client `http::*` calls made through `tx.run` use.
	#[cfg(feature = "http")]
	pub(crate) http_client: Arc<HttpClient>,
}

/// Where an invocation's WASI-style standard output goes.
pub struct Stdio {
	pub stdout: Arc<dyn Fn(&str) + Send + Sync>,
	pub stderr: Arc<dyn Fn(&str) + Send + Sync>,
}

/// The bounds one invocation runs within.
#[derive(Clone, Copy, Debug)]
pub struct Limits {
	/// When the calling query times out.
	pub deadline: Option<Instant>,
	/// How many invocations enclose this one. Zero for a call made by a query;
	/// more for one an extension made through `tx.run`. A nested invocation
	/// never waits for capacity.
	pub depth: u8,
}

/// One invocation of a prepared extension.
pub struct Invocation<'a> {
	/// The export to call; `None` is the default export.
	pub function: Option<&'a str>,
	/// One argument list per row. The rows share the invocation's memory.
	pub rows: Vec<Vec<PublicValue>>,
	/// Lent for this call only; `None` for a function that declared no
	/// transaction parameter.
	pub tx: Option<TransactionHandle>,
	/// What the guest's `binding` import answers from, lent for this call
	/// whether or not the function declared a transaction.
	pub bindings: Option<Arc<dyn surrealism_runtime::host::Bindings>>,
	pub stdio: Stdio,
	pub capabilities: &'a RunnerCapabilities,
	pub limits: Limits,
}

/// Executes extensions. Installed through the datastore builder.
pub trait ExtensionRunner: Send + Sync + 'static {
	fn name(&self) -> &'static str;

	/// Refuse capabilities this runner cannot honour, such as a memory limit
	/// above what the platform allows.
	fn check(&self, capabilities: &RunnerCapabilities) -> Result<()>;

	/// Acquire what every invocation will need, so that a failure to get it
	/// shows when the datastore starts rather than on the first call. Called
	/// once at startup when extensions are defined.
	fn start(&self) -> Result<()> {
		Ok(())
	}

	/// Compile an archive and build its memory image. Called once per
	/// [`ManifestDigest`]; core caches the result and shares it between every
	/// definition and tenant with that digest.
	fn prepare<'a>(
		&'a self,
		source: &'a VerifiedSource,
	) -> BoxFut<'a, Result<Arc<dyn PreparedExtension>>>;
}

/// An archive prepared by a runner. Holds no tenant state.
pub trait PreparedExtension: Send + Sync {
	/// The package's own manifest: its name and the capabilities it asks for.
	fn package(&self) -> &PackageConfig;

	fn exports(&self) -> &ExportsManifest;

	/// Do now whatever an invocation under `capabilities` would otherwise do
	/// first, such as compiling for the engine they select. Concurrent callers
	/// do the work once. Invocations call it themselves, so it is only ever an
	/// optimisation.
	fn warm<'a>(&'a self, capabilities: &'a RunnerCapabilities) -> BoxFut<'a, Result<()>> {
		let _ = capabilities;
		Box::pin(async { Ok(()) })
	}

	/// One invocation, in fresh memory. Nothing written survives the call.
	/// Returns one value per row.
	fn invoke<'a>(&'a self, call: Invocation<'a>) -> BoxFut<'a, Result<Vec<PublicValue>>>;
}

/// Runs Surrealism archives on wasmtime.
///
/// Every invocation instantiates afresh from the archive's compiled
/// `InstancePre` into a slot of a pooling allocator, with the module's memory
/// image mapped copy-on-write, and drops the instance on return. The slot count
/// is the concurrency limit.
pub struct SurrealismRunner {
	engines: Arc<Engines>,
}

impl SurrealismRunner {
	/// A runner whose pool holds `max_concurrent_invocations` slots, each able
	/// to hold `max_memory_bytes` of linear memory.
	pub fn new(max_concurrent_invocations: u32, max_memory_bytes: Option<usize>) -> Self {
		Self {
			engines: surrealism_runtime::epoch::engines(EngineSettings {
				max_concurrent_invocations,
				max_memory_bytes,
			}),
		}
	}

	/// A runner sized by the datastore's Surrealism configuration.
	pub(crate) fn from_config(config: &crate::surrealism::config::SurrealismConfig) -> Self {
		Self::new(config.surrealism_max_concurrent_invocations, config.surrealism_max_memory)
	}

	/// A runner sized by the configuration in `config`.
	pub fn from_config_map(config: &surrealdb_cnf::ConfigMap) -> Self {
		Self::from_config(&config.load())
	}
}

impl ExtensionRunner for SurrealismRunner {
	fn name(&self) -> &'static str {
		"surrealism"
	}

	/// Creates the guarded engine, which reserves the pool's address space
	/// and starts its epoch ticker.
	fn start(&self) -> Result<()> {
		self.engines.engine(true)?;
		Ok(())
	}

	fn check(&self, capabilities: &RunnerCapabilities) -> Result<()> {
		let ceiling = self.engines.settings().slot_memory();
		if let Some(memory) = capabilities.memory
			&& memory > ceiling
		{
			bail!(
				"The extension may use {memory} bytes of memory, but this server's instances hold \
				 at most {ceiling}"
			);
		}
		Ok(())
	}

	fn prepare<'a>(
		&'a self,
		source: &'a VerifiedSource,
	) -> BoxFut<'a, Result<Arc<dyn PreparedExtension>>> {
		Box::pin(async move {
			let engines = Arc::clone(&self.engines);
			let archive = source.archive.clone();
			let temp_base = source.temp_base.clone();
			// The first 8 bytes of the digest name the unpack directory, so it
			// is recognisable without making the path long.
			let ManifestDigest::Sha256(bytes) = &source.digest;
			let short = hex::encode(&bytes[..bytes.len().min(8)]);
			let temp_prefix = format!("SURREAL_MODFS_{short}_");
			let max_fs_bytes = source.max_fs_bytes;
			let digest = source.digest.clone();
			// Unpacking writes the attached filesystem and compiling runs
			// Cranelift; both block.
			let runtime = tokio::task::spawn_blocking(move || -> Result<Runtime> {
				let opts = UnpackOptions {
					temp_base: temp_base.as_deref(),
					temp_prefix: &temp_prefix,
					max_fs_bytes,
				};
				// Unpacking checks every file against the manifest, so a
				// package whose manifest is the verified one holds exactly
				// the verified files.
				let package = SurrealismPackage::from_reader(std::io::Cursor::new(archive), &opts)?;
				let unpacked = package.digest().map(|d| ManifestDigest::sha256(d.0));
				if unpacked.as_ref() != Some(&digest) {
					bail!("The extension's archive changed after it was verified");
				}
				Ok(Runtime::new(package, engines)?)
			})
			.await
			.context("WASM compile task aborted")??;
			Ok(Arc::new(PreparedSurrealism {
				runtime: Arc::new(runtime),
			}) as Arc<dyn PreparedExtension>)
		})
	}
}

/// A Surrealism archive compiled by [`SurrealismRunner`].
struct PreparedSurrealism {
	runtime: Arc<Runtime>,
}

impl PreparedExtension for PreparedSurrealism {
	fn package(&self) -> &PackageConfig {
		self.runtime.config()
	}

	fn exports(&self) -> &ExportsManifest {
		self.runtime.exports()
	}

	/// Compile for the engine `capabilities` select. `Runtime::new` compiles
	/// the guarded one; the fast one is compiled here, once.
	fn warm<'a>(&'a self, capabilities: &'a RunnerCapabilities) -> BoxFut<'a, Result<()>> {
		Box::pin(async move {
			self.runtime.prepare_async(capabilities.strict_timeout).await?;
			Ok(())
		})
	}

	fn invoke<'a>(&'a self, call: Invocation<'a>) -> BoxFut<'a, Result<Vec<PublicValue>>> {
		Box::pin(async move {
			let caps = call.capabilities;
			self.warm(caps).await?;
			let values = self
				.runtime
				.invoke(Call {
					function: call.function.map(str::to_owned),
					rows: call.rows,
					tx: call.tx,
					settings: InstanceSettings {
						net: Arc::clone(&caps.net),
						max_memory_bytes: caps.memory,
						max_execution_time: caps.timeout,
						strict_timeout: caps.strict_timeout,
						stdout: call.stdio.stdout,
						stderr: call.stdio.stderr,
						bindings: call.bindings,
					},
					deadline: call.limits.deadline,
					nested: call.limits.depth > 0,
				})
				.await?;
			Ok(values)
		})
	}
}
