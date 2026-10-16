//! A compiled extension, and invocations of it.
//!
//! # Architecture
//!
//! - **`Runtime`**: one compiled component, prepared once and shared by every caller of the same
//!   archive. Thread-safe (`Arc<Runtime>`). It holds no tenant or request state: the compiled code,
//!   the resolved imports (`InstancePre`), the exports manifest and the attached filesystem, opened
//!   once.
//!
//! - **`Controller`**: one fresh instance serving one invocation. It is created from the
//!   `InstancePre` into a pooling-allocator slot, runs `init` when the module exports one, serves
//!   the invocation's rows, and is dropped. Dropping it returns the slot with its memory reset to
//!   the module's image.
//!
//! # Isolation
//!
//! Each invocation runs in memory initialised from the module's image, and nothing it writes
//! survives it. Two invocations never share memory, whichever tenant made them. The rows of one
//! batched invocation share that invocation's memory.
//!
//! What an invocation may do is its own, too: the network policy, memory limit and execution
//! time limit arrive with each call in [`InstanceSettings`], so two definitions of one archive
//! can grant it different capabilities.
//!
//! # Usage
//!
//! ```ignore
//! use std::sync::Arc;
//! use surrealism_runtime::epoch::{EngineSettings, engines};
//! use surrealism_runtime::runtime::{Call, InstanceSettings, Runtime};
//!
//! // Compile once (expensive, blocking).
//! let runtime = Arc::new(Runtime::new(package, engines(EngineSettings::default()))?);
//!
//! // Each invocation gets a fresh instance.
//! let values = runtime.invoke(Call { .. }).await?;
//! ```

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use surrealism_types::err::{PrefixErr, SurrealismError, SurrealismResult};
use wasmtime::*;
use web_time::Instant;

use crate::config::{AbiVersion, SurrealismConfig};
use crate::controller::Controller;
use crate::epoch::{Engines, SlotWait};
use crate::exports::ExportsManifest;
use crate::host::{InvocationContext, implement_host_functions};
use crate::net_allow::NetPolicy;
use crate::package::{AttachedFs, SurrealismPackage};
use crate::store::StoreData;
use crate::wasi_context::{StdioCallback, WasiTemplate};

/// The longest a top-level invocation waits for a slot when its query has no
/// timeout of its own.
const SLOT_WAIT_WITHOUT_DEADLINE: Duration = Duration::from_secs(30);

/// A compiled component for one engine flavour.
struct Linked {
	instance_pre: component::InstancePre<StoreData>,
	/// The component's exports, looked up by name once here rather than on
	/// every instantiation.
	exports: ExportIndices,
	/// Whether the filtering resolver shadowed the component's own import.
	dns_filtered: bool,
}

/// Indices of the exports an instance is driven through. `None` for an
/// optional export the component does not have.
#[derive(Clone, Copy)]
struct ExportIndices {
	invoke: component::ComponentExportIndex,
	args: Option<component::ComponentExportIndex>,
	returns: Option<component::ComponentExportIndex>,
	list: Option<component::ComponentExportIndex>,
	writeable: Option<component::ComponentExportIndex>,
	declares_tx: Option<component::ComponentExportIndex>,
	comment: Option<component::ComponentExportIndex>,
}

/// One compiled extension. Thread-safe; share it as `Arc<Runtime>`.
pub struct Runtime {
	engines: Arc<Engines>,
	/// The component bytes, kept to compile the second engine flavour should
	/// an invocation ask for it.
	wasm: Arc<[u8]>,
	/// The component compiled for each engine flavour. Async cells, so that
	/// callers racing for a flavour that is compiling wait for the one
	/// compile without holding a thread.
	guarded: tokio::sync::OnceCell<Linked>,
	fast: tokio::sync::OnceCell<Linked>,
	config: Arc<SurrealismConfig>,
	/// Holds the extracted filesystem alive for the lifetime of the runtime.
	/// When present, its root is mounted as a read-only preopened dir.
	fs_dir: Option<AttachedFs>,
	/// The WASI state every invocation starts from: the attached filesystem,
	/// opened once.
	wasi: WasiTemplate,
	/// Function signatures loaded from the exports manifest at build time.
	exports: ExportsManifest,
}

impl fmt::Debug for Runtime {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("Runtime")
			.field("config", &self.config)
			.field("wasm_size", &self.wasm.len())
			.field("fs_dir", &self.fs_dir)
			.field("guarded", &self.guarded.get().is_some())
			.field("fast", &self.fast.get().is_some())
			.field("exported_functions", &self.exports.functions.len())
			.finish_non_exhaustive()
	}
}

/// What one invocation was granted. Applied to the fresh instance that serves
/// it, and to nothing else.
#[derive(Clone)]
pub struct InstanceSettings {
	/// Where the guest may connect, checked by name in the DNS shim and by
	/// address on every connect.
	pub net: Arc<NetPolicy>,
	/// The largest linear memory the instance may grow to.
	pub max_memory_bytes: Option<usize>,
	/// The longest one call into the guest may run, before the caller's own
	/// remaining time is applied.
	pub max_execution_time: Option<Duration>,
	/// Whether the guest runs on the guarded engine, which enforces the
	/// execution time limit. `false` runs it on the fast engine, which cannot.
	pub strict_timeout: bool,
	pub stdout: StdioCallback,
	pub stderr: StdioCallback,
}

impl InstanceSettings {
	/// No network, no limits, stdio to the process's own.
	pub fn local() -> Self {
		Self {
			net: NetPolicy::deny_all(),
			max_memory_bytes: None,
			max_execution_time: None,
			strict_timeout: true,
			stdout: Arc::new(|output| print!("{output}")),
			stderr: Arc::new(|output| eprint!("{output}")),
		}
	}
}

/// One invocation: a function, the rows to call it with, and what it may do.
pub struct Call {
	/// The export to call; `None` is the default export.
	pub function: Option<String>,
	/// One argument list per row. All rows run on the same instance, in order.
	pub rows: Vec<Vec<surrealdb_types::Value>>,
	/// The handle lent to the guest for each row, or `None` for a function
	/// that declared no transaction parameter.
	pub tx: Option<Arc<dyn InvocationContext>>,
	pub settings: InstanceSettings,
	/// When the caller's query times out. Bounds the wait for a slot as well
	/// as each call into the guest.
	pub deadline: Option<Instant>,
	/// Whether this invocation was made from inside another one. A nested
	/// invocation never waits for a slot.
	pub nested: bool,
}

impl Runtime {
	/// Unpack and compile a package. This is expensive and blocking — do it
	/// once per archive and share the result.
	///
	/// Compiles for the guarded engine; an invocation granted the other one
	/// compiles it on demand through [`Self::prepare`].
	pub fn new(
		SurrealismPackage {
			wasm,
			config,
			exports,
			fs,
			logo: _,
			manifest: _,
			signature: _,
		}: SurrealismPackage,
		engines: Arc<Engines>,
	) -> SurrealismResult<Self> {
		if config.abi != AbiVersion::CURRENT {
			return Err(SurrealismError::UnsupportedAbi {
				expected: AbiVersion::CURRENT.0,
				got: config.abi.0,
			});
		}
		let wasi = WasiTemplate::new(fs.as_ref().map(|fs| fs.path()))?;
		let runtime = Self {
			engines,
			wasm: wasm.into(),
			guarded: tokio::sync::OnceCell::new(),
			fast: tokio::sync::OnceCell::new(),
			config: Arc::new(config),
			fs_dir: fs,
			wasi,
			exports,
		};
		// The guarded engine is what a definition gets unless its
		// `CAPABILITIES` set `strict_timeout: false`; the other flavour is
		// compiled on the first invocation that asks for it.
		runtime.prepare(true)?;
		Ok(runtime)
	}

	/// Returns the size of the original WASM binary in bytes.
	pub fn wasm_size(&self) -> usize {
		self.wasm.len()
	}

	/// Returns the package configuration.
	pub fn config(&self) -> &SurrealismConfig {
		&self.config
	}

	/// The engine pool this runtime instantiates into.
	pub fn engines(&self) -> &Arc<Engines> {
		&self.engines
	}

	fn linked(&self, guarded: bool) -> &tokio::sync::OnceCell<Linked> {
		if guarded {
			&self.guarded
		} else {
			&self.fast
		}
	}

	/// Whether the component is compiled for one engine flavour.
	pub fn is_prepared(&self, guarded: bool) -> bool {
		self.linked(guarded).get().is_some()
	}

	/// Compile the component for one engine flavour, if it is not already.
	/// Expensive and blocking: for a caller that owns the runtime, such as
	/// [`Self::new`] and the command-line tools. A server compiles with
	/// [`Self::prepare_async`].
	pub fn prepare(&self, guarded: bool) -> SurrealismResult<()> {
		let cell = self.linked(guarded);
		if cell.initialized() {
			return Ok(());
		}
		let engine = self.engines.engine(guarded)?;
		let linked = Self::build(&engine, &self.wasm)?;
		// A compile that raced this one produced the same thing.
		let _ = cell.set(linked);
		Ok(())
	}

	/// Compile the component for one engine flavour, if it is not already,
	/// on a blocking thread. Callers that race for the same flavour wait,
	/// without a thread of their own, for the one compile.
	pub async fn prepare_async(&self, guarded: bool) -> SurrealismResult<()> {
		self.linked(guarded)
			.get_or_try_init(|| async {
				let engines = Arc::clone(&self.engines);
				let wasm = Arc::clone(&self.wasm);
				tokio::task::spawn_blocking(move || Self::build(&engines.engine(guarded)?, &wasm))
					.await
					.map_err(|e| {
						SurrealismError::Other(anyhow::anyhow!("WASM compile task aborted: {e}"))
					})?
			})
			.await?;
		Ok(())
	}

	/// Whether the filtering DNS resolver is bound for this module.
	///
	/// `false` only when the component imports no `wasi:sockets/ip-name-lookup`
	/// at all, in which case it has no resolver to call. It is never `false`
	/// because the shim failed to find the interface: the name it binds is read
	/// off the component's own imports.
	pub fn dns_filtered(&self) -> bool {
		self.guarded.get().or_else(|| self.fast.get()).is_some_and(|l| l.dns_filtered)
	}

	fn build(engine: &Engine, wasm: &[u8]) -> SurrealismResult<Linked> {
		let t0 = Instant::now();

		let comp = component::Component::new(engine, wasm)
			.prefix_err(|| "Failed to construct component from bytes")?;
		tracing::debug!(elapsed = ?t0.elapsed(), "build: Component::new");

		// `surreal ext build` runs `init` once and removes the export, so
		// the snapshot is what every instance starts from. A component that
		// still exports it was not built that way, and running `init` here
		// would run it on every invocation, outside the query's deadline.
		if comp.get_export_index(None, "init").is_some() {
			return Err(SurrealismError::Other(anyhow::anyhow!(
				"the component still exports `init`, so it was not pre-initialised; rebuild it \
				 with `surreal ext build`"
			)));
		}

		let mut linker: component::Linker<StoreData> = component::Linker::new(engine);
		wasmtime_wasi::p2::add_to_linker_async(&mut linker)
			.prefix_err(|| "failed to add WASI P2 to component linker")?;
		// Replaces WASI's own resolver, so it must come after the line above.
		// The name is taken from the component's own imports, so the shim binds
		// the interface the guest actually calls whatever WASI version it was
		// built against.
		let dns_filtered = crate::dns::add_to_linker(engine, &comp, &mut linker)
			.prefix_err(|| "failed to add the filtering DNS resolver")?;
		implement_host_functions(&mut linker)
			.prefix_err(|| "failed to implement host functions")?;

		let instance_pre = linker
			.instantiate_pre(&comp)
			.prefix_err(|| "failed to pre-instantiate component (import resolution)")?;
		let export = |name| comp.get_export_index(None, name);
		let exports = ExportIndices {
			invoke: export("invoke").ok_or_else(|| {
				SurrealismError::Other(anyhow::anyhow!(
					"component is missing required export 'invoke'. Ensure the module is \
					 built with `surreal ext build`"
				))
			})?,
			args: export("function-args"),
			returns: export("function-returns"),
			list: export("list-functions"),
			writeable: export("function-writeable"),
			declares_tx: export("function-declares-transaction"),
			comment: export("function-comment"),
		};

		tracing::debug!(elapsed = ?t0.elapsed(), "build: total");
		Ok(Linked {
			instance_pre,
			exports,
			dns_filtered,
		})
	}

	/// Look up a function signature from the exports manifest.
	pub fn get_signature(
		&self,
		sub: Option<&str>,
	) -> SurrealismResult<&crate::exports::FunctionExport> {
		self.exports.get_signature(sub).ok_or_else(|| {
			let name = sub.unwrap_or("<default>");
			SurrealismError::Other(anyhow::anyhow!(
				"function '{name}' not found in exports manifest"
			))
		})
	}

	/// Access the full exports manifest.
	pub fn exports(&self) -> &ExportsManifest {
		&self.exports
	}

	/// Run one invocation on a fresh instance.
	///
	/// Takes a slot, instantiates, calls the function once per row, and drops
	/// the instance. Returns one
	/// value per row. A row that fails or traps fails the whole invocation.
	///
	/// A nested invocation runs on a task of its own. It is reached from a host
	/// call, which the calling instance's component-model event loop is
	/// polling, and wasmtime does not drive one store's event loop from inside
	/// another's. The task is aborted if the caller stops waiting for it.
	///
	/// The flavour the call asks for must already be compiled
	/// ([`Self::prepare`]); compiling is blocking, so it is the caller's to
	/// schedule.
	#[tracing::instrument(skip_all, fields(function = call.function.as_deref().unwrap_or("<default>")))]
	pub async fn invoke(
		self: &Arc<Self>,
		call: Call,
	) -> SurrealismResult<Vec<surrealdb_types::Value>> {
		if call.nested {
			let this = Arc::clone(self);
			wasmtime_wasi::runtime::spawn(async move { this.run_invocation(call).await }).await
		} else {
			self.run_invocation(call).await
		}
	}

	async fn run_invocation(&self, call: Call) -> SurrealismResult<Vec<surrealdb_types::Value>> {
		let wait = if call.nested {
			SlotWait::Never
		} else {
			// Bounded by the caller's own deadline whatever `strict_timeout`
			// says: that flag concerns the guest's execution, and this wait is
			// the host's.
			SlotWait::Bounded(call.deadline.map_or(SLOT_WAIT_WITHOUT_DEADLINE, |d| {
				d.saturating_duration_since(Instant::now())
			}))
		};
		let slot = self.engines.acquire_slot(wait).await?;
		let mut controller = self.instantiate(&call.settings, Some(slot), call.deadline).await?;

		let mut values = Vec::with_capacity(call.rows.len());
		for row in call.rows {
			let remaining = call.deadline.map(|d| d.saturating_duration_since(Instant::now()));
			let value = controller
				.invoke_with_timeout(call.tx.clone(), call.function.clone(), row, remaining)
				.await?;
			values.push(value);
		}
		Ok(values)
	}

	/// Create a fresh instance with the given settings.
	///
	/// `slot` is the invocation slot it occupies until dropped. Pass `None`
	/// only outside the server's slot accounting, such as for the build tool:
	/// the pooling allocator then fails rather than waits when it is full.
	///
	/// Instantiating can run guest code, the core modules' `start`
	/// functions, so on the guarded engine it is bounded by the smaller of
	/// `deadline` and the settings' execution time limit, and fails with
	/// [`SurrealismError::Timeout`] past it.
	#[tracing::instrument(skip_all)]
	pub async fn instantiate(
		&self,
		settings: &InstanceSettings,
		slot: Option<crate::epoch::SlotPermit>,
		deadline: Option<Instant>,
	) -> SurrealismResult<Controller> {
		let linked = self.linked(settings.strict_timeout).get().ok_or_else(|| {
			SurrealismError::Other(anyhow::anyhow!(
				"the component is not compiled for the {} engine",
				if settings.strict_timeout {
					"guarded"
				} else {
					"fast"
				}
			))
		})?;

		let net = settings.net.for_invocation().await;
		let (wasi, table) =
			self.wasi.instantiate(&net, Arc::clone(&settings.stdout), Arc::clone(&settings.stderr));
		let mut limits = StoreLimitsBuilder::new();
		if let Some(max_mem) = settings.max_memory_bytes {
			limits = limits.memory_size(max_mem);
		}
		let store_data = StoreData {
			wasi,
			table,
			net,
			limiter: limits.build(),
			deadline: None,
		};
		let mut store = Store::new(linked.instance_pre.engine(), store_data);
		store.limiter(|data| &mut data.limiter);
		let epoch_counter = Arc::clone(self.engines.epoch_counter());
		if settings.strict_timeout {
			crate::epoch::yield_until_deadline(&mut store, Arc::clone(&epoch_counter), |data| {
				data.deadline
			});
		}
		let remaining = deadline.map(|d| d.saturating_duration_since(Instant::now()));
		let limit = crate::controller::effective_timeout(remaining, settings.max_execution_time);
		store.data_mut().deadline =
			limit.map(|limit| crate::epoch::deadline_after(&epoch_counter, limit));

		let instance = linked.instance_pre.instantiate_async(&mut store).await.map_err(|e| {
			if e.downcast_ref::<Trap>() == Some(&Trap::Interrupt) {
				SurrealismError::Timeout {
					effective: limit,
					context_timeout: remaining,
					module_limit: settings.max_execution_time,
				}
			} else {
				SurrealismError::Instantiation(e)
			}
		})?;

		let exports = &linked.exports;
		let invoke_fn = instance.get_func(&mut store, exports.invoke).ok_or_else(|| {
			SurrealismError::Other(anyhow::anyhow!(
				"the component's 'invoke' export is not a function"
			))
		})?;
		let mut func = |index: Option<component::ComponentExportIndex>| {
			index.and_then(|index| instance.get_func(&mut store, index))
		};
		let args_fn = func(exports.args);
		let returns_fn = func(exports.returns);
		let list_fn = func(exports.list);
		let writeable_fn = func(exports.writeable);
		let declares_tx_fn = func(exports.declares_tx);
		let comment_fn = func(exports.comment);

		Ok(Controller::new(
			store,
			invoke_fn,
			args_fn,
			returns_fn,
			list_fn,
			writeable_fn,
			declares_tx_fn,
			comment_fn,
			settings.max_execution_time,
			epoch_counter,
			slot,
		))
	}
}

#[cfg(test)]
mod tests {
	use std::time::Duration;

	use web_time::Instant;

	use super::{InstanceSettings, Runtime};
	use crate::epoch::{EngineSettings, engines};

	/// Instantiating runs a core module's `start` function, and that is
	/// bounded by the execution time limit like any other guest code.
	#[tokio::test(flavor = "current_thread")]
	async fn a_start_function_is_bounded_by_the_deadline() {
		let engines = engines(EngineSettings {
			max_concurrent_invocations: 2,
			max_memory_bytes: Some(1 << 20),
		});
		let mut config = crate::package_config::PackageConfig::parse(
			"[package]\norganisation = \"t\"\nname = \"spin\"\nversion = \"1.0.0\"\n",
		)
		.unwrap()
		.into_config();
		config.abi = crate::config::AbiVersion::CURRENT;
		let package = crate::package::SurrealismPackage {
			config,
			wasm: br#"(component
				(core module $m
					(func $spin (loop br 0))
					(start $spin)
					(func (export "f")))
				(core instance $i (instantiate $m))
				(func (export "invoke") (canon lift (core func $i "f"))))"#
				.to_vec(),
			exports: crate::exports::ExportsManifest::empty(),
			fs: None,
			logo: None,
			manifest: None,
			signature: None,
		};
		let runtime = Runtime::new(package, engines).expect("compiles");
		let settings = InstanceSettings {
			max_execution_time: Some(Duration::from_millis(100)),
			..InstanceSettings::local()
		};
		let start = Instant::now();
		let err = runtime.instantiate(&settings, None, None).await.expect_err("times out");
		assert!(matches!(err, crate::SurrealismError::Timeout { .. }), "{err}");
		assert!(start.elapsed() < Duration::from_secs(5));

		// The caller's deadline bounds it too.
		let settings = InstanceSettings::local();
		let deadline = Instant::now() + Duration::from_millis(100);
		let err =
			runtime.instantiate(&settings, None, Some(deadline)).await.expect_err("times out");
		assert!(matches!(err, crate::SurrealismError::Timeout { .. }), "{err}");
	}

	/// A component that still exports `init` was not pre-initialised by
	/// `surreal ext build`, and is refused before anything runs.
	#[test]
	fn a_component_exporting_init_is_refused() {
		let engines = engines(EngineSettings {
			max_concurrent_invocations: 1,
			max_memory_bytes: Some(1 << 20),
		});
		let engine = engines.engine(true).expect("engine");
		let component = |export: &str| {
			format!(
				r#"(component
					(core module $m (func (export "f")))
					(core instance $i (instantiate $m))
					(func (export "{export}") (canon lift (core func $i "f"))))"#
			)
		};
		let err = Runtime::build(&engine, component("init").as_bytes()).err().expect("refused");
		assert!(err.to_string().contains("surreal ext build"), "{err}");
		// The same component under another export name gets past the check
		// and fails only where it lacks what a module needs.
		let other = Runtime::build(&engine, component("other").as_bytes());
		assert!(other.err().is_none_or(|e| !e.to_string().contains("`init`")));
	}
}
