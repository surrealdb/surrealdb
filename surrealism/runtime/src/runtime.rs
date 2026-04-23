//! Compiled WASM runtime (shared, immutable, thread-safe).
//!
//! # Architecture
//!
//! - **`Runtime`**: Compiled WASM component. Thread-safe, shareable (`Arc<Runtime>`). Compile once,
//!   instantiate many times. Holds a pool of initialized Controllers for reuse. A Tokio semaphore
//!   caps **checked-out** controllers (in-flight use). Idle controllers in the pool **release**
//!   their semaphore permits so waiters can proceed; permits are re-acquired when a pooled
//!   controller is checked out again. `max_pool_size` bounds the pool size and the maximum
//!   concurrent instances.
//!
//! - **`Controller`**: Per-execution instance. Single-threaded. Can be reused across invocations by
//!   swapping the host context between calls, preserving WASM linear memory (statics, heap).
//!
//! # Instance Reuse
//!
//! Controllers are pooled inside the Runtime. Between invocations, the host `InvocationContext`
//! (which carries per-request auth, permissions, KV store) is swapped out. The WASM linear memory
//! persists, so Rust statics (`OnceLock`, etc.) survive across calls. Security is enforced by the
//! host context, not by memory isolation — the module never sees user identity directly.
//!
//! # Concurrency Patterns
//!
//! ```no_run
//! use std::sync::Arc;
//! use surrealism_runtime::{runtime::Runtime, package::SurrealismPackage};
//!
//! // Compile once (expensive)
//! let runtime = Arc::new(Runtime::new(package, 8, None, None, None, None)?);
//!
//! // For each concurrent request:
//! let runtime = runtime.clone();
//! tokio::spawn(async move {
//!     let context = Box::new(MyContext::new());
//!     let mut controller = runtime.acquire_controller(context).await?;
//!     let result = controller.invoke(None, args).await;
//!     // Return to pool on success; drop on trap
//!     if result.is_ok() {
//!         runtime.release_controller(controller);
//!     }
//!     result
//! });
//! # Ok::<(), surrealism_types::err::SurrealismError>(())
//! ```

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use surrealism_types::err::{PrefixErr, SurrealismError, SurrealismResult};
use tokio::sync::Semaphore;
use wasmtime::*;
use web_time::Instant;

use crate::config::{AbiVersion, SurrealismConfig};
use crate::controller::Controller;
use crate::epoch::{self, EngineHandle};
use crate::exports::ExportsManifest;
use crate::host::{InvocationContext, implement_host_functions};
use crate::kv::BTreeMapStore;
use crate::net_allow::{ResolvedNetAllow, resolve_allow_net};
use crate::package::{AttachedFs, SurrealismPackage};
use crate::store::StoreData;

/// Compiled WASM runtime. Thread-safe, can be shared across threads via Arc.
/// Compiles WASM once, then each controller gets its own isolated Store/Instance.
/// Holds a pool of initialized controllers for reuse across invocations.
pub struct Runtime {
	/// Shared engine handle. Keeps the global epoch ticker alive.
	engine_handle: EngineHandle,
	instance_pre: component::InstancePre<StoreData>,
	config: Arc<SurrealismConfig>,
	wasm_size: usize,
	/// Holds the extracted filesystem alive for the lifetime of the runtime.
	/// When present, its root is mounted as a read-only preopened dir for WASM modules.
	fs_dir: Option<AttachedFs>,
	/// Pool of initialized, reusable controllers (retention capped at `max_pool_size`).
	/// Controllers in the pool have a NullContext and have already run init().
	/// Uses `parking_lot::Mutex` for non-poisoning, lower-overhead locking.
	pool: parking_lot::Mutex<Vec<Controller>>,
	/// Bounds concurrent **in-use** `Controller` instances. Permits are held only while a
	/// controller is actively checked out; idle pooled controllers release their permits.
	controller_slots: Arc<Semaphore>,
	/// Function signatures loaded from the exports manifest at build time.
	exports: ExportsManifest,
	/// Per-module KV store shared across all invocations. Persists for the
	/// lifetime of the Runtime and is passed to each `InvocationContext`.
	kv_store: Arc<BTreeMapStore>,
	/// Effective pool size ceiling: `min(server_cap, module_config.unwrap_or(server_cap))`.
	max_pool_size: usize,
	/// Effective memory limit: `min(server_cap, module_config)` when both set.
	max_memory_bytes: Option<usize>,
	/// Effective per-invocation execution time limit from module config.
	/// Combined with context timeout and server cap at invoke time.
	module_execution_time: Option<Duration>,
	/// `allow_net` resolved once at load (DNS, etc.); shared by WASI and core capabilities.
	resolved_allow_net: Arc<Vec<ResolvedNetAllow>>,
}

impl fmt::Debug for Runtime {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		let pool_size = self.pool.lock().len();
		f.debug_struct("Runtime")
			.field("config", &self.config)
			.field("wasm_size", &self.wasm_size)
			.field("fs_dir", &self.fs_dir)
			.field("pool_size", &pool_size)
			.field("max_pool_size", &self.max_pool_size)
			.field("max_memory_bytes", &self.max_memory_bytes)
			.field("module_execution_time", &self.module_execution_time)
			.field("exported_functions", &self.exports.functions.len())
			.finish_non_exhaustive()
	}
}

impl Runtime {
	/// Compile the WASM and prepare the runtime.
	/// This is expensive — do it once and share via `Arc<Runtime>`.
	///
	/// `server_pool_size`, `server_max_memory`, `server_max_execution_time`,
	/// `server_max_kv_entries`, and `server_max_kv_value_bytes` are the
	/// server-level ceilings from environment variables.
	pub fn new(
		SurrealismPackage {
			wasm,
			config,
			exports,
			fs,
			logo: _,
		}: SurrealismPackage,
		server_pool_size: usize,
		server_max_memory: Option<usize>,
		server_max_execution_time: Option<Duration>,
		server_max_kv_entries: Option<usize>,
		server_max_kv_value_bytes: Option<usize>,
	) -> SurrealismResult<Self> {
		if config.abi != AbiVersion::CURRENT {
			return Err(SurrealismError::UnsupportedAbi {
				expected: AbiVersion::CURRENT.0,
				got: config.abi.0,
			});
		}

		let t0 = Instant::now();

		let max_pool_size = config
			.capabilities
			.max_pool_size
			.map(|m| m.min(server_pool_size))
			.unwrap_or(server_pool_size);

		let max_memory_bytes = match (server_max_memory, config.capabilities.max_memory_bytes) {
			(Some(s), Some(m)) => Some(s.min(m)),
			(s, m) => s.or(m),
		};

		let module_execution_time =
			match (server_max_execution_time, config.capabilities.max_execution_time) {
				(Some(s), Some(m)) => Some(s.min(m)),
				(s, m) => s.or(m),
			};

		let max_kv_entries = match (server_max_kv_entries, config.capabilities.max_kv_entries) {
			(Some(s), Some(m)) => Some(s.min(m)),
			(s, m) => s.or(m),
		};

		let max_kv_value_bytes =
			match (server_max_kv_value_bytes, config.capabilities.max_kv_value_bytes) {
				(Some(s), Some(m)) => Some(s.min(m)),
				(s, m) => s.or(m),
			};

		let kv_store = Arc::new(BTreeMapStore::with_limits(max_kv_entries, max_kv_value_bytes));

		let config = Arc::new(config);
		let wasm_size = wasm.len();
		tracing::debug!(
			wasm_size,
			fs = fs.is_some(),
			max_pool_size,
			?max_memory_bytes,
			?module_execution_time,
			"Runtime::new starting"
		);

		let guarded = config.capabilities.strict_timeout;
		let engine_handle = epoch::shared_engine(guarded);
		tracing::debug!(
			strict_timeout = guarded,
			engine = if guarded {
				"guarded"
			} else {
				"fast"
			},
			"Runtime::new: selected engine"
		);
		let instance_pre = Self::build(engine_handle.engine(), &wasm)?;
		tracing::debug!(elapsed = ?t0.elapsed(), "Runtime::new build done");

		let resolved_allow_net = resolve_allow_net(&config.capabilities.allow_net)
			.prefix_err(|| "Failed to resolve allow_net entries")?;

		let controller_slots = Arc::new(Semaphore::new(max_pool_size.max(1)));

		Ok(Self {
			engine_handle,
			instance_pre,
			config,
			wasm_size,
			fs_dir: fs,
			pool: parking_lot::Mutex::new(Vec::new()),
			controller_slots,
			exports,
			kv_store,
			max_pool_size,
			max_memory_bytes,
			module_execution_time,
			resolved_allow_net,
		})
	}

	/// Returns the size of the original WASM binary in bytes.
	pub fn wasm_size(&self) -> usize {
		self.wasm_size
	}

	/// Returns the per-module KV store. This store is shared across all
	/// invocations and persists for the lifetime of the Runtime.
	pub fn kv_store(&self) -> &Arc<BTreeMapStore> {
		&self.kv_store
	}

	/// Returns the module configuration.
	pub fn config(&self) -> &SurrealismConfig {
		&self.config
	}

	/// Resolved `allow_net` from module load (same snapshot used for WASI socket filtering).
	pub fn resolved_allow_net(&self) -> Arc<Vec<ResolvedNetAllow>> {
		self.resolved_allow_net.clone()
	}

	/// Compute the maximum epoch delta that won't overflow when wasmtime adds
	/// it to the current epoch. Wasmtime uses wrapping `+` internally in
	/// `set_epoch_deadline`, so `u64::MAX` overflows once the epoch > 0.
	/// We subtract the shadow counter (which is always >= the real engine
	/// epoch) plus a small margin to absorb any ticks that land between
	/// the load and the `set_epoch_deadline` call.
	pub(crate) fn epoch_deadline_max(&self) -> u64 {
		let epoch = self.engine_handle.epoch_counter().load(Ordering::Acquire);
		u64::MAX.saturating_sub(epoch).saturating_sub(1)
	}

	fn build(engine: &Engine, wasm: &[u8]) -> SurrealismResult<component::InstancePre<StoreData>> {
		let t0 = Instant::now();

		let comp = component::Component::new(engine, wasm)
			.prefix_err(|| "Failed to construct component from bytes")?;
		tracing::debug!(elapsed = ?t0.elapsed(), "build: Component::new");

		let t1 = Instant::now();
		let mut linker: component::Linker<StoreData> = component::Linker::new(engine);
		wasmtime_wasi::p2::add_to_linker_async(&mut linker)
			.prefix_err(|| "failed to add WASI P2 to component linker")?;
		implement_host_functions(&mut linker)
			.prefix_err(|| "failed to implement host functions")?;
		tracing::debug!(elapsed = ?t1.elapsed(), "build: linker setup");

		let t2 = Instant::now();
		let instance_pre = linker
			.instantiate_pre(&comp)
			.prefix_err(|| "failed to pre-instantiate component (import resolution)")?;
		tracing::debug!(elapsed = ?t2.elapsed(), "build: instantiate_pre");

		tracing::debug!(elapsed = ?t0.elapsed(), "build: total");
		Ok(instance_pre)
	}

	/// Acquire a controller ready for invocation. Reuses a pooled controller if available
	/// (preserving WASM memory / statics from prior runs), otherwise creates and initializes
	/// a fresh one — waiting on [`Semaphore`] if `max_pool_size` controllers are already checked
	/// out. The supplied context is installed before returning.
	///
	/// The semaphore permit is acquired **before** checking the pool so that a
	/// popped controller is never outstanding without a matching permit.
	#[tracing::instrument(skip_all)]
	pub async fn acquire_controller(
		&self,
		context: Box<dyn InvocationContext>,
	) -> SurrealismResult<Controller> {
		let permit = self.acquire_slot().await?;

		let pooled = {
			let mut pool = self.pool.lock();
			let size = pool.len();
			let ctrl = pool.pop();
			tracing::debug!(
				pool_size_before = size,
				got_pooled = ctrl.is_some(),
				"acquire_controller: pool.pop()"
			);
			ctrl
		};

		match pooled {
			Some(mut ctrl) => {
				tracing::debug!("acquire_controller: reusing pooled controller");
				ctrl.attach_controller_slot(permit);
				ctrl.reset_epoch_deadline();
				ctrl.set_context(context);
				Ok(ctrl)
			}
			None => {
				tracing::info!("acquire_controller: creating NEW controller + init()");
				let mut ctrl = self.create_controller(context, permit).await?;
				ctrl.init().await?;
				Ok(ctrl)
			}
		}
	}

	/// Return a controller to the pool for reuse. The invocation context is cleared
	/// (replaced with a NullContext) so no per-request state is retained on the host side.
	/// WASM linear memory (statics, heap) is preserved for the next invocation.
	///
	/// Do NOT release a controller after a WASM trap — drop it instead to discard
	/// potentially inconsistent instance state.
	pub fn release_controller(&self, mut controller: Controller) {
		controller.clear_context();
		// Idle pool slots must not hold semaphore permits, or `acquire_owned` starves once the
		// pool fills even though controllers are available (see controller pool + semaphore
		// design).
		drop(controller.take_controller_slot());
		let mut pool = self.pool.lock();
		if pool.len() < self.max_pool_size {
			tracing::debug!(
				pool_size_after = pool.len() + 1,
				max_pool_size = self.max_pool_size,
				"release_controller: returned to pool"
			);
			pool.push(controller);
		} else {
			tracing::info!(
				pool_size = pool.len(),
				max_pool_size = self.max_pool_size,
				"release_controller: pool full, dropping controller"
			);
		}
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

	/// Create a new Controller with its own isolated Store and Instance.
	/// Import resolution is already done (in `InstancePre`); this only allocates
	/// memory, initializes state, and runs any start functions.
	///
	/// Prefer `acquire_controller` for the reuse path. This is the low-level constructor.
	#[tracing::instrument(skip_all)]
	pub async fn new_controller(
		&self,
		context: Box<dyn InvocationContext>,
	) -> SurrealismResult<Controller> {
		let permit = self.acquire_slot().await?;
		self.create_controller(context, permit).await
	}

	async fn acquire_slot(&self) -> SurrealismResult<tokio::sync::OwnedSemaphorePermit> {
		self.controller_slots.clone().acquire_owned().await.map_err(|_| {
			SurrealismError::Other(anyhow::anyhow!(
				"Surrealism controller semaphore closed (runtime shutdown?)"
			))
		})
	}

	/// Inner constructor that takes a pre-acquired semaphore permit.
	#[tracing::instrument(skip_all)]
	async fn create_controller(
		&self,
		context: Box<dyn InvocationContext>,
		controller_slot: tokio::sync::OwnedSemaphorePermit,
	) -> SurrealismResult<Controller> {
		let t0 = Instant::now();

		let fs_root = self.fs_dir.as_ref().map(|fs| fs.path());
		let stdout_cb = crate::wasi_context::new_stdout_callback();
		let stderr_cb = crate::wasi_context::new_stderr_callback();
		*stdout_cb.lock() = context.stdout_callback();
		*stderr_cb.lock() = context.stderr_callback();
		let (wasi_ctx, table) = crate::wasi_context::build(
			fs_root,
			self.resolved_allow_net.clone(),
			stdout_cb.clone(),
			stderr_cb.clone(),
		)?;
		tracing::debug!(elapsed = ?t0.elapsed(), "new_controller: wasi_context::build");

		let mut limits_builder = StoreLimitsBuilder::new();
		if let Some(max_mem) = self.max_memory_bytes {
			limits_builder = limits_builder.memory_size(max_mem);
		}
		let limiter = limits_builder.build();

		let store_data = StoreData {
			wasi: wasi_ctx,
			table,
			config: self.config.clone(),
			context,
			limiter,
			stdout_cb,
			stderr_cb,
		};
		let mut store = Store::new(self.engine_handle.engine(), store_data);
		store.limiter(|data| &mut data.limiter);
		store.set_epoch_deadline(self.epoch_deadline_max());

		let t1 = Instant::now();
		let instance = self
			.instance_pre
			.instantiate_async(&mut store)
			.await
			.map_err(SurrealismError::Instantiation)?;
		tracing::debug!(elapsed = ?t1.elapsed(), "new_controller: instantiate_async");

		let t2 = Instant::now();

		let invoke_fn = instance.get_func(&mut store, "invoke").ok_or_else(|| {
			SurrealismError::Other(anyhow::anyhow!(
				"component is missing required export 'invoke'. \
				 Ensure the module is built with `surreal module build`"
			))
		})?;

		let args_fn = instance.get_func(&mut store, "function-args");
		let returns_fn = instance.get_func(&mut store, "function-returns");
		let list_fn = instance.get_func(&mut store, "list-functions");
		let writeable_fn = instance.get_func(&mut store, "function-writeable");
		let comment_fn = instance.get_func(&mut store, "function-comment");
		let init_fn = instance.get_func(&mut store, "init");

		tracing::debug!(
			elapsed = ?t2.elapsed(),
			has_invoke = true,
			has_args = args_fn.is_some(),
			has_returns = returns_fn.is_some(),
			has_list = list_fn.is_some(),
			has_writeable = writeable_fn.is_some(),
			has_comment = comment_fn.is_some(),
			has_init = init_fn.is_some(),
			"new_controller: export lookup"
		);
		tracing::info!(elapsed = ?t0.elapsed(), "new_controller: total");

		Ok(Controller::new(
			store,
			invoke_fn,
			args_fn,
			returns_fn,
			list_fn,
			writeable_fn,
			comment_fn,
			init_fn,
			self.module_execution_time,
			self.engine_handle.epoch_counter().clone(),
			controller_slot,
		))
	}
}
