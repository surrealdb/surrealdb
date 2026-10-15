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
//!   lending a fresh `transaction` handle per call, preserving WASM linear memory (statics, heap).
//!
//! # Instance Reuse
//!
//! Controllers are pooled inside the Runtime and hold no request state. Datastore access is
//! granted per call by the `transaction` handle `invoke` lends, and revoked when it returns, so a
//! pooled controller carries nothing from the request before it. The WASM linear memory persists,
//! so Rust statics (`OnceLock`, etc.) survive across calls. Security is enforced by the lent
//! handle, not by memory isolation — the module never sees user identity directly.
//!
//! # Concurrency Patterns
//!
//! ```ignore
//! use std::sync::Arc;
//! use surrealism_runtime::{NetDeny, runtime::Runtime, package::SurrealismPackage};
//!
//! // Compile once (expensive)
//! let runtime = Arc::new(Runtime::new(package, 8, None, None, NetDeny::Nothing)?);
//!
//! // For each concurrent request:
//! let runtime = runtime.clone();
//! tokio::spawn(async move {
//!     let context = Arc::new(MyContext::new());
//!     let mut controller = runtime.acquire_controller(context.as_ref(), None).await?;
//!     let result = controller.invoke(Some(context), None, args).await;
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
use crate::net_allow::{NetDeny, NetPolicy};
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
	/// Controllers in the pool hold no request state and have already run init().
	/// Uses `parking_lot::Mutex` for non-poisoning, lower-overhead locking.
	pool: parking_lot::Mutex<Vec<Controller>>,
	/// Bounds concurrent **in-use** `Controller` instances. Permits are held only while a
	/// controller is actively checked out; idle pooled controllers release their permits.
	controller_slots: Arc<Semaphore>,
	/// Function signatures loaded from the exports manifest at build time.
	exports: ExportsManifest,
	/// Effective pool size ceiling: `min(server_cap, module_config.unwrap_or(server_cap))`.
	max_pool_size: usize,
	/// Effective memory limit: `min(server_cap, module_config)` when both set.
	max_memory_bytes: Option<usize>,
	/// Effective per-invocation execution time limit from module config.
	/// Combined with context timeout and server cap at invoke time.
	module_execution_time: Option<Duration>,
	/// The module's outbound network policy. Shared by the DNS shim (which
	/// filters by name) and WASI's `socket_addr_check` (which filters connects).
	net_policy: Arc<NetPolicy>,
	/// Whether the filtering resolver shadowed the component's own import.
	dns_filtered: bool,
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
	/// `server_pool_size`, `server_max_memory` and `server_max_execution_time`
	/// are the server-level ceilings from environment variables. `server_deny_net`
	/// is the operator's `--deny-net`, which the module's own `allow_net` cannot
	/// widen.
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
		server_pool_size: usize,
		server_max_memory: Option<usize>,
		server_max_execution_time: Option<Duration>,
		server_deny_net: NetDeny,
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
		// No DNS here: hostnames are matched as names when the guest resolves
		// them, so nothing is pinned at load time.
		//
		// The operator's deny rules are carried into the policy rather than
		// checked only on the SurrealQL path: a guest's own WASI socket is a
		// second way out of the process, and one that answered differently
		// would let a module reach what a query from the same module cannot.
		let net_policy = NetPolicy::parse(&config.capabilities.allow_net, server_deny_net)
			.prefix_err(|| "Failed to parse allow_net entries")?;

		let (instance_pre, dns_filtered) = Self::build(engine_handle.engine(), &wasm, &net_policy)?;
		tracing::debug!(elapsed = ?t0.elapsed(), "Runtime::new build done");

		let controller_slots = Arc::new(Semaphore::new(max_pool_size.max(1)));

		Ok(Self {
			engine_handle,
			instance_pre,
			config,
			wasm_size,
			fs_dir: fs,
			pool: parking_lot::Mutex::new(Vec::new()),
			dns_filtered,
			controller_slots,
			exports,
			max_pool_size,
			max_memory_bytes,
			module_execution_time,
			net_policy,
		})
	}

	/// Returns the size of the original WASM binary in bytes.
	pub fn wasm_size(&self) -> usize {
		self.wasm_size
	}

	/// Returns the module configuration.
	pub fn config(&self) -> &SurrealismConfig {
		&self.config
	}

	/// The module's outbound network policy.
	pub fn net_policy(&self) -> Arc<NetPolicy> {
		Arc::clone(&self.net_policy)
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

	/// Whether the filtering DNS resolver is bound for this module.
	///
	/// `false` only when the component imports no `wasi:sockets/ip-name-lookup`
	/// at all, in which case it has no resolver to call. It is never `false`
	/// because the shim failed to find the interface: the name it binds is read
	/// off the component's own imports.
	pub fn dns_filtered(&self) -> bool {
		self.dns_filtered
	}

	fn build(
		engine: &Engine,
		wasm: &[u8],
		net_policy: &Arc<NetPolicy>,
	) -> SurrealismResult<(component::InstancePre<StoreData>, bool)> {
		let t0 = Instant::now();

		let comp = component::Component::new(engine, wasm)
			.prefix_err(|| "Failed to construct component from bytes")?;
		tracing::debug!(elapsed = ?t0.elapsed(), "build: Component::new");

		let t1 = Instant::now();
		let mut linker: component::Linker<StoreData> = component::Linker::new(engine);
		wasmtime_wasi::p2::add_to_linker_async(&mut linker)
			.prefix_err(|| "failed to add WASI P2 to component linker")?;
		// Replaces WASI's own resolver, so it must come after the line above.
		// The name is taken from the component's own imports, so the shim binds
		// the interface the guest actually calls whatever WASI version it was
		// built against.
		let dns_filtered = crate::dns::add_to_linker(engine, &comp, &mut linker, net_policy)
			.prefix_err(|| "failed to add the filtering DNS resolver")?;
		implement_host_functions(&mut linker)
			.prefix_err(|| "failed to implement host functions")?;
		tracing::debug!(elapsed = ?t1.elapsed(), "build: linker setup");

		let t2 = Instant::now();
		let instance_pre = linker
			.instantiate_pre(&comp)
			.prefix_err(|| "failed to pre-instantiate component (import resolution)")?;
		tracing::debug!(elapsed = ?t2.elapsed(), "build: instantiate_pre");

		tracing::debug!(elapsed = ?t0.elapsed(), "build: total");
		Ok((instance_pre, dns_filtered))
	}

	/// Acquire a controller ready for invocation. Reuses a pooled controller if available
	/// (preserving WASM memory / statics from prior runs), otherwise creates and initializes
	/// a fresh one — waiting on [`Semaphore`] if `max_pool_size` controllers are already checked
	/// out.
	///
	/// The controller carries no request state; `context` is only used to point
	/// the WASI stdio callbacks at this request's logging path. Datastore access
	/// is granted separately, by the handle `invoke` lends.
	///
	/// The semaphore permit is acquired **before** checking the pool so that a
	/// popped controller is never outstanding without a matching permit.
	///
	/// `wait` is how long the caller is prepared to wait for one, normally what
	/// is left of its own query timeout. The wait is the shorter of that and
	/// `SLOT_WAIT`, so a `TIMEOUT 1s` query does not sit here for
	/// thirty seconds holding a transaction open.
	#[tracing::instrument(skip_all)]
	pub async fn acquire_controller(
		&self,
		context: &dyn InvocationContext,
		wait: Option<Duration>,
	) -> SurrealismResult<Controller> {
		let permit = self.acquire_slot(wait).await?;

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
				ctrl.set_stdio(context);
				Ok(ctrl)
			}
			None => {
				tracing::info!("acquire_controller: creating NEW controller + init()");
				let mut ctrl = self.create_controller(context, permit).await?;
				ctrl.init().await?;
				// Whatever `init` opened is this module's own and outlives every
				// caller; a socket above this count later is the guest holding
				// a connection across an invocation.
				ctrl.mark_resource_baseline();
				Ok(ctrl)
			}
		}
	}

	/// Return a controller to the pool for reuse. The WASI stdio callbacks are
	/// reset to host defaults so no per-request logging context is retained.
	/// WASM linear memory (statics, heap) is preserved for the next invocation.
	///
	/// A controller that ends its invocation holding a socket it did not start
	/// with is retired instead: the pool is shared by every caller on this
	/// namespace and database, and an open connection left in the table would
	/// be reachable from the next caller's query. See
	/// `Controller::retains_guest_resources`.
	///
	/// Do NOT release a controller after a WASM trap — drop it instead to discard
	/// potentially inconsistent instance state.
	pub fn release_controller(&self, mut controller: Controller) {
		controller.clear_stdio();
		if controller.retains_guest_resources() {
			tracing::warn!(
				"release_controller: guest left a socket open across the invocation; \
				 retiring the controller rather than pooling it"
			);
			drop(controller.take_controller_slot());
			return;
		}
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
		context: &dyn InvocationContext,
	) -> SurrealismResult<Controller> {
		let permit = self.acquire_slot(None).await?;
		self.create_controller(context, permit).await
	}

	/// The longest anyone waits for a controller slot before the pool is
	/// reported exhausted.
	///
	/// The wait is bounded because it can be circular: a guest function that
	/// calls back into its own module blocks here while the outer invocation
	/// holds the permit the nested one needs, and nothing breaks that on its
	/// own. The epoch deadline interrupts *guest* execution only, and this wait
	/// is host-side. Unbounded, the request hangs holding an open transaction
	/// and recovery depends on an outer `transaction_timeout` being configured.
	///
	/// Generous enough that ordinary contention — every controller busy on a
	/// loaded node — waits rather than fails. A caller with a shorter deadline
	/// of its own passes it to [`Self::acquire_controller`] and waits that long
	/// instead.
	const SLOT_WAIT: Duration = Duration::from_secs(30);

	async fn acquire_slot(
		&self,
		wait: Option<Duration>,
	) -> SurrealismResult<tokio::sync::OwnedSemaphorePermit> {
		let wait = wait.map_or(Self::SLOT_WAIT, |w| w.min(Self::SLOT_WAIT));
		let slots = Arc::clone(&self.controller_slots);
		match tokio::time::timeout(wait, slots.acquire_owned()).await {
			Ok(Ok(permit)) => Ok(permit),
			Ok(Err(_)) => Err(SurrealismError::Other(anyhow::anyhow!(
				"Surrealism controller semaphore closed (runtime shutdown?)"
			))),
			Err(_) => {
				tracing::error!(
					pool_size = self.max_pool_size,
					waited = ?wait,
					"acquire_slot: no controller became available; the pool may be held by an \
					 invocation waiting on a nested call into the same module"
				);
				Err(SurrealismError::ControllerPoolExhausted {
					waited: wait,
					pool_size: self.max_pool_size,
				})
			}
		}
	}

	/// Inner constructor that takes a pre-acquired semaphore permit.
	#[tracing::instrument(skip_all)]
	async fn create_controller(
		&self,
		context: &dyn InvocationContext,
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
			&self.net_policy,
			Arc::clone(&stdout_cb),
			Arc::clone(&stderr_cb),
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
			config: Arc::clone(&self.config),
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
		let declares_tx_fn = instance.get_func(&mut store, "function-declares-transaction");
		let comment_fn = instance.get_func(&mut store, "function-comment");
		let init_fn = instance.get_func(&mut store, "init");

		tracing::debug!(
			elapsed = ?t2.elapsed(),
			has_invoke = true,
			has_args = args_fn.is_some(),
			has_returns = returns_fn.is_some(),
			has_list = list_fn.is_some(),
			has_writeable = writeable_fn.is_some(),
			has_declares_tx = declares_tx_fn.is_some(),
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
			declares_tx_fn,
			comment_fn,
			init_fn,
			self.module_execution_time,
			Arc::clone(self.engine_handle.epoch_counter()),
			controller_slot,
		))
	}
}
