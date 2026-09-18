//! Per-execution WASM controller.

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use surrealism_types::args::Args;
use surrealism_types::err::{SurrealismError, SurrealismResult};
use tokio::sync::OwnedSemaphorePermit;
use wasmtime::component::Resource;
use wasmtime::*;
use web_time::Instant;

use crate::epoch::EPOCH_TICK_MS;
use crate::host::{InvocationContext, Transaction};
use crate::store::StoreData;

fn effective_timeout(
	context_remaining: Option<Duration>,
	module_limit: Option<Duration>,
) -> Option<Duration> {
	[context_remaining, module_limit].into_iter().flatten().min()
}

/// Per-execution controller. Not thread-safe — use one at a time.
///
/// Reusable across invocations: the controller holds no request state, because
/// host access is granted per call by the `transaction` handle `invoke` lends.
/// WASM linear memory persists between calls, so Rust statics survive.
pub struct Controller {
	store: Store<StoreData>,
	invoke_fn: component::Func,
	/// Only present when the module exports the `function-args` function.
	args_fn: Option<component::Func>,
	/// Only present when the module exports the `function-returns` function.
	returns_fn: Option<component::Func>,
	/// Only present when the module exports the `list-functions` function.
	list_fn: Option<component::Func>,
	/// Only present when the module exports the `function-writeable` function.
	writeable_fn: Option<component::Func>,
	/// Only present when the module exports `function-declares-transaction`.
	declares_tx_fn: Option<component::Func>,
	/// Only present when the module exports the `function-comment` function.
	comment_fn: Option<component::Func>,
	init_fn: Option<component::Func>,
	/// Effective execution time limit from module config + server cap (without
	/// context timeout, which varies per invocation).
	module_execution_time: Option<Duration>,
	/// Shared epoch counter from the global engine, used to compute safe epoch deltas.
	epoch_counter: Arc<std::sync::atomic::AtomicU64>,
	/// While [`None`], this controller is idle in the pool and does **not** consume a slot on
	/// [`Runtime`](crate::runtime::Runtime)'s controller semaphore. While [`Some`], the permit
	/// counts toward the concurrent-instance cap.
	controller_slot: Option<OwnedSemaphorePermit>,
	/// Sockets open in the store's resource table when this controller was
	/// clean — after instantiation and `init`, before any invocation.
	///
	/// Set by [`Self::mark_resource_baseline`] and compared by
	/// [`Self::retains_guest_resources`].
	socket_baseline: usize,
}

impl fmt::Debug for Controller {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("Controller").finish_non_exhaustive()
	}
}

impl Controller {
	#[allow(clippy::too_many_arguments)]
	pub(crate) fn new(
		store: Store<StoreData>,
		invoke_fn: component::Func,
		args_fn: Option<component::Func>,
		returns_fn: Option<component::Func>,
		list_fn: Option<component::Func>,
		writeable_fn: Option<component::Func>,
		declares_tx_fn: Option<component::Func>,
		comment_fn: Option<component::Func>,
		init_fn: Option<component::Func>,
		module_execution_time: Option<Duration>,
		epoch_counter: Arc<std::sync::atomic::AtomicU64>,
		controller_slot: OwnedSemaphorePermit,
	) -> Self {
		Self {
			store,
			invoke_fn,
			args_fn,
			returns_fn,
			list_fn,
			writeable_fn,
			declares_tx_fn,
			comment_fn,
			init_fn,
			module_execution_time,
			epoch_counter,
			controller_slot: Some(controller_slot),
			socket_baseline: 0,
		}
	}

	/// Record how many sockets are open once instantiation and `init` are done
	/// and before any invocation runs.
	pub(crate) fn mark_resource_baseline(&mut self) {
		self.socket_baseline = self.open_sockets();
	}

	/// Whether the guest left a socket open in the store's table.
	///
	/// A controller is shared by every caller on its namespace and database,
	/// and linear memory is deliberately preserved across them. An open socket
	/// is different: a guest that connects during one caller's invocation,
	/// keeps the handle index in linear memory and never drops it could read
	/// and write that still-open connection during the next caller's query,
	/// with whatever it buffered still there. So a controller that ends an
	/// invocation holding a socket it did not start with is retired rather than
	/// pooled.
	///
	/// Sockets specifically, not table occupancy: WASI pushes the stdio streams
	/// and the instance network into this same table lazily, on first guest
	/// use, and never drops them — so a module that merely prints would look
	/// like it was leaking for ever after, and would never be pooled again.
	/// Counting is also blind to net-zero churn, where a guest drops one entry
	/// and opens a socket in the same invocation. A socket's own streams are
	/// pushed as its children and cannot outlive it, so the socket is the whole
	/// anchor.
	///
	/// The table's occupancy is bounded separately, by its maximum capacity.
	pub(crate) fn retains_guest_resources(&mut self) -> bool {
		self.open_sockets() > self.socket_baseline
	}

	fn open_sockets(&mut self) -> usize {
		self.store
			.data_mut()
			.table
			.iter_mut()
			.filter(|entry| {
				entry.is::<wasmtime_wasi::sockets::TcpSocket>()
					|| entry.is::<wasmtime_wasi::sockets::UdpSocket>()
			})
			.count()
	}

	/// Attach a semaphore permit after taking this controller from the idle pool.
	pub(crate) fn attach_controller_slot(&mut self, permit: OwnedSemaphorePermit) {
		debug_assert!(self.controller_slot.is_none(), "controller already holds a slot permit");
		self.controller_slot = Some(permit);
	}

	/// Remove and return the slot permit so the controller can sit in the pool without holding
	/// capacity on the runtime semaphore.
	pub(crate) fn take_controller_slot(&mut self) -> Option<OwnedSemaphorePermit> {
		self.controller_slot.take()
	}

	/// Point the WASI stdio callbacks at this request's logging path, so guest
	/// `println!` output is attributed to the right module, namespace and
	/// database. Unlike `sql`/`run`, stdio has no handle to hang off:
	/// `wasi:cli/stdout` is ambient, so it is still swapped per request.
	pub fn set_stdio(&mut self, context: &dyn InvocationContext) {
		let data = self.store.data_mut();
		*data.stdout_cb.lock() = context.stdout_callback();
		*data.stderr_cb.lock() = context.stderr_callback();
	}

	/// Reset WASI stdio to the host defaults. Called before returning a
	/// controller to the pool so no per-request logging context is retained.
	pub fn clear_stdio(&mut self) {
		let data = self.store.data_mut();
		*data.stdout_cb.lock() = Arc::new(|output| print!("{}", output));
		*data.stderr_cb.lock() = Arc::new(|output| eprint!("{}", output));
	}

	/// Reset the epoch deadline to the maximum safe value. Wasmtime internally
	/// computes `current_epoch + delta` with wrapping arithmetic, so passing
	/// `u64::MAX` overflows once the epoch advances past 0. We subtract the
	/// shadow counter (always >= engine epoch thanks to the ticker thread
	/// increment ordering) plus a small margin so that a concurrent tick
	/// between the load and the `set_epoch_deadline` call cannot overflow.
	pub fn reset_epoch_deadline(&mut self) {
		let epoch = self.epoch_counter.load(Ordering::Acquire);
		self.store.set_epoch_deadline(u64::MAX.saturating_sub(epoch).saturating_sub(1));
	}

	/// Apply the module-level execution time limit as an epoch deadline.
	/// If no limit is configured, resets to the maximum safe value.
	fn apply_module_deadline(&mut self) {
		match self.module_execution_time {
			Some(timeout) => {
				let ticks = (timeout.as_millis() as u64) / EPOCH_TICK_MS;
				self.store.set_epoch_deadline(ticks.max(1));
			}
			None => self.reset_epoch_deadline(),
		}
	}

	#[tracing::instrument(skip_all)]
	pub async fn init(&mut self) -> SurrealismResult<()> {
		let t0 = Instant::now();
		let Some(func) = self.init_fn else {
			tracing::debug!("controller.init(): no init_fn, skipping");
			return Ok(());
		};
		self.apply_module_deadline();
		tracing::info!(
			module_execution_time = ?self.module_execution_time,
			"controller.init(): calling init function..."
		);
		let typed = func.typed::<(), (Result<(), String>,)>(&self.store)?;
		match typed.call_async(&mut self.store, ()).await {
			Ok((result,)) => {
				tracing::info!(elapsed = ?t0.elapsed(), ok = result.is_ok(), "controller.init(): completed");
				result.map_err(SurrealismError::FunctionCallError)
			}
			Err(e) => {
				if e.downcast_ref::<Trap>() == Some(&Trap::Interrupt) {
					tracing::error!(elapsed = ?t0.elapsed(), "controller.init(): timed out");
					return Err(SurrealismError::Timeout {
						effective: self.module_execution_time,
						context_timeout: None,
						module_limit: self.module_execution_time,
					});
				}
				tracing::error!(elapsed = ?t0.elapsed(), error = %e, "controller.init(): WASM TRAP");
				Err(e.into())
			}
		}
	}

	#[tracing::instrument(skip_all, fields(name))]
	pub async fn invoke<A: Args>(
		&mut self,
		context: Option<Arc<dyn InvocationContext>>,
		name: Option<String>,
		args: A,
	) -> SurrealismResult<surrealdb_types::Value> {
		self.invoke_with_timeout(context, name, args, None).await
	}

	/// Invoke with an optional context-level timeout. The effective deadline is
	/// `min(context_remaining, module_config, server_cap)`.
	///
	/// `context` is lent to the guest as a `transaction` handle for the duration
	/// of the call and revoked before returning, on every path including a trap
	/// and including a failure to build the typed view of `invoke`.
	/// Pass `None` for a function that declared no transaction parameter: it then
	/// receives no handle and has no route to the datastore.
	#[tracing::instrument(skip_all, fields(name))]
	pub async fn invoke_with_timeout<A: Args>(
		&mut self,
		context: Option<Arc<dyn InvocationContext>>,
		name: Option<String>,
		args: A,
		context_timeout: Option<Duration>,
	) -> SurrealismResult<surrealdb_types::Value> {
		let display_name = name.as_deref().unwrap_or("<default>");
		let effective = effective_timeout(context_timeout, self.module_execution_time);

		match effective {
			Some(timeout) => {
				let ticks = (timeout.as_millis() as u64) / EPOCH_TICK_MS;
				self.store.set_epoch_deadline(ticks.max(1));
			}
			None => {
				self.reset_epoch_deadline();
			}
		}

		let args_values = args.to_values();
		let args_bytes = surrealdb_types::encode_value_list(&args_values)?;

		// Lend the capability for exactly this call. The counter it carries
		// bounds how many host calls the guest may hold in flight through it;
		// which of them may run at the same time is the context's decision,
		// because only the context can read what each statement does.
		let owned = match context {
			Some(context) => Some(
				self.store
					.data_mut()
					.table
					.push(Transaction {
						context,
						in_flight: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
					})
					.map_err(|e| SurrealismError::Other(e.into()))?,
			),
			None => None,
		};
		let borrowed = owned.as_ref().map(|rep| Resource::<Transaction>::new_borrow(rep.rep()));

		// Nothing between lending the handle and revoking it may return early,
		// or the handle outlives the call it was lent to. So the fallible step
		// that builds the typed view is folded into the same result the call
		// produces, rather than propagated with `?` from here.
		let call_result: SurrealismResult<(Result<Vec<u8>, String>,)> = match self
			.invoke_fn
			.typed::<(Option<Resource<Transaction>>, Option<&str>, &[u8]), (Result<Vec<u8>, String>,)>(
				&self.store,
			) {
			Ok(typed) => typed
				.call_async(&mut self.store, (borrowed, name.as_deref(), &args_bytes))
				.await
				.map_err(|e| {
					if e.downcast_ref::<Trap>() == Some(&Trap::Interrupt) {
						SurrealismError::Timeout {
							effective,
							context_timeout,
							module_limit: self.module_execution_time,
						}
					} else {
						SurrealismError::from(e)
					}
				}),
			Err(e) => Err(SurrealismError::from(e)),
		};

		// Revoke it before anything else can observe the result, including on the
		// trap path. A retained handle then refers to nothing and traps on use.
		//
		// Logged rather than propagated: the delete can fail for reasons that say
		// nothing about the call — the destructor already removed the entry, or
		// borrow accounting still holds a child after an async-lifted `invoke`
		// unwinds. Letting that replace `call_result` would throw away the
		// `Trap`/`Timeout` the caller needs, and `run_on_runtime` would then read
		// the generic error as "not a trap" and return a trapped component to the
		// pool, where every later invocation fails with `CannotEnterComponent`.
		if let Some(rep) = owned
			&& let Err(e) = self.store.data_mut().table.delete(rep)
		{
			tracing::error!(
				name = %display_name,
				error = %e,
				"invoke_with_timeout: could not revoke the transaction handle"
			);
		}

		if let Err(e) = &call_result {
			tracing::error!(name = %display_name, error = %e, "invoke_with_timeout: call_async FAILED");
		}

		let (result,) = call_result?;

		if let Err(guest_err) = &result {
			tracing::warn!(name = %display_name, guest_error = %guest_err, "invoke_with_timeout: guest returned Err");
		}

		let result_bytes = result.map_err(SurrealismError::FunctionCallError)?;
		let value = surrealdb_types::decode::<surrealdb_types::Value>(&result_bytes)?;
		Ok(value)
	}

	/// Convert a `Trap::Interrupt` into a `SurrealismError::Timeout`, otherwise
	/// wrap as a generic wasmtime error. Used by metadata helpers that only have
	/// the module-level timeout (no per-invocation context timeout).
	fn trap_to_timeout(&self, e: wasmtime::Error) -> SurrealismError {
		if e.downcast_ref::<Trap>() == Some(&Trap::Interrupt) {
			SurrealismError::Timeout {
				effective: self.module_execution_time,
				context_timeout: None,
				module_limit: self.module_execution_time,
			}
		} else {
			SurrealismError::from(e)
		}
	}

	/// Query named argument types for a function via the WASM export.
	/// Only available when the module has the `function-args` export (build tool).
	#[tracing::instrument(skip_all, fields(name))]
	pub async fn args(
		&mut self,
		name: Option<String>,
	) -> SurrealismResult<Vec<(String, surrealdb_types::Kind)>> {
		let display_name = name.as_deref().unwrap_or("<default>");
		tracing::debug!(name = %display_name, "controller.args(): calling function-args");
		let func = self.args_fn.ok_or_else(|| {
			SurrealismError::Other(anyhow::anyhow!("function-args export not available"))
		})?;
		self.apply_module_deadline();
		let typed = func.typed::<(Option<&str>,), (Result<Vec<u8>, String>,)>(&self.store)?;

		match typed.call_async(&mut self.store, (name.as_deref(),)).await {
			Ok((result,)) => {
				tracing::debug!(name = %display_name, ok = result.is_ok(), "controller.args(): call_async completed");
				let result_bytes = result.map_err(SurrealismError::FunctionCallError)?;
				Ok(surrealdb_types::decode_argument_list(&result_bytes)?)
			}
			Err(e) => {
				tracing::error!(name = %display_name, error = %e, error_debug = ?e, "controller.args(): WASM TRAP");
				Err(self.trap_to_timeout(e))
			}
		}
	}

	/// Query return type for a function via the WASM export.
	/// Only available when the module has the `function-returns` export (build tool).
	#[tracing::instrument(skip_all, fields(name))]
	pub async fn returns(
		&mut self,
		name: Option<String>,
	) -> SurrealismResult<surrealdb_types::Kind> {
		let display_name = name.as_deref().unwrap_or("<default>");
		tracing::debug!(name = %display_name, "controller.returns(): calling function-returns");
		let func = self.returns_fn.ok_or_else(|| {
			SurrealismError::Other(anyhow::anyhow!("function-returns export not available"))
		})?;
		self.apply_module_deadline();
		let typed = func.typed::<(Option<&str>,), (Result<Vec<u8>, String>,)>(&self.store)?;

		match typed.call_async(&mut self.store, (name.as_deref(),)).await {
			Ok((result,)) => {
				tracing::debug!(name = %display_name, ok = result.is_ok(), "controller.returns(): call_async completed");
				let result_bytes = result.map_err(SurrealismError::FunctionCallError)?;
				Ok(surrealdb_types::decode_kind(&result_bytes)?)
			}
			Err(e) => {
				tracing::error!(name = %display_name, error = %e, error_debug = ?e, "controller.returns(): WASM TRAP");
				Err(self.trap_to_timeout(e))
			}
		}
	}

	/// Query whether a function is marked as writeable via the WASM export.
	/// Only available when the module has the `function-writeable` export (build tool).
	#[tracing::instrument(skip_all, fields(name))]
	pub async fn writeable(&mut self, name: Option<String>) -> SurrealismResult<bool> {
		let display_name = name.as_deref().unwrap_or("<default>");
		tracing::debug!(name = %display_name, "controller.writeable(): calling function-writeable");
		let func = self.writeable_fn.ok_or_else(|| {
			SurrealismError::Other(anyhow::anyhow!("function-writeable export not available"))
		})?;
		self.apply_module_deadline();
		let typed = func.typed::<(Option<&str>,), (Result<bool, String>,)>(&self.store)?;

		match typed.call_async(&mut self.store, (name.as_deref(),)).await {
			Ok((result,)) => {
				tracing::debug!(name = %display_name, ok = result.is_ok(), "controller.writeable(): call_async completed");
				result.map_err(SurrealismError::FunctionCallError)
			}
			Err(e) => {
				tracing::error!(name = %display_name, error = %e, error_debug = ?e, "controller.writeable(): WASM TRAP");
				Err(self.trap_to_timeout(e))
			}
		}
	}

	/// Query whether a function declared a transaction parameter, via the WASM
	/// export. Only available when the module has the
	/// `function-declares-transaction` export (build tool).
	///
	/// Used to *write* the exports manifest, never to check one: the serve path
	/// reads [`FunctionExport::declares_transaction`](crate::exports::FunctionExport::declares_transaction)
	/// and lends the handle on that alone.
	#[tracing::instrument(skip_all, fields(name))]
	pub async fn declares_transaction(&mut self, name: Option<String>) -> SurrealismResult<bool> {
		let Some(func) = self.declares_tx_fn else {
			return Err(SurrealismError::Other(anyhow::anyhow!(
				"function-declares-transaction export not available"
			)));
		};
		self.apply_module_deadline();
		let typed = func.typed::<(Option<&str>,), (Result<bool, String>,)>(&self.store)?;
		match typed.call_async(&mut self.store, (name.as_deref(),)).await {
			Ok((result,)) => result.map_err(SurrealismError::FunctionCallError),
			Err(e) => Err(self.trap_to_timeout(e)),
		}
	}

	/// Query the comment for a function via the WASM export.
	/// Only available when the module has the `function-comment` export (build tool).
	#[tracing::instrument(skip_all, fields(name))]
	pub async fn comment(&mut self, name: Option<String>) -> SurrealismResult<Option<String>> {
		let display_name = name.as_deref().unwrap_or("<default>");
		tracing::debug!(name = %display_name, "controller.comment(): calling function-comment");
		let func = self.comment_fn.ok_or_else(|| {
			SurrealismError::Other(anyhow::anyhow!("function-comment export not available"))
		})?;
		self.apply_module_deadline();
		let typed =
			func.typed::<(Option<&str>,), (Result<Option<String>, String>,)>(&self.store)?;

		match typed.call_async(&mut self.store, (name.as_deref(),)).await {
			Ok((result,)) => {
				tracing::debug!(name = %display_name, ok = result.is_ok(), "controller.comment(): call_async completed");
				result.map_err(SurrealismError::FunctionCallError)
			}
			Err(e) => {
				tracing::error!(name = %display_name, error = %e, error_debug = ?e, "controller.comment(): WASM TRAP");
				Err(self.trap_to_timeout(e))
			}
		}
	}

	/// List all exported function names via the WASM export.
	/// Only available when the module has the `list-functions` export (build tool).
	#[tracing::instrument(skip_all)]
	pub async fn list(&mut self) -> SurrealismResult<Vec<Option<String>>> {
		tracing::debug!("controller.list(): calling list-functions");
		let func = self.list_fn.ok_or_else(|| {
			SurrealismError::Other(anyhow::anyhow!("list-functions export not available"))
		})?;
		self.apply_module_deadline();
		let typed = func.typed::<(), (Vec<Option<String>>,)>(&self.store)?;

		match typed.call_async(&mut self.store, ()).await {
			Ok((names,)) => {
				tracing::debug!(count = names.len(), names = ?names, "controller.list(): completed");
				Ok(names)
			}
			Err(e) => {
				tracing::error!(error = %e, error_debug = ?e, "controller.list(): WASM TRAP");
				Err(self.trap_to_timeout(e))
			}
		}
	}
}
