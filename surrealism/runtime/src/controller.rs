//! Per-execution WASM controller.

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use surrealism_types::args::Args;
use surrealism_types::err::{SurrealismError, SurrealismResult};
use tokio::sync::OwnedSemaphorePermit;
use wasmtime::*;
use web_time::Instant;

use crate::epoch::EPOCH_TICK_MS;
use crate::host::{InvocationContext, NullContext};
use crate::store::StoreData;

fn effective_timeout(
	context_remaining: Option<Duration>,
	module_limit: Option<Duration>,
) -> Option<Duration> {
	[context_remaining, module_limit].into_iter().flatten().min()
}

/// Per-execution controller. Not thread-safe — use one at a time.
/// Can be reused across invocations by swapping the host context.
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
			comment_fn,
			init_fn,
			module_execution_time,
			epoch_counter,
			controller_slot: Some(controller_slot),
		}
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

	/// Replace the invocation context. Used when reusing a pooled controller
	/// for a new request with different permissions. Also updates the WASI
	/// stdio callbacks so guest `println!` output routes through the same
	/// structured logging path as the WIT `stdout`/`stderr` imports.
	pub fn set_context(&mut self, context: Box<dyn InvocationContext>) {
		let data = self.store.data_mut();
		*data.stdout_cb.lock() = context.stdout_callback();
		*data.stderr_cb.lock() = context.stderr_callback();
		data.context = context;
	}

	/// Clear the invocation context, replacing it with a NullContext.
	/// Called before returning a controller to the pool so no per-request state
	/// (auth, permissions, KV) is retained. Resets WASI stdio to host defaults.
	pub fn clear_context(&mut self) {
		let data = self.store.data_mut();
		*data.stdout_cb.lock() = Arc::new(|output| print!("{}", output));
		*data.stderr_cb.lock() = Arc::new(|output| eprint!("{}", output));
		data.context = Box::new(NullContext);
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
		name: Option<String>,
		args: A,
	) -> SurrealismResult<surrealdb_types::Value> {
		self.invoke_with_timeout(name, args, None).await
	}

	/// Invoke with an optional context-level timeout. The effective deadline is
	/// `min(context_remaining, module_config, server_cap)`.
	#[tracing::instrument(skip_all, fields(name))]
	pub async fn invoke_with_timeout<A: Args>(
		&mut self,
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

		let typed = self
			.invoke_fn
			.typed::<(Option<&str>, &[u8]), (Result<Vec<u8>, String>,)>(&self.store)?;

		let call_result = typed.call_async(&mut self.store, (name.as_deref(), &args_bytes)).await;

		if let Err(e) = &call_result {
			tracing::error!(name = %display_name, error = %e, "invoke_with_timeout: call_async FAILED");
		}

		let (result,) = call_result.map_err(|e| {
			if e.downcast_ref::<Trap>() == Some(&Trap::Interrupt) {
				SurrealismError::Timeout {
					effective,
					context_timeout,
					module_limit: self.module_execution_time,
				}
			} else {
				SurrealismError::from(e)
			}
		})?;

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
