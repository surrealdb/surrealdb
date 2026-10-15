//! Per-execution WASM controller.

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use surrealism_types::args::Args;
use surrealism_types::err::{SurrealismError, SurrealismResult};
use wasmtime::component::Resource;
use wasmtime::*;

use crate::host::{InvocationContext, Transaction};
use crate::store::StoreData;

pub(crate) fn effective_timeout(
	context_remaining: Option<Duration>,
	module_limit: Option<Duration>,
) -> Option<Duration> {
	[context_remaining, module_limit].into_iter().flatten().min()
}

/// One instance of a component, serving one invocation.
///
/// Created fresh from the component's `InstancePre` and dropped when the
/// invocation returns, which hands its pool slot back with its memory reset to
/// the module's image. Nothing it wrote — linear memory, globals, tables, the
/// WASI resource table — survives it. Not thread-safe; use one at a time.
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
	/// The execution time limit this invocation was granted, without the
	/// caller's remaining query time, which is applied per call.
	module_execution_time: Option<Duration>,
	/// Shared epoch counter from the engine pool, used to compute safe epoch deltas.
	epoch_counter: Arc<std::sync::atomic::AtomicU64>,
	/// The invocation slot this instance occupies, released on drop. `None`
	/// for an instance created outside the slot accounting, such as the build
	/// tool's.
	_slot: Option<crate::epoch::SlotPermit>,
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
		module_execution_time: Option<Duration>,
		epoch_counter: Arc<std::sync::atomic::AtomicU64>,
		slot: Option<crate::epoch::SlotPermit>,
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
			module_execution_time,
			epoch_counter,
			_slot: slot,
		}
	}

	/// Trap the next call `timeout` from now, or never for `None`. Takes
	/// effect on the guarded engine only; the fast engine has no epoch checks.
	fn set_deadline(&mut self, timeout: Option<Duration>) {
		let deadline = timeout.map(|t| crate::epoch::deadline_after(&self.epoch_counter, t));
		self.store.data_mut().deadline = deadline;
	}

	/// Apply the module-level execution time limit as the deadline.
	fn apply_module_deadline(&mut self) {
		self.set_deadline(self.module_execution_time);
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

		self.set_deadline(effective);

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
		// trap path. A retained handle then refers to nothing and traps on use;
		// within a batch, the next row is lent a handle of its own.
		//
		// Logged rather than propagated: the delete can fail for reasons that say
		// nothing about the call — the destructor already removed the entry, or
		// borrow accounting still holds a child after an async-lifted `invoke`
		// unwinds. Letting that replace `call_result` would throw away the
		// `Trap`/`Timeout` the caller needs.
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
