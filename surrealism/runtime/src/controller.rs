//! WASM execution runtime and controller.
//!
//! # Architecture
//!
//! - **`Runtime`**: Compiled WASM module or component. Thread-safe, shareable (`Arc<Runtime>`).
//!   Compile once, instantiate many times. Supports both WASI P1 (core modules) and P2 (component
//!   model).
//!
//! - **`Controller`**: Per-execution instance. Single-threaded, created from Runtime. Cheap to
//!   create, can be done per-request or pooled.
//!
//! # Concurrency Patterns
//!
//! ```no_run
//! use std::sync::Arc;
//! use surrealism_runtime::{controller::Runtime, package::SurrealismPackage};
//!
//! // Compile once (expensive)
//! let runtime = Arc::new(Runtime::new(package)?);
//!
//! // For each concurrent request:
//! let runtime = runtime.clone();
//! tokio::spawn(async move {
//!     let context = Box::new(MyContext::new());
//!     let mut controller = runtime.new_controller(context).await?;
//!     controller.invoke(None, args).await
//! });
//! # Ok::<(), surrealism_types::err::SurrealismError>(())
//! ```

use std::fmt;
use std::sync::Arc;

use anyhow::Context;
use async_trait::async_trait;
use surrealism_types::args::Args;
use surrealism_types::err::{PrefixErr, SurrealismError, SurrealismResult};
use surrealism_types::transfer::AsyncTransfer;
use wasmtime::component::ResourceTable;
use wasmtime::*;
use wasmtime_wasi::p1::{self, WasiP1Ctx};
use wasmtime_wasi::{WasiCtx, WasiCtxView, WasiView};

use crate::config::SurrealismConfig;
use crate::host::{InvocationContext, implement_p1_host_functions, implement_p2_host_functions};
use crate::package::{ModuleKind, SurrealismPackage};

// ---------------------------------------------------------------------------
// Store data
// ---------------------------------------------------------------------------

/// Store data for WASI P1 (core module) execution.
pub struct P1StoreData {
	pub wasi: WasiP1Ctx,
	pub config: Arc<SurrealismConfig>,
	pub(crate) context: Box<dyn InvocationContext>,
}

impl fmt::Debug for P1StoreData {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "P1StoreData {{ config: {:?}, .. }}", self.config)
	}
}

/// Store data for WASI P2 (component model) execution.
pub struct P2StoreData {
	pub wasi: WasiCtx,
	pub table: ResourceTable,
	pub config: Arc<SurrealismConfig>,
	pub(crate) context: Box<dyn InvocationContext>,
}

impl WasiView for P2StoreData {
	fn ctx(&mut self) -> WasiCtxView<'_> {
		WasiCtxView {
			ctx: &mut self.wasi,
			table: &mut self.table,
		}
	}
}

impl fmt::Debug for P2StoreData {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "P2StoreData {{ config: {:?}, .. }}", self.config)
	}
}

// ---------------------------------------------------------------------------
// Runtime (compiled artefact — shared, immutable, thread-safe)
// ---------------------------------------------------------------------------

enum RuntimeKind {
	P1 {
		engine: Engine,
		module: Module,
		linker: Linker<P1StoreData>,
	},
	P2 {
		engine: Engine,
		component: component::Component,
		linker: component::Linker<P2StoreData>,
	},
}

impl fmt::Debug for RuntimeKind {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::P1 {
				..
			} => write!(f, "RuntimeKind::P1"),
			Self::P2 {
				..
			} => write!(f, "RuntimeKind::P2"),
		}
	}
}

/// Compiled WASM runtime. Thread-safe, can be shared across threads via Arc.
/// Compiles WASM once, then each controller gets its own isolated Store/Instance.
#[derive(Debug)]
pub struct Runtime {
	inner: RuntimeKind,
	config: Arc<SurrealismConfig>,
	wasm_size: usize,
}

fn build_engine_config() -> Config {
	let mut cfg = Config::new();
	#[cfg(debug_assertions)]
	{
		cfg.strategy(Strategy::Winch);
	}
	#[cfg(not(debug_assertions))]
	{
		cfg.cranelift_opt_level(OptLevel::Speed);
	}
	cfg
}

impl Runtime {
	/// Compile the WASM and prepare the runtime.
	/// This is expensive — do it once and share via `Arc<Runtime>`.
	pub fn new(
		SurrealismPackage {
			wasm,
			config,
			kind,
		}: SurrealismPackage,
	) -> SurrealismResult<Self> {
		let config = Arc::new(config);
		let wasm_size = wasm.len();

		let inner = match kind {
			ModuleKind::CoreModule => Self::build_p1(&wasm)?,
			ModuleKind::Component => Self::build_p2(&wasm)?,
		};

		Ok(Self {
			inner,
			config,
			wasm_size,
		})
	}

	/// Returns the size of the original WASM binary in bytes.
	pub fn wasm_size(&self) -> usize {
		self.wasm_size
	}

	fn build_p1(wasm: &[u8]) -> SurrealismResult<RuntimeKind> {
		let engine_config = build_engine_config();
		let engine = Engine::new(&engine_config)?;
		let module =
			Module::new(&engine, wasm).prefix_err(|| "Failed to construct module from bytes")?;

		let mut linker: Linker<P1StoreData> = Linker::new(&engine);
		p1::add_to_linker_async(&mut linker, |data| &mut data.wasi)
			.prefix_err(|| "failed to add WASI P1 to linker")?;
		implement_p1_host_functions(&mut linker)
			.prefix_err(|| "failed to implement P1 host functions")?;

		Ok(RuntimeKind::P1 {
			engine,
			module,
			linker,
		})
	}

	fn build_p2(wasm: &[u8]) -> SurrealismResult<RuntimeKind> {
		let engine_config = build_engine_config();
		let engine = Engine::new(&engine_config)?;
		let comp = component::Component::new(&engine, wasm)
			.prefix_err(|| "Failed to construct component from bytes")?;

		let mut linker: component::Linker<P2StoreData> = component::Linker::new(&engine);
		wasmtime_wasi::p2::add_to_linker_async(&mut linker)
			.prefix_err(|| "failed to add WASI P2 to component linker")?;
		implement_p2_host_functions(&mut linker)
			.prefix_err(|| "failed to implement P2 host functions")?;

		Ok(RuntimeKind::P2 {
			engine,
			component: comp,
			linker,
		})
	}

	/// Create a new Controller with its own isolated Store and Instance.
	pub async fn new_controller(
		&self,
		context: Box<dyn InvocationContext>,
	) -> SurrealismResult<Controller> {
		match &self.inner {
			RuntimeKind::P1 {
				engine,
				module,
				linker,
			} => {
				let wasi_ctx = super::wasi_context::build_p1()?;
				let store_data = P1StoreData {
					wasi: wasi_ctx,
					config: self.config.clone(),
					context,
				};
				let mut store = Store::new(engine, store_data);
				let instance = linker
					.instantiate_async(&mut store, module)
					.await
					.map_err(SurrealismError::Compilation)?;
				let memory = instance
					.get_memory(&mut store, "memory")
					.context("WASM module must export 'memory'")?;
				let alloc_fn = instance
					.get_typed_func::<(u32,), i32>(&mut store, "__sr_alloc")
					.map_err(|e| anyhow::anyhow!("WASM module must export '__sr_alloc': {e}"))?;
				let free_fn =
					instance
						.get_typed_func::<(u32, u32), i32>(&mut store, "__sr_free")
						.map_err(|e| anyhow::anyhow!("WASM module must export '__sr_free': {e}"))?;

				Ok(Controller {
					inner: ControllerKind::P1(P1Controller {
						store,
						instance,
						memory,
						alloc_fn,
						free_fn,
					}),
				})
			}
			RuntimeKind::P2 {
				engine,
				component,
				linker,
			} => {
				let (wasi_ctx, table) = super::wasi_context::build_p2()?;
				let store_data = P2StoreData {
					wasi: wasi_ctx,
					table,
					config: self.config.clone(),
					context,
				};
				let mut store = Store::new(engine, store_data);
				let instance = linker
					.instantiate_async(&mut store, component)
					.await
					.map_err(SurrealismError::Compilation)?;

				Ok(Controller {
					inner: ControllerKind::P2(P2Controller {
						store,
						instance,
					}),
				})
			}
		}
	}
}

// ---------------------------------------------------------------------------
// Controller (per-execution instance)
// ---------------------------------------------------------------------------

enum ControllerKind {
	P1(P1Controller),
	P2(P2Controller),
}

impl fmt::Debug for ControllerKind {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::P1(_) => write!(f, "ControllerKind::P1"),
			Self::P2(_) => write!(f, "ControllerKind::P2"),
		}
	}
}

/// Per-execution controller. Not thread-safe — create one per concurrent call.
/// Lightweight, created from Runtime.
#[derive(Debug)]
pub struct Controller {
	inner: ControllerKind,
}

impl Controller {
	pub async fn init(&mut self) -> SurrealismResult<()> {
		match &mut self.inner {
			ControllerKind::P1(c) => c.init().await,
			ControllerKind::P2(c) => c.init().await,
		}
	}

	pub async fn invoke<A: Args>(
		&mut self,
		name: Option<String>,
		args: A,
	) -> SurrealismResult<surrealdb_types::Value> {
		match &mut self.inner {
			ControllerKind::P1(c) => c.invoke(name, args).await,
			ControllerKind::P2(c) => c.invoke(name, args).await,
		}
	}

	pub async fn args(
		&mut self,
		name: Option<String>,
	) -> SurrealismResult<Vec<surrealdb_types::Kind>> {
		match &mut self.inner {
			ControllerKind::P1(c) => c.args(name).await,
			ControllerKind::P2(c) => c.args(name).await,
		}
	}

	pub async fn returns(
		&mut self,
		name: Option<String>,
	) -> SurrealismResult<surrealdb_types::Kind> {
		match &mut self.inner {
			ControllerKind::P1(c) => c.returns(name).await,
			ControllerKind::P2(c) => c.returns(name).await,
		}
	}

	pub async fn list(&mut self) -> SurrealismResult<Vec<String>> {
		match &mut self.inner {
			ControllerKind::P1(c) => c.list(),
			ControllerKind::P2(c) => c.list().await,
		}
	}
}

// ---------------------------------------------------------------------------
// P1 Controller (core module path — existing logic, unchanged)
// ---------------------------------------------------------------------------

pub(crate) struct P1Controller {
	pub(super) store: Store<P1StoreData>,
	pub(super) instance: Instance,
	pub(super) memory: Memory,
	alloc_fn: TypedFunc<(u32,), i32>,
	free_fn: TypedFunc<(u32, u32), i32>,
}

impl P1Controller {
	async fn init(&mut self) -> SurrealismResult<()> {
		let init: Option<Extern> = self.instance.get_export(&mut self.store, "__sr_init");
		if init.is_none() {
			return Ok(());
		}
		let init = self.instance.get_typed_func::<(), ()>(&mut self.store, "__sr_init")?;
		init.call_async(&mut self.store, ()).await?;
		Ok(())
	}

	async fn invoke<A: Args>(
		&mut self,
		name: Option<String>,
		args: A,
	) -> SurrealismResult<surrealdb_types::Value> {
		let name = format!("__sr_fnc__{}", name.unwrap_or_default());
		let args = AsyncTransfer::transfer(args.to_values(), self).await?;
		let invoke = self.instance.get_typed_func::<(u32,), (i32,)>(&mut self.store, &name)?;
		let (ptr,) = invoke.call_async(&mut self.store, (*args,)).await?;
		if ptr == -1 {
			return Err(SurrealismError::FunctionCallError(
				"WASM function returned error (-1)".to_string(),
			));
		}
		let ptr_u32: u32 = ptr.try_into()?;
		let inner: anyhow::Result<surrealdb_types::Value> =
			AsyncTransfer::receive(ptr_u32.into(), self).await?;
		Ok(inner?)
	}

	async fn args(&mut self, name: Option<String>) -> SurrealismResult<Vec<surrealdb_types::Kind>> {
		let name = format!("__sr_args__{}", name.unwrap_or_default());
		let args = self.instance.get_typed_func::<(), (i32,)>(&mut self.store, &name)?;
		let (ptr,) = args.call_async(&mut self.store, ()).await?;
		Ok(AsyncTransfer::receive(ptr.try_into()?, self).await?)
	}

	async fn returns(&mut self, name: Option<String>) -> SurrealismResult<surrealdb_types::Kind> {
		let name = format!("__sr_returns__{}", name.unwrap_or_default());
		let returns = self.instance.get_typed_func::<(), (i32,)>(&mut self.store, &name)?;
		let (ptr,) = returns.call_async(&mut self.store, ()).await?;
		if ptr == -1 {
			return Err(SurrealismError::FunctionCallError(
				"WASM function returned error (-1)".into(),
			));
		}
		Ok(AsyncTransfer::receive(ptr.try_into()?, self).await?)
	}

	fn list(&mut self) -> SurrealismResult<Vec<String>> {
		let mut functions = Vec::new();
		let function_names: Vec<String> = {
			let exports = self.instance.exports(&mut self.store);
			exports
				.filter_map(|export| {
					let name = export.name();
					if name.starts_with("__sr_fnc__") {
						Some(name.to_string())
					} else {
						None
					}
				})
				.collect()
		};
		for name in function_names {
			if let Some(export) = self.instance.get_export(&mut self.store, &name)
				&& let ExternType::Func(_) = export.ty(&self.store)
			{
				let function_name = name.strip_prefix("__sr_fnc__").unwrap_or(&name).to_string();
				functions.push(function_name);
			}
		}
		Ok(functions)
	}
}

#[async_trait]
impl surrealism_types::controller::AsyncMemoryController for P1Controller {
	async fn alloc(&mut self, len: u32) -> SurrealismResult<u32> {
		let result = self.alloc_fn.call_async(&mut self.store, (len,)).await?;
		if result == -1 {
			return Err(SurrealismError::AllocFailed);
		}
		Ok(result as u32)
	}

	async fn free(&mut self, ptr: u32, len: u32) -> SurrealismResult<()> {
		let result = self.free_fn.call_async(&mut self.store, (ptr, len)).await?;
		if result == -1 {
			return Err(SurrealismError::FreeFailed);
		}
		Ok(())
	}

	fn mut_mem(&mut self, ptr: u32, len: u32) -> SurrealismResult<&mut [u8]> {
		let mem = self.memory.data_mut(&mut self.store);
		let start = ptr as usize;
		let end = start.checked_add(len as usize).ok_or_else(|| {
			SurrealismError::OutOfBounds(format!("Memory access overflow: ptr={ptr}, len={len}"))
		})?;
		if end > mem.len() {
			return Err(SurrealismError::OutOfBounds(format!(
				"Memory access out of bounds: attempting to access [{start}..{end}), but memory size is {}",
				mem.len()
			)));
		}
		Ok(&mut mem[start..end])
	}
}

// ---------------------------------------------------------------------------
// P2 Controller (component model path — no manual alloc/free)
// ---------------------------------------------------------------------------

pub(crate) struct P2Controller {
	store: Store<P2StoreData>,
	instance: component::Instance,
}

impl P2Controller {
	async fn init(&mut self) -> SurrealismResult<()> {
		let func = self.instance.get_func(&mut self.store, "init");
		let Some(func) = func else {
			return Ok(());
		};
		let typed = func.typed::<(), (Result<(), String>,)>(&self.store)?;
		let (result,) = typed.call_async(&mut self.store, ()).await?;
		result.map_err(SurrealismError::FunctionCallError)
	}

	async fn invoke<A: Args>(
		&mut self,
		name: Option<String>,
		args: A,
	) -> SurrealismResult<surrealdb_types::Value> {
		let args_bytes = surrealdb_types::encode_value_list(&args.to_values())?;

		let func = self
			.instance
			.get_func(&mut self.store, "invoke")
			.context("component must export 'invoke'")?;
		let typed = func.typed::<(&str, &[u8]), (Result<Vec<u8>, String>,)>(&self.store)?;

		let call_name = name.unwrap_or_default();
		let (result,) = typed.call_async(&mut self.store, (&call_name, &args_bytes)).await?;

		let result_bytes = result.map_err(SurrealismError::FunctionCallError)?;
		let value = surrealdb_types::decode::<surrealdb_types::Value>(&result_bytes)?;
		Ok(value)
	}

	async fn args(&mut self, name: Option<String>) -> SurrealismResult<Vec<surrealdb_types::Kind>> {
		let func = self
			.instance
			.get_func(&mut self.store, "function-args")
			.context("component must export 'function-args'")?;
		let typed = func.typed::<(&str,), (Result<Vec<u8>, String>,)>(&self.store)?;

		let call_name = name.unwrap_or_default();
		let (result,) = typed.call_async(&mut self.store, (&call_name,)).await?;

		let result_bytes = result.map_err(SurrealismError::FunctionCallError)?;
		Ok(surrealdb_types::decode_kind_list(&result_bytes)?)
	}

	async fn returns(&mut self, name: Option<String>) -> SurrealismResult<surrealdb_types::Kind> {
		let func = self
			.instance
			.get_func(&mut self.store, "function-returns")
			.context("component must export 'function-returns'")?;
		let typed = func.typed::<(&str,), (Result<Vec<u8>, String>,)>(&self.store)?;

		let call_name = name.unwrap_or_default();
		let (result,) = typed.call_async(&mut self.store, (&call_name,)).await?;

		let result_bytes = result.map_err(SurrealismError::FunctionCallError)?;
		Ok(surrealdb_types::decode_kind(&result_bytes)?)
	}

	async fn list(&mut self) -> SurrealismResult<Vec<String>> {
		let func = self
			.instance
			.get_func(&mut self.store, "list-functions")
			.context("component must export 'list-functions'")?;
		let typed = func.typed::<(), (Vec<String>,)>(&self.store)?;

		let (names,) = typed.call_async(&mut self.store, ()).await?;
		Ok(names)
	}
}
