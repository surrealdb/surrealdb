//! WASM execution runtime and controller.
//!
//! # Architecture
//!
//! - **`Runtime`**: Compiled WASM module. Thread-safe, shareable (Arc<Runtime>). Compile once,
//!   instantiate many times.
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
//! # Ok::<(), anyhow::Error>(())
//! ```

use std::fmt;
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use surrealism_types::args::Args;
use surrealism_types::err::PrefixError;
use surrealism_types::transfer::AsyncTransfer;
use wasmtime::*;
use wasmtime_wasi::preview1::{self, WasiP1Ctx};

use crate::config::SurrealismConfig;
use crate::host::{InvocationContext, implement_host_functions};
use crate::package::SurrealismPackage;

/// Store data for WASM execution. Each Controller has its own isolated StoreData.
pub struct StoreData {
	pub wasi: WasiP1Ctx,
	pub config: Arc<SurrealismConfig>,
	pub(crate) context: Box<dyn InvocationContext>,
}

impl fmt::Debug for StoreData {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "StoreData {{ wasi: ?, context: ?, config: {:?} }}", self.config)?;
		Ok(())
	}
}

/// Compiled WASM runtime. Thread-safe, can be shared across threads via Arc.
/// Compiles WASM once, then each controller gets its own isolated Store/Instance.
/// The Engine, Module, and Linker are immutable and safely shared.
#[derive(Debug)]
pub struct Runtime {
	engine: Engine,
	module: Module,
	linker: Linker<StoreData>,
	config: Arc<SurrealismConfig>,
}

impl Runtime {
	/// Compile the WASM module and prepare the runtime.
	/// This is expensive - do it once and share via Arc<Runtime>.
	/// The compiled artifacts (Engine, Module, Linker) are immutable and thread-safe.
	pub fn new(
		SurrealismPackage {
			wasm,
			config,
		}: SurrealismPackage,
	) -> Result<Self> {
		println!("Compiling WASM module");

		// Configure engine for fast compilation in debug, optimized runtime in release
		let mut engine_config = Config::new();
		// Enable async support for async host functions
		engine_config.async_support(true);
		#[cfg(debug_assertions)]
		{
			// Use Winch baseline compiler for extremely fast compilation in debug builds
			// Falls back to Cranelift if Winch doesn't support the WASM features used
			engine_config.strategy(Strategy::Winch);
		}
		#[cfg(not(debug_assertions))]
		{
			// Optimize for runtime performance in release builds
			engine_config.cranelift_opt_level(OptLevel::Speed);
		}
		let engine = Engine::new(&engine_config)?;
		let module =
			Module::new(&engine, wasm).prefix_err(|| "Failed to construct module from bytes")?;

		let mut linker: Linker<StoreData> = Linker::new(&engine);
		preview1::add_to_linker_async(&mut linker, |data| &mut data.wasi)
			.prefix_err(|| "failed to add WASI to linker")?;
		implement_host_functions(&mut linker)
			.prefix_err(|| "failed to implement host functions")?;

		Ok(Self {
			engine,
			module,
			linker,
			config: Arc::new(config),
		})
	}

	/// Create a new Controller with its own isolated Store and Instance.
	/// This is cheap (relative to compilation) - the expensive compilation is shared.
	/// Each controller has its own mutable Store, ensuring no shared mutable state.
	/// Safe for concurrent execution: no mutable state is shared between controllers.
	pub async fn new_controller(&self, context: Box<dyn InvocationContext>) -> Result<Controller> {
		let wasi_ctx = super::wasi_context::build()?;

		let store_data = StoreData {
			wasi: wasi_ctx,
			config: self.config.clone(),
			context,
		};
		let mut store = Store::new(&self.engine, store_data);
		let instance = self
			.linker
			.instantiate_async(&mut store, &self.module)
			.await
			.prefix_err(|| "failed to instantiate WASM module")?;
		let memory = instance
			.get_memory(&mut store, "memory")
			.prefix_err(|| "WASM module must export 'memory'")?;

		Ok(Controller {
			store,
			instance,
			memory,
		})
	}
}

/// Per-execution controller. Not thread-safe - create one per concurrent call.
/// Lightweight, created from Runtime. Each controller has its own isolated Store and Instance.
#[derive(Debug)]
pub struct Controller {
	pub(super) store: Store<StoreData>,
	pub(super) instance: Instance,
	pub(super) memory: Memory,
}

impl Controller {
	pub async fn alloc(&mut self, len: u32) -> Result<u32> {
		let alloc = self.instance.get_typed_func::<(u32,), i32>(&mut self.store, "__sr_alloc")?;
		let result = alloc.call_async(&mut self.store, (len,)).await?;
		if result == -1 {
			anyhow::bail!("Memory allocation failed");
		}
		Ok(result as u32)
	}

	pub async fn free(&mut self, ptr: u32, len: u32) -> Result<()> {
		let free = self.instance.get_typed_func::<(u32, u32), i32>(&mut self.store, "__sr_free")?;
		let result = free.call_async(&mut self.store, (ptr, len)).await?;
		if result == -1 {
			anyhow::bail!("Memory deallocation failed");
		}
		Ok(())
	}

	pub async fn init(&mut self) -> Result<()> {
		let init: Option<Extern> = self.instance.get_export(&mut self.store, "__sr_init");
		if init.is_none() {
			return Ok(());
		}

		let init = self.instance.get_typed_func::<(), ()>(&mut self.store, "__sr_init")?;
		init.call_async(&mut self.store, ()).await
	}

	pub async fn invoke<A: Args>(
		&mut self,
		name: Option<String>,
		args: A,
	) -> Result<surrealdb_types::Value> {
		let name = format!("__sr_fnc__{}", name.unwrap_or_default());
		let args = AsyncTransfer::transfer(args.to_values(), self).await?;
		let invoke = self.instance.get_typed_func::<(u32,), (i32,)>(&mut self.store, &name)?;
		let (ptr,) = invoke.call_async(&mut self.store, (*args,)).await?;
		if ptr == -1 {
			anyhow::bail!("WASM function returned error (-1)");
		}
		let ptr_u32: u32 = ptr.try_into()?;
		let result: Result<surrealdb_types::Value, String> =
			AsyncTransfer::receive(ptr_u32.into(), self).await?;
		result.map_err(|e| anyhow::anyhow!("WASM function returned error: {}", e))
	}

	pub async fn args(&mut self, name: Option<String>) -> Result<Vec<surrealdb_types::Kind>> {
		let name = format!("__sr_args__{}", name.unwrap_or_default());
		let args = self.instance.get_typed_func::<(), (i32,)>(&mut self.store, &name)?;
		let (ptr,) = args.call_async(&mut self.store, ()).await?;
		AsyncTransfer::receive(ptr.try_into()?, self).await
	}

	pub async fn returns(&mut self, name: Option<String>) -> Result<surrealdb_types::Kind> {
		let name = format!("__sr_returns__{}", name.unwrap_or_default());
		let returns = self.instance.get_typed_func::<(), (i32,)>(&mut self.store, &name)?;
		let (ptr,) = returns.call_async(&mut self.store, ()).await?;
		if ptr == -1 {
			anyhow::bail!("WASM function returned error (-1)");
		}
		AsyncTransfer::receive(ptr.try_into()?, self).await
	}

	pub fn list(&mut self) -> Result<Vec<String>> {
		// scan the exported functions and return a list of available functions
		let mut functions = Vec::new();

		// First, collect all export names that start with __sr_fnc__
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

		// Then check each one to see if it's actually a function
		for name in function_names {
			if let Some(export) = self.instance.get_export(&mut self.store, &name)
				&& let ExternType::Func(_) = export.ty(&self.store)
			{
				// strip the prefix
				let function_name = name.strip_prefix("__sr_fnc__").unwrap_or(&name).to_string();
				functions.push(function_name);
			}
		}

		Ok(functions)
	}
}

#[async_trait]
impl surrealism_types::controller::AsyncMemoryController for Controller {
	async fn alloc(&mut self, len: u32) -> Result<u32> {
		Controller::alloc(self, len).await
	}

	async fn free(&mut self, ptr: u32, len: u32) -> Result<()> {
		Controller::free(self, ptr, len).await
	}

	fn mut_mem(&mut self, ptr: u32, len: u32) -> Result<&mut [u8]> {
		let mem = self.memory.data_mut(&mut self.store);
		let start = ptr as usize;
		let end = start
			.checked_add(len as usize)
			.ok_or_else(|| anyhow::anyhow!("Memory access overflow: ptr={ptr}, len={len}"))?;

		if end > mem.len() {
			anyhow::bail!(
				"Memory access out of bounds: attempting to access [{start}..{end}), but memory size is {}",
				mem.len()
			);
		}

		Ok(&mut mem[start..end])
	}
}
