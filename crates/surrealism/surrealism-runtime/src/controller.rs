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
//!     let mut controller = runtime.new_controller()?;
//!     controller.with_context(&context, |ctrl| {
//!         ctrl.invoke(None, args)
//!     })
//! });
//! # Ok::<(), anyhow::Error>(())
//! ```

use std::cell::UnsafeCell;
use std::fmt;
use std::ptr::NonNull;
use std::sync::Arc;

use anyhow::Result;
use surrealism_types::args::Args;
use surrealism_types::err::PrefixError;
use surrealism_types::transfer::Transfer;
use wasmtime::*;
use wasmtime_wasi::preview1::{self, WasiP1Ctx};

use crate::config::SurrealismConfig;
use crate::host::{implement_host_functions, InvocationContext};
use crate::package::SurrealismPackage;

/// Wrapper that stores a raw pointer to InvocationContext.
/// SAFETY: The context pointer is only valid during `with_context` calls.
/// The Controller must never outlive the borrowed context.
pub(crate) struct ContextCell(UnsafeCell<Option<NonNull<dyn InvocationContext>>>);

unsafe impl Send for ContextCell {}

impl ContextCell {
	pub fn new() -> Self {
		Self(UnsafeCell::new(None))
	}

	/// SAFETY: Caller must ensure the context outlives any access to it through get_mut.
	/// The context must remain valid until `clear` is called.
	pub unsafe fn set_raw(&self, context: *mut dyn InvocationContext) {
		*self.0.get() = NonNull::new(context);
	}

	pub fn clear(&self) {
		unsafe {
			*self.0.get() = None;
		}
	}

	/// SAFETY: Caller must ensure no other references exist and single-threaded access.
	/// This is guaranteed by Store being effectively !Send (due to ContextCell) and Controller
	/// taking &mut self. The context pointer must still be valid (set_raw was called and clear
	/// wasn't called yet).
	pub unsafe fn get_mut(&self) -> &mut dyn InvocationContext {
		(*self.0.get()).expect("context not set").as_mut()
	}
}

/// Store data for WASM execution. Each Controller has its own isolated StoreData.
pub struct StoreData {
	pub wasi: WasiP1Ctx,
	pub config: Arc<SurrealismConfig>,
	pub(crate) context: ContextCell,
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
		// Configure engine for fast compilation in debug, optimized runtime in release
		let mut engine_config = Config::new();
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
		// Enable async support for native async host functions
		engine_config.async_support(true);
        
		let engine = Engine::new(&engine_config)?;
		let module =
			Module::new(&engine, wasm).prefix_err(|| "Failed to construct module from bytes")?;

		let mut linker: Linker<StoreData> = Linker::new(&engine);
		preview1::add_to_linker_sync(&mut linker, |data| &mut data.wasi)
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
	pub async fn new_controller(&self) -> Result<Controller> {
		let wasi_ctx = super::wasi_context::build()?;

		let store_data = StoreData {
			wasi: wasi_ctx,
			config: self.config.clone(),
			context: ContextCell::new(),
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
	/// Run a closure with a temporary context. The context will be cleaned up after the closure
	/// returns. SAFETY: The context must not escape the closure - it's only valid during the
	/// closure execution.
	pub fn with_context<F, R>(&mut self, context: &mut dyn InvocationContext, f: F) -> R
	where
		F: FnOnce(&mut Self) -> R,
	{
		// SAFETY: We're storing a raw pointer to the context, which is valid for the duration
		// of this function call. The context is cleared before returning, ensuring no dangling
		// pointers. We use transmute to erase the lifetime without the compiler inferring
		// 'static.
		unsafe {
			let ptr: *mut dyn InvocationContext = std::mem::transmute(context);
			self.store.data().context.set_raw(ptr);
		}
		let result = f(self);
		self.store.data().context.clear();
		result
	}

	pub fn alloc(&mut self, len: u32, align: u32) -> Result<u32> {
		let alloc =
			self.instance.get_typed_func::<(u32, u32), i32>(&mut self.store, "__sr_alloc")?;
		let result = alloc.call(&mut self.store, (len, align))?;
		if result == -1 {
			anyhow::bail!("Memory allocation failed");
		}
		Ok(result as u32)
	}

	pub fn free(&mut self, ptr: u32, len: u32) -> Result<()> {
		let free = self.instance.get_typed_func::<(u32, u32), i32>(&mut self.store, "__sr_free")?;
		let result = free.call(&mut self.store, (ptr, len))?;
		if result == -1 {
			anyhow::bail!("Memory deallocation failed");
		}
		Ok(())
	}

	pub fn init(&mut self) -> Result<()> {
		let init = self.instance.get_export(&mut self.store, "__sr_init");
		if init.is_none() {
			return Ok(());
		}

		let init = self.instance.get_typed_func::<(), ()>(&mut self.store, "__sr_init")?;
		init.call(&mut self.store, ())
	}

	pub fn invoke<A: Args>(
		&mut self,
		name: Option<String>,
		args: A,
	) -> Result<surrealdb_types::Value> {
		let name = format!("__sr_fnc__{}", name.unwrap_or_default());
		let args = args.to_values().transfer(self)?;
		let invoke = self.instance.get_typed_func::<(u32,), (i32,)>(&mut self.store, &name)?;
		let (ptr,) = invoke.call(&mut self.store, (*args,))?;
		Result::<surrealdb_types::Value>::receive(ptr.try_into()?, self)?
	}

	pub fn args(&mut self, name: Option<String>) -> Result<Vec<surrealdb_types::Kind>> {
		let name = format!("__sr_args__{}", name.unwrap_or_default());
		let args = self.instance.get_typed_func::<(), (i32,)>(&mut self.store, &name)?;
		let (ptr,) = args.call(&mut self.store, ())?;
		Vec::<surrealdb_types::Kind>::receive(ptr.try_into()?, self)
	}

	pub fn returns(&mut self, name: Option<String>) -> Result<surrealdb_types::Kind> {
		let name = format!("__sr_returns__{}", name.unwrap_or_default());
		let returns = self.instance.get_typed_func::<(), (i32,)>(&mut self.store, &name)?;
		let (ptr,) = returns.call(&mut self.store, ())?;
		surrealdb_types::Kind::receive(ptr.try_into()?, self)
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
			if let Some(export) = self.instance.get_export(&mut self.store, &name) {
				if let ExternType::Func(_) = export.ty(&self.store) {
					// strip the prefix
					let function_name =
						name.strip_prefix("__sr_fnc__").unwrap_or(&name).to_string();
					functions.push(function_name);
				}
			}
		}

		Ok(functions)
	}
}

impl surrealism_types::controller::MemoryController for Controller {
	fn alloc(&mut self, len: u32, align: u32) -> Result<u32> {
		Controller::alloc(self, len, align)
	}

	fn free(&mut self, ptr: u32, len: u32) -> Result<()> {
		Controller::free(self, ptr, len)
	}

	fn mut_mem(&mut self, ptr: u32, len: u32) -> &mut [u8] {
		let mem = self.memory.data_mut(&mut self.store);
		&mut mem[(ptr as usize)..(ptr as usize) + (len as usize)]
	}
}
