//! WASM execution runtime and controller.
//!
//! # Architecture
//!
//! - **`Runtime`**: Compiled WASM module. Thread-safe, shareable (Arc<Runtime>).
//!   Compile once, instantiate many times.
//!
//! - **`Controller`**: Per-execution instance. Single-threaded, created from Runtime.
//!   Cheap to create, can be done per-request or pooled.
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

use crate::{
    config::SurrealismConfig, host::{implement_host_functions, InvocationContext}, package::SurrealismPackage
};
use anyhow::Result;
use surrealism_types::{
    args::Args,
    controller::MemoryController,
    err::PrefixError,
    transfer::Transfer,
};
use wasmtime::*;
use wasmtime_wasi::preview1::{self, WasiP1Ctx};
use std::sync::Arc;

/// Wrapper for context reference that's Send (safe because WASM is single-threaded)
/// Stores a mutable pointer since all InvocationContext methods require &mut self.
/// SAFETY: Only one WASM execution happens at a time (single-threaded), and lifetime
/// is guaranteed by with_context() which clears the pointer before returning.
pub(crate) struct ContextRef(Option<*mut dyn InvocationContext>);
unsafe impl Send for ContextRef {}

impl ContextRef {
    pub(crate) fn none() -> Self {
        ContextRef(None)
    }
    
    pub(crate) fn is_none(&self) -> bool {
        self.0.is_none()
    }
    
    pub(crate) fn set(&mut self, ctx: &mut dyn InvocationContext) {
        // SAFETY: Caller (with_context) guarantees lifetime validity and exclusive access.
        // The pointer is cleared before with_context returns.
        // We explicitly cast through a raw pointer to erase the lifetime.
        self.0 = Some(unsafe { 
            std::mem::transmute::<*mut dyn InvocationContext, *mut dyn InvocationContext>(
                ctx as *mut dyn InvocationContext
            )
        });
    }
    
    pub(crate) fn get_mut(&mut self) -> Option<&mut dyn InvocationContext> {
        // SAFETY: Pointer is only valid during WASM execution within with_context scope.
        // Single-threaded execution guarantees no aliasing.
        self.0.map(|ptr| unsafe { &mut *ptr })
    }
}

pub struct StoreData {
    pub wasi: WasiP1Ctx,
    /// Context reference, set per-call. Must be None when not executing.
    /// SAFETY: Only valid during WASM execution, caller ensures lifetime.
    pub(crate) context: ContextRef,
    pub config: Arc<SurrealismConfig>,
}

/// Compiled WASM runtime. Thread-safe, can be shared across threads.
/// Compile once, instantiate many times for concurrent execution.
pub struct Runtime {
    engine: Engine,
    module: Module,
    linker: Linker<StoreData>,
    config: Arc<SurrealismConfig>,
}

impl Runtime {
    /// Compile the WASM module and prepare the runtime.
    /// This is expensive - do it once and reuse via Arc<Runtime>.
    pub fn new(SurrealismPackage { wasm, config }: SurrealismPackage) -> Result<Self> {
        let engine = Engine::default();
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

    /// Create a new Controller for execution.
    /// Cheap relative to compilation - can be done per-request or pooled.
    pub fn new_controller(&self) -> Result<Controller> {
        let wasi_ctx = super::wasi_context::build()?;

        let store_data = StoreData {
            wasi: wasi_ctx,
            context: ContextRef::none(),
            config: self.config.clone(),
        };
        let mut store = Store::new(&self.engine, store_data);
        let instance = self.linker
            .instantiate(&mut store, &self.module)
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
/// Lightweight, created from Runtime.
pub struct Controller {
    pub(crate) store: Store<StoreData>,
    pub(crate) instance: Instance,
    pub(crate) memory: Memory,
}

impl Controller {
    /// Legacy constructor for backwards compatibility.
    /// Consider using Runtime::new() + runtime.new_controller() instead.
    #[deprecated(note = "Use Runtime::new() + runtime.new_controller() for better concurrency")]
    pub fn new(package: SurrealismPackage) -> Result<Self> {
        Runtime::new(package)?.new_controller()
    }
    
    /// Execute a function with the given invocation context.
    /// SAFETY: Context must outlive the call. Panics if context is already set.
    pub fn with_context<R>(&mut self, context: &mut dyn InvocationContext, f: impl FnOnce(&mut Self) -> Result<R>) -> Result<R> {
        assert!(self.store.data().context.is_none(), "Context already set - re-entrant calls not supported");
        
        // Set context
        self.store.data_mut().context.set(context);
        
        // Execute
        let result = f(self);
        
        // Clear context
        self.store.data_mut().context = ContextRef::none();
        
        result
    }

    pub fn alloc(&mut self, len: u32, align: u32) -> Result<u32> {
        let alloc = self
            .instance
            .get_typed_func::<(u32, u32), i32>(&mut self.store, "__sr_alloc")?;
        let result = alloc.call(&mut self.store, (len, align))?;
        if result == -1 {
            anyhow::bail!("Memory allocation failed");
        }
        Ok(result as u32)
    }

    pub fn free(&mut self, ptr: u32, len: u32) -> Result<()> {
        let free = self
            .instance
            .get_typed_func::<(u32, u32), i32>(&mut self.store, "__sr_free")?;
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

        let init = self
            .instance
            .get_typed_func::<(), ()>(&mut self.store, "__sr_init")?;
        init.call(&mut self.store, ())
    }

    pub fn invoke<A: Args>(&mut self, name: Option<String>, args: A) -> Result<surrealdb_types::Value> {
        let name = format!("__sr_fnc__{}", name.unwrap_or_default());
        let args = args.to_values().transfer(self)?;
        let invoke = self
            .instance
            .get_typed_func::<(u32,), (i32,)>(&mut self.store, &name)?;
        let (ptr,) = invoke.call(&mut self.store, (*args,))?;
        Result::<surrealdb_types::Value>::receive(ptr.try_into()?, self)?
    }

    pub fn args(&mut self, name: Option<String>) -> Result<Vec<surrealdb_types::Kind>> {
        let name = format!("__sr_args__{}", name.unwrap_or_default());
        let args = self
            .instance
            .get_typed_func::<(), (i32,)>(&mut self.store, &name)?;
        let (ptr,) = args.call(&mut self.store, ())?;
        Vec::<surrealdb_types::Kind>::receive(ptr.try_into()?, self)
    }

    pub fn returns(&mut self, name: Option<String>) -> Result<surrealdb_types::Kind> {
        let name = format!("__sr_returns__{}", name.unwrap_or_default());
        let returns = self
            .instance
            .get_typed_func::<(), (i32,)>(&mut self.store, &name)?;
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

impl MemoryController for Controller {
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
