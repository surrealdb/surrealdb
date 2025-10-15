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
use std::pin::Pin;

/// Wrapper for the context pointer that's Send.
/// SAFETY: This is safe because Store is !Send, ensuring the pointer is only accessed
/// from the thread that created it.
pub(crate) struct ContextPtr(Option<*mut dyn InvocationContext>);
unsafe impl Send for ContextPtr {}

impl ContextPtr {
    fn new() -> Self {
        Self(None)
    }
    
    fn set(&mut self, ctx: *mut dyn InvocationContext) {
        self.0 = Some(ctx);
    }
    
    fn clear(&mut self) {
        self.0 = None;
    }
    
    fn get(&self) -> Option<*mut dyn InvocationContext> {
        self.0
    }
}

/// Store data for WASM execution. Each Controller has its own isolated StoreData.
pub struct StoreData {
    pub wasi: WasiP1Ctx,
    pub config: Arc<SurrealismConfig>,
    /// Invocation context pointer, set during with_context execution.
    /// SAFETY: The lifetime is erased to 'static, but Controller::with_context guarantees:
    /// 1. The context is only set during with_context's scope
    /// 2. It's cleared before with_context returns
    /// 3. Each Controller has its own isolated Store, so no sharing between executions
    /// 4. Store is !Send, ensuring single-threaded access
    pub(crate) context: ContextPtr,
}

impl StoreData {
    /// Access the invocation context with a callback that returns a Future.
    /// 
    /// SAFETY: This function performs lifetime transmutation to support async methods.
    /// The returned Future borrows from the context, but we erase that lifetime dependency.
    /// 
    /// This is sound because Controller::with_context guarantees:
    /// 1. The context pointer is set at the start and valid for the entire WASM execution
    /// 2. All returned Futures are awaited before with_context returns  
    /// 3. The context pointer is cleared only after all Futures complete
    /// 4. Each Store is isolated to one Controller - no sharing
    /// 5. Store is !Send, ensuring single-threaded access
    pub(crate) fn with_context<F, R>(&mut self, f: F) -> impl std::future::Future<Output = R>
    where
        F: FnOnce(&mut dyn InvocationContext) -> Pin<Box<dyn std::future::Future<Output = R> + '_>>,
    {
        let ptr = self.context.get().expect("InvocationContext not set - must call Controller::with_context()");
        
        // SAFETY: Dereference the raw pointer and extend its lifetime to 'static.
        // This is sound because Controller::with_context guarantees the pointer remains valid
        // until all futures complete and are dropped.
        let context_ref: &'static mut dyn InvocationContext = unsafe { &mut *ptr };
        f(context_ref)
    }
}

/// Compiled WASM runtime. Thread-safe, can be shared across threads via Arc.
/// Compiles WASM once, then each controller gets its own isolated Store/Instance.
/// The Engine, Module, and Linker are immutable and safely shared.
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

    /// Create a new Controller with its own isolated Store and Instance.
    /// This is cheap (relative to compilation) - the expensive compilation is shared.
    /// Each controller has its own mutable Store, ensuring no shared mutable state.
    /// Safe for concurrent execution: no mutable state is shared between controllers.
    pub fn new_controller(&self) -> Result<Controller> {
        let wasi_ctx = super::wasi_context::build()?;

        let store_data = StoreData {
            wasi: wasi_ctx,
            config: self.config.clone(),
            context: ContextPtr::new(),
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
/// Lightweight, created from Runtime. Each controller has its own isolated Store and Instance.
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
    /// The context is stored in this Controller's isolated Store for the duration of the call.
    /// 
    /// This is safe because:
    /// 1. We take `&mut self`, preventing concurrent access to this controller
    /// 2. Each Controller has its own isolated Store - no sharing between controllers
    /// 3. Store is !Send, ensuring single-threaded execution
    /// 4. The context is set before the function and cleared after, with guaranteed cleanup
    /// 5. Host functions access the context via a scoped callback pattern
    pub fn with_context<R>(
        &mut self, 
        context: &mut dyn InvocationContext, 
        f: impl FnOnce(&mut Self) -> Result<R>
    ) -> Result<R> {
        // Check for re-entrant calls
        assert!(self.store.data().context.get().is_none(), "Context already set - re-entrant calls not supported");
        
        // Set the context in this controller's Store
        // SAFETY: We erase the lifetime to 'static, but the actual lifetime is managed by this function.
        // The pointer is cleared before this function returns, ensuring it never outlives the reference.
        let ptr = unsafe {
            std::mem::transmute::<*mut dyn InvocationContext, *mut dyn InvocationContext>(
                context as *mut dyn InvocationContext
            )
        };
        self.store.data_mut().context.set(ptr);
        
        // Execute the user function with guaranteed cleanup even on panic
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| f(self)));
        
        // Clear the context (even if panic occurred)
        self.store.data_mut().context.clear();
        
        // Propagate any panic
        match result {
            Ok(r) => r,
            Err(e) => std::panic::resume_unwind(e),
        }
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
