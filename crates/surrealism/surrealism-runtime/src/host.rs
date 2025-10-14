use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use surrealism_types::serialize::SerializableRange;
use surrealism_types::{
    controller::MemoryController,
    err::PrefixError,
    transfer::Transfer,
};
use wasmtime::{Caller, Linker};

use crate::config::SurrealismConfig;
use crate::{controller::StoreData, kv::KVStore};

macro_rules! host_try_or_return {
    ($error:expr,$expr:expr) => {
        match $expr {
            Ok(val) => val,
            Err(e) => {
                eprintln!("{}: {}", $error, e);
                return -1;
            }
        }
    };
}

/// Macro to register an async host function with automatic argument conversion and error handling.
/// Returns -1 on error (logged to stderr), positive values are valid pointers.
/// Uses tokio::runtime::Handle::block_on to bridge async functions to sync WASM bindings.
#[macro_export]
macro_rules! register_host_function {
    // Async version with dynamic arguments
    ($linker:expr, $name:expr, |$controller:ident : $controller_ty:ty, $($arg:ident : $arg_ty:ty),*| -> Result<$ret:ty> $body:tt) => {{
        $linker
            .func_wrap(
                "env",
                $name,
                |caller: Caller<StoreData>, $($arg: u32),*| -> i32 {
                    let mut $controller: $controller_ty = HostController::from(caller);

                    // Handle argument receiving errors gracefully
                    $(let $arg = host_try_or_return!("Failed to receive argument", <$arg_ty>::receive($arg.into(), &mut $controller));)*

                    // Execute the async function body in a blocking context
                    let result = tokio::runtime::Handle::current().block_on(async $body);

                    (*host_try_or_return!("Transfer error", result.transfer(&mut $controller))) as i32
                }
            )
            .prefix_err(|| "failed to register host function")?
    }};
}

/// Context provided for each WASM function invocation.
/// Created per-call with borrowed execution context (stack, query context, etc).
#[async_trait(?Send)]
pub trait InvocationContext {
    async fn sql(&self, config: &SurrealismConfig, query: String, vars: surrealdb_types::Object) -> Result<surrealdb_types::Value>;
    async fn run(
        &self,
        config: &SurrealismConfig,
        fnc: String,
        version: Option<String>,
        args: Vec<surrealdb_types::Value>,
    ) -> Result<surrealdb_types::Value>;

    fn kv(&self) -> &dyn KVStore;

    async fn ml_invoke_model(
        &self,
        config: &SurrealismConfig,
        model: String,
        input: surrealdb_types::Value,
        weight: i64,
        weight_dir: String,
    ) -> Result<surrealdb_types::Value>;
    async fn ml_tokenize(&self, config: &SurrealismConfig, model: String, input: surrealdb_types::Value) -> Result<Vec<f64>>;

    /// Handle stdout output from the WASM module
    fn stdout(&self, output: &str) -> Result<()> {
        // Default implementation: print to standard output
        print!("{}", output);
        Ok(())
    }

    /// Handle stderr output from the WASM module
    fn stderr(&self, output: &str) -> Result<()> {
        // Default implementation: print to standard error
        eprint!("{}", output);
        Ok(())
    }
}

// Legacy alias for backwards compatibility during transition
pub trait Host: InvocationContext {}

pub fn implement_host_functions(linker: &mut Linker<StoreData>) -> Result<()> {
    // SQL function
    #[rustfmt::skip]
    register_host_function!(linker, "__sr_sql", |controller: HostController, sql: String, vars: Vec<(String, surrealdb_types::Value)>| -> Result<surrealdb_types::Value> {
        let vars = surrealdb_types::Object::from_iter(vars.into_iter());
        controller
            .context()
            .sql(controller.config(), sql, vars)
            .await
    });

    // Run function
    #[rustfmt::skip]
    register_host_function!(linker, "__sr_run", |controller: HostController, fnc: String, version: Option<String>, args: Vec<surrealdb_types::Value>| -> Result<surrealdb_types::Value> {
        controller
            .context()
            .run(controller.config(), fnc, version, args)
            .await
    });

    // KV functions
    #[rustfmt::skip]
    register_host_function!(linker, "__sr_kv_get", |controller: HostController, key: String| -> Result<Option<surrealdb_types::Value>> {
        controller
            .context()
            .kv()
            .get(key)
            .await
    });

    #[rustfmt::skip]
    register_host_function!(linker, "__sr_kv_set", |controller: HostController, key: String, value: surrealdb_types::Value| -> Result<()> {
        controller.context().kv().set(key, value).await
    });

    #[rustfmt::skip]
    register_host_function!(linker, "__sr_kv_del", |controller: HostController, key: String| -> Result<()> {
        controller.context().kv().del(key).await
    });

    #[rustfmt::skip]
    register_host_function!(linker, "__sr_kv_exists", |controller: HostController, key: String| -> Result<bool> {
        controller.context().kv().exists(key).await
    });

    #[rustfmt::skip]
    register_host_function!(linker, "__sr_kv_del_rng", |controller: HostController, range: SerializableRange<String>| -> Result<()> {
        controller.context().kv().del_rng(range.beg, range.end).await
    });

    #[rustfmt::skip]
    register_host_function!(linker, "__sr_kv_get_batch", |controller: HostController, keys: Vec<String>| -> Result<Vec<Option<surrealdb_types::Value>>> {
        controller.context().kv().get_batch(keys).await
    });

    #[rustfmt::skip]
    register_host_function!(linker, "__sr_kv_set_batch", |controller: HostController, entries: Vec<(String, surrealdb_types::Value)>| -> Result<()> {
        controller.context().kv().set_batch(entries).await
    });

    #[rustfmt::skip]
    register_host_function!(linker, "__sr_kv_del_batch", |controller: HostController, keys: Vec<String>| -> Result<()> {
        controller.context().kv().del_batch(keys).await
    });

    #[rustfmt::skip]
    register_host_function!(linker, "__sr_kv_keys", |controller: HostController, range: SerializableRange<String>| -> Result<Vec<String>> {
        controller.context().kv().keys(range.beg, range.end).await
    });

    #[rustfmt::skip]
    register_host_function!(linker, "__sr_kv_values", |controller: HostController, range: SerializableRange<String>| -> Result<Vec<surrealdb_types::Value>> {
        controller.context().kv().values(range.beg, range.end).await
    });

    #[rustfmt::skip]
    register_host_function!(linker, "__sr_kv_entries", |controller: HostController, range: SerializableRange<String>| -> Result<Vec<(String, surrealdb_types::Value)>> {
        controller.context().kv().entries(range.beg, range.end).await
    });

    #[rustfmt::skip]
    register_host_function!(linker, "__sr_kv_count", |controller: HostController, range: SerializableRange<String>| -> Result<u64> {
        controller.context().kv().count(range.beg, range.end).await
    });

    // ML invoke model function
    #[rustfmt::skip]
    register_host_function!(linker, "__sr_ml_invoke_model", |controller: HostController, model: String, input: surrealdb_types::Value, weight: i64, weight_dir: String| -> Result<surrealdb_types::Value> {
        controller
            .context()
            .ml_invoke_model(controller.config(), model, input, weight, weight_dir)
            .await
    });

    // ML tokenize function
    #[rustfmt::skip]
    register_host_function!(linker, "__sr_ml_tokenize", |controller: HostController, model: String, input: surrealdb_types::Value| -> Result<Vec<f64>> {
        controller
            .context()
            .ml_tokenize(controller.config(), model, input)
            .await
    });
    
    Ok(())
}

struct HostController<'a>(Caller<'a, StoreData>);

impl<'a> HostController<'a> {
    pub fn context(&self) -> &dyn InvocationContext {
        self.0.data().context.get()
            .expect("InvocationContext not set - must call with_context()")
    }

    pub fn config(&self) -> &SurrealismConfig {
        &self.0.data().config
    }
}

impl<'a> From<Caller<'a, StoreData>> for HostController<'a> {
    fn from(caller: Caller<'a, StoreData>) -> Self {
        Self(caller)
    }
}

impl<'a> Deref for HostController<'a> {
    type Target = Caller<'a, StoreData>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<'a> DerefMut for HostController<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<'a> MemoryController for HostController<'a> {
    fn alloc(&mut self, len: u32, align: u32) -> Result<u32> {
        let alloc_func = self
            .get_export("__sr_alloc")
            .ok_or_else(|| anyhow::anyhow!("Export __sr_alloc not found"))?
            .into_func()
            .ok_or_else(|| anyhow::anyhow!("Export __sr_alloc is not a function"))?;
        let result = alloc_func
            .typed::<(u32, u32), i32>(&mut self.0)?
            .call(&mut self.0, (len, align))?;
        if result == -1 {
            anyhow::bail!("Memory allocation failed");
        }
        Ok(result as u32)
    }

    fn free(&mut self, ptr: u32, len: u32) -> Result<()> {
        let free_func = self
            .get_export("__sr_free")
            .ok_or_else(|| anyhow::anyhow!("Export __sr_free not found"))?
            .into_func()
            .ok_or_else(|| anyhow::anyhow!("Export __sr_free is not a function"))?;
        let result = free_func
            .typed::<(u32, u32), i32>(&mut self.0)?
            .call(&mut self.0, (ptr, len))?;
        if result == -1 {
            anyhow::bail!("Memory deallocation failed");
        }
        Ok(())
    }

    fn mut_mem(&mut self, ptr: u32, len: u32) -> &mut [u8] {
        let memory = self
            .get_export("memory")
            .ok_or_else(|| anyhow::anyhow!("Export memory not found"))
            .unwrap()
            .into_memory()
            .ok_or_else(|| anyhow::anyhow!("Export memory is not a memory"))
            .unwrap();
        let mem = memory.data_mut(&mut self.0);
        if (ptr as usize) + (len as usize) > mem.len() {
            println!(
                "[ERROR] Out of bounds: ptr + len = {} > mem.len() = {}",
                (ptr as usize) + (len as usize),
                mem.len()
            );
        }
        &mut mem[(ptr as usize)..(ptr as usize) + (len as usize)]
    }
}
