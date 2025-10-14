use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use anyhow::Result;
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

/// Macro to register a host function with automatic argument conversion and error handling.
/// Returns -1 on error (logged to stderr), positive values are valid pointers.
#[macro_export]
macro_rules! register_host_function {
    // Sync version with dynamic arguments
    ($linker:expr, $name:expr, |$controller:ident : $controller_ty:ty, $($arg:ident : $arg_ty:ty),*| -> Result<$ret:ty> $body:tt) => {{
        $linker
            .func_wrap(
                "env",
                $name,
                |caller: Caller<StoreData>, $($arg: u32),*| -> i32 {
                    let mut $controller: $controller_ty = HostController::from(caller);

                    // Handle argument receiving errors gracefully
                    $(let $arg = host_try_or_return!("Failed to receive argument", <$arg_ty>::receive($arg.into(), &mut $controller));)*

                    // Execute the main function body and handle errors gracefully
                    let result = (|| -> Result<$ret> $body)();

                    (*host_try_or_return!("Transfer error", result.transfer(&mut $controller))) as i32
                }
            )
            .prefix_err(|| "failed to register host function")?
    }};
}

pub trait Host: Send + Sync {
    fn sql(&self, config: &SurrealismConfig, query: String, vars: surrealdb_types::Object) -> Result<surrealdb_types::Value>;
    fn run(
        &self,
        config: &SurrealismConfig,
        fnc: String,
        version: Option<String>,
        args: Vec<surrealdb_types::Value>,
    ) -> Result<surrealdb_types::Value>;

    fn kv(&self) -> &dyn KVStore;

    fn ml_invoke_model(
        &self,
        config: &SurrealismConfig,
        model: String,
        input: surrealdb_types::Value,
        weight: i64,
        weight_dir: String,
    ) -> Result<surrealdb_types::Value>;
    fn ml_tokenize(&self, config: &SurrealismConfig, model: String, input: surrealdb_types::Value) -> Result<Vec<f64>>;

    /// Handle stdout output from the WASM module
    ///
    /// This method is called whenever the WASM module writes to stdout (e.g., via println!).
    /// The default implementation prints to standard output.
    ///
    /// # Example
    /// ```rust
    /// use surrealism_runtime::host::Host;
    /// use std::sync::{Arc, Mutex};
    ///
    /// struct CapturingHost {
    ///     stdout: Arc<Mutex<String>>,
    /// }
    ///
    /// impl Host for CapturingHost {
    ///     // ... implement other required methods ...
    ///     
    ///     fn stdout(&self, output: &str) -> Result<()> {
    ///         // Capture stdout to our string
    ///         self.stdout.lock().unwrap().push_str(output);
    ///         Ok(())
    ///     }
    /// }
    /// ```
    fn stdout(&self, output: &str) -> Result<()> {
        // Default implementation: print to standard output
        print!("{}", output);
        Ok(())
    }

    /// Handle stderr output from the WASM module
    ///
    /// This method is called whenever the WASM module writes to stderr (e.g., via eprintln!).
    /// The default implementation prints to standard error.
    ///
    /// # Example
    /// ```rust
    /// use surrealism_runtime::host::Host;
    /// use std::sync::{Arc, Mutex};
    ///
    /// struct CapturingHost {
    ///     stderr: Arc<Mutex<String>>,
    /// }
    ///
    /// impl Host for CapturingHost {
    ///     // ... implement other required methods ...
    ///     
    ///     fn stderr(&self, output: &str) -> Result<()> {
    ///         // Capture stderr to our string
    ///         self.stderr.lock().unwrap().push_str(output);
    ///         Ok(())
    ///     }
    /// }
    /// ```
    fn stderr(&self, output: &str) -> Result<()> {
        // Default implementation: print to standard error
        eprint!("{}", output);
        Ok(())
    }
}

pub fn implement_host_functions(linker: &mut Linker<StoreData>) -> Result<()> {
    // SQL function
    #[rustfmt::skip]
    register_host_function!(linker, "__sr_sql", |controller: HostController, sql: String, vars: Vec<(String, surrealdb_types::Value)>| -> Result<surrealdb_types::Value> {
        let vars = surrealdb_types::Object::from_iter(vars.into_iter());
        controller
            .host()
            .sql(controller.config(), sql, vars)
    });

    // Run function
    #[rustfmt::skip]
    register_host_function!(linker, "__sr_run", |controller: HostController, fnc: String, version: Option<String>, args: Vec<surrealdb_types::Value>| -> Result<surrealdb_types::Value> {
        controller
            .host()
            .run(controller.config(), fnc, version, args)
    });

    // KV functions
    #[rustfmt::skip]
    register_host_function!(linker, "__sr_kv_get", |controller: HostController, key: String| -> Result<Option<surrealdb_types::Value>> {
        controller
            .host()
            .kv()
            .get(key)
    });

    #[rustfmt::skip]
    register_host_function!(linker, "__sr_kv_set", |controller: HostController, key: String, value: surrealdb_types::Value| -> Result<()> {
        controller.host().kv().set(key, value)
    });

    #[rustfmt::skip]
    register_host_function!(linker, "__sr_kv_del", |controller: HostController, key: String| -> Result<()> {
        controller.host().kv().del(key)
    });

    #[rustfmt::skip]
    register_host_function!(linker, "__sr_kv_exists", |controller: HostController, key: String| -> Result<bool> {
        controller.host().kv().exists(key)
    });

    #[rustfmt::skip]
    register_host_function!(linker, "__sr_kv_del_rng", |controller: HostController, range: SerializableRange<String>| -> Result<()> {
        controller.host().kv().del_rng(range.beg, range.end)
    });

    #[rustfmt::skip]
    register_host_function!(linker, "__sr_kv_get_batch", |controller: HostController, keys: Vec<String>| -> Result<Vec<Option<surrealdb_types::Value>>> {
        controller.host().kv().get_batch(keys)
    });

    #[rustfmt::skip]
    register_host_function!(linker, "__sr_kv_set_batch", |controller: HostController, entries: Vec<(String, surrealdb_types::Value)>| -> Result<()> {
        controller.host().kv().set_batch(entries)
    });

    #[rustfmt::skip]
    register_host_function!(linker, "__sr_kv_del_batch", |controller: HostController, keys: Vec<String>| -> Result<()> {
        controller.host().kv().del_batch(keys)
    });

    #[rustfmt::skip]
    register_host_function!(linker, "__sr_kv_keys", |controller: HostController, range: SerializableRange<String>| -> Result<Vec<String>> {
        controller.host().kv().keys(range.beg, range.end)
    });

    #[rustfmt::skip]
    register_host_function!(linker, "__sr_kv_values", |controller: HostController, range: SerializableRange<String>| -> Result<Vec<surrealdb_types::Value>> {
        controller.host().kv().values(range.beg, range.end)
    });

    #[rustfmt::skip]
    register_host_function!(linker, "__sr_kv_entries", |controller: HostController, range: SerializableRange<String>| -> Result<Vec<(String, surrealdb_types::Value)>> {
        controller.host().kv().entries(range.beg, range.end)
    });

    #[rustfmt::skip]
    register_host_function!(linker, "__sr_kv_count", |controller: HostController, range: SerializableRange<String>| -> Result<u64> {
        controller.host().kv().count(range.beg, range.end)
    });

    // ML invoke model function
    #[rustfmt::skip]
    register_host_function!(linker, "__sr_ml_invoke_model", |controller: HostController, model: String, input: surrealdb_types::Value, weight: i64, weight_dir: String| -> Result<surrealdb_types::Value> {
        controller
            .host()
            .ml_invoke_model(controller.config(), model, input, weight, weight_dir)
    });

    // ML tokenize function
    #[rustfmt::skip]
    register_host_function!(linker, "__sr_ml_tokenize", |controller: HostController, model: String, input: surrealdb_types::Value| -> Result<Vec<f64>> {
        controller
            .host()
            .ml_tokenize(controller.config(), model, input)
    });
    
    Ok(())
}

struct HostController<'a>(Caller<'a, StoreData>);

impl<'a> HostController<'a> {
    pub fn host(&self) -> &Arc<dyn Host> {
        &self.0.data().host
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
