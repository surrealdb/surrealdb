use std::ops::{Deref, DerefMut};

use anyhow::Result;
use async_trait::async_trait;
use surrealism_types::controller::AsyncMemoryController;
use surrealism_types::err::PrefixError;
use surrealism_types::serialize::SerializableRange;
use surrealism_types::transfer::AsyncTransfer;
use wasmtime::{Caller, Linker};

use crate::config::SurrealismConfig;
use crate::controller::StoreData;
use crate::kv::KVStore;

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

// Helper macro to convert any type to u32 for argument repetition
macro_rules! force_u32 {
	($ty:ty) => {
		u32
	};
}

/// Macro to register an async host function with automatic argument conversion and error handling.
/// Returns -1 on error (logged to stderr), positive values are valid pointers.
/// Uses Wasmtime's native async support with func_wrap_async.
#[macro_export]
macro_rules! register_host_function {
    // Async version with mutable controller - single argument
    ($linker:expr, $name:expr, |mut $controller:ident : $controller_ty:ty, $arg:ident : $arg_ty:ty| -> Result<$ret:ty> $body:tt) => {{
        $linker
            .func_wrap_async(
                "env",
                $name,
                |caller: Caller<'_, StoreData>, ($arg,): (u32,)| {
                    Box::new(async move {
                        eprintln!("🔵 Host function called: {}", $name);
                        let mut $controller: $controller_ty = HostController::from(caller);
                        let $arg = host_try_or_return!("Failed to receive argument", <$arg_ty>::receive($arg.into(), &mut $controller).await);

                        eprintln!("🟡 Executing async body for: {}", $name);
                        let result = $body;
                        eprintln!("🟢 Async body completed for: {}", $name);

                        (*host_try_or_return!("Transfer error", result.transfer(&mut $controller).await)) as i32
                    })
                }
            )
            .prefix_err(|| "failed to register host function")?
    }};
    // Async version with mutable controller - multiple arguments
    ($linker:expr, $name:expr, |mut $controller:ident : $controller_ty:ty, $($arg:ident : $arg_ty:ty),+| -> Result<$ret:ty> $body:tt) => {{
        $linker
            .func_wrap_async(
                "env",
                $name,
                |caller: Caller<'_, StoreData>, ($($arg),+): ($(force_u32!($arg_ty)),+)| {
                    Box::new(async move {
                        eprintln!("🔵 Host function called: {}", $name);
                        let mut $controller: $controller_ty = HostController::from(caller);
                        $(let $arg = host_try_or_return!("Failed to receive argument", <$arg_ty>::receive($arg.into(), &mut $controller).await);)+

                        eprintln!("🟡 Executing async body for: {}", $name);
                        let result = $body;
                        eprintln!("🟢 Async body completed for: {}", $name);

                        (*host_try_or_return!("Transfer error", result.transfer(&mut $controller).await)) as i32
                    })
                }
            )
            .prefix_err(|| "failed to register host function")?
    }};
    // Async version without mutable controller
    ($linker:expr, $name:expr, |$controller:ident : $controller_ty:ty, $($arg:ident : $arg_ty:ty),+| -> Result<$ret:ty> $body:tt) => {{
        $linker
            .func_wrap_async(
                "env",
                $name,
                |caller: Caller<'_, StoreData>, ($($arg),+): ($(force_u32!($arg_ty)),+)| {
                    Box::new(async move {
                        eprintln!("🔵 Host function called: {}", $name);
                        let mut $controller: $controller_ty = HostController::from(caller);
                        $(let $arg = host_try_or_return!("Failed to receive argument", <$arg_ty>::receive($arg.into(), &mut $controller).await);)+

                        eprintln!("🟡 Executing async body for: {}", $name);
                        let result = $body;
                        eprintln!("🟢 Async body completed for: {}", $name);

                        (*host_try_or_return!("Transfer error", result.transfer(&mut $controller).await)) as i32
                    })
                }
            )
            .prefix_err(|| "failed to register host function")?
    }};
}

macro_rules! map_ok {
	($expr:expr => |$x:ident| $body:expr) => {
		match $expr {
			Ok($x) => $body,
			Err(e) => Err(e),
		}
	};
}

/// Context provided for each WASM function invocation.
/// Created per-call with borrowed execution context (stack, query context, etc).
#[async_trait]
pub trait InvocationContext: Send + Sync {
	async fn sql(
		&mut self,
		config: &SurrealismConfig,
		query: String,
		vars: surrealdb_types::Object,
	) -> Result<surrealdb_types::Value>;
	async fn run(
		&mut self,
		config: &SurrealismConfig,
		fnc: String,
		version: Option<String>,
		args: Vec<surrealdb_types::Value>,
	) -> Result<surrealdb_types::Value>;

	fn kv(&mut self) -> Result<&dyn KVStore>;

	/// Handle stdout output from the WASM module
	fn stdout(&mut self, output: &str) -> Result<()> {
		// Default implementation: print to standard output
		print!("{}", output);
		Ok(())
	}

	/// Handle stderr output from the WASM module
	fn stderr(&mut self, output: &str) -> Result<()> {
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
    register_host_function!(linker, "__sr_sql", |mut controller: HostController, sql: String, vars: Vec<(String, surrealdb_types::Value)>| -> Result<surrealdb_types::Value> {
        let vars = surrealdb_types::Object::from_iter(vars.into_iter());
        let config = controller.config().clone();
        controller.context_mut().sql(&config, sql, vars).await
    });

	// Run function
	#[rustfmt::skip]
    register_host_function!(linker, "__sr_run", |mut controller: HostController, fnc: String, version: Option<String>, args: Vec<surrealdb_types::Value>| -> Result<surrealdb_types::Value> {
        let config = controller.config().clone();
        controller.context_mut().run(&config, fnc, version, args).await
    });

	// KV functions
	#[rustfmt::skip]
    register_host_function!(linker, "__sr_kv_get", |mut controller: HostController, key: String| -> Result<Option<surrealdb_types::Value>> {
        map_ok!(controller.context_mut().kv() => |kv| kv.get(key).await)
    });

	#[rustfmt::skip]
    register_host_function!(linker, "__sr_kv_set", |mut controller: HostController, key: String, value: surrealdb_types::Value| -> Result<()> {
        map_ok!(controller.context_mut().kv() => |kv| kv.set(key, value).await)
    });

	#[rustfmt::skip]
    register_host_function!(linker, "__sr_kv_del", |mut controller: HostController, key: String| -> Result<()> {
        map_ok!(controller.context_mut().kv() => |kv| kv.del(key).await)
    });

	#[rustfmt::skip]
    register_host_function!(linker, "__sr_kv_exists", |mut controller: HostController, key: String| -> Result<bool> {
        map_ok!(controller.context_mut().kv() => |kv| kv.exists(key).await)
    });

	#[rustfmt::skip]
    register_host_function!(linker, "__sr_kv_del_rng", |mut controller: HostController, range: SerializableRange<String>| -> Result<()> {
        map_ok!(controller.context_mut().kv() => |kv| kv.del_rng(range.beg, range.end).await)
    });

	#[rustfmt::skip]
    register_host_function!(linker, "__sr_kv_get_batch", |mut controller: HostController, keys: Vec<String>| -> Result<Vec<Option<surrealdb_types::Value>>> {
        map_ok!(controller.context_mut().kv() => |kv| kv.get_batch(keys).await)
    });

	#[rustfmt::skip]
    register_host_function!(linker, "__sr_kv_set_batch", |mut controller: HostController, entries: Vec<(String, surrealdb_types::Value)>| -> Result<()> {
        map_ok!(controller.context_mut().kv() => |kv| kv.set_batch(entries).await)
    });

	#[rustfmt::skip]
    register_host_function!(linker, "__sr_kv_del_batch", |mut controller: HostController, keys: Vec<String>| -> Result<()> {
        map_ok!(controller.context_mut().kv() => |kv| kv.del_batch(keys).await)
    });

	#[rustfmt::skip]
    register_host_function!(linker, "__sr_kv_keys", |mut controller: HostController, range: SerializableRange<String>| -> Result<Vec<String>> {
        map_ok!(controller.context_mut().kv() => |kv| kv.keys(range.beg, range.end).await)
    });

	#[rustfmt::skip]
    register_host_function!(linker, "__sr_kv_values", |mut controller: HostController, range: SerializableRange<String>| -> Result<Vec<surrealdb_types::Value>> {
        map_ok!(controller.context_mut().kv() => |kv| kv.values(range.beg, range.end).await)
    });

	#[rustfmt::skip]
    register_host_function!(linker, "__sr_kv_entries", |mut controller: HostController, range: SerializableRange<String>| -> Result<Vec<(String, surrealdb_types::Value)>> {
        map_ok!(controller.context_mut().kv() => |kv| kv.entries(range.beg, range.end).await)
    });

	#[rustfmt::skip]
    register_host_function!(linker, "__sr_kv_count", |mut controller: HostController, range: SerializableRange<String>| -> Result<u64> {
        map_ok!(controller.context_mut().kv() => |kv| kv.count(range.beg, range.end).await)
    });

	Ok(())
}

struct HostController<'a>(Caller<'a, StoreData>);

impl<'a> HostController<'a> {
	/// Get mutable reference to the invocation context.
	pub fn context_mut(&mut self) -> &mut dyn InvocationContext {
		&mut *self.0.data_mut().context
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

#[async_trait]
impl<'a> AsyncMemoryController for HostController<'a> {
	async fn alloc(&mut self, len: u32) -> Result<u32> {
		let alloc_func = self
			.get_export("__sr_alloc")
			.ok_or_else(|| anyhow::anyhow!("Export __sr_alloc not found"))?
			.into_func()
			.ok_or_else(|| anyhow::anyhow!("Export __sr_alloc is not a function"))?;
		let result =
			alloc_func.typed::<(u32,), u32>(&mut self.0)?.call_async(&mut self.0, (len,)).await?;
		if result == 0 {
			anyhow::bail!("Memory allocation failed");
		}
		Ok(result)
	}

	async fn free(&mut self, ptr: u32, len: u32) -> Result<()> {
		let free_func = self
			.get_export("__sr_free")
			.ok_or_else(|| anyhow::anyhow!("Export __sr_free not found"))?
			.into_func()
			.ok_or_else(|| anyhow::anyhow!("Export __sr_free is not a function"))?;
		let result = free_func
			.typed::<(u32, u32), u32>(&mut self.0)?
			.call_async(&mut self.0, (ptr, len))
			.await?;
		if result == 0 {
			anyhow::bail!("Memory deallocation failed");
		}
		Ok(())
	}

	fn mut_mem(&mut self, ptr: u32, len: u32) -> Result<&mut [u8]> {
		let memory = self
			.get_export("memory")
			.ok_or_else(|| anyhow::anyhow!("Export memory not found"))?
			.into_memory()
			.ok_or_else(|| anyhow::anyhow!("Export memory is not a memory"))?;
		let mem = memory.data_mut(&mut self.0);
		if (ptr as usize) + (len as usize) > mem.len() {
			anyhow::bail!(
				"[ERROR] Out of bounds: ptr + len = {} > mem.len() = {}",
				(ptr as usize) + (len as usize),
				mem.len()
			);
		}
		Ok(&mut mem[(ptr as usize)..(ptr as usize) + (len as usize)])
	}
}
