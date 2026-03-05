use std::ops::{Deref, DerefMut};

use anyhow::Result;
use async_trait::async_trait;
use surrealism_types::controller::AsyncMemoryController;
use surrealism_types::err::{PrefixErr, SurrealismError, SurrealismResult};
use surrealism_types::serialize::SerializableRange;
use surrealism_types::transfer::AsyncTransfer;

fn p2_decode_string_range(
	bytes: &[u8],
) -> Result<(std::ops::Bound<String>, std::ops::Bound<String>), String> {
	surrealdb_types::decode_string_range(bytes).map_err(|e| e.to_string())
}
use wasmtime::{Caller, Linker, StoreContextMut};

use crate::config::SurrealismConfig;
use crate::controller::{P1StoreData, P2StoreData};
use crate::kv::KVStore;

// ============================================================================
// InvocationContext trait (shared by P1 and P2)
// ============================================================================

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

	fn stdout(&mut self, output: &str) -> Result<()> {
		print!("{}", output);
		Ok(())
	}

	fn stderr(&mut self, output: &str) -> Result<()> {
		eprint!("{}", output);
		Ok(())
	}
}

// ============================================================================
// P1 host functions (core module path — unchanged)
// ============================================================================

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

macro_rules! force_u32 {
	($ty:ty) => {
		u32
	};
}

#[macro_export]
macro_rules! register_host_function {
    ($linker:expr, $name:expr, |mut $controller:ident : $controller_ty:ty, $arg:ident : $arg_ty:ty| -> Result<$ret:ty> $body:tt) => {{
        $linker
            .func_wrap_async(
                "env",
                $name,
                |caller: Caller<'_, P1StoreData>, ($arg,): (u32,)| {
                    Box::new(async move {
                        let mut $controller: $controller_ty = P1HostController::from(caller);
                        let $arg = host_try_or_return!("Failed to receive argument", <$arg_ty>::receive($arg.into(), &mut $controller).await);
                        let result = $body;
                        (*host_try_or_return!("Transfer error", result.transfer(&mut $controller).await)) as i32
                    })
                }
            )
            .prefix_err(|| "failed to register host function")?
    }};
    ($linker:expr, $name:expr, |mut $controller:ident : $controller_ty:ty, $($arg:ident : $arg_ty:ty),+| -> Result<$ret:ty> $body:tt) => {{
        $linker
            .func_wrap_async(
                "env",
                $name,
                |caller: Caller<'_, P1StoreData>, ($($arg),+): ($(force_u32!($arg_ty)),+)| {
                    Box::new(async move {
                        let mut $controller: $controller_ty = P1HostController::from(caller);
                        $(let $arg = host_try_or_return!("Failed to receive argument", <$arg_ty>::receive($arg.into(), &mut $controller).await);)+
                        let result = $body;
                        (*host_try_or_return!("Transfer error", result.transfer(&mut $controller).await)) as i32
                    })
                }
            )
            .prefix_err(|| "failed to register host function")?
    }};
    ($linker:expr, $name:expr, |$controller:ident : $controller_ty:ty, $($arg:ident : $arg_ty:ty),+| -> Result<$ret:ty> $body:tt) => {{
        $linker
            .func_wrap_async(
                "env",
                $name,
                |caller: Caller<'_, P1StoreData>, ($($arg),+): ($(force_u32!($arg_ty)),+)| {
                    Box::new(async move {
                        let mut $controller: $controller_ty = P1HostController::from(caller);
                        $(let $arg = host_try_or_return!("Failed to receive argument", <$arg_ty>::receive($arg.into(), &mut $controller).await);)+
                        let result = $body;
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

pub fn implement_p1_host_functions(linker: &mut Linker<P1StoreData>) -> SurrealismResult<()> {
	#[rustfmt::skip]
    register_host_function!(linker, "__sr_sql", |mut controller: P1HostController, sql: String, vars: Vec<(String, surrealdb_types::Value)>| -> Result<surrealdb_types::Value> {
        let vars = surrealdb_types::Object::from_iter(vars.into_iter());
        let config = controller.config().clone();
        controller.context_mut().sql(&config, sql, vars).await
    });

	#[rustfmt::skip]
    register_host_function!(linker, "__sr_run", |mut controller: P1HostController, fnc: String, version: Option<String>, args: Vec<surrealdb_types::Value>| -> Result<surrealdb_types::Value> {
        let config = controller.config().clone();
        controller.context_mut().run(&config, fnc, version, args).await
    });

	#[rustfmt::skip]
    register_host_function!(linker, "__sr_kv_get", |mut controller: P1HostController, key: String| -> Result<Option<surrealdb_types::Value>> {
        map_ok!(controller.context_mut().kv() => |kv| kv.get(key).await)
    });

	#[rustfmt::skip]
    register_host_function!(linker, "__sr_kv_set", |mut controller: P1HostController, key: String, value: surrealdb_types::Value| -> Result<()> {
        map_ok!(controller.context_mut().kv() => |kv| kv.set(key, value).await)
    });

	#[rustfmt::skip]
    register_host_function!(linker, "__sr_kv_del", |mut controller: P1HostController, key: String| -> Result<()> {
        map_ok!(controller.context_mut().kv() => |kv| kv.del(key).await)
    });

	#[rustfmt::skip]
    register_host_function!(linker, "__sr_kv_exists", |mut controller: P1HostController, key: String| -> Result<bool> {
        map_ok!(controller.context_mut().kv() => |kv| kv.exists(key).await)
    });

	#[rustfmt::skip]
    register_host_function!(linker, "__sr_kv_del_rng", |mut controller: P1HostController, range: SerializableRange<String>| -> Result<()> {
        map_ok!(controller.context_mut().kv() => |kv| kv.del_rng(range.beg, range.end).await)
    });

	#[rustfmt::skip]
    register_host_function!(linker, "__sr_kv_get_batch", |mut controller: P1HostController, keys: Vec<String>| -> Result<Vec<Option<surrealdb_types::Value>>> {
        map_ok!(controller.context_mut().kv() => |kv| kv.get_batch(keys).await)
    });

	#[rustfmt::skip]
    register_host_function!(linker, "__sr_kv_set_batch", |mut controller: P1HostController, entries: Vec<(String, surrealdb_types::Value)>| -> Result<()> {
        map_ok!(controller.context_mut().kv() => |kv| kv.set_batch(entries).await)
    });

	#[rustfmt::skip]
    register_host_function!(linker, "__sr_kv_del_batch", |mut controller: P1HostController, keys: Vec<String>| -> Result<()> {
        map_ok!(controller.context_mut().kv() => |kv| kv.del_batch(keys).await)
    });

	#[rustfmt::skip]
    register_host_function!(linker, "__sr_kv_keys", |mut controller: P1HostController, range: SerializableRange<String>| -> Result<Vec<String>> {
        map_ok!(controller.context_mut().kv() => |kv| kv.keys(range.beg, range.end).await)
    });

	#[rustfmt::skip]
    register_host_function!(linker, "__sr_kv_values", |mut controller: P1HostController, range: SerializableRange<String>| -> Result<Vec<surrealdb_types::Value>> {
        map_ok!(controller.context_mut().kv() => |kv| kv.values(range.beg, range.end).await)
    });

	#[rustfmt::skip]
    register_host_function!(linker, "__sr_kv_entries", |mut controller: P1HostController, range: SerializableRange<String>| -> Result<Vec<(String, surrealdb_types::Value)>> {
        map_ok!(controller.context_mut().kv() => |kv| kv.entries(range.beg, range.end).await)
    });

	#[rustfmt::skip]
    register_host_function!(linker, "__sr_kv_count", |mut controller: P1HostController, range: SerializableRange<String>| -> Result<u64> {
        map_ok!(controller.context_mut().kv() => |kv| kv.count(range.beg, range.end).await)
    });

	Ok(())
}

// ---------------------------------------------------------------------------
// P1 HostController (wraps Caller for core-module host functions)
// ---------------------------------------------------------------------------

struct P1HostController<'a>(Caller<'a, P1StoreData>);

impl<'a> P1HostController<'a> {
	pub fn context_mut(&mut self) -> &mut dyn InvocationContext {
		&mut *self.0.data_mut().context
	}

	pub fn config(&self) -> &SurrealismConfig {
		&self.0.data().config
	}
}

impl<'a> From<Caller<'a, P1StoreData>> for P1HostController<'a> {
	fn from(caller: Caller<'a, P1StoreData>) -> Self {
		Self(caller)
	}
}

impl<'a> Deref for P1HostController<'a> {
	type Target = Caller<'a, P1StoreData>;
	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl<'a> DerefMut for P1HostController<'a> {
	fn deref_mut(&mut self) -> &mut Self::Target {
		&mut self.0
	}
}

#[async_trait]
impl<'a> AsyncMemoryController for P1HostController<'a> {
	async fn alloc(&mut self, len: u32) -> SurrealismResult<u32> {
		let alloc_func = self
			.get_export("__sr_alloc")
			.ok_or_else(|| anyhow::anyhow!("Export __sr_alloc not found"))?
			.into_func()
			.ok_or_else(|| anyhow::anyhow!("Export __sr_alloc is not a function"))?;
		let result =
			alloc_func.typed::<(u32,), u32>(&mut self.0)?.call_async(&mut self.0, (len,)).await?;
		if result == 0 {
			return Err(SurrealismError::AllocFailed);
		}
		Ok(result)
	}

	async fn free(&mut self, ptr: u32, len: u32) -> SurrealismResult<()> {
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
			return Err(SurrealismError::FreeFailed);
		}
		Ok(())
	}

	fn mut_mem(&mut self, ptr: u32, len: u32) -> SurrealismResult<&mut [u8]> {
		let memory = self
			.get_export("memory")
			.ok_or_else(|| anyhow::anyhow!("Export memory not found"))?
			.into_memory()
			.ok_or_else(|| anyhow::anyhow!("Export memory is not a memory"))?;
		let mem = memory.data_mut(&mut self.0);
		if (ptr as usize) + (len as usize) > mem.len() {
			return Err(SurrealismError::OutOfBounds(format!(
				"ptr + len = {} > mem.len() = {}",
				(ptr as usize) + (len as usize),
				mem.len()
			)));
		}
		Ok(&mut mem[(ptr as usize)..(ptr as usize) + (len as usize)])
	}
}

// ============================================================================
// P2 host functions (component model path — FlatBuffers serialization)
// ============================================================================

pub fn implement_p2_host_functions(
	linker: &mut wasmtime::component::Linker<P2StoreData>,
) -> SurrealismResult<()> {
	let mut root = linker.root();
	let mut host_instance =
		root.instance("surrealism:plugin/host").prefix_err(|| "failed to define host instance")?;

	// sql(query: string, vars: list<u8>) -> result<list<u8>, string>
	host_instance
		.func_wrap_async(
			"sql",
			|mut store: StoreContextMut<'_, P2StoreData>,
			 (query, vars_bytes): (String, Vec<u8>)| {
				Box::new(async move {
					let inner: Result<Vec<u8>, String> = async {
						let vars_vec = surrealdb_types::decode_string_key_values(&vars_bytes)
							.map_err(|e| e.to_string())?;
						let vars = surrealdb_types::Object::from_iter(vars_vec.into_iter());
						let config = store.data().config.clone();
						let val = store
							.data_mut()
							.context
							.sql(&config, query, vars)
							.await
							.map_err(|e| e.to_string())?;
						surrealdb_types::encode(&val).map_err(|e| e.to_string())
					}
					.await;
					Ok((inner,))
				})
			},
		)
		.prefix_err(|| "failed to register P2 sql")?;

	// run(fnc: string, version: option<string>, args: list<u8>) -> result<list<u8>, string>
	host_instance
		.func_wrap_async(
			"run",
			|mut store: StoreContextMut<'_, P2StoreData>,
			 (fnc, version, args_bytes): (String, Option<String>, Vec<u8>)| {
				Box::new(async move {
					let inner: Result<Vec<u8>, String> = async {
						let args = surrealdb_types::decode_value_list(&args_bytes)
							.map_err(|e| e.to_string())?;
						let config = store.data().config.clone();
						let val = store
							.data_mut()
							.context
							.run(&config, fnc, version, args)
							.await
							.map_err(|e| e.to_string())?;
						surrealdb_types::encode(&val).map_err(|e| e.to_string())
					}
					.await;
					Ok((inner,))
				})
			},
		)
		.prefix_err(|| "failed to register P2 run")?;

	// kv-get(key: string) -> result<option<list<u8>>, string>
	host_instance
		.func_wrap_async(
			"kv-get",
			|mut store: StoreContextMut<'_, P2StoreData>, (key,): (String,)| {
				Box::new(async move {
					let inner: Result<Option<Vec<u8>>, String> = match store.data_mut().context.kv()
					{
						Ok(kv) => match kv.get(key).await {
							Ok(Some(v)) => {
								surrealdb_types::encode(&v).map(Some).map_err(|e| e.to_string())
							}
							Ok(None) => Ok(None),
							Err(e) => Err(e.to_string()),
						},
						Err(e) => Err(e.to_string()),
					};
					Ok((inner,))
				})
			},
		)
		.prefix_err(|| "failed to register P2 kv-get")?;

	// kv-set(key: string, value: list<u8>) -> result<_, string>
	host_instance
		.func_wrap_async(
			"kv-set",
			|mut store: StoreContextMut<'_, P2StoreData>, (key, value_bytes): (String, Vec<u8>)| {
				Box::new(async move {
					let inner: Result<(), String> = async {
						let value: surrealdb_types::Value =
							surrealdb_types::decode(&value_bytes).map_err(|e| e.to_string())?;
						match store.data_mut().context.kv() {
							Ok(kv) => kv.set(key, value).await.map_err(|e| e.to_string()),
							Err(e) => Err(e.to_string()),
						}
					}
					.await;
					Ok((inner,))
				})
			},
		)
		.prefix_err(|| "failed to register P2 kv-set")?;

	// kv-del(key: string) -> result<_, string>
	host_instance
		.func_wrap_async(
			"kv-del",
			|mut store: StoreContextMut<'_, P2StoreData>, (key,): (String,)| {
				Box::new(async move {
					let result: Result<(), String> = match store.data_mut().context.kv() {
						Ok(kv) => kv.del(key).await.map_err(|e| e.to_string()),
						Err(e) => Err(e.to_string()),
					};
					Ok((result,))
				})
			},
		)
		.prefix_err(|| "failed to register P2 kv-del")?;

	// kv-exists(key: string) -> result<bool, string>
	host_instance
		.func_wrap_async(
			"kv-exists",
			|mut store: StoreContextMut<'_, P2StoreData>, (key,): (String,)| {
				Box::new(async move {
					let result: Result<bool, String> = match store.data_mut().context.kv() {
						Ok(kv) => kv.exists(key).await.map_err(|e| e.to_string()),
						Err(e) => Err(e.to_string()),
					};
					Ok((result,))
				})
			},
		)
		.prefix_err(|| "failed to register P2 kv-exists")?;

	// kv-del-rng(range: list<u8>) -> result<_, string>
	host_instance
		.func_wrap_async(
			"kv-del-rng",
			|mut store: StoreContextMut<'_, P2StoreData>, (range_bytes,): (Vec<u8>,)| {
				Box::new(async move {
					let result: Result<(), String> = match p2_decode_string_range(&range_bytes) {
						Ok((start, end)) => match store.data_mut().context.kv() {
							Ok(kv) => kv.del_rng(start, end).await.map_err(|e| e.to_string()),
							Err(e) => Err(e.to_string()),
						},
						Err(e) => Err(e),
					};
					Ok((result,))
				})
			},
		)
		.prefix_err(|| "failed to register P2 kv-del-rng")?;

	// kv-get-batch(keys: list<string>) -> result<list<u8>, string>
	host_instance
		.func_wrap_async(
			"kv-get-batch",
			|mut store: StoreContextMut<'_, P2StoreData>, (keys,): (Vec<String>,)| {
				Box::new(async move {
					let result: Result<Vec<u8>, String> = match store.data_mut().context.kv() {
						Ok(kv) => match kv.get_batch(keys).await {
							Ok(vals) => surrealdb_types::encode_optional_values(&vals)
								.map_err(|e| e.to_string()),
							Err(e) => Err(e.to_string()),
						},
						Err(e) => Err(e.to_string()),
					};
					Ok((result,))
				})
			},
		)
		.prefix_err(|| "failed to register P2 kv-get-batch")?;

	// kv-set-batch(entries: list<u8>) -> result<_, string>
	host_instance
		.func_wrap_async(
			"kv-set-batch",
			|mut store: StoreContextMut<'_, P2StoreData>, (entries_bytes,): (Vec<u8>,)| {
				Box::new(async move {
					let result: Result<(), String> =
						match surrealdb_types::decode_string_key_values(&entries_bytes) {
							Ok(entries) => match store.data_mut().context.kv() {
								Ok(kv) => kv.set_batch(entries).await.map_err(|e| e.to_string()),
								Err(e) => Err(e.to_string()),
							},
							Err(e) => Err(e.to_string()),
						};
					Ok((result,))
				})
			},
		)
		.prefix_err(|| "failed to register P2 kv-set-batch")?;

	// kv-del-batch(keys: list<string>) -> result<_, string>
	host_instance
		.func_wrap_async(
			"kv-del-batch",
			|mut store: StoreContextMut<'_, P2StoreData>, (keys,): (Vec<String>,)| {
				Box::new(async move {
					let result: Result<(), String> = match store.data_mut().context.kv() {
						Ok(kv) => kv.del_batch(keys).await.map_err(|e| e.to_string()),
						Err(e) => Err(e.to_string()),
					};
					Ok((result,))
				})
			},
		)
		.prefix_err(|| "failed to register P2 kv-del-batch")?;

	// kv-keys(range: list<u8>) -> result<list<string>, string>
	host_instance
		.func_wrap_async(
			"kv-keys",
			|mut store: StoreContextMut<'_, P2StoreData>, (range_bytes,): (Vec<u8>,)| {
				Box::new(async move {
					let result: Result<Vec<String>, String> =
						match p2_decode_string_range(&range_bytes) {
							Ok((start, end)) => match store.data_mut().context.kv() {
								Ok(kv) => kv.keys(start, end).await.map_err(|e| e.to_string()),
								Err(e) => Err(e.to_string()),
							},
							Err(e) => Err(e),
						};
					Ok((result,))
				})
			},
		)
		.prefix_err(|| "failed to register P2 kv-keys")?;

	// kv-values(range: list<u8>) -> result<list<u8>, string>
	host_instance
		.func_wrap_async(
			"kv-values",
			|mut store: StoreContextMut<'_, P2StoreData>, (range_bytes,): (Vec<u8>,)| {
				Box::new(async move {
					let result: Result<Vec<u8>, String> = match p2_decode_string_range(&range_bytes)
					{
						Ok((start, end)) => match store.data_mut().context.kv() {
							Ok(kv) => match kv.values(start, end).await {
								Ok(vals) => surrealdb_types::encode_value_list(&vals)
									.map_err(|e| e.to_string()),
								Err(e) => Err(e.to_string()),
							},
							Err(e) => Err(e.to_string()),
						},
						Err(e) => Err(e),
					};
					Ok((result,))
				})
			},
		)
		.prefix_err(|| "failed to register P2 kv-values")?;

	// kv-entries(range: list<u8>) -> result<list<u8>, string>
	host_instance
		.func_wrap_async(
			"kv-entries",
			|mut store: StoreContextMut<'_, P2StoreData>, (range_bytes,): (Vec<u8>,)| {
				Box::new(async move {
					let result: Result<Vec<u8>, String> = match p2_decode_string_range(&range_bytes)
					{
						Ok((start, end)) => match store.data_mut().context.kv() {
							Ok(kv) => match kv.entries(start, end).await {
								Ok(entries) => surrealdb_types::encode_string_key_values(&entries)
									.map_err(|e| e.to_string()),
								Err(e) => Err(e.to_string()),
							},
							Err(e) => Err(e.to_string()),
						},
						Err(e) => Err(e),
					};
					Ok((result,))
				})
			},
		)
		.prefix_err(|| "failed to register P2 kv-entries")?;

	// kv-count(range: list<u8>) -> result<u64, string>
	host_instance
		.func_wrap_async(
			"kv-count",
			|mut store: StoreContextMut<'_, P2StoreData>, (range_bytes,): (Vec<u8>,)| {
				Box::new(async move {
					let result: Result<u64, String> = match p2_decode_string_range(&range_bytes) {
						Ok((start, end)) => match store.data_mut().context.kv() {
							Ok(kv) => kv.count(start, end).await.map_err(|e| e.to_string()),
							Err(e) => Err(e.to_string()),
						},
						Err(e) => Err(e),
					};
					Ok((result,))
				})
			},
		)
		.prefix_err(|| "failed to register P2 kv-count")?;

	Ok(())
}
