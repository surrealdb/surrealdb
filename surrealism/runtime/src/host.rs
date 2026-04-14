//! InvocationContext trait and WIT host implementation.
//!
//! Implementations supply `sql`/`run`/`kv`/stdio per call. Host functions
//! decode FlatBuffers, call the context, and encode results.

use std::sync::Arc;

use anyhow::{Result, bail};
use async_trait::async_trait;
use surrealism_types::err::{PrefixErr, SurrealismResult};
use wasmtime::StoreContextMut;

use crate::config::SurrealismConfig;
use crate::kv::KVStore;
use crate::store::StoreData;

// ============================================================================
// InvocationContext trait
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

	/// Returns a self-contained callback for forwarding WASI stdout output.
	///
	/// This is used by the WASI output stream to route guest `println!` / C `printf`
	/// output through the same path as the WIT `stdout` import. Override this to
	/// capture structured context (module name, namespace, database, etc.) inside
	/// the closure so the callback can be invoked independently of `&mut self`.
	///
	/// The returned `Arc` is cheap to clone and allows the WASI stream to
	/// snapshot the callback without holding a lock during invocation.
	fn stdout_callback(&self) -> Arc<dyn Fn(&str) + Send + Sync> {
		Arc::new(|output| print!("{}", output))
	}

	/// Same as [`stdout_callback`](Self::stdout_callback) but for stderr.
	fn stderr_callback(&self) -> Arc<dyn Fn(&str) + Send + Sync> {
		Arc::new(|output| eprint!("{}", output))
	}
}

// ============================================================================
// NullContext — placeholder for pooled controllers with no active invocation
// ============================================================================

pub(crate) struct NullContext;

#[async_trait]
impl InvocationContext for NullContext {
	async fn sql(
		&mut self,
		_config: &SurrealismConfig,
		_query: String,
		_vars: surrealdb_types::Object,
	) -> Result<surrealdb_types::Value> {
		bail!("no active invocation context")
	}

	async fn run(
		&mut self,
		_config: &SurrealismConfig,
		_fnc: String,
		_version: Option<String>,
		_args: Vec<surrealdb_types::Value>,
	) -> Result<surrealdb_types::Value> {
		bail!("no active invocation context")
	}

	fn kv(&mut self) -> Result<&dyn KVStore> {
		bail!("no active invocation context")
	}
}

// ============================================================================
// Helper
// ============================================================================

fn decode_range_bounds(
	bytes: &[u8],
) -> Result<(std::ops::Bound<String>, std::ops::Bound<String>), String> {
	surrealdb_types::decode_string_range(bytes).map_err(|e| e.to_string())
}

fn stringify<E: std::fmt::Display>(e: E) -> String {
	e.to_string()
}

// ============================================================================
// Component model host functions (FlatBuffers serialization)
// ============================================================================

/// Register a host function with the common `func_wrap_async` boilerplate.
///
/// All host functions follow the same pattern: receive args from the WASM
/// component, run an async body that returns `Result<T, String>`, and wrap
/// the result in `Ok((inner,))` for the component model.
macro_rules! register_host_fn {
	($host:ident, $name:literal,
	 |$store:ident, ($($arg:ident : $ty:ty),* $(,)?)| -> Result<$ret:ty> $body:block
	) => {
		$host
			.func_wrap_async(
				$name,
				|mut $store: StoreContextMut<'_, StoreData>,
				 ($($arg,)*): ($($ty,)*)| {
					Box::new(async move {
						let inner: Result<$ret, String> = async $body.await;
						Ok((inner,))
					})
				},
			)
			.prefix_err(|| concat!("failed to register ", $name))?;
	};
}

pub fn implement_host_functions(
	linker: &mut wasmtime::component::Linker<StoreData>,
) -> SurrealismResult<()> {
	let mut root = linker.root();
	let mut host =
		root.instance("surrealism:plugin/host").prefix_err(|| "failed to define host instance")?;

	register_host_fn!(host, "sql",
		|store, (query: String, vars_bytes: Vec<u8>)| -> Result<Vec<u8>> {
			let vars_vec = surrealdb_types::decode_string_key_values(&vars_bytes)
				.map_err(stringify)?;
			let vars = surrealdb_types::Object::from_iter(vars_vec.into_iter());
			let config = store.data().config.clone();
			let val = store.data_mut().context.sql(&config, query, vars).await.map_err(stringify)?;
			surrealdb_types::encode(&val).map_err(stringify)
		}
	);

	register_host_fn!(host, "run",
		|store, (fnc: String, version: Option<String>, args_bytes: Vec<u8>)| -> Result<Vec<u8>> {
			let args = surrealdb_types::decode_value_list(&args_bytes).map_err(stringify)?;
			let config = store.data().config.clone();
			let val = store.data_mut().context.run(&config, fnc, version, args).await.map_err(stringify)?;
			surrealdb_types::encode(&val).map_err(stringify)
		}
	);

	register_host_fn!(host, "kv-get",
		|store, (key: String)| -> Result<Option<Vec<u8>>> {
			let kv = store.data_mut().context.kv().map_err(stringify)?;
			match kv.get(key).await.map_err(stringify)? {
				Some(v) => surrealdb_types::encode(&v).map(Some).map_err(stringify),
				None => Ok(None),
			}
		}
	);

	register_host_fn!(host, "kv-set",
		|store, (key: String, value_bytes: Vec<u8>)| -> Result<()> {
			let value: surrealdb_types::Value =
				surrealdb_types::decode(&value_bytes).map_err(stringify)?;
			let kv = store.data_mut().context.kv().map_err(stringify)?;
			kv.set(key, value).await.map_err(stringify)
		}
	);

	register_host_fn!(host, "kv-del",
		|store, (key: String)| -> Result<()> {
			let kv = store.data_mut().context.kv().map_err(stringify)?;
			kv.del(key).await.map_err(stringify)
		}
	);

	register_host_fn!(host, "kv-exists",
		|store, (key: String)| -> Result<bool> {
			let kv = store.data_mut().context.kv().map_err(stringify)?;
			kv.exists(key).await.map_err(stringify)
		}
	);

	register_host_fn!(host, "kv-del-rng",
		|store, (range_bytes: Vec<u8>)| -> Result<()> {
			let (start, end) = decode_range_bounds(&range_bytes)?;
			let kv = store.data_mut().context.kv().map_err(stringify)?;
			kv.del_rng(start, end).await.map_err(stringify)
		}
	);

	register_host_fn!(host, "kv-get-batch",
		|store, (keys: Vec<String>)| -> Result<Vec<u8>> {
			let kv = store.data_mut().context.kv().map_err(stringify)?;
			let vals = kv.get_batch(keys).await.map_err(stringify)?;
			surrealdb_types::encode_optional_values(&vals).map_err(stringify)
		}
	);

	register_host_fn!(host, "kv-set-batch",
		|store, (entries_bytes: Vec<u8>)| -> Result<()> {
			let entries = surrealdb_types::decode_string_key_values(&entries_bytes)
				.map_err(stringify)?;
			let kv = store.data_mut().context.kv().map_err(stringify)?;
			kv.set_batch(entries).await.map_err(stringify)
		}
	);

	register_host_fn!(host, "kv-del-batch",
		|store, (keys: Vec<String>)| -> Result<()> {
			let kv = store.data_mut().context.kv().map_err(stringify)?;
			kv.del_batch(keys).await.map_err(stringify)
		}
	);

	register_host_fn!(host, "kv-keys",
		|store, (range_bytes: Vec<u8>)| -> Result<Vec<String>> {
			let (start, end) = decode_range_bounds(&range_bytes)?;
			let kv = store.data_mut().context.kv().map_err(stringify)?;
			kv.keys(start, end).await.map_err(stringify)
		}
	);

	register_host_fn!(host, "kv-values",
		|store, (range_bytes: Vec<u8>)| -> Result<Vec<u8>> {
			let (start, end) = decode_range_bounds(&range_bytes)?;
			let kv = store.data_mut().context.kv().map_err(stringify)?;
			let vals = kv.values(start, end).await.map_err(stringify)?;
			surrealdb_types::encode_value_list(&vals).map_err(stringify)
		}
	);

	register_host_fn!(host, "kv-entries",
		|store, (range_bytes: Vec<u8>)| -> Result<Vec<u8>> {
			let (start, end) = decode_range_bounds(&range_bytes)?;
			let kv = store.data_mut().context.kv().map_err(stringify)?;
			let entries = kv.entries(start, end).await.map_err(stringify)?;
			surrealdb_types::encode_string_key_values(&entries).map_err(stringify)
		}
	);

	register_host_fn!(host, "kv-count",
		|store, (range_bytes: Vec<u8>)| -> Result<u64> {
			let (start, end) = decode_range_bounds(&range_bytes)?;
			let kv = store.data_mut().context.kv().map_err(stringify)?;
			kv.count(start, end).await.map_err(stringify)
		}
	);

	Ok(())
}
