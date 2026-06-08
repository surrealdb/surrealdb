//! Guest SDK for Surrealism WASM plugins.
//!
//! Compiled into modules that run inside SurrealDB. Provides WIT bindings,
//! host imports (`sql`, `run`, `kv`), and the `#[surrealism]` macro for
//! registering exported functions via inventory.

#[allow(clippy::all, unused)]
pub mod bindings;
mod dispatch;
pub mod imports;
pub mod registry;

pub use imports::{kv, run, sql, sql_with_vars};
pub use inventory;
pub use registry::{SurrealismEntry, SurrealismInit};
pub use surrealism_macros::surrealism;
pub use surrealism_types as types;
#[doc(hidden)]
pub use tokio;

#[doc(hidden)]
pub fn async_runtime() -> &'static tokio::runtime::Runtime {
	static RT: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
	RT.get_or_init(|| {
		tokio::runtime::Builder::new_current_thread()
			.enable_io()
			.enable_time()
			.build()
			.expect("failed to build async runtime")
	})
}

inventory::collect!(SurrealismEntry);
inventory::collect!(SurrealismInit);
