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
pub use registry::{SurrealismEntry, SurrealismInit};
pub use surrealism_macros::surrealism;
pub use {inventory, surrealism_types as types};

inventory::collect!(SurrealismEntry);
inventory::collect!(SurrealismInit);
