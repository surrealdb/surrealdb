//! Guest SDK for Surrealism WASM plugins.
//!
//! Compiled into modules that run inside SurrealDB. Provides WIT bindings, the
//! [`Transaction`] capability handle through which a plugin reaches the host,
//! and the `#[surrealism]` macro for registering exported functions via
//! inventory.
//!
//! There is no async runtime here. A plugin's `async fn` is driven by the
//! component model itself: `invoke` is lifted async, so the host re-enters the
//! guest as each pending host call resolves. Two `tx` calls awaited together
//! genuinely overlap.
//!
//! A plugin may still build its own runtime for a dependency that demands one,
//! but must never await a `tx` method inside a nested `block_on`: a `tx` call
//! is a component-model subtask that can only complete once the guest yields
//! back to the host, which a blocking executor never does.

#[allow(clippy::all, unused)]
pub mod bindings;
mod dispatch;
pub mod registry;
pub mod tx;

pub use inventory;
pub use registry::{SurrealismEntry, SurrealismInit};
pub use surrealism_macros::surrealism;
pub use surrealism_types as types;
pub use tx::{ReadOnly, ReadWrite, Transaction, TxMode};

inventory::collect!(SurrealismEntry);
inventory::collect!(SurrealismInit);
