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
//! This covers host imports only. Guest network I/O is sync-lowered on WASI p2,
//! so a sync client blocks the task and a tokio-shaped crate needs its own
//! runtime.
//!
//! A plugin may run its own executor for a crate that demands one. Such an
//! export must not take a [`Transaction`], and should be declared `fn` rather
//! than `async fn`: a `tx` call completes only once the guest returns to the
//! host, which a blocking executor never does, so the await would hang.
//! `#[surrealism]` preserves your attributes, so
//! `#[tokio::main(flavor = "current_thread")]` composes with it when applied as
//! the outer attribute.

// TODO: Remove once this crate no longer needs it; see `absolute_paths` in the workspace lints.
#![allow(clippy::absolute_paths)]

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
