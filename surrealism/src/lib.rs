// P1-only modules (core module ABI with manual memory management)
#[cfg(not(feature = "p2"))]
pub mod controller;
#[cfg(not(feature = "p2"))]
pub mod imports;
#[cfg(not(feature = "p2"))]
pub mod memory;

// P2-only modules (component model ABI — no manual memory management)
#[cfg(feature = "p2")]
#[allow(warnings)]
pub mod p2_bindings;
#[cfg(feature = "p2")]
pub mod p2_imports;

// Shared modules
pub mod err;
pub mod registry;

// P1 re-exports
#[cfg(not(feature = "p2"))]
pub use controller::Controller;
#[cfg(not(feature = "p2"))]
pub use imports::{kv, run, sql};
// P2 re-exports
#[cfg(feature = "p2")]
pub use p2_imports::{kv, run, sql};
pub use registry::SurrealismFunction;
pub use surrealism_macros::surrealism;
pub use surrealism_types as types;
