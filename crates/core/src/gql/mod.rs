#![cfg(all(not(target_arch = "wasm32"), surrealdb_unstable))]

pub mod cache;
pub mod error;
mod ext;
mod functions;
pub mod schema;
mod tables;
mod utils;

pub use error::GqlError;

pub use cache::*;
