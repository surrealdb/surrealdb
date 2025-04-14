#![cfg(not(target_family = "wasm"))]

pub mod cache;
pub mod error;
mod ext;
mod functions;
pub mod schema;
mod tables;
mod utils;

pub use error::GqlError;

pub use cache::*;
