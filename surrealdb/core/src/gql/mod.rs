#![cfg(not(target_family = "wasm"))]

mod auth;
pub mod cache;
pub mod error;
mod ext;
mod functions;
mod mutations;
mod relations;
pub mod schema;
mod tables;
mod utils;

pub use cache::*;
pub use error::GqlError;
