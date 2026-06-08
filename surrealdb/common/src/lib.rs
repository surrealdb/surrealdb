//! # Surrealdb common
//!
//! Crate implementing common utlities used through out the surrealdb codebase.
//!
//! <section class="warning">
//! <h3>Unstable!</h3>
//! This crate is <b>SurrealDB internal API</b>. It does not adhere to SemVer and its API is
//! free to change and break code even between patch versions. If you are looking for a stable
//! interface to the SurrealDB library please have a look at
//! <a href="https://crates.io/crates/surrealdb">the Rust SDK</a>.
//! </section>

#[macro_use]
pub mod ids;

mod error;

pub use error::{Error, ErrorCode, ErrorTrait, TypedError, source as source_error};

pub mod non_max;
pub mod span;
