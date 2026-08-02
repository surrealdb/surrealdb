//! # Surrealdb rpc
//!
//! Crate implementing the pure RPC wire data types shared across the SurrealDB codebase.
//!
//! <section class="warning">
//! <h3>Unstable!</h3>
//! This crate is <b>SurrealDB internal API</b>. It does not adhere to SemVer and its API is
//! free to change and break code even between patch versions. If you are looking for a stable
//! interface to the SurrealDB library please have a look at
//! <a href="https://crates.io/crates/surrealdb">the Rust SDK</a>.
//! </section>

pub mod args;
pub mod capabilities;
pub mod error;
pub mod export;
pub mod format;
pub mod method;
pub mod query;
pub mod request;
pub mod response;
pub mod token;

pub use method::Method;
pub use query::{QueryResult, QueryResultBuilder, QueryType, Status};
pub use request::{Request, check_protected_param};
pub use response::{DbResponse, DbResult, DbResultStats, db_response_from_bytes};
pub use token::Token;
