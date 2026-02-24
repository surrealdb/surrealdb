//! # Surrealdb parser
//!
//! This crate is the internal library of SurrealDB. It contains a implemention of the surrealql
//! parser
//!
//! <section class="warning">
//! <h3>Unstable!</h3>
//! This crate is <b>SurrealDB internal API</b>. It does not adhere to SemVer and its API is
//! free to change and break code even between patch versions. If you are looking for a stable
//! interface to the SurrealDB library please have a look at
//! <a href="https://crates.io/crates/surrealdb">the Rust SDK</a>.
//! </section>

#![allow(dead_code)]

pub mod parse;
mod test;
pub use parse::{Config, Parse, ParseSync, Parser};
pub mod peekable;
