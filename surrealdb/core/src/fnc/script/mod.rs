#![cfg(feature = "scripting")]
// rquickjs `#[js::methods]`, `#[qjs(constructor)]`, and `FromJs` glue require owned
// `Ctx`/`Value`/`String` parameters in user-written signatures, so this lint fires
// throughout the module by design.
#![allow(clippy::needless_pass_by_value)]

pub use main::run;
pub(crate) use surrealdb_expr::js::classes;
pub(crate) mod config;
mod error;
mod globals;
mod main;
mod modules;

#[cfg(feature = "http")]
mod fetch;
#[cfg(not(feature = "http"))]
mod fetch_stub;
#[cfg(not(feature = "http"))]
use self::fetch_stub as fetch;

#[cfg(test)]
mod tests;
