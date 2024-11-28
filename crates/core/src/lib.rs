//! # Surrealdb Core
//!
//! This crate is the internal core library of SurrealDB.
//! It contains most of the database functionality on top of which the surreal binary is
//! implemented.
//!
//! <section class="warning">
//! <h3>Unstable!</h3>
//! This crate is <b>SurrealDB internal API</b>. It does not adhere to semver and it's API is free to
//! change and break code even between patch versions. If you are looking for a stable interface
//! to the Surrealdb library please have a look at <a href="https://crates.io/crates/surrealdb">the rust SDK</a>
//! </section>
//!

#[macro_use]
extern crate tracing;

#[macro_use]
mod mac;

mod cf;
pub mod ctx;
mod doc;
mod exe;
mod fnc;
mod vs;

pub mod sql;

pub mod cnf;
pub mod dbs;
pub mod env;
pub mod err;
pub mod fflags;
#[cfg(all(not(target_arch = "wasm32"), surrealdb_unstable))]
pub mod gql;
pub mod iam;
pub mod idg;
pub mod idx;
pub mod key;
pub mod kvs;
pub mod options;
pub mod rpc;
pub mod syn;

#[cfg(feature = "ml")]
pub use surrealml as ml;
#[cfg(feature = "ml")]
pub mod obs;

pub mod test_helpers {
	pub use crate::vs::conv::to_u128_be;
	pub use crate::vs::generate_versionstamp_sequences;
}

/// Channels for receiving a SurrealQL database export
pub mod channel {
	pub use async_channel::bounded;
	pub use async_channel::unbounded;
	pub use async_channel::Receiver;
	pub use async_channel::Sender;
}
