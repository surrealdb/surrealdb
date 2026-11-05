//! # Surrealdb Core
//!
//! This crate is the internal core library of SurrealDB. It contains most of the database
//! functionality on top of which the surreal binary is implemented.
//!
//! <section class="warning">
//! <h3>Unstable!</h3>
//! This crate is <b>SurrealDB internal API</b>. It does not adhere to SemVer and its API is
//! free to change and break code even between patch versions. If you are looking for a stable
//! interface to the SurrealDB library please have a look at
//! <a href="https://crates.io/crates/surrealdb">the Rust SDK</a>.
//! </section>

// This triggers because we have regex's in our Value type which have a unsafecell inside.
#![allow(clippy::mutable_key_type)]
#![deny(clippy::wildcard_enum_match_arm)]
// Increased to support #[instrument] on complex async functions. Those are compiled out in release
// builds.
#![recursion_limit = "256"]
#![doc(html_favicon_url = "https://surrealdb.s3.amazonaws.com/favicon.png")]
#![doc(html_logo_url = "https://surrealdb.s3.amazonaws.com/icon.png")]
// TODO: Remove once this crate no longer needs it; see `absolute_paths` in the workspace lints.
#![allow(clippy::absolute_paths)]
// The unit tests that reach the test-only helpers run on the in-memory
// datastore. A test build without `kv-mem`, as each storage engine's job makes,
// compiles those tests out and leaves the helpers unreached.
#![cfg_attr(all(test, not(feature = "kv-mem")), allow(dead_code))]

#[macro_use]
extern crate surrealdb_collections;
#[macro_use]
extern crate tracing;

// `fail!` lives in `surrealdb-common` so every layer of the engine reports a
// broken invariant the same way. Imported crate-wide rather than per file
// because it is used from nested modules, which do not inherit a file-level
// `use`. Verified collision-free against `mac` below: common exports `fail`,
// `lazy_env_parse` and `id`; `mac` defines none of those.
#[macro_use]
extern crate common;

#[macro_use]
mod mac;
#[cfg(all(test, feature = "kv-mem"))]
mod corpus_slices;
#[cfg(test)]
mod sql_test;
#[cfg(test)]
mod sql_to_sql_test;

#[doc(hidden)]
pub mod buc;
mod cf;
mod ddl;
#[doc(hidden)]
pub mod doc;
mod exe;
pub mod extension;
#[cfg(test)]
mod fmt_roundtrip_test;
mod fnc;
pub(crate) mod key;
pub mod lq;
#[cfg(feature = "surrealism")]
mod surrealism;

mod sys;

pub mod api;
pub(crate) use surrealdb_catalog as catalog;
/// The SurrealQL abstract syntax tree, as produced by [`syn`].
mod config;
pub mod ctx;
pub mod dbs;
pub mod env;
pub mod err;
pub mod exec;
/// The phase names a compile reports under the `compile-timers` feature.
#[cfg(feature = "compile-timers")]
pub use exec::ir::timers::COMPILE_PHASES;
pub(crate) use surrealdb_expr::expr;
#[cfg(feature = "gql")]
pub mod gql;
#[cfg(feature = "graphql")]
pub mod graphql;
#[cfg(feature = "mysql")]
pub mod mysql;
#[cfg(feature = "postgres")]
pub mod pgsql;
// Gated on either feature: `jwks` builds need `http::config` for their fetch
// client even when the `http::*` SurrealQL functions are compiled out. The
// client itself is `http`-gated inside the module.
#[cfg(any(feature = "http", feature = "jwks"))]
mod http;
pub mod iam;
pub mod idx;
pub mod kvs;
pub mod mem;
// Capability-aware networking helpers shared by the outbound HTTP clients
// (`http` feature) and the JWKS fetch client (`jwks` feature). Not available on
// WASM, where the clients are built without a custom DNS resolver.
#[cfg(all(not(target_family = "wasm"), any(feature = "http", feature = "jwks")))]
mod net;
/// `wasm32-unknown-unknown` has no synchronous DNS resolver: `ToSocketAddrs`
/// is unsupported there and fails with "operation not supported on this
/// platform". The hostname allow/deny check in
/// [`Context::check_allowed_net`](crate::ctx::Context) has already run before
/// resolution; the IP-level pass only exists to catch a host that resolves to
/// a denied address, and the sole wasm deployment target (a Cloudflare
/// Worker) cannot reach loopback/link-local/private ranges — the runtime
/// enforces that. So this stub skips IP resolution rather than erroring.
#[cfg(all(target_family = "wasm", feature = "http"))]
mod net {
	use crate::dbs::capabilities::NetTarget;

	pub(crate) fn resolve_net_target(
		_target: &NetTarget,
	) -> Result<Vec<NetTarget>, std::io::Error> {
		Ok(Vec::new())
	}
}
pub mod obs;
pub mod observe;
pub mod options;
pub mod rpc;
pub mod syn;
pub(crate) use surrealdb_expr::val;

pub(crate) mod types {
	//! Re-export the types from the types crate for internal use prefixed with Public.

	pub use surrealdb_types::{
		Action as PublicAction, Array as PublicArray, Bytes as PublicBytes,
		Datetime as PublicDatetime, Duration as PublicDuration, Notification as PublicNotification,
		Number as PublicNumber, Object as PublicObject, RecordId as PublicRecordId,
		RecordIdKey as PublicRecordIdKey, SurrealValue, Uuid as PublicUuid, Value as PublicValue,
		Variables as PublicVariables,
	};
}

/// Named by the exported `mrg!` macro as `$crate::VecMap`, so it has to be
/// reachable wherever that macro expands. Not public API.
#[doc(hidden)]
pub use surrealdb_collections::VecMap;

/// Channels for receiving a SurrealQL database export
pub mod channel {
	pub use async_channel::{Receiver, Sender, bounded, unbounded};
}

/// Composer for the community edition of SurrealDB.
///
/// This struct implements the composer pattern for dependency injection, providing
/// default implementations of the traits required to initialize and run SurrealDB.
///
/// # Implemented Traits
/// - `TransactionBuilderFactory` - Selects and validates the datastore backend
/// - `RouterFactory` - Constructs the HTTP router with standard routes
/// - `ConfigCheck` - Validates configuration before initialization
///
/// # Usage
/// This is the default composer used by the `surreal` binary. Embedders can create
/// their own composer structs implementing these traits to customize behavior.
///
/// # Example
/// ```ignore
/// use surrealdb_core::CommunityComposer;
///
/// // Pass the composer to init functions
/// surreal::init(CommunityComposer())
/// ```
#[derive(Default)]
pub struct CommunityComposer();
