//! [`BackendProvider`] implementations for the first-party storage engines,
//! one per enabled `kv-*` cargo feature.
#![cfg_attr(
	not(any(feature = "kv-mem", feature = "kv-surrealkv",)),
	allow(unused_imports, dead_code)
)]

use surrealdb_kvs::TransactionBuilder;
use surrealdb_kvs::api::BoxFut;
use surrealdb_kvs::err::Result;
use tracing::info;
#[cfg(feature = "kv-surrealkv")]
use tracing::warn;

use crate::provider::{BackendProvider, ConnectContext};
use crate::registry::Backends;

const TARGET: &str = "surrealdb::core::kvs::ds";

impl Backends<'static> {
	/// A registry holding every first-party storage engine enabled by a
	/// `kv-*` cargo feature.
	pub fn community() -> Self {
		#[allow(unused_mut)]
		let mut backends = Self::empty();
		#[cfg(feature = "kv-mem")]
		backends.register(MemProvider);
		#[cfg(feature = "kv-surrealkv")]
		backends.register(SurrealKvProvider);
		backends
	}
}

/// Provider for the in-memory (SurrealMX) storage engine.
#[cfg(feature = "kv-mem")]
struct MemProvider;

#[cfg(feature = "kv-mem")]
impl BackendProvider for MemProvider {
	fn schemes(&self) -> &[&'static str] {
		&["memory", "mem"]
	}

	fn accepts_bare(&self) -> bool {
		true
	}

	fn connect<'a>(
		&'a self,
		ctx: ConnectContext<'a>,
	) -> BoxFut<'a, Result<Box<dyn TransactionBuilder>>> {
		Box::pin(async move {
			// Persist path comes from the URL path; do not inject an empty
			// string or `parse_key_with` logs a spurious DATASTORE_PERSIST warning.
			let config = if ctx.path.is_empty() {
				ctx.config
			} else {
				ctx.config.with_key_value("datastore_persist", ctx.path)
			};
			// Parse SurrealMX configuration from URL path and query parameters
			let config = config.load();
			// Initialise the storage engine
			let v = crate::mem::Datastore::new(config).await?;
			info!(target: TARGET, "Started kvs store in memory");
			Ok(Box::new(v) as Box<dyn TransactionBuilder>)
		})
	}
}

/// Provider for the SurrealKV storage engine.
///
/// The `rocksdb` and `indxdb` schemes of earlier storage engines are claimed
/// too and open the same path with SurrealKV, which migrates what an earlier
/// engine left there; they are deprecated spellings of `surrealkv`.
#[cfg(feature = "kv-surrealkv")]
struct SurrealKvProvider;

#[cfg(feature = "kv-surrealkv")]
impl BackendProvider for SurrealKvProvider {
	fn schemes(&self) -> &[&'static str] {
		&["surrealkv", "rocksdb", "indxdb"]
	}

	fn connect<'a>(
		&'a self,
		ctx: ConnectContext<'a>,
	) -> BoxFut<'a, Result<Box<dyn TransactionBuilder>>> {
		Box::pin(async move {
			if matches!(ctx.scheme, "rocksdb" | "indxdb") {
				warn!(
					target: TARGET,
					"The `{}` scheme is deprecated: the path is opened with SurrealKV, which \
					 migrates any existing data automatically. Use `surrealkv://` instead",
					ctx.scheme
				);
			}
			// Parse SurrealKV-specific configuration from query parameters
			let config = ctx.config.load();
			// Initialise the storage engine
			let v = crate::surrealkv::Datastore::new(ctx.path, config).await?;
			info!(target: TARGET, "Started surrealkv kvs store");
			Ok(Box::new(v) as Box<dyn TransactionBuilder>)
		})
	}
}
