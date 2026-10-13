//! [`BackendProvider`] implementations for the first-party storage engines,
//! one per enabled `kv-*` cargo feature.
#![cfg_attr(
	not(any(
		feature = "kv-mem",
		feature = "kv-rocksdb",
		feature = "kv-indxdb",
		feature = "kv-tikv",
		feature = "kv-surrealkv",
	)),
	allow(unused_imports, dead_code)
)]

use surrealdb_kvs::TransactionBuilder;
use surrealdb_kvs::api::BoxFut;
use surrealdb_kvs::err::Result;
use tracing::info;

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
		#[cfg(feature = "kv-rocksdb")]
		backends.register(RocksDbProvider);
		#[cfg(feature = "kv-surrealkv")]
		backends.register(SurrealKvProvider);
		#[cfg(feature = "kv-indxdb")]
		backends.register(IndxDbProvider);
		#[cfg(feature = "kv-tikv")]
		backends.register(TikvProvider);
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

/// Provider for the RocksDB storage engine.
#[cfg(feature = "kv-rocksdb")]
struct RocksDbProvider;

#[cfg(feature = "kv-rocksdb")]
impl BackendProvider for RocksDbProvider {
	fn schemes(&self) -> &[&'static str] {
		&["rocksdb"]
	}

	fn connect<'a>(
		&'a self,
		ctx: ConnectContext<'a>,
	) -> BoxFut<'a, Result<Box<dyn TransactionBuilder>>> {
		Box::pin(async move {
			// Parse RocksDB-specific configuration from query parameters
			let config = ctx.config.load();
			// Initialise the storage engine
			let v = crate::rocksdb::Datastore::new(ctx.path, config).await?;
			info!(target: TARGET, "Started rocksdb kvs store");
			Ok(Box::new(v) as Box<dyn TransactionBuilder>)
		})
	}
}

/// Provider for the SurrealKV storage engine.
#[cfg(feature = "kv-surrealkv")]
struct SurrealKvProvider;

#[cfg(feature = "kv-surrealkv")]
impl BackendProvider for SurrealKvProvider {
	fn schemes(&self) -> &[&'static str] {
		&["surrealkv"]
	}

	fn connect<'a>(
		&'a self,
		ctx: ConnectContext<'a>,
	) -> BoxFut<'a, Result<Box<dyn TransactionBuilder>>> {
		Box::pin(async move {
			// Parse SurrealKV-specific configuration from query parameters
			let config = ctx.config.load();
			// Initialise the storage engine
			let v = crate::surrealkv::Datastore::new(ctx.path, config).await?;
			info!(target: TARGET, "Started surrealkv kvs store");
			Ok(Box::new(v) as Box<dyn TransactionBuilder>)
		})
	}
}

/// Provider for the IndexedDB (browser) storage engine.
#[cfg(feature = "kv-indxdb")]
struct IndxDbProvider;

#[cfg(feature = "kv-indxdb")]
impl BackendProvider for IndxDbProvider {
	fn schemes(&self) -> &[&'static str] {
		&["indxdb"]
	}

	fn connect<'a>(
		&'a self,
		ctx: ConnectContext<'a>,
	) -> BoxFut<'a, Result<Box<dyn TransactionBuilder>>> {
		Box::pin(async move {
			let v = crate::indxdb::Datastore::new(ctx.path).await?;
			info!(target: TARGET, "Started indxdb kvs store");
			Ok(Box::new(v) as Box<dyn TransactionBuilder>)
		})
	}
}

/// Provider for the TiKV storage engine.
#[cfg(feature = "kv-tikv")]
struct TikvProvider;

#[cfg(feature = "kv-tikv")]
impl BackendProvider for TikvProvider {
	fn schemes(&self) -> &[&'static str] {
		&["tikv"]
	}

	fn connect<'a>(
		&'a self,
		ctx: ConnectContext<'a>,
	) -> BoxFut<'a, Result<Box<dyn TransactionBuilder>>> {
		Box::pin(async move {
			// Parse TiKV-specific configuration from env vars
			// (SURREAL_TIKV_*) and query parameters.
			let tikv_config = ctx.config.load();
			let v = crate::tikv::Datastore::new(ctx.path, tikv_config).await?;
			info!(target: TARGET, "Started tikv kvs store");
			Ok(Box::new(v) as Box<dyn TransactionBuilder>)
		})
	}
}
