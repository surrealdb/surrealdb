//! # SurrealDB KVS — any backend
//!
//! Facade over every SurrealDB key-value store backend: selects and
//! constructs the right datastore from a connection path string (e.g.
//! `memory`, `rocksdb://path`, `tikv://...`), exposing it behind the
//! [`TransactionBuilder`] abstraction. Each backend is gated behind a
//! matching `kv-*` cargo feature.
//!
//! <section class="warning">
//! <h3>Unstable!</h3>
//! This crate is <b>SurrealDB internal API</b>. It does not adhere to SemVer and its API is
//! free to change and break code even between patch versions. If you are looking for a stable
//! interface to the SurrealDB library please have a look at
//! <a href="https://crates.io/crates/surrealdb">the Rust SDK</a>.
//! </section>

use std::any::{Any, TypeId};
use std::fmt::{self, Display};
use std::sync::Arc;

use common::config::ConfigMap;
use surrealdb_kvs::api::{BoxFut, Transactable};
use surrealdb_kvs::builder::requirements::TransactionBuilderRequirements;
use surrealdb_kvs::err::{Error, Result};
use surrealdb_kvs::{Metrics, TransactionBuilder, TransactionType};
#[cfg(feature = "kv-indxdb")]
pub use surrealdb_kvs_indxdb as indxdb;
#[cfg(feature = "kv-mem")]
pub use surrealdb_kvs_mem as mem;
#[cfg(feature = "kv-rocksdb")]
pub use surrealdb_kvs_rocksdb as rocksdb;
#[cfg(feature = "kv-surrealkv")]
pub use surrealdb_kvs_surrealkv as surrealkv;
#[cfg(feature = "kv-tikv")]
pub use surrealdb_kvs_tikv as tikv;
use tokio_util::sync::CancellationToken;
use tracing::info;

const TARGET: &str = "surrealdb::core::kvs::ds";

/// The concrete storage backend a datastore runs on top of, one variant per
/// enabled `kv-*` feature.
pub enum DatastoreFlavor {
	#[cfg(feature = "kv-mem")]
	Mem(mem::Datastore),
	#[cfg(feature = "kv-rocksdb")]
	RocksDB(rocksdb::Datastore),
	#[cfg(feature = "kv-indxdb")]
	IndxDB(indxdb::Datastore),
	#[cfg(feature = "kv-tikv")]
	TiKV(tikv::Datastore),
	#[cfg(feature = "kv-surrealkv")]
	SurrealKV(surrealkv::Datastore),
}

/// Parse a datastore connection path (e.g. `memory`, `rocksdb://path`,
/// `tikv://...`, optionally with `?key=value` configuration parameters) and
/// construct the matching storage backend.
///
/// The `canceller` parameter is currently unused, but kept so embedders can
/// thread a graceful-shutdown token through without a future signature
/// change.
#[allow(unused_variables)]
pub async fn new_transaction_builder(
	path: &str,
	canceller: CancellationToken,
	config: ConfigMap,
) -> Result<Box<dyn TransactionBuilder>> {
	// Extract query parameters from the path before scheme extraction
	let (raw_path, config_string) = match path.split_once('?') {
		Some((p, q)) => (p, Some(q)),
		None => (path, None),
	};

	let config = if let Some(config_string) = config_string {
		config.join(
			ConfigMap::from_config_string(config_string).map_keys(|x| format!("datastore_{x}")),
		)
	} else {
		config
	};

	// Extract the scheme and path components
	let (flavour, path) = match raw_path.split_once("://").or_else(|| raw_path.split_once(':')) {
		None if raw_path == "memory" => ("memory", ""),
		// Treat "mem" as an alias for "memory"
		None if raw_path == "mem" => ("memory", ""),
		Some(("mem", path)) => ("memory", path),
		Some((flavour, path)) => (flavour, path),
		// Validated already in the CLI, should never happen
		_ => return Err(Error::Internal("Provide a valid database path parameter".to_owned())),
	};

	let path = if path.starts_with("/") {
		// if absolute, remove all slashes except one
		let normalised = format!("/{}", path.trim_start_matches("/"));
		info!(target: TARGET, "Starting kvs store at absolute path {flavour}:{normalised}");
		normalised
	} else if path.is_empty() {
		info!(target: TARGET, "Starting kvs store in memory");
		"".to_string()
	} else {
		info!(target: TARGET, "Starting kvs store at relative path {flavour}://{path}");
		path.to_string()
	};
	// Initiate the desired datastore
	match (flavour, path) {
		// Initiate an in-memory datastore
		(flavour @ "memory", path) => {
			#[cfg(feature = "kv-mem")]
			{
				// Persist path comes from the URL path; do not inject an empty
				// string or `parse_key_with` logs a spurious DATASTORE_PERSIST warning.
				let config = if path.is_empty() {
					config
				} else {
					config.with_key_value("datastore_persist", path)
				};
				// Parse SurrealMX configuration from URL path and query parameters
				let config = config.load();
				// Initialise the storage engine
				let v = mem::Datastore::new(config).await.map(DatastoreFlavor::Mem)?;
				info!(target: TARGET, "Started kvs store in {flavour}");
				Ok(Box::new(v) as Box<dyn TransactionBuilder>)
			}
			#[cfg(not(feature = "kv-mem"))]
			Err(Error::Datastore("Cannot connect to the `memory` storage engine as it is not enabled in this build of SurrealDB".to_owned()))
		}
		// The `file:` scheme has been removed. Catch it here so users
		// with legacy paths get a targeted message instead of the
		// generic fallback below.
		("file", _) => Err(Error::Datastore(
			"The `file://` scheme is no longer supported; use `rocksdb://` or `surrealkv://` instead"
				.into(),
		)),
		// Initiate a RocksDB datastore
		(flavour @ "rocksdb", path) => {
			#[cfg(feature = "kv-rocksdb")]
			{
				// Parse RocksDB-specific configuration from query parameters
				let config = config.load();
				// Initialise the storage engine
				let v = rocksdb::Datastore::new(&path, config).await.map(DatastoreFlavor::RocksDB)?;
				info!(target: TARGET, "Started {flavour} kvs store");
				Ok(Box::new(v) as Box<dyn TransactionBuilder>)
			}
			#[cfg(not(feature = "kv-rocksdb"))]
			Err(Error::Datastore("Cannot connect to the `rocksdb` storage engine as it is not enabled in this build of SurrealDB".to_owned()))
		}
		// Initiate a SurrealKV database
		(flavour @ "surrealkv", path) => {
			#[cfg(feature = "kv-surrealkv")]
			{
				// Parse SurrealKV-specific configuration from query parameters
				let config = config.load();
				// Initialise the storage engine
				let v =
					surrealkv::Datastore::new(&path, config).await.map(DatastoreFlavor::SurrealKV)?;
				info!(target: TARGET, "Started {flavour} kvs store");
				Ok(Box::new(v) as Box<dyn TransactionBuilder>)
			}
			#[cfg(not(feature = "kv-surrealkv"))]
			Err(Error::Datastore("Cannot connect to the `surrealkv` storage engine as it is not enabled in this build of SurrealDB".to_owned()))
		}
		// Initiate an IndxDB database
		(flavour @ "indxdb", path) => {
			#[cfg(feature = "kv-indxdb")]
			{
				let v = indxdb::Datastore::new(&path).await.map(DatastoreFlavor::IndxDB)?;
				info!(target: TARGET, "Started {flavour} kvs store");
				Ok(Box::new(v) as Box<dyn TransactionBuilder>)
			}
			#[cfg(not(feature = "kv-indxdb"))]
			Err(Error::Datastore("Cannot connect to the `indxdb` storage engine as it is not enabled in this build of SurrealDB".to_owned()))
		}
		// Initiate a TiKV datastore
		(flavour @ "tikv", path) => {
			#[cfg(feature = "kv-tikv")]
			{
				// Parse TiKV-specific configuration from env vars
				// (SURREAL_TIKV_*) and query parameters.
				let tikv_config = config.load();
				let v = tikv::Datastore::new(&path, tikv_config).await.map(DatastoreFlavor::TiKV)?;
				info!(target: TARGET, "Started {flavour} kvs store");
				Ok(Box::new(v) as Box<dyn TransactionBuilder>)
			}
			#[cfg(not(feature = "kv-tikv"))]
			Err(Error::Datastore("Cannot connect to the `tikv` storage engine as it is not enabled in this build of SurrealDB".to_owned()))
		}
		// The datastore path is not valid
		(flavour, path) => {
			info!(target: TARGET, "Unable to load the specified datastore {flavour}{path}");
			Err(Error::Datastore("Unable to load the specified datastore".into()))
		}
	}
}

/// Validate a datastore connection path string against the known schemes.
pub fn path_valid(v: &str) -> Result<String> {
	// Strip query parameters before validating the scheme
	let scheme_part = v.split_once('?').map(|(s, _)| s).unwrap_or(v);
	match scheme_part {
		"memory" => Ok(v.to_string()),
		"mem" => Ok(v.to_string()),
		v_s if v_s.starts_with("file:") => Ok(v.to_string()),
		v_s if v_s.starts_with("rocksdb:") => Ok(v.to_string()),
		v_s if v_s.starts_with("surrealkv:") => Ok(v.to_string()),
		v_s if v_s.starts_with("mem:") => Ok(v.to_string()),
		v_s if v_s.starts_with("tikv:") => Ok(v.to_string()),
		_ => Err(Error::Datastore("Provide a valid database path parameter".to_owned())),
	}
}

impl TransactionBuilderRequirements for DatastoreFlavor {}

impl TransactionBuilder for DatastoreFlavor {
	#[allow(
		unreachable_code,
		unreachable_patterns,
		unused_variables,
		reason = "Some variables are unused when no backends are enabled."
	)]
	fn new_transaction(
		&self,
		write: TransactionType,
	) -> BoxFut<'_, Result<(Box<dyn Transactable>, bool)>> {
		Box::pin(async move {
			Ok(match self {
				#[cfg(feature = "kv-mem")]
				Self::Mem(v) => {
					let tx = v.transaction(write).await?;
					(tx, true)
				}
				#[cfg(feature = "kv-rocksdb")]
				Self::RocksDB(v) => {
					let tx = v.transaction(write).await?;
					(tx, true)
				}
				#[cfg(feature = "kv-indxdb")]
				Self::IndxDB(v) => {
					let tx = v.transaction(write).await?;
					(tx, true)
				}
				#[cfg(feature = "kv-tikv")]
				Self::TiKV(v) => {
					let tx = v.transaction(write).await?;
					(tx, false)
				}
				#[cfg(feature = "kv-surrealkv")]
				Self::SurrealKV(v) => {
					let tx = v.transaction(write).await?;
					(tx, true)
				}
				_ => unreachable!(),
			})
		})
	}

	/// Registers metrics for the current datastore flavor if supported.
	fn register_metrics(&self) -> Option<Metrics> {
		match self {
			#[cfg(feature = "kv-rocksdb")]
			DatastoreFlavor::RocksDB(v) => Some(v.register_metrics()),
			#[allow(unreachable_patterns)]
			_ => None,
		}
	}

	/// Collects a specific u64 metric by name if supported by the datastore flavor.
	// Allow unused variable when kv-rocksdb feature is not enabled
	#[allow(unused_variables)]
	fn collect_u64_metric(&self, metric: &str) -> Option<u64> {
		match self {
			#[cfg(feature = "kv-rocksdb")]
			DatastoreFlavor::RocksDB(v) => v.collect_u64_metric(metric),
			#[allow(unreachable_patterns)]
			_ => None,
		}
	}

	fn shutdown(&self) -> BoxFut<'_, Result<()>> {
		Box::pin(async move {
			match self {
				#[cfg(feature = "kv-mem")]
				Self::Mem(v) => v.shutdown().await,
				#[cfg(feature = "kv-rocksdb")]
				Self::RocksDB(v) => v.shutdown().await,
				#[cfg(feature = "kv-indxdb")]
				Self::IndxDB(v) => v.shutdown().await,
				#[cfg(feature = "kv-tikv")]
				Self::TiKV(v) => v.shutdown().await,
				#[cfg(feature = "kv-surrealkv")]
				Self::SurrealKV(v) => v.shutdown().await,
				#[allow(unreachable_patterns)]
				_ => unreachable!(),
			}
		})
	}

	#[allow(
		unused_variables,
		reason = "type_id is only consumed when a backend feature is enabled"
	)]
	fn extension(&self, type_id: TypeId) -> Option<Arc<dyn Any + Send + Sync>> {
		match self {
			#[cfg(feature = "kv-tikv")]
			Self::TiKV(v) if type_id == TypeId::of::<tikv::TikvOpsHandle>() => Some(v.ops_handle()),
			#[allow(unreachable_patterns)]
			_ => None,
		}
	}
}

impl Display for DatastoreFlavor {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		#![allow(unused_variables)]
		match self {
			#[cfg(feature = "kv-mem")]
			Self::Mem(_) => write!(f, "memory"),
			#[cfg(feature = "kv-rocksdb")]
			Self::RocksDB(_) => write!(f, "rocksdb"),
			#[cfg(feature = "kv-indxdb")]
			Self::IndxDB(_) => write!(f, "indxdb"),
			#[cfg(feature = "kv-tikv")]
			Self::TiKV(_) => write!(f, "tikv"),
			#[cfg(feature = "kv-surrealkv")]
			Self::SurrealKV(_) => write!(f, "surrealkv"),
			#[allow(unreachable_patterns)]
			_ => unreachable!(),
		}
	}
}
