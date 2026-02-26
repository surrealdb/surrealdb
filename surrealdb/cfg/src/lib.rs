#[cfg(feature = "cli")]
mod parsers;

#[cfg(any(feature = "kv-rocksdb", feature = "kv-surrealkv"))]
mod system;

mod core;
#[cfg(feature = "kv-rocksdb")]
mod rocksdb;
#[cfg(feature = "kv-surrealkv")]
mod surrealkv;
#[cfg(feature = "kv-tikv")]
mod tikv;

pub use crate::core::*;
#[cfg(feature = "kv-rocksdb")]
pub use crate::rocksdb::RocksDbEngineConfig;
#[cfg(feature = "kv-surrealkv")]
pub use crate::surrealkv::SurrealKvEngineConfig;
#[cfg(feature = "kv-tikv")]
pub use crate::tikv::TiKvEngineConfig;

// ---------------------------------------------------------------------------
// CoreConfig – the top-level config struct
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default)]
#[cfg_attr(feature = "cli", derive(clap::Args))]
pub struct CoreConfig {
	#[cfg_attr(feature = "cli", command(flatten))]
	pub limits: LimitsConfig,
	#[cfg_attr(feature = "cli", command(flatten))]
	pub scripting: ScriptingConfig,
	#[cfg_attr(feature = "cli", command(flatten))]
	pub http_client: HttpClientConfig,
	#[cfg_attr(feature = "cli", command(flatten))]
	pub caches: CacheConfig,
	#[cfg_attr(feature = "cli", command(flatten))]
	pub batching: BatchConfig,
	#[cfg_attr(feature = "cli", command(flatten))]
	pub security: SecurityConfig,
	#[cfg_attr(feature = "cli", command(flatten))]
	pub files: FileConfig,
	#[cfg(feature = "kv-rocksdb")]
	#[cfg_attr(feature = "cli", command(flatten))]
	pub rocksdb_engine: RocksDbEngineConfig,
	#[cfg(feature = "kv-surrealkv")]
	#[cfg_attr(feature = "cli", command(flatten))]
	pub surrealkv_engine: SurrealKvEngineConfig,
	#[cfg(feature = "kv-tikv")]
	#[cfg_attr(feature = "cli", command(flatten))]
	pub tikv_engine: TiKvEngineConfig,
}
