use crate::cnf::Config;

/// Per-datastore TiKV configuration, parsed from the workspace
/// [`crate::cnf::ConfigMap`] via [`Config::parse`].
///
/// Sources, in increasing precedence:
/// - Built-in defaults below.
/// - `SURREAL_TIKV_*` environment variables (collected by [`crate::cnf::ConfigMap::from_env`],
///   which lower-cases the suffix to produce keys like `tikv_gc_lifetime`).
/// - URL query parameters on the datastore path (prefixed with `datastore_` by the composer; not
///   used by any TiKV-specific keys today, so this is currently a no-op for TiKV).
///
/// Replaces the previous module of `lazy_env_parse!` statics so the
/// TiKV backend matches the rest of the workspace (mem, rocksdb, …):
/// each datastore instance carries its own typed config rather than
/// reading process-wide environment globals at first use.
#[derive(Clone, Debug)]
pub struct TikvConfig {
	/// Cluster API version to use.
	///
	/// Defaults to `1`. Set to `2` to opt into TiKV's V2 keyspace model.
	pub api_version: u8,

	/// Keyspace identifier for data isolation under cluster API V2.
	///
	/// Ignored when [`Self::api_version`] is `1`. When `None` under V2,
	/// the client connects to the default keyspace.
	pub keyspace: Option<String>,

	/// Request timeout in seconds.
	///
	/// Applied to every RPC against the PD and TiKV stores. Also bounds
	/// the startup health probe at `2 * request_timeout_secs`.
	pub request_timeout_secs: u64,

	/// Whether to use asynchronous transaction commit.
	pub async_commit: bool,

	/// Whether to try one-phase commit when a transaction writes to a
	/// single region.
	pub one_phase_commit: bool,

	/// gRPC response-side message size limit, in bytes. Default 4 MB.
	///
	/// Caps the response side of the channel: the largest single message
	/// the client will accept from the cluster. Applied via
	/// `Config::with_grpc_max_decoding_message_size`. Mirrors
	/// [`Self::grpc_max_encoding_message_size`] on the request side.
	pub grpc_max_decoding_message_size: usize,

	/// gRPC request-side message size limit, in bytes. Default 4 MB.
	///
	/// Caps the request side of the channel: the largest single message
	/// the client will send to the cluster. Applied via
	/// `Config::with_grpc_max_encoding_message_size` (added in
	/// `surrealdb-tikv-client 0.5.0`). Mirrors
	/// [`Self::grpc_max_decoding_message_size`] on the response side.
	pub grpc_max_encoding_message_size: usize,

	/// Maximum number of keys a single `delr` (transactional range
	/// delete) is allowed to touch in one call.
	///
	/// Beyond this safety cap the engine returns
	/// `TransactionRangeTooLarge` rather than silently issuing a
	/// non-transactional `unsafe_destroy_range` or letting the
	/// surrounding TiKV transaction grow unbounded. The default is
	/// intentionally generous — typical schema drops and index-builder
	/// cleanups must continue to work — but writes are still bounded so
	/// pathological ranges produce a clear error instead of an opaque
	/// gRPC payload-too-large or runaway commit.
	pub delr_max_keys: u32,

	/// PEM-encoded root CA the client trusts when verifying the TiKV/PD
	/// server certificate.
	///
	/// When set together with [`Self::tls_cert_path`] and
	/// [`Self::tls_key_path`] the connection switches to mTLS.
	pub tls_ca_path: Option<String>,

	/// Client certificate presented during mTLS. Must be set alongside
	/// [`Self::tls_ca_path`] and [`Self::tls_key_path`].
	pub tls_cert_path: Option<String>,

	/// Private key matching [`Self::tls_cert_path`].
	pub tls_key_path: Option<String>,

	/// Whether to run the background TiKV GC task at all. When disabled
	/// the lock-cleanup task is also skipped.
	pub gc_enabled: bool,

	/// How far behind the current TSO the shutdown advisory GC pass
	/// treats as the safepoint, in seconds. Mirrors TiDB's default of
	/// 10 minutes.
	///
	/// The periodic GC + lock-cleanup tasks read their interval and
	/// lifetime from [`crate::options::EngineOptions`] (set via the CLI
	/// `--tikv-gc-*` flags or the matching `SURREAL_TIKV_GC_*` env
	/// vars). This field is the fallback used by
	/// `Datastore::shutdown` so the final advisory pass still has a
	/// sensible lifetime even when the datastore is driven without
	/// `EngineOptions`.
	pub gc_lifetime_secs: u64,

	/// Whether to run the startup health probe after the client
	/// connects. Disabled deployments lose the early-failure signal;
	/// enabled is the default.
	pub health_probe: bool,

	/// How long shutdown will wait for in-flight transactions to drain
	/// before proceeding regardless, in seconds. Mirrors the HTTP
	/// graceful-shutdown loop's polling pattern.
	pub shutdown_grace_secs: u64,

	/// Bounded budget for the final advisory GC pass at shutdown, in
	/// seconds. A slow GC should not stall process exit.
	pub shutdown_gc_timeout_secs: u64,
}

/// Batch size used by the TiKV `delr` scan loop. Each pass fetches up to
/// this many keys, deletes them, then advances past the last key for the
/// next batch. Kept as a constant because it's an internal tuning knob,
/// not an operator-facing setting.
pub(super) const TIKV_DELR_BATCH_SIZE: u32 = 1_024;

impl Default for TikvConfig {
	fn default() -> Self {
		Self {
			api_version: 1,
			keyspace: None,
			request_timeout_secs: 10,
			async_commit: true,
			one_phase_commit: true,
			grpc_max_decoding_message_size: 4 * 1024 * 1024,
			grpc_max_encoding_message_size: 4 * 1024 * 1024,
			delr_max_keys: 1_000_000,
			tls_ca_path: None,
			tls_cert_path: None,
			tls_key_path: None,
			gc_enabled: true,
			gc_lifetime_secs: 600,
			health_probe: true,
			shutdown_grace_secs: 30,
			shutdown_gc_timeout_secs: 10,
		}
	}
}

impl Config for TikvConfig {
	fn parse(&mut self, map: &crate::cnf::ConfigMap) {
		map.parse_key("tikv_api_version", &mut self.api_version)
			.parse_key_option("tikv_keyspace", &mut self.keyspace)
			.parse_key("tikv_request_timeout", &mut self.request_timeout_secs)
			.parse_key_bool("tikv_async_commit", &mut self.async_commit)
			.parse_key_bool("tikv_one_phase_commit", &mut self.one_phase_commit)
			.parse_key(
				"tikv_grpc_max_decoding_message_size",
				&mut self.grpc_max_decoding_message_size,
			)
			.parse_key(
				"tikv_grpc_max_encoding_message_size",
				&mut self.grpc_max_encoding_message_size,
			)
			.parse_key("tikv_delr_max_keys", &mut self.delr_max_keys)
			.parse_key_option("tikv_tls_ca_path", &mut self.tls_ca_path)
			.parse_key_option("tikv_tls_cert_path", &mut self.tls_cert_path)
			.parse_key_option("tikv_tls_key_path", &mut self.tls_key_path)
			.parse_key_bool("tikv_gc_enabled", &mut self.gc_enabled)
			.parse_key("tikv_gc_lifetime", &mut self.gc_lifetime_secs)
			.parse_key_bool("tikv_health_probe", &mut self.health_probe)
			.parse_key("tikv_shutdown_grace", &mut self.shutdown_grace_secs)
			.parse_key("tikv_shutdown_gc_timeout", &mut self.shutdown_gc_timeout_secs);
	}
}

#[cfg(test)]
mod test {
	use super::TikvConfig;
	use crate::cnf::ConfigMap;

	#[test]
	fn defaults_when_map_empty() {
		let config = ConfigMap::empty().load::<TikvConfig>();
		assert_eq!(config.api_version, 1);
		assert!(config.keyspace.is_none());
		assert_eq!(config.request_timeout_secs, 10);
		assert!(config.async_commit);
		assert!(config.one_phase_commit);
		assert_eq!(config.grpc_max_decoding_message_size, 4 * 1024 * 1024);
		assert_eq!(config.grpc_max_encoding_message_size, 4 * 1024 * 1024);
		assert_eq!(config.delr_max_keys, 1_000_000);
		assert!(config.tls_ca_path.is_none());
		assert!(config.gc_enabled);
		assert_eq!(config.gc_lifetime_secs, 600);
		assert!(config.health_probe);
		assert_eq!(config.shutdown_grace_secs, 30);
		assert_eq!(config.shutdown_gc_timeout_secs, 10);
	}

	#[test]
	fn parses_tikv_keys_via_configmap() {
		let map = ConfigMap::empty()
			.with_key_value("tikv_api_version", "2")
			.with_key_value("tikv_keyspace", "my-keyspace")
			.with_key_value("tikv_request_timeout", "20")
			.with_key_value("tikv_async_commit", "false")
			.with_key_value("tikv_one_phase_commit", "false")
			.with_key_value("tikv_grpc_max_decoding_message_size", "8388608")
			.with_key_value("tikv_grpc_max_encoding_message_size", "16777216")
			.with_key_value("tikv_delr_max_keys", "50000")
			.with_key_value("tikv_tls_ca_path", "/etc/tikv/ca.pem")
			.with_key_value("tikv_tls_cert_path", "/etc/tikv/cert.pem")
			.with_key_value("tikv_tls_key_path", "/etc/tikv/key.pem")
			.with_key_value("tikv_gc_enabled", "false")
			.with_key_value("tikv_gc_lifetime", "1200")
			.with_key_value("tikv_health_probe", "false")
			.with_key_value("tikv_shutdown_grace", "60")
			.with_key_value("tikv_shutdown_gc_timeout", "5");
		let config = map.load::<TikvConfig>();
		assert_eq!(config.api_version, 2);
		assert_eq!(config.keyspace.as_deref(), Some("my-keyspace"));
		assert_eq!(config.request_timeout_secs, 20);
		assert!(!config.async_commit);
		assert!(!config.one_phase_commit);
		assert_eq!(config.grpc_max_decoding_message_size, 8 * 1024 * 1024);
		assert_eq!(config.grpc_max_encoding_message_size, 16 * 1024 * 1024);
		assert_eq!(config.delr_max_keys, 50_000);
		assert_eq!(config.tls_ca_path.as_deref(), Some("/etc/tikv/ca.pem"));
		assert_eq!(config.tls_cert_path.as_deref(), Some("/etc/tikv/cert.pem"));
		assert_eq!(config.tls_key_path.as_deref(), Some("/etc/tikv/key.pem"));
		assert!(!config.gc_enabled);
		assert_eq!(config.gc_lifetime_secs, 1200);
		assert!(!config.health_probe);
		assert_eq!(config.shutdown_grace_secs, 60);
		assert_eq!(config.shutdown_gc_timeout_secs, 5);
	}
}
