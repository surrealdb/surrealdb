use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use clap::Args;
use surrealdb::engine::{any, tasks};
use surrealdb_core::buc::BucketStoreProvider;
use surrealdb_core::kvs::TransactionBuilderFactory;
use surrealdb_core::observe::{ExecutionObserver, FanOutObserver};
use surrealdb_core::options::EngineOptions;
use tokio_util::sync::CancellationToken;

use super::config::Config;
use crate::cli::ConfigCheck;
use crate::cnf::{LOGO, METRICS_ENABLED, PROCESS_METRICS_REFRESH_INTERVAL};
use crate::dbs::StartCommandDbsOptions;
use crate::ntw::RouterFactory;
use crate::ntw::client_ip::ClientIp;
use crate::observe::instruments::scope;
use crate::observe::{MetricsObserver, MetricsState, ObservabilityProvider, ObservabilityRuntime};
use crate::telemetry::metrics::otlp_metrics_active;
use crate::{dbs, env, ntw};

#[derive(Args, Debug)]
pub struct StartCommandArguments {
	#[arg(help = "Database path used for storing data")]
	#[arg(env = "SURREAL_PATH", index = 1)]
	#[arg(default_value = "memory")]
	path: String,
	#[arg(help = "Whether to hide the startup banner")]
	#[arg(env = "SURREAL_NO_BANNER", long)]
	#[arg(default_value_t = false)]
	no_banner: bool,
	#[arg(help = "Encryption key to use for on-disk encryption")]
	#[arg(env = "SURREAL_KEY", short = 'k', long = "key")]
	#[arg(value_parser = super::validator::key_valid)]
	#[arg(hide = true)] // Not currently in use
	key: Option<String>,
	//
	// Tasks
	#[arg(
		help = "The interval at which to refresh node registration information",
		help_heading = "Database"
	)]
	#[arg(env = "SURREAL_NODE_MEMBERSHIP_REFRESH_INTERVAL", long = "node-membership-refresh-interval", value_parser = super::validator::duration)]
	#[arg(default_value = "3s")]
	node_membership_refresh_interval: Duration,
	#[arg(
		help = "The interval at which to process and archive inactive nodes",
		help_heading = "Database"
	)]
	#[arg(env = "SURREAL_NODE_MEMBERSHIP_CHECK_INTERVAL", long = "node-membership-check-interval", value_parser = super::validator::duration)]
	#[arg(default_value = "15s")]
	node_membership_check_interval: Duration,
	#[arg(
		help = "The interval at which to process and cleanup archived nodes",
		help_heading = "Database"
	)]
	#[arg(env = "SURREAL_NODE_MEMBERSHIP_CLEANUP_INTERVAL", long = "node-membership-cleanup-interval", value_parser = super::validator::duration)]
	#[arg(default_value = "300s")]
	node_membership_cleanup_interval: Duration,
	#[arg(
		help = "The interval at which to perform changefeed garbage collection",
		help_heading = "Database"
	)]
	#[arg(env = "SURREAL_CHANGEFEED_GC_INTERVAL", long = "changefeed-gc-interval", value_parser = super::validator::duration)]
	#[arg(default_value = "30s")]
	changefeed_gc_interval: Duration,
	#[arg(env = "SURREAL_INDEX_COMPACTION_INTERVAL", long = "index-compaction-interval", value_parser = super::validator::duration)]
	#[arg(default_value = "5s")]
	index_compaction_interval: Duration,
	#[arg(env = "SURREAL_ASYNC_EVENT_PROCESSING_INTERVAL", long = "async-event-interval", value_parser = super::validator::duration)]
	#[arg(default_value = "5s")]
	event_processing_interval: Duration,
	#[arg(
		help = "The interval at which the TiKV MVCC garbage collector runs",
		help_heading = "Database"
	)]
	#[arg(env = "SURREAL_TIKV_GC_INTERVAL", long = "tikv-gc-interval", value_parser = super::validator::duration)]
	#[arg(default_value = "10m")]
	tikv_gc_interval: Duration,
	#[arg(
		help = "How far behind the current TSO the TiKV GC safepoint is allowed to sit",
		help_heading = "Database"
	)]
	#[arg(env = "SURREAL_TIKV_GC_LIFETIME", long = "tikv-gc-lifetime", value_parser = super::validator::duration)]
	#[arg(default_value = "10m")]
	tikv_gc_lifetime: Duration,
	#[arg(
		help = "The interval at which TiKV stale transactional locks are resolved",
		help_heading = "Database"
	)]
	#[arg(env = "SURREAL_TIKV_LOCK_CLEANUP_INTERVAL", long = "tikv-lock-cleanup-interval", value_parser = super::validator::duration)]
	#[arg(default_value = "60s")]
	tikv_lock_cleanup_interval: Duration,
	//
	// Authentication
	#[arg(
		help = "The username for the initial database root user. Only if no other root user exists",
		help_heading = "Authentication"
	)]
	#[arg(
		env = "SURREAL_USER",
		short = 'u',
		long = "username",
		visible_alias = "user",
		requires = "password"
	)]
	username: Option<String>,
	#[arg(
		help = "The password for the initial database root user. Only if no other root user exists",
		help_heading = "Authentication"
	)]
	#[arg(
		env = "SURREAL_PASS",
		short = 'p',
		long = "password",
		visible_alias = "pass",
		requires = "username"
	)]
	password: Option<String>,
	//
	// Datastore connection
	#[command(next_help_heading = "Datastore connection")]
	#[command(flatten)]
	kvs: Option<StartCommandRemoteTlsOptions>,
	//
	// HTTP Server
	#[command(next_help_heading = "HTTP server")]
	#[command(flatten)]
	web: Option<StartCommandWebTlsOptions>,
	#[arg(help = "The method of detecting the client's IP address")]
	#[arg(env = "SURREAL_CLIENT_IP", long)]
	#[arg(default_value = "socket", value_enum)]
	client_ip: ClientIp,
	#[arg(help = "The hostname or IP address to listen for connections on")]
	#[arg(env = "SURREAL_BIND", short = 'b', long = "bind")]
	#[arg(default_value = "127.0.0.1:8000")]
	listen_addresses: Vec<SocketAddr>,
	#[arg(help = "Whether to suppress the server name and version headers")]
	#[arg(env = "SURREAL_NO_IDENTIFICATION_HEADERS", long)]
	#[arg(default_value_t = false)]
	no_identification_headers: bool,
	#[arg(help = "The allowed origins for CORS requests. Defaults to allow all origins")]
	#[arg(env = "SURREAL_ALLOW_ORIGIN", long = "allow-origin")]
	#[arg(value_delimiter = ',', value_parser = super::validator::cors_origin)]
	allow_origin: Vec<String>,
	//
	// Database options
	#[command(flatten)]
	#[command(next_help_heading = "Database")]
	dbs: StartCommandDbsOptions,
}

#[derive(Args, Debug)]
#[group(requires_all = ["kvs_ca", "kvs_crt", "kvs_key"], multiple = true)]
struct StartCommandRemoteTlsOptions {
	#[arg(help = "Path to the CA file used when connecting to the remote KV store")]
	#[arg(env = "SURREAL_KVS_CA", long = "kvs-ca", value_parser = super::validator::file_exists)]
	kvs_ca: Option<PathBuf>,
	#[arg(help = "Path to the certificate file used when connecting to the remote KV store")]
	#[arg(env = "SURREAL_KVS_CRT", long = "kvs-crt", value_parser = super::validator::file_exists)]
	kvs_crt: Option<PathBuf>,
	#[arg(help = "Path to the private key file used when connecting to the remote KV store")]
	#[arg(env = "SURREAL_KVS_KEY", long = "kvs-key", value_parser = super::validator::file_exists)]
	kvs_key: Option<PathBuf>,
}

#[derive(Args, Debug)]
#[group(requires_all = ["web_crt", "web_key"], multiple = true)]
struct StartCommandWebTlsOptions {
	#[arg(help = "Path to the certificate file for encrypted client connections")]
	#[arg(env = "SURREAL_WEB_CRT", long = "web-crt", value_parser = super::validator::file_exists)]
	web_crt: Option<PathBuf>,
	#[arg(help = "Path to the private key file for encrypted client connections")]
	#[arg(env = "SURREAL_WEB_KEY", long = "web-key", value_parser = super::validator::file_exists)]
	web_key: Option<PathBuf>,
}

/// Start the server.
///
/// Initializes and starts the SurrealDB server with the provided configuration.
///
/// # Parameters
/// - `composer`: A composer implementing the required traits for dependency injection
///
/// # Generic parameters
/// - `C`: A composer type that implements:
///   - `TransactionBuilderFactory` (datastore transaction builder for storage/backend selection)
///   - `RouterFactory` (HTTP router factory for route/middleware customization)
///   - `ConfigCheck` (validates configuration before initialization)
pub async fn init<
	C: TransactionBuilderFactory
		+ RouterFactory
		+ ConfigCheck
		+ BucketStoreProvider
		+ ObservabilityProvider,
>(
	mut composer: C,
	StartCommandArguments {
		path,
		username: user,
		password: pass,
		client_ip,
		listen_addresses,
		dbs,
		web,
		node_membership_refresh_interval,
		node_membership_check_interval,
		node_membership_cleanup_interval,
		changefeed_gc_interval,
		index_compaction_interval,
		event_processing_interval,
		tikv_gc_interval,
		tikv_gc_lifetime,
		tikv_lock_cleanup_interval,
		no_banner,
		no_identification_headers,
		allow_origin,
		..
	}: StartCommandArguments,
	runtime: ObservabilityRuntime,
) -> Result<()> {
	// Install the rustls process-default crypto provider before any TLS
	// operations occur. Under `feature = "fips"` this asserts FIPS mode is
	// active and aborts startup otherwise.
	crate::tls::install_default_crypto_provider()?;
	// Check the path is valid
	C::path_valid(&path)?;
	// Check if we should output a banner
	if !no_banner {
		println!("{LOGO}");
	}
	// Clean the path
	let endpoint = any::__into_endpoint(path)?;
	let path = if endpoint.path.is_empty() {
		endpoint.url.to_string()
	} else {
		endpoint.path
	};
	// Extract the certificate and key
	let (crt, key) = if let Some(val) = web {
		(val.web_crt, val.web_key)
	} else {
		(None, None)
	};
	// Configure the engine
	let engine = EngineOptions::default()
		.with_node_membership_refresh_interval(node_membership_refresh_interval)
		.with_node_membership_check_interval(node_membership_check_interval)
		.with_node_membership_cleanup_interval(node_membership_cleanup_interval)
		.with_changefeed_gc_interval(changefeed_gc_interval)
		.with_index_compaction_interval(index_compaction_interval)
		.with_event_processing_interval(event_processing_interval)
		.with_tikv_gc_interval(tikv_gc_interval)
		.with_tikv_gc_lifetime(tikv_gc_lifetime)
		.with_tikv_lock_cleanup_interval(tikv_lock_cleanup_interval);
	// Configure the config
	let Some(bind) = listen_addresses.first().copied() else {
		return Err(anyhow::anyhow!("No listen address provided"));
	};
	let config = Config {
		bind,
		client_ip,
		path,
		user,
		pass,
		no_identification_headers,
		allow_origin,
		engine,
		crt,
		key,
	};
	composer.check_config(&config).await?;
	// Setup the command-line options
	// Initiate environment
	env::init()?;

	// Build the observability pipeline before starting the datastore so the
	// observer can be installed at construction time. The community metrics
	// observer is only instantiated when /metrics is enabled; composer
	// extensions may contribute an additional audit observer regardless.
	let (metrics_state, combined_observer) = build_observability::<C>(&composer, &runtime)?;

	// Create a token to cancel tasks
	let canceller = CancellationToken::new();

	// Keep the cached process snapshot fresh for both readers. The
	// `/metrics` handler used to refresh on each scrape; OTLP push has
	// no equivalent path, so a background task is the only way to
	// avoid flat-lined `surrealdb.process.*` values on OTLP-only
	// deployments. Only spawn when at least one reader is configured;
	// otherwise nobody reads the cache.
	if *METRICS_ENABLED || otlp_metrics_active() {
		spawn_process_snapshot_refresh(canceller.clone());
	}
	// Start the datastore
	let (datastore, recv, router_state) =
		dbs::init::<C>(composer, &config, canceller.clone(), combined_observer, dbs).await?;
	let datastore = Arc::new(datastore);
	// Eagerly load surrealism modules in the background unless opted out
	#[cfg(feature = "surrealism")]
	if !datastore.is_lazy_surrealism() {
		let ds = Arc::clone(&datastore);
		tokio::spawn(async move {
			ds.eager_load_surrealism_modules().await;
		});
	}
	// Register datastore metrics against the unified meter provider. The
	// instruments flow to both the Prometheus text exporter (rendered by
	// `/metrics`) and the OTLP push exporter (when configured), so
	// operators get the same storage-engine gauges via either path.
	// Storage-backend metric names are not in `PUBLIC_METRICS`, so
	// unauthenticated `/metrics` scrapers never see them.
	if let Err(err) = crate::observe::register_storage_metrics(&datastore, &runtime) {
		warn!("failed to register storage metrics: {err}");
	}
	// Start the node agent
	let nodetasks = tasks::init(Arc::clone(&datastore), canceller.clone(), &config.engine);
	// Build and run the HTTP server using the provided RouterFactory implementation
	ntw::init_with_metrics::<C>(
		&config,
		Arc::clone(&datastore),
		recv,
		canceller.clone(),
		router_state,
		metrics_state,
	)
	.await?;
	// Shutdown and stop closed tasks
	canceller.cancel();
	// Wait for background tasks to finish
	nodetasks.resolve().await?;
	// Shutdown the datastore
	datastore.shutdown().await?;
	// All ok
	Ok(())
}

/// Build the observer that will be installed on the datastore along with the
/// `/metrics` state that will be attached to the HTTP router.
///
/// Behaviour:
///
/// - Every metric is recorded through the unified [`opentelemetry_sdk::metrics::SdkMeterProvider`]
///   built in [`crate::telemetry::metrics::init`]. The provider routes instruments to both the
///   Prometheus text exporter (rendered by `/metrics`) and the OTLP push exporter (when
///   configured).
/// - The process / pipeline observable gauges (`surrealdb.build.info`, `surrealdb.process.*`, audit
///   / slow-query self-metrics) are registered whenever any reader is attached -- either Prometheus
///   pull or OTLP push. This matches the gate used by [`spawn_process_snapshot_refresh`] so the
///   cache and its readers stay symmetrical, and keeps OTLP-only deployments
///   (`SURREAL_METRICS_ENABLED=false` + `SURREAL_TELEMETRY_PROVIDER=otlp`) wired up to the same
///   gauge surface as Prometheus scrapers.
/// - When [`METRICS_ENABLED`] is `true`, a [`MetricsObserver`] is constructed and returned as part
///   of the [`MetricsState`] so the `/metrics` handler can reach the Prometheus text exporter; it
///   also lands in the fan-out so the labelled `surrealdb.*` family is recorded on every emit.
/// - The composer's [`ObservabilityProvider::create_observer`] is always invoked; composer
///   extensions use this hook to install per-tenant rollups, SurrealDS cluster, and audit /
///   slow-query observers under their own signal-domain scopes.
/// - The resulting fan-out is `[MetricsObserver?, composer]`: one labelled recording site for the
///   primary surface, plus whatever the composer contributes.
///
/// The returned [`MetricsState`] is `None` when metrics are disabled, which
/// keeps the `/metrics` route from being mounted at all.
#[allow(clippy::clone_on_ref_ptr)]
fn build_observability<C: ObservabilityProvider>(
	composer: &C,
	runtime: &ObservabilityRuntime,
) -> Result<(Option<MetricsState>, Arc<dyn ExecutionObserver>)> {
	let extra = composer.create_observer_with_runtime(runtime);

	// Register the process snapshot and pipeline self-metric gauges
	// against the unified meter provider whenever any reader -- Prometheus
	// pull or OTLP push -- is configured. Without this hoist OTLP-only
	// deployments would build the `SdkMeterProvider` (see
	// `telemetry::metrics::init`) and refresh the process snapshot via
	// `spawn_process_snapshot_refresh` but never expose any gauges that
	// read from the cache, leaving OTLP collectors with zero
	// `surrealdb.build.info` / `surrealdb.process.*` /
	// `surrealdb_audit_*` / `surrealdb_slow_query_*` samples.
	if *METRICS_ENABLED || otlp_metrics_active() {
		crate::observe::metrics::register_process_metrics(runtime);
		if let Some(counters) = composer.audit_counters() {
			MetricsObserver::register_pipeline_self_metrics(
				runtime,
				scope::AUDIT,
				"audit",
				counters,
			)?;
		}
		if let Some(counters) = composer.slow_query_counters() {
			MetricsObserver::register_pipeline_self_metrics(
				runtime,
				scope::SLOW_QUERY,
				"slow-query",
				counters,
			)?;
		}
	}

	if !*METRICS_ENABLED {
		// `/metrics` is disabled; keep the composer observers attached
		// so OTLP push and audit pipelines still receive events. The
		// gauge registrations above already wired up the observable
		// instruments against the OTLP reader.
		return Ok((None, extra));
	}

	// Build the unified labelled observer that exposes the
	// SurrealDB-native families via `/metrics`. The shared process /
	// pipeline gauge registrations above already ran on this branch.
	let metrics_observer = Arc::new(MetricsObserver::new(runtime)?);
	let metrics_obs: Arc<dyn ExecutionObserver> = metrics_observer.clone();
	let combined: Arc<dyn ExecutionObserver> = Arc::new(FanOutObserver::new([metrics_obs, extra]));
	// `/metrics` is mounted only when the runtime carries a Prometheus
	// exporter; otherwise the route is disabled but the labelled
	// observer still records to whichever reader (OTLP push, in-test
	// `ManualReader`, ...) the runtime carries.
	let metrics_state = runtime.prometheus_exporter().map(|exporter| MetricsState {
		exporter,
		observer: metrics_observer,
	});
	Ok((metrics_state, combined))
}

/// Spawn a background task that refreshes the cached process snapshot
/// used by the `surrealdb.process.{memory,cpu_percent}` observable
/// gauges every [`PROCESS_METRICS_REFRESH_INTERVAL`] seconds.
///
/// The task runs an eager refresh once before entering its tick loop
/// so the first OTLP export and the first `/metrics` scrape both
/// observe non-zero values; without that the cold-start window can
/// take up to one full interval before the cache populates. The task
/// cancels cleanly via the supplied [`CancellationToken`] on graceful
/// shutdown.
fn spawn_process_snapshot_refresh(canceller: CancellationToken) {
	// Floor the configured value at 1s so a misconfigured zero does
	// not produce a tight refresh loop.
	let secs = (*PROCESS_METRICS_REFRESH_INTERVAL).max(1);
	let period = Duration::from_secs(secs);
	tokio::spawn(async move {
		// Eager initial refresh so cold-start exports observe a real
		// snapshot rather than the all-zero default.
		let _ = surrealdb_core::observe::refresh_process_snapshot().await;
		let mut ticker = tokio::time::interval(period);
		// `tokio::time::interval` fires immediately on first tick; we
		// already refreshed above, so skip the leading tick.
		ticker.tick().await;
		loop {
			tokio::select! {
				biased;
				_ = canceller.cancelled() => break,
				_ = ticker.tick() => {
					let _ = surrealdb_core::observe::refresh_process_snapshot().await;
				}
			}
		}
	});
}
