use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use anyhow::Result;
use clap::Args;
use surrealdb::engine::any;
use surrealdb_core::kvs::{NODE_ARCHIVE_THRESHOLD, TransactionBuilderFactory};
use surrealdb_core::options::EngineOptions;
use surrealdb_observe::{ExecutionObserver, FanOutObserver};
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
		help = "How stale this node's cluster heartbeat may get before /ready reports it unhealthy (defaults to three refresh intervals)",
		help_heading = "Database"
	)]
	#[arg(env = "SURREAL_READINESS_HEARTBEAT_MAX_AGE", long = "readiness-heartbeat-max-age", value_parser = super::validator::duration)]
	readiness_heartbeat_max_age: Option<Duration>,
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
	#[arg(
		help = "The interval at which to resume index builds left unfinished by a crashed node (0 to disable)",
		help_heading = "Database"
	)]
	#[arg(env = "SURREAL_INDEX_BUILD_RESUME_INTERVAL", long = "index-build-resume-interval", value_parser = super::validator::duration)]
	#[arg(default_value = "30s")]
	index_build_resume_interval: Duration,
	#[arg(env = "SURREAL_ASYNC_EVENT_PROCESSING_INTERVAL", long = "async-event-interval", value_parser = super::validator::duration)]
	#[arg(default_value = "5s")]
	event_processing_interval: Duration,
	#[arg(env = "SURREAL_RECLAIM_INTERVAL", long = "reclaim-interval", value_parser = super::validator::duration)]
	#[arg(default_value = "60s")]
	reclaim_interval: Duration,
	#[arg(
		help = "Minimum age a removed namespace/database/index must reach before its data is physically reclaimed (snapshot-safety grace; effective value is max(this, --tikv-gc-lifetime))",
		help_heading = "Database"
	)]
	#[arg(env = "SURREAL_RECLAIM_GRACE", long = "reclaim-grace", value_parser = super::validator::duration)]
	#[arg(default_value = "10m")]
	reclaim_grace: Duration,
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
	#[arg(
		help = "Whether to persist client-attached HTTP RPC sessions in the datastore so they survive server restarts and can be resumed on any cluster node. Intended for deployments that route a given session to one node at a time (sticky routing / one runtime per session); a session used concurrently from multiple nodes is best-effort. The durable copy contains the session's authentication state, stored unencrypted in the datastore",
		help_heading = "Database"
	)]
	#[arg(env = "SURREAL_DURABLE_SESSIONS", long = "durable-sessions")]
	#[arg(default_value_t = false)]
	durable_sessions: bool,
	#[arg(
		help = "How long a persisted RPC session survives without being used; each use refreshes the expiry",
		help_heading = "Database"
	)]
	#[arg(env = "SURREAL_DURABLE_SESSION_TTL", long = "durable-session-ttl", value_parser = super::validator::duration)]
	#[arg(default_value = "24h")]
	durable_session_ttl: Duration,
	#[arg(
		help = "The interval at which expired persisted RPC sessions are purged (0 to disable)",
		help_heading = "Database"
	)]
	#[arg(env = "SURREAL_DURABLE_SESSION_GC_INTERVAL", long = "durable-session-gc-interval", value_parser = super::validator::duration)]
	#[arg(default_value = "60s")]
	durable_session_gc_interval: Duration,
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
	// Postgres server
	#[arg(
		help = "The hostname or IP address to listen for Postgres wire protocol connections on",
		help_heading = "Postgres server"
	)]
	#[arg(env = "SURREAL_POSTGRES_BIND", long = "postgres-bind")]
	postgres_bind: Option<SocketAddr>,
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
	C: TransactionBuilderFactory + RouterFactory + ConfigCheck + ObservabilityProvider,
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
		readiness_heartbeat_max_age,
		changefeed_gc_interval,
		index_compaction_interval,
		index_build_resume_interval,
		event_processing_interval,
		reclaim_interval,
		reclaim_grace,
		tikv_gc_interval,
		tikv_gc_lifetime,
		tikv_lock_cleanup_interval,
		durable_sessions,
		durable_session_ttl,
		durable_session_gc_interval,
		no_banner,
		no_identification_headers,
		allow_origin,
		postgres_bind,
		..
	}: StartCommandArguments,
	runtime: ObservabilityRuntime,
) -> Result<()> {
	// Check the path is valid
	composer.path_valid(&path)?;
	// Persisted sessions must have a finite expiration
	if durable_sessions && durable_session_ttl.is_zero() {
		return Err(anyhow::anyhow!("The durable session TTL must be greater than zero"));
	}
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
		.with_readiness_heartbeat_max_age(readiness_heartbeat_max_age)
		.with_changefeed_gc_interval(changefeed_gc_interval)
		.with_index_compaction_interval(index_compaction_interval)
		.with_index_build_resume_interval(index_build_resume_interval)
		.with_event_processing_interval(event_processing_interval)
		.with_reclaim_interval(reclaim_interval)
		.with_reclaim_grace(reclaim_grace)
		.with_tikv_gc_interval(tikv_gc_interval)
		.with_tikv_gc_lifetime(tikv_gc_lifetime)
		.with_tikv_lock_cleanup_interval(tikv_lock_cleanup_interval)
		.with_rpc_session_gc_interval(durable_session_gc_interval)
		// Floor the configured value at 1s so a misconfigured zero neither
		// produces a tight refresh loop nor unregisters the job.
		.with_system_metrics_refresh_interval(Duration::from_secs(
			(*PROCESS_METRICS_REFRESH_INTERVAL).max(1),
		));
	// Configure the config
	let Some(bind) = listen_addresses.first().copied() else {
		return Err(anyhow::anyhow!("No listen address provided"));
	};
	let config = Config {
		bind,
		postgres_bind,
		client_ip,
		path,
		user,
		pass,
		no_identification_headers,
		allow_origin,
		engine,
		crt,
		key,
		durable_session_ttl: durable_sessions.then_some(durable_session_ttl),
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

	// Start the datastore. The startup import and credential initialisation are
	// returned rather than run here, so the web server can bind before they run.
	let (datastore, recv, router_state, pending_startup) =
		dbs::init::<C>(composer, &config, canceller.clone(), combined_observer, dbs).await?;
	let datastore = datastore;
	// Tracks whether the instance has finished starting up (import + credentials)
	// and is ready to serve user-facing queries. The HTTP listener binds
	// immediately; until this flips to `true`, query/auth endpoints return 503
	// and `/ready` reports not-ready.
	let ready = Arc::new(AtomicBool::new(false));
	if pending_startup.has_work() {
		// Run the deferred startup work (import, then credentials) concurrently
		// so the listener comes up right away, flipping `ready` once it succeeds.
		let ds = Arc::clone(&datastore);
		let ready = Arc::clone(&ready);
		let startup_canceller = canceller.clone();
		// Registered with the datastore, so the shutdown that closes the storage
		// engine waits for this task to stop first: an import runs statements of
		// its own, and the engine must not close under one. The wait is short
		// because the task selects on the token, which shutdown trips before it
		// joins.
		if !datastore.spawn_joined_on_shutdown(async move {
			tokio::select! {
				biased;
				// Stop promptly if the server is shutting down; leave `ready`
				// unset so we never flip to ready mid-shutdown.
				_ = startup_canceller.cancelled() => {
					debug!("Startup aborted before completion due to shutdown");
				}
				res = dbs::finish_startup(&ds, &pending_startup) => match res {
					Ok(()) => {
						ready.store(true, Ordering::SeqCst);
						info!("Startup complete; instance is now ready to serve");
					}
					Err(err) => {
						error!("Startup failed; instance will not become ready: {err}");
					}
				}
			}
		}) {
			// The datastore is already shutting down, so the deferred work is not
			// started and the instance never reports ready.
			debug!("Startup skipped: the datastore is shutting down");
		}
	} else {
		// Nothing deferred (no import, credentials already initialised): ready to
		// serve as soon as the listener binds.
		ready.store(true, Ordering::SeqCst);
	}
	// Eagerly load surrealism modules in the background unless opted out
	#[cfg(feature = "surrealism")]
	if !datastore.is_lazy_surrealism() {
		let ds = Arc::clone(&datastore);
		let load_canceller = canceller.clone();
		// The load reads the catalog, so it is registered with the datastore and
		// joined before the storage engine closes. It selects on the token rather
		// than being awaited to completion: the modules it has not reached yet
		// are loaded on first use, so abandoning the rest costs nothing but
		// waiting out the whole load during shutdown would.
		datastore.spawn_joined_on_shutdown(async move {
			tokio::select! {
				biased;
				_ = load_canceller.cancelled() => {
					debug!("Surrealism eager load aborted due to shutdown");
				}
				_ = ds.eager_load_surrealism_modules() => {}
			}
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
	// The `/ready` probe treats the node as unhealthy if its cluster heartbeat
	// hasn't refreshed within this window (the refresh also confirms the storage
	// read and write paths are working). Configured, or else derived from the
	// refresh interval.
	let max_heartbeat_age = config.engine.resolved_readiness_heartbeat_max_age();
	// A peer archives a node whose heartbeat passes `NODE_ARCHIVE_THRESHOLD`,
	// and then collects its live queries. A readiness window that reaches that
	// far leaves this node taking traffic after the cluster has written it off,
	// which is worse than either outcome alone. The window is not clamped —
	// an operator who has raised the threshold in their own build, or who
	// accepts the consequence, keeps what they configured — but it is never
	// silently accepted either.
	if max_heartbeat_age >= NODE_ARCHIVE_THRESHOLD {
		warn!(
			"Readiness heartbeat window ({max_heartbeat_age:?}) is at or beyond the interval after \
			 which peers archive an unresponsive node ({NODE_ARCHIVE_THRESHOLD:?}); this node can \
			 be reported ready after the cluster has already archived it"
		);
	}
	let readiness = ntw::Readiness {
		ready: Arc::clone(&ready),
		// The heartbeat freshness check applies on the server path, where the
		// node-membership refresh task keeps the heartbeat current.
		max_heartbeat_age: Some(max_heartbeat_age),
	};
	// Start the Postgres wire protocol listener when configured
	#[cfg(feature = "postgres")]
	if let Some(postgres_addr) = config.postgres_bind {
		crate::pg::start(
			postgres_addr,
			Arc::clone(&datastore),
			Arc::clone(&ready),
			canceller.clone(),
			config.crt.clone(),
			config.key.clone(),
		)
		.await?;
	}
	#[cfg(not(feature = "postgres"))]
	if config.postgres_bind.is_some() {
		return Err(anyhow::anyhow!(
			"The --postgres-bind option requires a binary built with the 'postgres' feature"
		));
	}
	// Build and run the HTTP server using the provided RouterFactory implementation
	ntw::init_with_metrics::<C>(
		&config,
		Arc::clone(&datastore),
		recv,
		canceller.clone(),
		router_state,
		metrics_state,
		readiness,
	)
	.await?;
	// Tell every task holding this token to stop. The datastore shutdown below
	// then waits for the ones registered with it — including the startup work
	// spawned above — before closing the storage engine.
	canceller.cancel();
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
///   pull or OTLP push. This keeps OTLP-only deployments (`SURREAL_METRICS_ENABLED=false` +
///   `SURREAL_TELEMETRY_PROVIDER=otlp`) wired up to the same gauge surface as Prometheus scrapers.
///   The snapshot these gauges read is refreshed by the engine's maintenance scheduler regardless
///   of whether any reader is attached, because `INFO FOR ROOT` reads the same cache.
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
	// `telemetry::metrics::init`) and keep the process snapshot fresh but
	// never expose any gauges that read from the cache, leaving OTLP
	// collectors with zero `surrealdb.build.info` / `surrealdb.process.*` /
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
