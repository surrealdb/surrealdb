use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

#[cfg(feature = "http")]
use anyhow::Context as _;
use anyhow::Result;
use arc_swap::ArcSwap;
use async_channel::Sender;
use surrealdb_cnf::ConfigMap;
use surrealdb_cnf::dynamic::DynamicConfiguration;
use surrealdb_datastore::FeatureLevelCell;
use surrealdb_kvs::Release;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::config::FeatureLevelSetting;
use crate::CommunityComposer;
use crate::buc::BucketStoreProvider;
use crate::buc::manager::BucketsManager;
use crate::config::RuntimeConfig;
use crate::dbs::{Capabilities, MessageBroker};
use crate::exec::function::FunctionRegistry;
#[cfg(feature = "http")]
use crate::http::HttpClient;
use crate::idx::trees::store::IndexStores;
use crate::kvs::cache::ds::DatastoreCache;
use crate::kvs::index::IndexBuilder;
use crate::kvs::migration::{self, LedgerVerdict, MIGRATIONS};
use crate::kvs::sequences::Sequences;
use crate::kvs::slowlog::SlowLog;
use crate::kvs::{
	Datastore, TransactionBuilder, TransactionBuilderFactory, TransactionBuilderParts,
	TransactionConfig, TransactionFactory,
};
use crate::lq::LiveQueryRouter;
use crate::observe::{ExecutionObserver, NoopObserver};
use crate::options::EngineOptions;
#[cfg(feature = "surrealism")]
use crate::surrealism::cache::SurrealismCache;
use crate::types::PublicNotification;

/// A builder struct for creating a Datastore.
pub struct Builder {
	capabilities: Capabilities,
	shutdown: CancellationToken,
	/// Optional sender for live-query notifications. Wrapped into a [`LocalMessageBroker`]
	/// when `live_query_broker` is left unset; composers may consume this channel and return a
	/// custom broker that owns it instead.
	notify_channel: Option<Sender<PublicNotification>>,
	live_query_broker: Option<Arc<dyn MessageBroker>>,
	/// Public HTTP endpoint this datastore publishes for cross-node messaging
	/// (live-query relay). Surfaced by the composer via
	/// [`TransactionBuilderFactory::http_endpoint`] when present.
	http_endpoint: Option<String>,
	id: Option<Uuid>,
	slow_log: Option<SlowLog>,
	/// The cadences the maintenance tasks run at.
	engine_options: EngineOptions,
	/// Whether to start the maintenance tasks. On by default; see
	/// [`Builder::without_maintenance_tasks`].
	maintenance: bool,
	/// Whether building opens transactions; see
	/// [`Builder::without_startup_transactions`].
	startup_transactions: bool,
	transaction_timeout: Option<Duration>,
	query_timeout: Option<Duration>,
	temporary_directory: Option<Arc<PathBuf>>,
	/// The governor granting each query its memory; see
	/// [`Builder::with_memory_governor`].
	memory_governor: Option<Arc<dyn crate::exec::memory::MemoryGovernor>>,
	/// The store a query's breakers spill to; see [`Builder::with_temp_store`].
	temp_store: Option<Arc<dyn crate::exec::memory::TempStore>>,
	/// Whether queries spill nowhere even where the platform has a default
	/// store; see [`Builder::without_temp_store`].
	#[cfg(test)]
	no_temp_store: bool,
	authenticate: bool,
	config: ConfigMap,
	/// Overrides the built-in graph fold threshold; see
	/// [`Builder::with_graph_fold_threshold`].
	graph_fold_threshold: Option<usize>,
	/// The feature level a new datastore is created at, and an existing one is
	/// raised to, or the hold that keeps a Legacy one; see
	/// [`Builder::with_feature_level`] and [`Builder::with_legacy_feature_level`].
	/// The last call of the two wins.
	feature_level: Option<FeatureLevelSetting>,
	/// Object store provider for file buckets. When unset the datastore uses the
	/// built-in [`CommunityComposer`] (memory, and — off wasm — file/S3/GCS/Azure).
	/// Embedders on platforms the built-in backends can't reach (e.g. a wasm
	/// Durable Object backing buckets with an R2 binding) install their own here.
	bucket_store_provider: Option<Box<dyn BucketStoreProvider>>,
	#[cfg(feature = "surrealism")]
	lazy_surrealism: bool,
	observer: Arc<dyn ExecutionObserver>,
}

impl Default for Builder {
	fn default() -> Self {
		Self::new()
	}
}

impl Builder {
	pub fn new() -> Self {
		Builder {
			capabilities: Default::default(),
			shutdown: CancellationToken::new(),
			notify_channel: None,
			live_query_broker: None,
			http_endpoint: None,
			id: None,
			slow_log: None,
			engine_options: EngineOptions::default(),
			// A build without `maintenance-tasks` leaves that work to its host.
			maintenance: cfg!(feature = "maintenance-tasks"),
			startup_transactions: true,
			transaction_timeout: None,
			query_timeout: None,
			temporary_directory: None,
			memory_governor: None,
			temp_store: None,
			#[cfg(test)]
			no_temp_store: false,
			authenticate: false,
			config: ConfigMap::empty(),
			graph_fold_threshold: None,
			feature_level: None,
			bucket_store_provider: None,
			#[cfg(feature = "surrealism")]
			lazy_surrealism: false,
			observer: Arc::new(NoopObserver),
		}
	}

	/// Sets config values for the builder.
	pub fn with_config(mut self, config: ConfigMap) -> Self {
		self.config = config;
		self
	}

	/// Overrides the built-in graph fold threshold: how many unfolded
	/// per-edge adjacency keys a traversal tolerates on one scope before
	/// observing it for a background fold. The threshold is deliberately not
	/// a config-map key — every node against a shared datastore must fold
	/// identically — so this builder hook is the only override, for tests
	/// and benchmarks that need eager folding (`1`) or no fold initiation
	/// (`0`, which disables read-path observation; edge deletes on tables
	/// already folded still queue their scopes and the background pass
	/// still drains that debt).
	pub fn with_graph_fold_threshold(mut self, threshold: usize) -> Self {
		self.graph_fold_threshold = Some(threshold);
		self
	}

	/// Pins the datastore's feature level to `level`, which takes the place of
	/// the `feature_level` configuration key.
	///
	/// A new datastore is created at `level` rather than at this build's
	/// release, so a deployment that has not yet moved every node to this
	/// release can create datastores that older nodes read. An existing
	/// datastore is raised to at least `level` when
	/// [`Datastore::check_version`] runs, and a datastore already at or above it
	/// keeps its level: a pin never lowers. The raise is skipped, with a
	/// warning, while a running node is older than `level`.
	///
	/// `level` must lie between the first release with a feature level and this
	/// build's release, or the build fails.
	///
	/// Replaces an earlier call to this or to [`Builder::with_legacy_feature_level`].
	pub fn with_feature_level(mut self, level: Release) -> Self {
		self.feature_level = Some(FeatureLevelSetting::Pin(level));
		self
	}

	/// Keeps a datastore that records no feature level at that level, so that its
	/// records and catalog entries stay in the `revision` encoding, and takes the
	/// place of the `feature_level` configuration key.
	///
	/// The hold does not keep the datastore open to every 3.x release. Every
	/// start of this build records ledger id 6, an incompatible-class entry, so
	/// a release whose registry lacks it (one without the v1 envelope reader)
	/// refuses the datastore at its next start; a 3.x release that carries the
	/// reader and registers id 6 reads it.
	///
	/// Without it, [`Datastore::check_version`] raises such a datastore on its
	/// own when its storage engine admits no other writer, and an operator raises
	/// one that does. The hold applies only to a datastore that records no level
	/// when it is opened: a datastore this build creates records this build's
	/// release, and one that records a level keeps it, each with a warning that
	/// the hold did not apply. It never lowers a level.
	///
	/// Replaces an earlier call to this or to [`Builder::with_feature_level`].
	pub fn with_legacy_feature_level(mut self) -> Self {
		self.feature_level = Some(FeatureLevelSetting::Legacy);
		self
	}

	/// Inject the tokio runtime's worker thread count into the config map
	/// under the shared `runtime_worker_threads` key.
	///
	/// The RocksDB engine reads this key to size the inline-blocking
	/// `InlineGuard` permit cap from the actual runtime width, keeping the
	/// cap in lockstep with the executor that runs the storage ops.
	/// Embedders building a custom tokio runtime should call this with the
	/// `worker_threads` value they passed to `tokio::runtime::Builder`.
	/// When unset, the engine falls back to a `max(4, num_cpus::get())`
	/// default.
	pub fn with_runtime_worker_threads(mut self, count: usize) -> Self {
		self.config = std::mem::take(&mut self.config)
			.with_key_value("runtime_worker_threads", count.to_string());
		self
	}

	/// Sets the capabilities for the datastore.
	pub fn with_capabilities(mut self, cap: Capabilities) -> Self {
		self.capabilities = cap;
		self
	}

	/// Sets the cadences the datastore's maintenance tasks run at.
	pub fn with_engine_options(mut self, options: EngineOptions) -> Self {
		self.engine_options = options;
		self
	}

	/// Builds a datastore with **no** maintenance tasks.
	///
	/// Intended for tests and benchmarks that need a quiescent datastore —
	/// compaction running mid-benchmark measures something else, and a test
	/// asserting on a delta log needs it to stay where the test put it. Such a
	/// caller drives the individual passes itself (for example
	/// [`Datastore::index_compaction`]), or starts the whole set later with
	/// [`tasks::init`](crate::kvs::tasks::init).
	///
	/// Anything serving real queries wants the default. Without these tasks an
	/// index's delta log grows unbounded and every read of that index sums it,
	/// `REMOVE NAMESPACE` never destroys the data it detached, changefeed data
	/// is never collected, an interrupted index build is never resumed, and this
	/// node's cluster heartbeat never refreshes.
	///
	/// It also hands the caller the storage version. These tasks write, and a
	/// write that reaches a datastore before its version is stamped makes a
	/// datastore this process just created read as storage from before the
	/// version key existed — so the branch that starts them settles the version
	/// first, and opting out of the one opts out of the other. A caller that
	/// takes this route and then serves queries owes that ordering itself:
	/// [`Datastore::check_version`] first, then
	/// [`Datastore::start_maintenance_tasks`], which is what every entry point
	/// in this workspace does.
	pub fn without_maintenance_tasks(mut self) -> Self {
		self.maintenance = false;
		self
	}

	/// Builds a datastore without opening a transaction, for a backend that
	/// may not serve one until it reports serve-readiness.
	///
	/// Implies [`Builder::without_maintenance_tasks`]. The feature level, the
	/// ledger and the Router live-query engine's baseline, which a build
	/// otherwise reads, are settled by [`Datastore::check_version`], which the
	/// caller runs once [`Datastore::wait_until_serve_ready`] returns and
	/// before anything else opens a transaction or serves a query.
	pub fn without_startup_transactions(mut self) -> Self {
		self.maintenance = false;
		self.startup_transactions = false;
		self
	}

	pub fn with_auth(mut self, enabled: bool) -> Self {
		self.authenticate = enabled;
		self
	}

	/// Adds a channel for receiving notifications from this datastore
	pub fn with_notify(mut self, channel: Sender<PublicNotification>) -> Self {
		self.notify_channel = Some(channel);
		self
	}

	/// Installs the broker used to deliver buffered live-query notifications after commit.
	pub fn with_live_query_broker(mut self, broker: Arc<dyn MessageBroker>) -> Self {
		self.live_query_broker = Some(broker);
		self
	}

	/// Sets the transaction timeout for this datastore
	pub fn with_transaction_timeout(mut self, timeout: Option<Duration>) -> Self {
		self.transaction_timeout = timeout;
		self
	}

	/// Sets the transaction timeout for this datastore
	pub fn with_query_timeout(mut self, timeout: Option<Duration>) -> Self {
		self.query_timeout = timeout;
		self
	}

	/// Sets the node id for this datastore
	pub fn with_id(mut self, id: Uuid) -> Self {
		self.id = Some(id);
		self
	}

	/// Sets the node id for this datastore
	pub fn with_shutdown_cancel(mut self, cancel: CancellationToken) -> Self {
		self.shutdown = cancel;
		self
	}

	/// Set a global slow log configuration
	///
	/// Parameters:
	/// - `duration`: Minimum execution time for a statement to be considered "slow". When `None`,
	///   slow logging is disabled.
	/// - `param_allow`: If non-empty, only parameters with names present in this list will be
	///   logged when a query is slow.
	/// - `param_deny`: Parameter names that should never be logged. This list always takes
	///   precedence over `param_allow`.
	pub fn with_slow_log(
		mut self,
		timeout: Duration,
		allowed_params: Vec<String>,
		disallowed_params: Vec<String>,
	) -> Self {
		self.slow_log = Some(SlowLog::new(timeout, allowed_params, disallowed_params));
		self
	}

	/// Set the governor that grants each query its memory, for a host that
	/// sizes and shares memory itself (an edge isolate). Without one, the
	/// datastore holds its queries to the `memory_limit` setting.
	pub fn with_memory_governor(
		mut self,
		governor: Arc<dyn crate::exec::memory::MemoryGovernor>,
	) -> Self {
		self.memory_governor = Some(governor);
		self
	}

	/// Set the store a query's breakers spill to once they outgrow their
	/// query's memory. Without one, a native datastore spills to files in its
	/// temporary directory, or the system's.
	pub fn with_temp_store(mut self, store: Arc<dyn crate::exec::memory::TempStore>) -> Self {
		self.temp_store = Some(store);
		self
	}

	pub fn with_temporary_directory<P: AsRef<Path>>(mut self, directory: Option<P>) -> Self {
		self.temporary_directory = directory.map(|x| Arc::new(x.as_ref().to_path_buf()));
		self
	}

	#[cfg(feature = "surrealism")]
	pub fn with_lazy_surrealism(mut self, lazy_surrealism: bool) -> Self {
		self.lazy_surrealism = lazy_surrealism;
		self
	}

	/// Install the [`ExecutionObserver`] used for the datastore's lifetime.
	/// Defaults to [`NoopObserver`].
	pub fn with_observer(mut self, observer: Arc<dyn ExecutionObserver>) -> Self {
		self.observer = observer;
		self
	}

	/// Install a custom [`BucketStoreProvider`] backing file buckets, overriding
	/// the default [`CommunityComposer`]. Use this on platforms whose object
	/// store the built-in backends can't reach — e.g. a wasm Durable Object that
	/// serves buckets from an R2 binding.
	pub fn with_bucket_store_provider(mut self, provider: Box<dyn BucketStoreProvider>) -> Self {
		self.bucket_store_provider = Some(provider);
		self
	}

	pub async fn build_with_path(self, path: &str) -> Result<Arc<Datastore>> {
		self.build_with_factory_path(path, CommunityComposer()).await
	}

	pub async fn build_with_factory_path<F>(self, path: &str, composer: F) -> Result<Arc<Datastore>>
	where
		F: TransactionBuilderFactory + 'static,
	{
		let (datastore, _) = self.build_with_factory_path_and_router_state(path, composer).await?;
		Ok(datastore)
	}

	/// Build a datastore and return the router startup state produced by the composer.
	///
	/// The datastore owns the transaction builder. The returned router state is
	/// immutable and must be passed to the matching router factory during server
	/// startup.
	pub async fn build_with_factory_path_and_router_state<F>(
		self,
		path: &str,
		composer: F,
	) -> Result<(Arc<Datastore>, F::RouterState)>
	where
		F: TransactionBuilderFactory + 'static,
	{
		let mut this = self;
		let TransactionBuilderParts {
			builder,
			router_state,
		} = composer.new_transaction_builder(path, this.shutdown.clone(), this.config.clone()).await?;
		if this.id.is_none()
			&& let Some(id) = composer.datastore_node_id()
		{
			this.id = Some(Uuid::from_bytes(id));
		}
		// Pull the local node's public HTTP endpoint from the composer (clustered editions
		// surface it from their topology config). Persisted on every `Node` row this
		// datastore writes so other cluster members can discover it via the catalog.
		if this.http_endpoint.is_none() {
			this.http_endpoint = composer.http_endpoint();
		}
		// Resolve the broker once: explicit `with_live_query_broker` wins, otherwise let the
		// composer decide (community returns a `LocalMessageBroker`, enterprise returns its
		// relay broker). Skipped when notifications are disabled.
		if this.live_query_broker.is_none()
			&& let Some(channel) = this.notify_channel.as_ref()
		{
			this.live_query_broker = Some(composer.live_query_broker(channel.clone()));
		}
		let bucket_store_provider =
			this.bucket_store_provider.take().unwrap_or_else(|| Box::new(CommunityComposer()));
		let buckets = BucketsManager::new(
			bucket_store_provider,
			this.config.load(),
			Arc::clone(&this.observer),
		);

		let datastore = this.build_with_tx_builder_buckets(builder, buckets).await?;

		// The broker was built before the datastore (because the datastore owns it). Once
		// the catalog is reachable, hand the broker a resolver so cross-node routing can
		// look peer endpoints up from the `node:` keyspace without needing an in-memory
		// topology table. Brokers that don't need it ignore the call (default no-op).
		//
		// WASM doesn't run clustered brokers and its transaction types aren't `Send + Sync`,
		// so the catalog resolver is non-WASM only.
		#[cfg(not(target_family = "wasm"))]
		if let Some(broker) = datastore.live_query_broker.as_ref() {
			let ctx = crate::dbs::BrokerRoutingContext {
				local_node_id: *datastore.id.as_bytes(),
				endpoint_resolver: datastore.endpoint_resolver(),
			};
			broker.attach_routing_context(ctx);
		}

		Ok((datastore, router_state))
	}

	pub(crate) async fn build_with_tx_builder_buckets(
		self,
		builder: Box<dyn TransactionBuilder>,
		buckets: BucketsManager,
	) -> Result<Arc<Datastore>> {
		let triggers = Arc::new(surrealdb_datastore::triggers::CommitTriggers::new());
		// Read before `self` is partially moved below.
		let maintenance = self.maintenance;
		let startup_transactions = self.startup_transactions;
		let options = self.engine_options;
		let shutdown = self.shutdown.clone();
		let observer = self.observer;
		let mut runtime_config = RuntimeConfig::load(&self.config);
		if let Some(threshold) = self.graph_fold_threshold {
			runtime_config.idx.graph_fold_threshold = threshold;
		}
		// The explicit builder input beats the configuration key. Either way the
		// datastore keeps the validated pin, which a malformed or out-of-range
		// one fails the build for, or the Legacy hold, which has none.
		let setting = self.feature_level.unwrap_or(runtime_config.datastore.feature_level);
		runtime_config.datastore.feature_level = match setting.resolve()? {
			Some(pin) => FeatureLevelSetting::Pin(pin),
			None if setting.holds_legacy() => FeatureLevelSetting::Legacy,
			None => FeatureLevelSetting::Unset,
		};
		let config = Arc::new(runtime_config);
		// One cell per store, attached before the factory is cloned into the index
		// builder and the sequences, so that every transaction shares it.
		let tf = TransactionFactory::new(
			Arc::clone(&triggers),
			builder,
			Arc::new(self.config.load::<TransactionConfig>()),
		)
		.with_observer(Arc::clone(&observer))
		.with_feature_level(Arc::new(FeatureLevelCell::unsettled()));
		let id = self.id.unwrap_or_else(Uuid::new_v4);
		// One registry per datastore, shared by the executor and by the index
		// builder, so a build resolves the same names a query does.
		let function_registry = Arc::new(FunctionRegistry::with_builtins());
		let capabilities = self.capabilities;
		let dynamic_configuration = DynamicConfiguration::default();
		dynamic_configuration.set_query_timeout(self.query_timeout);
		#[cfg(feature = "http")]
		let http_client = Arc::new(
			HttpClient::new(
				capabilities.allow_net.clone(),
				capabilities.deny_net.clone(),
				&config.http,
			)
			.context("Could not create http client")?,
		);

		let datastore = Datastore {
			shutdown,
			maintenance: parking_lot::Mutex::new(None),
			engine_options: options,
			shutdown_lock: tokio::sync::Mutex::new(()),
			created_here: std::sync::atomic::AtomicBool::new(false),
			feature_level_settled: std::sync::atomic::AtomicBool::new(false),
			id,
			transaction_factory: tf.clone(),
			auth_enabled: self.authenticate,
			dynamic_configuration,
			slow_log: self.slow_log,
			transaction_timeout: self.transaction_timeout,
			live_query_broker: self.live_query_broker,
			live_query_router: Arc::new(LiveQueryRouter::new()),
			scheduler: None,
			memory_governor: self.memory_governor,
			temp_store: self.temp_store,
			#[cfg(test)]
			no_temp_store: self.no_temp_store,
			memory_host: Default::default(),
			http_endpoint: self.http_endpoint,
			capabilities: ArcSwap::from_pointee(capabilities),
			index_stores: IndexStores::new(
				config.idx.hnsw_cache_size,
				config.idx.diskann_cache_size,
				config.idx.graph_resolve_cache_size,
				config.idx.segment_dictionary_path.clone(),
			),
			index_builder: IndexBuilder::new(tf.clone()),
			#[cfg(storage)]
			temporary_directory: self.temporary_directory,
			cache: Arc::new(DatastoreCache::new(config.datastore.datastore_cache_size)),
			#[cfg(all(feature = "graphql", not(target_family = "wasm")))]
			graphql_schema_cache: crate::graphql::cache::GraphQLSchemaCache::default(),
			function_registry,
			live_units: Arc::default(),
			compiled_units: Arc::new(crate::exec::vm::units::CompiledUnits::for_config(
				&config.exec,
			)),
			buckets,
			sequences: Sequences::new(tf, id),
			triggers,
			#[cfg(feature = "surrealism")]
			surrealism_cache: Arc::new(SurrealismCache::new(
				config.surrealism.surrealism_cache_size,
			)),
			#[cfg(feature = "surrealism")]
			lazy_surrealism: self.lazy_surrealism,
			#[cfg(feature = "http")]
			http_client: ArcSwap::new(http_client),
			observer,
			parser_config: Arc::new(self.config.load::<crate::syn::ParserConfig>()),
			config,
		};
		// Under the Router live-query engine, establish the router's baseline
		// cursor now — before the datastore can accept any connection. A build
		// that may open no transaction leaves it to `check_version`, which runs
		// before the instance serves a query.
		if startup_transactions {
			datastore.establish_router_baseline().await?;
		}
		// Populate the system metrics cache before anyone can read it. The
		// cache is process-wide and starts zeroed, and `INFO FOR ROOT` reports
		// it verbatim — so without this a query arriving before the first
		// `SystemMetricsRefresh` pass sees `physical_cores: 0`,
		// `available_parallelism: 0` and no memory figures, which is wrong
		// rather than merely stale. The scheduler already gives that job a
		// `next_due` of `now`, but it runs on a spawned task, so its first pass
		// lands some time after this constructor returns. Doing it here closes
		// that window; the task keeps the values current from then on.
		//
		// Only when the cache is cold, because it is shared with every other
		// datastore in the process: once one of them has warmed it the values
		// are real and no older than one refresh interval, and refreshing again
		// here would only cut short the CPU-percentage window of the scheduled
		// refresher that owns the cadence.
		crate::sys::refresh_if_cold().await;
		// Shared from here on: the maintenance tasks hold a `Weak` to it, and
		// the datastore is only handed out behind this `Arc`.
		let datastore = Arc::new(datastore);
		// A build that may open no transaction leaves the feature level and the
		// ledger to `check_version`, which reads both once the backend serves.
		if !startup_transactions {
			return Ok(datastore);
		}
		// Read the store's feature level into the shared cell, creating it where
		// the store is new. A build settles it here with or without the
		// maintenance tasks, so a datastore a test drives by hand is created at
		// its pin too. It precedes the version stamp, as `get_version` allows.
		datastore.settle_feature_level().await?;
		// What the ledger allows this build to do with the store. A build asks here
		// with or without the maintenance tasks, so that a datastore a test drives
		// by hand is read-only on the same evidence as a served one. It follows the
		// feature level, which a store carrying entries this build does not
		// maintain still reads. The read waits out a backend that is still
		// forming, as the other startup steps do.
		let verdict = Datastore::retry_bootstrap("Read ledger", || {
			migration::ledger_verdict(&datastore, MIGRATIONS)
		})
		.await?;
		match verdict {
			LedgerVerdict::Open => {}
			LedgerVerdict::ReadOnly {
				features,
			} => datastore.open_read_only(features),
			// Nothing starts against a store this build refuses, and `check_version`
			// reports the refusal.
			LedgerVerdict::Refused {
				..
			} => return Ok(datastore),
		}
		// A datastore without its maintenance tasks is not a working database,
		// so starting them is the constructor's job rather than every embedder's
		// to remember. Opt out with `without_maintenance_tasks`.
		if maintenance {
			// Settle the storage version first. Every maintenance task writes,
			// and a write that reaches storage before the version key does makes
			// a datastore this process just created read as storage from before
			// that key existed — which `check_version` is then obliged to refuse,
			// permanently, on this and every later open. Starting the writers is
			// what creates that obligation, so it is this branch that owes the
			// ordering; a caller who opts out owns both halves and settles the
			// version itself.
			//
			// Only the marker, not the whole of `check_version`: the migrations
			// that follow it run under a lease and have no bound on how long they
			// take, which is not something a constructor may do. The caller still
			// calls `check_version`, which finds the marker already written and
			// reports whether *this process* created the datastore from the latch
			// set below.
			let (_, is_new) = datastore.settle_storage_version().await?;
			datastore.created_here.fetch_or(is_new, std::sync::atomic::Ordering::SeqCst);
			// A read-only store runs the membership refresh alone: every other task
			// writes.
			let tasks = crate::kvs::tasks::init(&datastore, datastore.shutdown.clone(), &options);
			// This is the only writer, and it runs before the datastore is
			// handed to anyone.
			*datastore.maintenance.lock() = Some(tasks);
		}
		Ok(datastore)
	}
}

#[cfg(test)]
mod tests {
	use std::fmt::{self, Display};
	use std::future::Future;
	use std::pin::Pin;
	use std::sync::Arc;

	use anyhow::{Result, bail};
	use surrealdb_cnf::ConfigMap;
	use surrealdb_kvs::api::{BoxFut, Transactable};
	use surrealdb_kvs::{Metrics, TransactionType};
	use tokio_util::sync::CancellationToken;

	use super::Builder;
	use crate::CommunityComposer;
	use crate::buc::store::ObjectStore;
	use crate::buc::{
		BucketStoreProvider, BucketStoreProviderRequirements, Config as BucketConfig,
	};
	use crate::kvs::{TransactionBuilder, TransactionBuilderFactory, TransactionBuilderParts};

	#[derive(Clone)]
	struct TestRouterState(Arc<usize>);

	struct TestComposer {
		state: TestRouterState,
	}

	impl BucketStoreProviderRequirements for TestComposer {}

	impl BucketStoreProvider for TestComposer {
		fn connect<'a>(
			&self,
			_url: &'a str,
			_global: bool,
			_readonly: bool,
			_config: BucketConfig,
		) -> Pin<Box<dyn Future<Output = Result<Arc<dyn ObjectStore>>> + 'a + Send>> {
			Box::pin(async { bail!("test bucket connections are not used") })
		}
	}

	impl TransactionBuilderFactory for TestComposer {
		type RouterState = TestRouterState;

		async fn new_transaction_builder(
			&self,
			_path: &str,
			canceller: CancellationToken,
			config: ConfigMap,
		) -> Result<TransactionBuilderParts<Self::RouterState>> {
			let inner = CommunityComposer()
				.new_transaction_builder("memory", canceller, config)
				.await?
				.builder;
			Ok(TransactionBuilderParts::new(
				Box::new(TestTransactionBuilder {
					inner,
				}),
				self.state.clone(),
			))
		}

		fn path_valid(&self, v: &str) -> Result<String> {
			Ok(v.to_owned())
		}
	}

	/// An in-memory backend under its own name, so a test can tell the
	/// composer's builder is the one the datastore runs on. Building a
	/// datastore opens transactions whatever its options (it settles the
	/// feature level and reads the ledger), so the stub has to serve them.
	struct TestTransactionBuilder {
		inner: Box<dyn TransactionBuilder>,
	}

	impl Display for TestTransactionBuilder {
		fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
			f.write_str("test")
		}
	}

	impl TransactionBuilder for TestTransactionBuilder {
		fn name(&self) -> &'static str {
			"test"
		}

		fn new_transaction(
			&self,
			write: TransactionType,
		) -> BoxFut<'_, crate::kvs::err::Result<(Box<dyn Transactable>, bool)>> {
			self.inner.new_transaction(write)
		}

		fn shutdown(&self) -> BoxFut<'_, crate::kvs::err::Result<()>> {
			self.inner.shutdown()
		}

		fn register_metrics(&self) -> Option<Metrics> {
			None
		}

		fn collect_u64_metric(&self, _metric: &str) -> Option<u64> {
			None
		}
	}

	/// Both of these assert how the builder plumbs a composer's router state,
	/// which says nothing about storage. They build without maintenance tasks,
	/// which would otherwise run against the stub's store.
	#[tokio::test]
	async fn build_with_factory_path_returns_router_state() -> Result<()> {
		let expected = Arc::new(7);
		let composer = TestComposer {
			state: TestRouterState(Arc::clone(&expected)),
		};

		let (_datastore, router_state) = Builder::new()
			.without_maintenance_tasks()
			.build_with_factory_path_and_router_state("test:", composer)
			.await?;

		assert!(Arc::ptr_eq(&expected, &router_state.0));
		Ok(())
	}

	#[tokio::test]
	async fn build_with_factory_path_keeps_legacy_shape() -> Result<()> {
		let composer = TestComposer {
			state: TestRouterState(Arc::new(7)),
		};

		let datastore = Builder::new()
			.without_maintenance_tasks()
			.build_with_factory_path("test:", composer)
			.await?;

		assert_eq!(datastore.to_string(), "test");
		Ok(())
	}

	/// Under the Router live-query engine a build seeds the router's baseline,
	/// and a build without startup transactions leaves it to `check_version`,
	/// which seeds it before the datastore serves.
	#[tokio::test]
	async fn the_router_baseline_waits_for_check_version_without_startup_transactions() -> Result<()>
	{
		let router =
			|| surrealdb_cnf::ConfigMap::empty().with_key_value("live_query_engine", "router");
		let eager = Builder::new()
			.without_maintenance_tasks()
			.with_config(router())
			.build_with_path("memory")
			.await?;
		assert!(eager.live_query_router.has_baseline());

		let deferred = Builder::new()
			.without_startup_transactions()
			.with_config(router())
			.build_with_path("memory")
			.await?;
		assert!(!deferred.live_query_router.has_baseline());
		deferred.check_version().await?;
		assert!(deferred.live_query_router.has_baseline());
		Ok(())
	}
}
