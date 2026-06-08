use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

#[cfg(feature = "http")]
use anyhow::Context as _;
use anyhow::Result;
use async_channel::Sender;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::CommunityComposer;
use crate::buc::BucketStoreProvider;
use crate::buc::manager::BucketsManager;
use crate::cnf::dynamic::DynamicConfiguration;
use crate::cnf::{CommonConfig, ConfigMap};
use crate::dbs::{Capabilities, MessageBroker};
use crate::exec::function::FunctionRegistry;
#[cfg(feature = "http")]
use crate::http::HttpClient;
use crate::idx::trees::store::IndexStores;
use crate::kvs::cache::ds::DatastoreCache;
use crate::kvs::index::IndexBuilder;
use crate::kvs::sequences::Sequences;
use crate::kvs::slowlog::SlowLog;
use crate::kvs::{
	Datastore, TransactionBuilder, TransactionBuilderFactory, TransactionBuilderParts,
	TransactionFactory,
};
use crate::observe::{ExecutionObserver, NoopObserver};
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
	transaction_timeout: Option<Duration>,
	query_timeout: Option<Duration>,
	temporary_directory: Option<Arc<PathBuf>>,
	authenticate: bool,
	config: ConfigMap,
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
			transaction_timeout: None,
			query_timeout: None,
			temporary_directory: None,
			authenticate: false,
			config: ConfigMap::empty(),
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

	pub async fn build_with_path(self, path: &str) -> Result<Datastore> {
		self.build_with_factory_path(path, CommunityComposer()).await
	}

	pub async fn build_with_factory_path<F>(self, path: &str, composer: F) -> Result<Datastore>
	where
		F: TransactionBuilderFactory + BucketStoreProvider + 'static,
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
	) -> Result<(Datastore, F::RouterState)>
	where
		F: TransactionBuilderFactory + BucketStoreProvider + 'static,
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
		let buckets = BucketsManager::new(Box::new(composer), this.config.load());

		let datastore = this.build_with_tx_builder_buckets(builder, buckets).await?;

		// The broker was built before the datastore (because the datastore owns it). Now that
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
	) -> Result<Datastore> {
		let async_event_trigger = Arc::new(Notify::new());
		let observer = self.observer;
		let config = Arc::new(self.config.load::<CommonConfig>());
		let tf =
			TransactionFactory::new(Arc::clone(&async_event_trigger), builder, Arc::clone(&config))
				.with_observer(Arc::clone(&observer));
		let id = self.id.unwrap_or_else(Uuid::new_v4);
		let capabilities = Arc::new(self.capabilities);
		let dynamic_configuration = DynamicConfiguration::default();
		dynamic_configuration.set_query_timeout(self.query_timeout);
		#[cfg(feature = "http")]
		let http_client = Arc::new(
			HttpClient::new(capabilities.allow_net.clone(), capabilities.deny_net.clone(), &config)
				.context("Could not create http client")?,
		);

		Ok(Datastore {
			id,
			transaction_factory: tf.clone(),
			auth_enabled: self.authenticate,
			dynamic_configuration,
			slow_log: self.slow_log,
			transaction_timeout: self.transaction_timeout,
			live_query_broker: self.live_query_broker,
			http_endpoint: self.http_endpoint,
			capabilities,
			index_stores: IndexStores::new(config.hnsw_cache_size, config.diskann_cache_size),
			index_builder: IndexBuilder::new(tf.clone()),
			#[cfg(storage)]
			temporary_directory: self.temporary_directory,
			cache: Arc::new(DatastoreCache::new(config.datastore_cache_size)),
			function_registry: Arc::new(FunctionRegistry::with_builtins()),
			buckets,
			sequences: Sequences::new(tf, id),
			async_event_trigger,
			#[cfg(feature = "surrealism")]
			surrealism_cache: Arc::new(SurrealismCache::new(config.surrealism_cache_size)),
			#[cfg(feature = "surrealism")]
			lazy_surrealism: self.lazy_surrealism,
			#[cfg(feature = "http")]
			http_client,
			observer,
			config,
		})
	}
}

#[cfg(test)]
mod tests {
	use std::fmt::{self, Display};
	use std::future::Future;
	use std::pin::Pin;
	use std::sync::Arc;

	use anyhow::{Result, bail};
	use tokio_util::sync::CancellationToken;

	use super::Builder;
	use crate::buc::store::ObjectStore;
	use crate::buc::{
		BucketStoreProvider, BucketStoreProviderRequirements, Config as BucketConfig,
	};
	use crate::cnf::ConfigMap;
	use crate::kvs::api::BoxFut;
	use crate::kvs::{
		Metrics, Transactable, TransactionBuilder, TransactionBuilderFactory,
		TransactionBuilderFactoryRequirements, TransactionBuilderParts,
		TransactionBuilderRequirements,
	};

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
		) -> Pin<Box<dyn Future<Output = Result<Arc<dyn ObjectStore>>> + 'a + Send + Sync>> {
			Box::pin(async { bail!("test bucket connections are not used") })
		}
	}

	impl TransactionBuilderFactoryRequirements for TestComposer {}

	impl TransactionBuilderFactory for TestComposer {
		type RouterState = TestRouterState;

		async fn new_transaction_builder(
			&self,
			_path: &str,
			_canceller: CancellationToken,
			_config: ConfigMap,
		) -> Result<TransactionBuilderParts<Self::RouterState>> {
			Ok(TransactionBuilderParts::new(Box::new(TestTransactionBuilder), self.state.clone()))
		}

		fn path_valid(v: &str) -> Result<String> {
			Ok(v.to_owned())
		}
	}

	struct TestTransactionBuilder;

	impl Display for TestTransactionBuilder {
		fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
			f.write_str("test")
		}
	}

	impl TransactionBuilderRequirements for TestTransactionBuilder {}

	impl TransactionBuilder for TestTransactionBuilder {
		fn new_transaction(
			&self,
			_write: bool,
			_lock: bool,
		) -> BoxFut<'_, Result<(Box<dyn Transactable>, bool)>> {
			Box::pin(async move { unreachable!("test does not open transactions") })
		}

		fn shutdown(&self) -> BoxFut<'_, Result<()>> {
			Box::pin(async move { Ok(()) })
		}

		fn register_metrics(&self) -> Option<Metrics> {
			None
		}

		fn collect_u64_metric(&self, _metric: &str) -> Option<u64> {
			None
		}
	}

	#[tokio::test]
	async fn build_with_factory_path_returns_router_state() -> Result<()> {
		let expected = Arc::new(7);
		let composer = TestComposer {
			state: TestRouterState(Arc::clone(&expected)),
		};

		let (_datastore, router_state) =
			Builder::new().build_with_factory_path_and_router_state("test:", composer).await?;

		assert!(Arc::ptr_eq(&expected, &router_state.0));
		Ok(())
	}

	#[tokio::test]
	async fn build_with_factory_path_keeps_legacy_shape() -> Result<()> {
		let composer = TestComposer {
			state: TestRouterState(Arc::new(7)),
		};

		let datastore = Builder::new().build_with_factory_path("test:", composer).await?;

		assert_eq!(datastore.to_string(), "test");
		Ok(())
	}
}
