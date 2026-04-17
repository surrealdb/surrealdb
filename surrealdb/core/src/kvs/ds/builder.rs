use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

#[cfg(feature = "http")]
use anyhow::Context as _;
use anyhow::Result;
use async_channel::Sender;
use tokio::sync::Notify;
#[cfg(feature = "jwks")]
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::CommunityComposer;
use crate::buc::BucketStoreProvider;
use crate::buc::manager::BucketsManager;
use crate::cnf::dynamic::DynamicConfiguration;
use crate::dbs::Capabilities;
#[cfg(feature = "http")]
use crate::http::HttpClient;
#[cfg(feature = "jwks")]
use crate::iam::jwks::JwksCache;
use crate::idx::trees::store::IndexStores;
use crate::kvs::cache::ds::DatastoreCache;
use crate::kvs::index::IndexBuilder;
use crate::kvs::sequences::Sequences;
use crate::kvs::slowlog::SlowLog;
use crate::kvs::{Datastore, TransactionBuilder, TransactionBuilderFactory, TransactionFactory};
#[cfg(feature = "surrealism")]
use crate::surrealism::cache::SurrealismCache;
use crate::types::PublicNotification;

/// A builder struct for creating a Datastore.
pub struct Builder {
	capabilities: Capabilities,
	shutdown: CancellationToken,
	notify_channel: Option<Sender<PublicNotification>>,
	id: Option<Uuid>,
	slow_log: Option<SlowLog>,
	transaction_timeout: Option<Duration>,
	query_timeout: Option<Duration>,
	temporary_directory: Option<Arc<PathBuf>>,
	authenticate: bool,
	#[cfg(feature = "surrealism")]
	lazy_surrealism: bool,
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
			id: None,
			slow_log: None,
			transaction_timeout: None,
			query_timeout: None,
			temporary_directory: None,
			authenticate: false,
			#[cfg(feature = "surrealism")]
			lazy_surrealism: false,
		}
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

	pub async fn build_with_path(self, path: &str) -> Result<Datastore> {
		self.build_with_factory_path(path, CommunityComposer()).await
	}

	pub async fn build_with_factory_path<F>(self, path: &str, composer: F) -> Result<Datastore>
	where
		F: TransactionBuilderFactory + BucketStoreProvider + 'static,
	{
		let tx_builder = composer.new_transaction_builder(path, self.shutdown.clone()).await?;
		let buckets = BucketsManager::new(Arc::new(composer));

		self.build_with_tx_builder_buckets(tx_builder, buckets).await
	}

	pub(crate) async fn build_with_tx_builder_buckets(
		self,
		builder: Box<dyn TransactionBuilder>,
		buckets: BucketsManager,
	) -> Result<Datastore> {
		let async_event_trigger = Arc::new(Notify::new());
		let tf = TransactionFactory::new(async_event_trigger.clone(), builder);
		let id = self.id.unwrap_or_else(Uuid::new_v4);
		let capabilities = Arc::new(self.capabilities);
		let dynamic_configuration = DynamicConfiguration::default();
		dynamic_configuration.set_query_timeout(self.query_timeout);
		#[cfg(feature = "http")]
		let http_client = Arc::new(
			HttpClient::new(capabilities.allow_net.clone(), capabilities.deny_net.clone())
				.context("Could not create http client")?,
		);

		Ok(Datastore {
			id,
			transaction_factory: tf.clone(),
			auth_enabled: self.authenticate,
			dynamic_configuration,
			slow_log: self.slow_log,
			transaction_timeout: self.transaction_timeout,
			notification_channel: self.notify_channel,
			capabilities,
			index_stores: IndexStores::default(),
			index_builder: IndexBuilder::new(tf.clone()),
			#[cfg(feature = "jwks")]
			jwks_cache: Arc::new(RwLock::new(JwksCache::new())),
			#[cfg(storage)]
			temporary_directory: self.temporary_directory,
			cache: Arc::new(DatastoreCache::new()),
			buckets,
			sequences: Sequences::new(tf, id),
			#[cfg(feature = "surrealism")]
			surrealism_cache: Arc::new(SurrealismCache::new()),
			async_event_trigger,
			#[cfg(feature = "surrealism")]
			lazy_surrealism: self.lazy_surrealism,
			#[cfg(feature = "http")]
			http_client,
		})
	}
}
