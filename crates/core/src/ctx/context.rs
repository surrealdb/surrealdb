use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt::{self, Debug};
#[cfg(storage)]
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use anyhow::{Result, bail};
use async_channel::Sender;
use trice::Instant;
#[cfg(feature = "http")]
use url::Url;

use crate::buc::store::ObjectStore;
use crate::buc::{self, BucketConnectionKey, BucketConnections};
use crate::catalog::{DatabaseDefinition, DatabaseId, NamespaceId};
use crate::cnf::PROTECTED_PARAM_NAMES;
use crate::ctx::canceller::Canceller;
use crate::ctx::reason::Reason;
#[cfg(feature = "http")]
use crate::dbs::capabilities::NetTarget;
use crate::dbs::{Capabilities, Notification, Options, Session, Variables};
use crate::err::Error;
use crate::idx::planner::executor::QueryExecutor;
use crate::idx::planner::{IterationStage, QueryPlanner};
use crate::idx::trees::store::IndexStores;
#[cfg(not(target_family = "wasm"))]
use crate::kvs::IndexBuilder;
use crate::kvs::Transaction;
use crate::kvs::cache::ds::DatastoreCache;
use crate::kvs::sequences::Sequences;
use crate::mem::ALLOC;
use crate::val::Value;

pub type Context = Arc<MutableContext>;

pub struct MutableContext {
	// An optional parent context.
	parent: Option<Context>,
	// An optional deadline.
	deadline: Option<Instant>,
	// An optional slow log threshold
	slow_log_threshold: Option<Duration>,
	// Whether or not this context is cancelled.
	cancelled: Arc<AtomicBool>,
	// A collection of read only values stored in this context.
	values: HashMap<Cow<'static, str>, Arc<Value>>,
	// Stores the notification channel if available
	notifications: Option<Sender<Notification>>,
	// An optional query planner
	query_planner: Option<Arc<QueryPlanner>>,
	// An optional query executor
	query_executor: Option<QueryExecutor>,
	// An optional iteration stage
	iteration_stage: Option<IterationStage>,
	// An optional datastore cache
	cache: Option<Arc<DatastoreCache>>,
	// The index store
	index_stores: IndexStores,
	// The index concurrent builders
	#[cfg(not(target_family = "wasm"))]
	index_builder: Option<IndexBuilder>,
	// The sequences
	sequences: Option<Sequences>,
	// Capabilities
	capabilities: Arc<Capabilities>,
	#[cfg(storage)]
	// The temporary directory
	temporary_directory: Option<Arc<PathBuf>>,
	// An optional transaction
	transaction: Option<Arc<Transaction>>,
	// Does not read from parent `values`.
	isolated: bool,
	// A map of bucket connections
	buckets: Option<Arc<BucketConnections>>,
}

impl Default for MutableContext {
	fn default() -> Self {
		MutableContext::background()
	}
}

impl From<Transaction> for MutableContext {
	fn from(txn: Transaction) -> Self {
		let mut ctx = MutableContext::background();
		ctx.set_transaction(Arc::new(txn));
		ctx
	}
}

impl Debug for MutableContext {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		f.debug_struct("Context")
			.field("parent", &self.parent)
			.field("deadline", &self.deadline)
			.field("cancelled", &self.cancelled)
			.field("values", &self.values)
			.finish()
	}
}

impl MutableContext {
	/// Creates a new empty background context.
	pub(crate) fn background() -> Self {
		Self {
			values: HashMap::default(),
			parent: None,
			deadline: None,
			slow_log_threshold: None,
			cancelled: Arc::new(AtomicBool::new(false)),
			notifications: None,
			query_planner: None,
			query_executor: None,
			iteration_stage: None,
			capabilities: Arc::new(Capabilities::default()),
			index_stores: IndexStores::default(),
			cache: None,
			#[cfg(not(target_family = "wasm"))]
			index_builder: None,
			sequences: None,
			#[cfg(storage)]
			temporary_directory: None,
			transaction: None,
			isolated: false,
			buckets: None,
		}
	}

	/// Creates a new context from a frozen parent context.
	pub(crate) fn new(parent: &Context) -> Self {
		MutableContext {
			values: HashMap::default(),
			deadline: parent.deadline,
			slow_log_threshold: parent.slow_log_threshold,
			cancelled: Arc::new(AtomicBool::new(false)),
			notifications: parent.notifications.clone(),
			query_planner: parent.query_planner.clone(),
			query_executor: parent.query_executor.clone(),
			iteration_stage: parent.iteration_stage.clone(),
			capabilities: parent.capabilities.clone(),
			index_stores: parent.index_stores.clone(),
			cache: parent.cache.clone(),
			#[cfg(not(target_family = "wasm"))]
			index_builder: parent.index_builder.clone(),
			sequences: parent.sequences.clone(),
			#[cfg(storage)]
			temporary_directory: parent.temporary_directory.clone(),
			transaction: parent.transaction.clone(),
			isolated: false,
			parent: Some(parent.clone()),
			buckets: parent.buckets.clone(),
		}
	}

	/// Create a new context from a frozen parent context.
	/// This context is isolated, and values specified on
	/// any parent contexts will not be accessible.
	pub(crate) fn new_isolated(parent: &Context) -> Self {
		Self {
			values: HashMap::default(),
			deadline: parent.deadline,
			slow_log_threshold: parent.slow_log_threshold,
			cancelled: Arc::new(AtomicBool::new(false)),
			notifications: parent.notifications.clone(),
			query_planner: parent.query_planner.clone(),
			query_executor: parent.query_executor.clone(),
			iteration_stage: parent.iteration_stage.clone(),
			capabilities: parent.capabilities.clone(),
			index_stores: parent.index_stores.clone(),
			cache: parent.cache.clone(),
			#[cfg(not(target_family = "wasm"))]
			index_builder: parent.index_builder.clone(),
			sequences: parent.sequences.clone(),
			#[cfg(storage)]
			temporary_directory: parent.temporary_directory.clone(),
			transaction: parent.transaction.clone(),
			isolated: true,
			parent: Some(parent.clone()),
			buckets: parent.buckets.clone(),
		}
	}

	/// Create a new context from a frozen parent context.
	/// This context is not linked to the parent context,
	/// and won't be cancelled if the parent is cancelled.
	#[cfg(not(target_family = "wasm"))]
	pub(crate) fn new_concurrent(from: &Context) -> Self {
		Self {
			values: HashMap::default(),
			deadline: None,
			slow_log_threshold: from.slow_log_threshold,
			cancelled: Arc::new(AtomicBool::new(false)),
			notifications: from.notifications.clone(),
			query_planner: from.query_planner.clone(),
			query_executor: from.query_executor.clone(),
			iteration_stage: from.iteration_stage.clone(),
			capabilities: from.capabilities.clone(),
			index_stores: from.index_stores.clone(),
			cache: from.cache.clone(),
			index_builder: from.index_builder.clone(),
			sequences: from.sequences.clone(),
			#[cfg(storage)]
			temporary_directory: from.temporary_directory.clone(),
			transaction: None,
			isolated: false,
			parent: None,
			buckets: from.buckets.clone(),
		}
	}

	/// Creates a new context from a configured datastore.
	#[expect(clippy::too_many_arguments)]
	pub(crate) fn from_ds(
		time_out: Option<Duration>,
		slow_log_threshold: Option<Duration>,
		capabilities: Arc<Capabilities>,
		index_stores: IndexStores,
		#[cfg(not(target_family = "wasm"))] index_builder: IndexBuilder,
		sequences: Sequences,
		cache: Arc<DatastoreCache>,
		#[cfg(storage)] temporary_directory: Option<Arc<PathBuf>>,
		buckets: Arc<BucketConnections>,
	) -> Result<MutableContext> {
		let mut ctx = Self {
			values: HashMap::default(),
			parent: None,
			deadline: None,
			slow_log_threshold,
			cancelled: Arc::new(AtomicBool::new(false)),
			notifications: None,
			query_planner: None,
			query_executor: None,
			iteration_stage: None,
			capabilities,
			index_stores,
			cache: Some(cache),
			#[cfg(not(target_family = "wasm"))]
			index_builder: Some(index_builder),
			sequences: Some(sequences),
			#[cfg(storage)]
			temporary_directory,
			transaction: None,
			isolated: false,
			buckets: Some(buckets),
		};
		if let Some(timeout) = time_out {
			ctx.add_timeout(timeout)?;
		}
		Ok(ctx)
	}

	/// Freezes this context, allowing it to be used as a parent context.
	pub(crate) fn freeze(self) -> Context {
		Arc::new(self)
	}

	/// Unfreezes this context, allowing it to be edited and configured.
	pub(crate) fn unfreeze(ctx: Context) -> Result<MutableContext> {
		let Some(x) = Arc::into_inner(ctx) else {
			fail!("Tried to unfreeze a Context with multiple references")
		};
		Ok(x)
	}

	/// Get the namespace id for the current context.
	/// If the namespace does not exist, it will be try to be created based on
	/// the `strict` option.
	pub(crate) async fn get_ns_id(&self, opt: &Options) -> Result<NamespaceId> {
		let ns = opt.ns()?;
		let ns_def = self.tx().get_or_add_ns(ns, opt.strict).await?;
		Ok(ns_def.namespace_id)
	}

	/// Get the namespace id for the current context.
	/// If the namespace does not exist, it will return an error.
	pub(crate) async fn expect_ns_id(&self, opt: &Options) -> Result<NamespaceId> {
		let ns = opt.ns()?;
		let Some(ns_def) = self.tx().get_ns_by_name(ns).await? else {
			return Err(Error::NsNotFound {
				name: ns.to_string(),
			}
			.into());
		};
		Ok(ns_def.namespace_id)
	}

	/// Get the namespace and database ids for the current context.
	/// If the namespace or database does not exist, it will be try to be
	/// created based on the `strict` option.
	pub(crate) async fn get_ns_db_ids(&self, opt: &Options) -> Result<(NamespaceId, DatabaseId)> {
		let (ns, db) = opt.ns_db()?;
		let db_def = self.tx().ensure_ns_db(ns, db, opt.strict).await?;
		Ok((db_def.namespace_id, db_def.database_id))
	}

	/// Get the namespace and database ids for the current context.
	/// If the namespace or database does not exist, it will return an error.
	pub(crate) async fn expect_ns_db_ids(
		&self,
		opt: &Options,
	) -> Result<(NamespaceId, DatabaseId)> {
		let (ns, db) = opt.ns_db()?;
		let Some(db_def) = self.tx().get_db_by_name(ns, db).await? else {
			return Err(Error::DbNotFound {
				name: db.to_string(),
			}
			.into());
		};
		Ok((db_def.namespace_id, db_def.database_id))
	}

	pub(crate) async fn get_db(&self, opt: &Options) -> Result<Arc<DatabaseDefinition>> {
		let (ns, db) = opt.ns_db()?;
		let db_def = self.tx().ensure_ns_db(ns, db, opt.strict).await?;
		Ok(db_def)
	}

	/// Add a value to the context. It overwrites any previously set values
	/// with the same key.
	pub(crate) fn add_value<K>(&mut self, key: K, value: Arc<Value>)
	where
		K: Into<Cow<'static, str>>,
	{
		self.values.insert(key.into(), value);
	}

	/// Add a value to the context. It overwrites any previously set values
	/// with the same key.
	pub(crate) fn add_values<T, K, V>(&mut self, iter: T)
	where
		T: IntoIterator<Item = (K, V)>,
		K: Into<Cow<'static, str>>,
		V: Into<Arc<Value>>,
	{
		self.values.extend(iter.into_iter().map(|(k, v)| (k.into(), v.into())))
	}

	/// Add cancellation to the context. The value that is returned will cancel
	/// the context and it's children once called.
	pub(crate) fn add_cancel(&mut self) -> Canceller {
		let cancelled = self.cancelled.clone();
		Canceller::new(cancelled)
	}

	/// Add a deadline to the context. If the current deadline is sooner than
	/// the provided deadline, this method does nothing.
	pub(crate) fn add_deadline(&mut self, deadline: Instant) {
		match self.deadline {
			Some(current) if current < deadline => (),
			_ => self.deadline = Some(deadline),
		}
	}

	/// Add a timeout to the context. If the current timeout is sooner than
	/// the provided timeout, this method does nothing. If the result of the
	/// addition causes an overflow, this method returns an error.
	pub(crate) fn add_timeout(&mut self, timeout: Duration) -> Result<(), Error> {
		match Instant::now().checked_add(timeout) {
			Some(deadline) => {
				self.add_deadline(deadline);
				Ok(())
			}
			None => Err(Error::InvalidTimeout(timeout.as_secs())),
		}
	}

	/// Add the LIVE query notification channel to the context, so that we
	/// can send notifications to any subscribers.
	pub(crate) fn add_notifications(&mut self, chn: Option<&Sender<Notification>>) {
		self.notifications = chn.cloned()
	}

	pub(crate) fn set_query_planner(&mut self, qp: QueryPlanner) {
		self.query_planner = Some(Arc::new(qp));
	}

	/// Cache a table-specific QueryExecutor in the Context.
	///
	/// This is set by the collector/processor when iterating over a specific
	/// table or index so that downstream per-record operations can access the
	/// executor without repeatedly looking it up from the QueryPlanner.
	pub(crate) fn set_query_executor(&mut self, qe: QueryExecutor) {
		self.query_executor = Some(qe);
	}

	pub(crate) fn set_iteration_stage(&mut self, is: IterationStage) {
		self.iteration_stage = Some(is);
	}

	pub(crate) fn set_transaction(&mut self, txn: Arc<Transaction>) {
		self.transaction = Some(txn);
	}

	pub(crate) fn tx(&self) -> Arc<Transaction> {
		self.transaction
			.clone()
			.unwrap_or_else(|| unreachable!("The context was not associated with a transaction"))
	}

	/// Get the timeout for this operation, if any. This is useful for
	/// checking if a long job should be started or not.
	pub(crate) fn timeout(&self) -> Option<Duration> {
		self.deadline.map(|v| v.saturating_duration_since(Instant::now()))
	}

	pub(crate) fn slow_log_threshold(&self) -> Option<Duration> {
		self.slow_log_threshold
	}

	pub(crate) fn notifications(&self) -> Option<Sender<Notification>> {
		self.notifications.clone()
	}

	pub(crate) fn has_notifications(&self) -> bool {
		self.notifications.is_some()
	}

	pub(crate) fn get_query_planner(&self) -> Option<&QueryPlanner> {
		self.query_planner.as_ref().map(|qp| qp.as_ref())
	}

	/// Get the cached QueryExecutor (if any) attached by the current iteration
	/// context.
	pub(crate) fn get_query_executor(&self) -> Option<&QueryExecutor> {
		self.query_executor.as_ref()
	}

	pub(crate) fn get_iteration_stage(&self) -> Option<&IterationStage> {
		self.iteration_stage.as_ref()
	}

	/// Get the index_store for this context/ds
	pub(crate) fn get_index_stores(&self) -> &IndexStores {
		&self.index_stores
	}

	/// Get the index_builder for this context/ds
	#[cfg(not(target_family = "wasm"))]
	pub(crate) fn get_index_builder(&self) -> Option<&IndexBuilder> {
		self.index_builder.as_ref()
	}

	/// Return the sequences manager
	pub(crate) fn get_sequences(&self) -> Option<&Sequences> {
		self.sequences.as_ref()
	}

	pub(crate) fn try_get_sequences(&self) -> Result<&Sequences> {
		if let Some(sqs) = self.get_sequences() {
			Ok(sqs)
		} else {
			bail!(Error::Internal("Sequences are not supported in this context.".to_string(),))
		}
	}

	// Get the current datastore cache
	pub(crate) fn get_cache(&self) -> Option<Arc<DatastoreCache>> {
		self.cache.clone()
	}

	/// Check if the context is done. If it returns `None` the operation may
	/// proceed, otherwise the operation should be stopped.
	/// Note regarding `deep_check`:
	/// Checking Instant::now() takes tens to hundreds of nanoseconds
	/// Checking an AtomicBool takes a single-digit nanoseconds.
	/// We may not want to check for the deadline on every call.
	/// An iteration loop may want to check it every 10 or 100 calls.
	/// Eg.: ctx.done(count % 100 == 0)
	pub(crate) fn done(&self, deep_check: bool) -> Result<Option<Reason>> {
		match self.deadline {
			Some(deadline) if deep_check && deadline <= Instant::now() => {
				Ok(Some(Reason::Timedout))
			}
			_ if self.cancelled.load(Ordering::Relaxed) => Ok(Some(Reason::Canceled)),
			_ => {
				if deep_check && ALLOC.is_beyond_threshold() {
					bail!(Error::QueryBeyondMemoryThreshold);
				}
				match &self.parent {
					Some(ctx) => ctx.done(deep_check),
					_ => Ok(None),
				}
			}
		}
	}

	/// Check if the context is ok to continue.
	pub(crate) async fn is_ok(&self, deep_check: bool) -> Result<bool> {
		if deep_check {
			yield_now!();
		}
		Ok(self.done(deep_check)?.is_none())
	}

	/// Check if there is some reason to stop processing the current query.
	///
	/// Returns true when the query is canceled or if check_deadline is true
	/// when the query deadline is met.
	pub(crate) async fn is_done(&self, deep_check: bool) -> Result<bool> {
		if deep_check {
			yield_now!();
		}
		Ok(self.done(deep_check)?.is_some())
	}

	/// Check if the context is not ok to continue, because it timed out.
	pub(crate) async fn is_timedout(&self) -> Result<bool> {
		yield_now!();
		Ok(matches!(self.done(true)?, Some(Reason::Timedout)))
	}

	#[cfg(storage)]
	/// Return the location of the temporary directory if any
	pub(crate) fn temporary_directory(&self) -> Option<&Arc<PathBuf>> {
		self.temporary_directory.as_ref()
	}

	/// Get a value from the context. If no value is stored under the
	/// provided key, then this will return None.
	pub(crate) fn value(&self, key: &str) -> Option<&Value> {
		match self.values.get(key) {
			Some(v) => Some(v.as_ref()),
			None if PROTECTED_PARAM_NAMES.contains(&key) || !self.isolated => match &self.parent {
				Some(p) => p.value(key),
				_ => None,
			},
			None => None,
		}
	}

	/// Get a 'static view into the cancellation status.
	#[cfg(feature = "scripting")]
	pub(crate) fn cancellation(&self) -> crate::ctx::cancellation::Cancellation {
		crate::ctx::cancellation::Cancellation::new(
			self.deadline,
			std::iter::successors(Some(self), |ctx| ctx.parent.as_ref().map(|c| c.as_ref()))
				.map(|ctx| ctx.cancelled.clone())
				.collect(),
		)
	}

	/// Attach a session to the context and add any session variables to the
	/// context.
	pub(crate) fn attach_session(&mut self, session: &Session) -> Result<(), Error> {
		self.add_values(session.values());
		if !session.variables.is_empty() {
			self.attach_variables(session.variables.clone())?;
		}
		Ok(())
	}

	/// Attach variables to the context.
	pub(crate) fn attach_variables(&mut self, vars: Variables) -> Result<(), Error> {
		for (key, val) in vars {
			if PROTECTED_PARAM_NAMES.contains(&key.as_str()) {
				return Err(Error::InvalidParam {
					name: key.clone(),
				});
			}
			self.add_value(key, val.into());
		}
		Ok(())
	}

	//
	// Capabilities
	//

	/// Set the capabilities for this context
	pub(crate) fn add_capabilities(&mut self, caps: Arc<Capabilities>) {
		self.capabilities = caps;
	}

	/// Get the capabilities for this context
	pub(crate) fn get_capabilities(&self) -> Arc<Capabilities> {
		self.capabilities.clone()
	}

	/// Check if scripting is allowed
	#[cfg_attr(not(feature = "scripting"), expect(dead_code))]
	pub(crate) fn check_allowed_scripting(&self) -> Result<()> {
		if !self.capabilities.allows_scripting() {
			warn!("Capabilities denied scripting attempt");
			bail!(Error::ScriptingNotAllowed);
		}
		trace!("Capabilities allowed scripting");
		Ok(())
	}

	/// Check if a function is allowed
	pub(crate) fn check_allowed_function(&self, target: &str) -> Result<()> {
		if !self.capabilities.allows_function_name(target) {
			warn!("Capabilities denied function execution attempt, target: '{target}'");
			bail!(Error::FunctionNotAllowed(target.to_string()));
		}
		trace!("Capabilities allowed function execution, target: '{target}'");
		Ok(())
	}

	/// Checks if the provided URL's network target is allowed based on current
	/// capabilities.
	///
	/// This function performs a validation to ensure that the outgoing network
	/// connection specified by the provided `url` is permitted. It checks the
	/// resolved network targets associated with the URL and ensures that all
	/// targets adhere to the configured capabilities.
	///
	/// # Features
	/// The function is only available if the `http` feature is enabled.
	///
	/// # Parameters
	/// - `url`: A reference to a [`Url`] object representing the target endpoint to check.
	///
	/// # Returns
	/// This function returns a [`Result<()>`]:
	/// - On success, it returns `Ok(())` indicating the network target is allowed.
	/// - On failure, it returns an error wrapped in the [`Error`] type:
	///   - `NetTargetNotAllowed` if the target is not permitted.
	///   - `InvalidUrl` if the provided URL is invalid.
	///
	/// # Behavior
	/// 1. Extracts the host and port information from the URL.
	/// 2. Constructs a [`NetTarget`] object and checks if it is allowed by the current network
	///    capabilities.
	/// 3. If the network target resolves to multiple targets (e.g., DNS resolution), each target is
	///    validated individually.
	/// 4. Logs a warning and prevents the connection if the target is denied by the capabilities.
	///
	/// # Logging
	/// - Logs a warning message if the network target is denied.
	/// - Logs a trace message if the network target is permitted.
	///
	/// # Errors
	/// - `NetTargetNotAllowed`: Returned if any of the resolved targets are not allowed.
	/// - `InvalidUrl`: Returned if the URL does not have a valid host.
	#[cfg(feature = "http")]
	pub(crate) async fn check_allowed_net(&self, url: &Url) -> Result<()> {
		let match_any_deny_net = |t| {
			if self.capabilities.matches_any_deny_net(t) {
				warn!("Capabilities denied outgoing network connection attempt, target: '{t}'");
				bail!(Error::NetTargetNotAllowed(t.to_string()));
			}
			Ok(())
		};
		match url.host() {
			Some(host) => {
				let target = NetTarget::Host(host.to_owned(), url.port_or_known_default());
				// Check the domain name (if any) matches the allow list
				let host_allowed = self.capabilities.matches_any_allow_net(&target);
				if !host_allowed {
					warn!(
						"Capabilities denied outgoing network connection attempt, target: '{target}'"
					);
					bail!(Error::NetTargetNotAllowed(target.to_string()));
				}
				// Check against the deny list
				match_any_deny_net(&target)?;
				// Resolve the domain name to a vector of IP addresses
				#[cfg(not(target_family = "wasm"))]
				let targets = target.resolve().await?;
				#[cfg(target_family = "wasm")]
				let targets = target.resolve()?;
				for t in &targets {
					// For each IP address resolved, check it is allowed
					match_any_deny_net(t)?;
				}
				trace!("Capabilities allowed outgoing network connection, target: '{target}'");
				Ok(())
			}
			_ => bail!(Error::InvalidUrl(url.to_string())),
		}
	}

	pub(crate) fn get_buckets(&self) -> Option<Arc<BucketConnections>> {
		self.buckets.clone()
	}

	/// Obtain the connection for a bucket
	pub(crate) async fn get_bucket_store(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		bu: &str,
	) -> Result<Arc<dyn ObjectStore>> {
		// Do we have a buckets context?
		if let Some(buckets) = &self.buckets {
			// Attempt to obtain an existing bucket connection
			let key = BucketConnectionKey::new(ns, db, bu);
			if let Some(bucket_ref) = buckets.get(&key) {
				Ok((*bucket_ref).clone())
			} else {
				// Obtain the bucket definition
				let tx = self.tx();
				let bd = tx.expect_db_bucket(ns, db, bu).await?;

				// Connect to the bucket
				let store = if let Some(ref backend) = bd.backend {
					buc::connect(backend, false, bd.readonly).await?
				} else {
					buc::connect_global(ns, db, bu).await?
				};

				// Persist the bucket connection
				buckets.insert(key, store.clone());
				Ok(store)
			}
		} else {
			bail!(Error::BucketUnavailable(bu.into()))
		}
	}
}

#[cfg(test)]
mod tests {
	#[cfg(feature = "http")]
	use std::str::FromStr;

	#[cfg(feature = "http")]
	use url::Url;

	#[cfg(feature = "http")]
	use crate::ctx::MutableContext;
	#[cfg(feature = "http")]
	use crate::dbs::Capabilities;
	#[cfg(feature = "http")]
	use crate::dbs::capabilities::{NetTarget, Targets};

	#[cfg(feature = "http")]
	#[tokio::test]
	async fn test_context_check_allowed_net() {
		let cap = Capabilities::all().without_network_targets(Targets::Some(
			[NetTarget::from_str("127.0.0.1").unwrap()].into(),
		));
		let mut ctx = MutableContext::background();
		ctx.capabilities = cap.into();
		let ctx = ctx.freeze();
		let r = ctx.check_allowed_net(&Url::parse("http://localhost").unwrap()).await;
		assert_eq!(
			r.err().unwrap().to_string(),
			"Access to network target '127.0.0.1/32' is not allowed"
		);
	}
}
