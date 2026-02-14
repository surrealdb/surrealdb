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
#[cfg(feature = "surrealism")]
use surrealism_runtime::controller::Runtime;
#[cfg(feature = "surrealism")]
use surrealism_runtime::package::SurrealismPackage;
use trice::Instant;
#[cfg(feature = "http")]
use url::Url;

use crate::buc::manager::BucketsManager;
#[cfg(feature = "surrealism")]
use crate::buc::store::ObjectKey;
use crate::buc::store::ObjectStore;
use crate::catalog::providers::{CatalogProvider, DatabaseProvider, NamespaceProvider};
use crate::catalog::{DatabaseDefinition, DatabaseId, NamespaceId};
use crate::cnf::PROTECTED_PARAM_NAMES;
use crate::ctx::canceller::Canceller;
use crate::ctx::reason::Reason;
#[cfg(feature = "surrealism")]
use crate::dbs::capabilities::ExperimentalTarget;
#[cfg(feature = "http")]
use crate::dbs::capabilities::NetTarget;
use crate::dbs::{Capabilities, NewPlannerStrategy, Options, Session, Variables};
use crate::err::Error;
use crate::exec::function::FunctionRegistry;
use crate::idx::planner::executor::QueryExecutor;
use crate::idx::planner::{IterationStage, QueryPlanner};
use crate::idx::trees::store::IndexStores;
use crate::kvs::Transaction;
use crate::kvs::cache::ds::DatastoreCache;
use crate::kvs::index::IndexBuilder;
use crate::kvs::sequences::Sequences;
use crate::kvs::slowlog::SlowLog;
use crate::mem::ALLOC;
use crate::sql::expression::convert_public_value_to_internal;
#[cfg(feature = "surrealism")]
use crate::surrealism::cache::{SurrealismCache, SurrealismCacheLookup};
use crate::types::{PublicNotification, PublicVariables};
use crate::val::Value;

pub type FrozenContext = Arc<Context>;

pub struct Context {
	// An optional parent context.
	parent: Option<FrozenContext>,
	// An optional deadline.
	deadline: Option<(Instant, Duration)>,
	// An optional slow log configuration used by the executor to log statements
	// that exceed a given duration threshold. This configuration is propagated
	// from the datastore into the context for the lifetime of a request.
	slow_log: Option<SlowLog>,
	// Whether or not this context is cancelled.
	cancelled: Arc<AtomicBool>,
	// A collection of read only values stored in this context.
	values: HashMap<Cow<'static, str>, Arc<Value>>,
	// Stores the notification channel if available
	notifications: Option<Sender<PublicNotification>>,
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
	buckets: Option<BucketsManager>,
	// The surrealism cache
	#[cfg(feature = "surrealism")]
	surrealism_cache: Option<Arc<SurrealismCache>>,
	// Function registry for built-in and custom functions
	function_registry: Arc<FunctionRegistry>,
	// Strategy for the new streaming planner/executor
	new_planner_strategy: NewPlannerStrategy,
	// When true, EXPLAIN ANALYZE omits elapsed durations for deterministic test output
	redact_duration: bool,
	// Matches context for index functions (search::highlight, search::score, etc.)
	matches_context: Option<Arc<crate::exec::function::MatchesContext>>,
	// KNN context for index functions (vector::distance::knn)
	knn_context: Option<Arc<crate::exec::function::KnnContext>>,
}

impl Default for Context {
	fn default() -> Self {
		Context::background()
	}
}

impl From<Transaction> for Context {
	fn from(txn: Transaction) -> Self {
		let mut ctx = Context::background();
		ctx.set_transaction(Arc::new(txn));
		ctx
	}
}

impl Debug for Context {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		f.debug_struct("Context")
			.field("parent", &self.parent)
			.field("deadline", &self.deadline)
			.field("cancelled", &self.cancelled)
			.field("values", &self.values)
			.finish()
	}
}

impl Context {
	/// Creates a new empty background context.
	pub(crate) fn background() -> Self {
		Self {
			values: HashMap::default(),
			parent: None,
			deadline: None,
			slow_log: None,
			cancelled: Arc::new(AtomicBool::new(false)),
			notifications: None,
			query_planner: None,
			query_executor: None,
			iteration_stage: None,
			capabilities: Arc::new(Capabilities::default()),
			index_stores: IndexStores::default(),
			cache: None,
			index_builder: None,
			sequences: None,
			#[cfg(storage)]
			temporary_directory: None,
			transaction: None,
			isolated: false,
			buckets: None,
			#[cfg(feature = "surrealism")]
			surrealism_cache: None,
			function_registry: Arc::new(FunctionRegistry::with_builtins()),
			new_planner_strategy: NewPlannerStrategy::default(),
			redact_duration: false,
			matches_context: None,
			knn_context: None,
		}
	}

	/// Creates a new context from a frozen parent context.
	pub(crate) fn new(parent: &FrozenContext) -> Self {
		Context {
			values: HashMap::default(),
			deadline: parent.deadline,
			slow_log: parent.slow_log.clone(),
			cancelled: Arc::new(AtomicBool::new(false)),
			notifications: parent.notifications.clone(),
			query_planner: parent.query_planner.clone(),
			query_executor: parent.query_executor.clone(),
			iteration_stage: parent.iteration_stage.clone(),
			capabilities: parent.capabilities.clone(),
			index_stores: parent.index_stores.clone(),
			cache: parent.cache.clone(),
			index_builder: parent.index_builder.clone(),
			sequences: parent.sequences.clone(),
			#[cfg(storage)]
			temporary_directory: parent.temporary_directory.clone(),
			transaction: parent.transaction.clone(),
			isolated: false,
			parent: Some(parent.clone()),
			buckets: parent.buckets.clone(),
			#[cfg(feature = "surrealism")]
			surrealism_cache: parent.surrealism_cache.clone(),
			function_registry: parent.function_registry.clone(),
			new_planner_strategy: parent.new_planner_strategy.clone(),
			redact_duration: parent.redact_duration,
			matches_context: parent.matches_context.clone(),
			knn_context: parent.knn_context.clone(),
		}
	}

	/// Create a new context from a frozen parent context.
	/// This context is isolated, and values specified on
	/// any parent contexts will not be accessible.
	pub(crate) fn new_isolated(parent: &FrozenContext) -> Self {
		Self {
			values: HashMap::default(),
			deadline: parent.deadline,
			slow_log: parent.slow_log.clone(),
			cancelled: Arc::new(AtomicBool::new(false)),
			notifications: parent.notifications.clone(),
			query_planner: parent.query_planner.clone(),
			query_executor: parent.query_executor.clone(),
			iteration_stage: parent.iteration_stage.clone(),
			capabilities: parent.capabilities.clone(),
			index_stores: parent.index_stores.clone(),
			cache: parent.cache.clone(),
			index_builder: parent.index_builder.clone(),
			sequences: parent.sequences.clone(),
			#[cfg(storage)]
			temporary_directory: parent.temporary_directory.clone(),
			transaction: parent.transaction.clone(),
			isolated: true,
			parent: Some(parent.clone()),
			buckets: parent.buckets.clone(),
			#[cfg(feature = "surrealism")]
			surrealism_cache: parent.surrealism_cache.clone(),
			function_registry: parent.function_registry.clone(),
			new_planner_strategy: parent.new_planner_strategy.clone(),
			redact_duration: parent.redact_duration,
			matches_context: parent.matches_context.clone(),
			knn_context: parent.knn_context.clone(),
		}
	}

	/// Create a new context from a frozen parent context.
	/// This context is not linked to the parent context,
	/// and won't be cancelled if the parent is cancelled.
	pub(crate) fn new_concurrent(from: &FrozenContext) -> Self {
		Self {
			values: HashMap::default(),
			deadline: None,
			slow_log: from.slow_log.clone(),
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
			#[cfg(feature = "surrealism")]
			surrealism_cache: from.surrealism_cache.clone(),
			function_registry: from.function_registry.clone(),
			new_planner_strategy: from.new_planner_strategy.clone(),
			redact_duration: from.redact_duration,
			matches_context: from.matches_context.clone(),
			knn_context: from.knn_context.clone(),
		}
	}

	/// Creates a new context from a configured datastore.
	#[expect(clippy::too_many_arguments)]
	pub(crate) fn from_ds(
		time_out: Option<Duration>,
		slow_log: Option<SlowLog>,
		capabilities: Arc<Capabilities>,
		index_stores: IndexStores,
		index_builder: IndexBuilder,
		sequences: Sequences,
		cache: Arc<DatastoreCache>,
		#[cfg(storage)] temporary_directory: Option<Arc<PathBuf>>,
		buckets: BucketsManager,
		#[cfg(feature = "surrealism")] surrealism_cache: Arc<SurrealismCache>,
	) -> Result<Context> {
		let planner_strategy = capabilities.planner_strategy().clone();
		let mut ctx = Self {
			values: HashMap::default(),
			parent: None,
			deadline: None,
			slow_log,
			cancelled: Arc::new(AtomicBool::new(false)),
			notifications: None,
			query_planner: None,
			query_executor: None,
			iteration_stage: None,
			capabilities,
			index_stores,
			cache: Some(cache),
			index_builder: Some(index_builder),
			sequences: Some(sequences),
			#[cfg(storage)]
			temporary_directory,
			transaction: None,
			isolated: false,
			buckets: Some(buckets),
			#[cfg(feature = "surrealism")]
			surrealism_cache: Some(surrealism_cache),
			function_registry: Arc::new(FunctionRegistry::with_builtins()),
			new_planner_strategy: planner_strategy,
			redact_duration: false,
			matches_context: None,
			knn_context: None,
		};
		if let Some(timeout) = time_out {
			ctx.add_timeout(timeout)?;
		}
		Ok(ctx)
	}

	/// Freezes this context, allowing it to be used as a parent context.
	pub(crate) fn freeze(self) -> FrozenContext {
		Arc::new(self)
	}

	/// Unfreezes this context, allowing it to be edited and configured.
	pub(crate) fn unfreeze(ctx: FrozenContext) -> Result<Context> {
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
		let tx = self.tx();
		let ns_def = tx.get_or_add_ns(Some(self), ns).await?;
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
		let db_def = self.tx().ensure_ns_db(Some(self), ns, db).await?;
		Ok((db_def.namespace_id, db_def.database_id))
	}

	/// Get the namespace and database ids for the current context.
	/// If the namespace or database does not exist, it will be try to be
	/// created based on the `strict` option.
	pub(crate) async fn try_ns_db_ids(
		&self,
		opt: &Options,
	) -> Result<Option<(NamespaceId, DatabaseId)>> {
		let (ns, db) = opt.ns_db()?;
		let Some(db_def) = self.tx().get_db_by_name(ns, db).await? else {
			return Ok(None);
		};
		Ok(Some((db_def.namespace_id, db_def.database_id)))
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
		let db_def = self.tx().ensure_ns_db(Some(self), ns, db).await?;
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
	pub(crate) fn add_deadline(&mut self, deadline: Instant, duration: Duration) {
		match self.deadline {
			Some((current, _)) if current < deadline => (),
			_ => self.deadline = Some((deadline, duration)),
		}
	}

	/// Add a timeout to the context. If the current timeout is sooner than
	/// the provided timeout, this method does nothing. If the result of the
	/// addition causes an overflow, this method returns an error.
	pub(crate) fn add_timeout(&mut self, timeout: Duration) -> Result<(), Error> {
		match Instant::now().checked_add(timeout) {
			Some(deadline) => {
				self.add_deadline(deadline, timeout);
				Ok(())
			}
			None => Err(Error::InvalidTimeout(timeout.as_secs())),
		}
	}

	/// Add the LIVE query notification channel to the context, so that we
	/// can send notifications to any subscribers.
	pub(crate) fn add_notifications(&mut self, chn: Option<&Sender<PublicNotification>>) {
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
		self.deadline.map(|(v, _)| v.saturating_duration_since(Instant::now()))
	}

	/// Returns the slow log configuration, if any, attached to this context.
	/// The executor consults this to decide whether to emit slow-query log lines.
	pub(crate) fn slow_log(&self) -> Option<&SlowLog> {
		self.slow_log.as_ref()
	}

	pub(crate) fn notifications(&self) -> Option<Sender<PublicNotification>> {
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
	///
	/// # Check Priority Order
	/// The checks are performed in the following order, with earlier checks taking priority:
	/// 1. **Cancellation** (always checked): Fast atomic flag check via `self.cancelled`
	/// 2. **Memory threshold** (only if `deep_check = true`): Expensive check via
	///    `ALLOC.is_beyond_threshold()`
	/// 3. **Deadline** (only if `deep_check = true`): Moderately expensive check via
	///    `Instant::now()`
	///
	/// # Parameters
	/// - `deep_check`: When `true`, performs all checks (cancellation, memory, deadline). When
	///   `false`, only checks the cancellation flag (fast atomic operation).
	///
	/// # Performance Note
	/// - Checking an `AtomicBool` (cancellation): single-digit nanoseconds
	/// - Checking `Instant::now()` (deadline): tens to hundreds of nanoseconds
	/// - Checking `ALLOC.is_beyond_threshold()` (memory): hundreds of nanoseconds (requires lock +
	///   traversal)
	///
	/// Use `deep_check = false` in hot loops to minimize overhead while still allowing
	/// cancellation.
	pub(crate) fn done(&self, deep_check: bool) -> Result<Option<Reason>> {
		// Check cancellation FIRST (fast atomic operation)
		if self.cancelled.load(Ordering::Relaxed) {
			return Ok(Some(Reason::Canceled));
		}
		if deep_check {
			if ALLOC.is_beyond_threshold() {
				bail!(Error::QueryBeyondMemoryThreshold);
			}
			let now = Instant::now();
			if let Some((deadline, timeout)) = self.deadline
				&& deadline <= now
			{
				return Ok(Some(Reason::Timedout(timeout.into())));
			}
		}
		if let Some(ctx) = &self.parent {
			return ctx.done(deep_check);
		}
		Ok(None)
	}

	/// Check if there is some reason to stop processing the current query.
	///
	/// Returns `true` when the query should be stopped (cancelled, timed out, or exceeded memory
	/// threshold).
	///
	/// # Parameters
	/// - `count`: Optional iteration count for optimization. Pass:
	///   - `Some(count)` when called in a loop - enables adaptive checking to balance
	///     responsiveness with performance. The method will:
	///     - Yield every 32 iterations to allow other tasks to run
	///     - Perform deep checks (memory/deadline) at iterations 1, 2, 4, 8, 16, 32, then every 64
	///   - `None` when called outside a loop (e.g., single operations) - always performs a deep
	///     check for immediate cancellation/timeout detection
	///
	/// # Performance
	/// The adaptive checking strategy ("jitter-based back-off") minimizes overhead in hot loops
	/// while maintaining reasonable responsiveness to cancellation and timeout events.
	pub(crate) async fn is_done(&self, count: Option<usize>) -> Result<bool> {
		let deep_check = if let Some(count) = count {
			// We yield every 32 iterations
			if count % 32 == 0 {
				yield_now!();
			}
			// Adaptive back-off strategy for deep checks based on iteration number:
			// Check frequently early (powers of 2), then settle into every 64 iterations
			match count {
				1 | 2 | 4 | 8 | 16 | 32 => true,
				_ => count % 64 == 0,
			}
		} else {
			// No count provided - perform a deep check immediately (single operation context)
			true
		};
		Ok(self.done(deep_check)?.is_some())
	}

	/// Check if the context is not ok to continue, because it timed out.
	pub(crate) async fn is_timedout(&self) -> Result<Option<Duration>> {
		yield_now!();
		if let Some(Reason::Timedout(d)) = self.done(true)? {
			Ok(Some(d.0))
		} else {
			Ok(None)
		}
	}

	pub(crate) async fn expect_not_timedout(&self) -> Result<()> {
		if let Some(d) = self.is_timedout().await? {
			bail!(Error::QueryTimedout(d.into()))
		} else {
			Ok(())
		}
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

	/// Collect context values into the provided map, walking up parent contexts
	/// unless this context is isolated.
	pub(crate) fn collect_values(
		&self,
		map: HashMap<Cow<'static, str>, Arc<Value>>,
	) -> HashMap<Cow<'static, str>, Arc<Value>> {
		let mut map = if !self.isolated
			&& let Some(p) = &self.parent
		{
			p.collect_values(map)
		} else {
			map
		};
		self.values.iter().for_each(|(k, v)| {
			map.insert(k.clone(), v.clone());
		});
		map
	}

	/// Get a 'static view into the cancellation status.
	#[cfg(feature = "scripting")]
	pub(crate) fn cancellation(&self) -> crate::ctx::cancellation::Cancellation {
		crate::ctx::cancellation::Cancellation::new(
			self.deadline.map(|(deadline, _)| deadline),
			std::iter::successors(Some(self), |ctx| ctx.parent.as_ref().map(|c| c.as_ref()))
				.map(|ctx| ctx.cancelled.clone())
				.collect(),
		)
	}

	/// Attach a session to the context and add any session variables to the
	/// context.
	pub(crate) fn attach_session(&mut self, session: &Session) -> Result<(), Error> {
		self.add_values(session.values());
		// Only override the planner strategy if the session explicitly sets a
		// non-default value (e.g. language tests). Otherwise the capability-level
		// strategy (set via from_ds) is preserved.
		if session.new_planner_strategy != NewPlannerStrategy::default() {
			self.new_planner_strategy = session.new_planner_strategy.clone();
		}
		// Propagate duration redaction flag from session.
		if session.redact_duration {
			self.redact_duration = true;
		}
		if !session.variables.is_empty() {
			self.attach_variables(session.variables.clone().into())?;
		}
		Ok(())
	}

	/// Attach variables to the context.
	pub(crate) fn attach_variables(&mut self, vars: Variables) -> Result<(), Error> {
		for (name, val) in vars {
			if PROTECTED_PARAM_NAMES.contains(&name.as_str()) {
				return Err(Error::InvalidParam {
					name,
				});
			}
			self.add_value(name, Arc::new(val));
		}
		Ok(())
	}

	pub(crate) fn attach_public_variables(&mut self, vars: PublicVariables) -> Result<(), Error> {
		for (name, val) in vars {
			if PROTECTED_PARAM_NAMES.contains(&name.as_str()) {
				return Err(Error::InvalidParam {
					name,
				});
			}
			self.add_value(name, Arc::new(convert_public_value_to_internal(val)));
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

	/// Get the function registry for this context
	pub(crate) fn function_registry(&self) -> &Arc<FunctionRegistry> {
		&self.function_registry
	}

	/// Set the matches context for index functions (search::highlight, etc.)
	pub(crate) fn set_matches_context(&mut self, ctx: crate::exec::function::MatchesContext) {
		self.matches_context = Some(Arc::new(ctx));
	}

	/// Get the matches context for index functions
	pub(crate) fn get_matches_context(
		&self,
	) -> Option<&Arc<crate::exec::function::MatchesContext>> {
		self.matches_context.as_ref()
	}

	/// Set the KNN context for index functions (vector::distance::knn)
	pub(crate) fn set_knn_context(&mut self, ctx: Arc<crate::exec::function::KnnContext>) {
		self.knn_context = Some(ctx);
	}

	/// Get the KNN context for index functions
	pub(crate) fn get_knn_context(&self) -> Option<&Arc<crate::exec::function::KnnContext>> {
		self.knn_context.as_ref()
	}

	/// Get the new planner strategy for this context
	pub(crate) fn new_planner_strategy(&self) -> &NewPlannerStrategy {
		&self.new_planner_strategy
	}

	/// Whether EXPLAIN ANALYZE should redact elapsed durations.
	pub(crate) fn redact_duration(&self) -> bool {
		self.redact_duration
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

	pub(crate) fn get_buckets(&self) -> Option<&BucketsManager> {
		self.buckets.as_ref()
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
			buckets.get_bucket_store(&self.tx(), ns, db, bu).await
		} else {
			bail!(Error::BucketUnavailable(bu.into()))
		}
	}

	#[cfg(feature = "surrealism")]
	pub(crate) fn get_surrealism_cache(&self) -> Option<Arc<SurrealismCache>> {
		self.surrealism_cache.as_ref().map(|sc| sc.clone())
	}

	#[cfg(feature = "surrealism")]
	pub(crate) async fn get_surrealism_runtime(
		&self,
		lookup: SurrealismCacheLookup<'_>,
	) -> Result<Arc<Runtime>> {
		if !self.get_capabilities().allows_experimental(&ExperimentalTarget::Surrealism) {
			bail!(
				"Failed to get surrealism runtime: Experimental capability `surrealism` is not enabled"
			);
		}

		let Some(cache) = self.get_surrealism_cache() else {
			bail!("Surrealism cache is not available");
		};

		cache
			.get_or_insert_with(&lookup, async || {
				let SurrealismCacheLookup::File(ns, db, bucket, key) = lookup else {
					bail!("silo lookups are not supported yet");
				};

				let bucket = self.get_bucket_store(*ns, *db, bucket).await?;
				let key = ObjectKey::new(key);
				let surli = bucket
					.get(&key)
					.await
					.map_err(|e| anyhow::anyhow!("failed to get file: {}", e))?;

				let Some(surli) = surli else {
					bail!("file not found");
				};

				let package = SurrealismPackage::from_reader(std::io::Cursor::new(surli))?;
				let runtime = Arc::new(Runtime::new(package)?);

				Ok(runtime)
			})
			.await
	}
}

#[cfg(test)]
mod tests {
	#[cfg(feature = "http")]
	use std::str::FromStr;
	use std::time::Duration;

	#[cfg(feature = "http")]
	use url::Url;

	use crate::ctx::Context;
	use crate::ctx::reason::Reason;
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
		let mut ctx = Context::background();
		ctx.capabilities = cap.into();
		let ctx = ctx.freeze();
		let r = ctx.check_allowed_net(&Url::parse("http://localhost").unwrap()).await;
		assert_eq!(
			r.err().unwrap().to_string(),
			"Access to network target '127.0.0.1/32' is not allowed"
		);
	}

	#[tokio::test]
	async fn test_context_cancellation_priority() {
		// Test that cancellation is detected even when a deadline is set and exceeded
		let mut ctx = Context::background();

		// Set a deadline in the past (already exceeded)
		ctx.add_timeout(Duration::from_nanos(1)).unwrap();
		// Give time for the deadline to pass
		tokio::time::sleep(Duration::from_millis(10)).await;

		// Cancel the context
		let canceller = ctx.add_cancel();
		canceller.cancel();

		let ctx = ctx.freeze();

		// Cancellation should be detected first, not timeout
		let result = ctx.done(true);
		assert!(result.is_ok());
		assert_eq!(result.unwrap(), Some(Reason::Canceled));
	}

	#[tokio::test]
	async fn test_context_deadline_detection() {
		// Test that deadline timeout is detected when context is not cancelled
		let mut ctx = Context::background();

		// Set a very short timeout
		ctx.add_timeout(Duration::from_nanos(1)).unwrap();
		// Give time for the deadline to pass
		tokio::time::sleep(Duration::from_millis(10)).await;

		let ctx = ctx.freeze();

		// Should detect timeout
		let result = ctx.done(true);
		assert!(result.is_ok());
		assert!(matches!(result.unwrap(), Some(Reason::Timedout(_))));
	}

	#[tokio::test]
	async fn test_context_no_deadline() {
		// Test that a context without deadline or cancellation returns None
		let ctx = Context::background();
		let ctx = ctx.freeze();

		// Should return None (ok to continue)
		let result = ctx.done(true);
		assert!(result.is_ok());
		assert_eq!(result.unwrap(), None);
	}

	#[tokio::test]
	async fn test_context_is_done_adaptive_backoff() {
		// Test the adaptive back-off strategy in is_done()
		let ctx = Context::background();
		let ctx = ctx.freeze();

		// Test that early iterations trigger deep checks (1, 2, 4, 8, 16, 32)
		for count in [1, 2, 4, 8, 16, 32] {
			let result = ctx.is_done(Some(count)).await;
			assert!(result.is_ok());
			assert_eq!(result.unwrap(), false, "Count {} should not be done", count);
		}

		// Test that later iterations only check every 64
		// Count 33-63 should not trigger deep checks (except at 64)
		for count in 33..64 {
			let result = ctx.is_done(Some(count)).await;
			assert!(result.is_ok());
			assert_eq!(result.unwrap(), false, "Count {} should not be done", count);
		}

		// Count 64 should trigger a deep check (64 % 64 == 0)
		let result = ctx.is_done(Some(64)).await;
		assert!(result.is_ok());
		assert_eq!(result.unwrap(), false);
	}

	#[tokio::test]
	async fn test_context_is_done_with_none() {
		// Test that is_done(None) always performs a deep check
		let ctx = Context::background();
		let ctx = ctx.freeze();

		// Should perform deep check and return Ok(false) since no cancellation/timeout
		let result = ctx.is_done(None).await;
		assert!(result.is_ok());
		assert_eq!(result.unwrap(), false);
	}

	#[tokio::test]
	async fn test_context_is_done_detects_cancellation() {
		// Test that is_done detects cancellation
		let mut ctx = Context::background();
		let canceller = ctx.add_cancel();
		canceller.cancel();
		let ctx = ctx.freeze();

		// Should detect cancellation
		let result = ctx.is_done(None).await;
		assert!(result.is_ok());
		assert_eq!(result.unwrap(), true);

		// Should also detect with count
		let result = ctx.is_done(Some(1)).await;
		assert!(result.is_ok());
		assert_eq!(result.unwrap(), true);
	}

	/// Test documenting the expected behavior when memory threshold is exceeded.
	///
	/// Note: This test documents the expected behavior but cannot easily test actual
	/// memory threshold violations without:
	/// 1. Setting MEMORY_THRESHOLD configuration (via cnf::MEMORY_THRESHOLD)
	/// 2. Actually allocating enough memory to exceed it
	/// 3. Having the "allocation-tracking" feature enabled
	///
	/// The key behavior tested elsewhere is that when is_beyond_threshold() returns true,
	/// it takes priority over deadline timeout, which prevents OOM crashes from being
	/// masked by timeout errors.
	#[tokio::test]
	async fn test_context_memory_threshold_priority_documentation() {
		// This test documents that the priority order in done() is:
		// 1. Cancellation (always checked, fast atomic operation)
		// 2. Memory threshold (checked when deep_check=true, if beyond threshold returns Error)
		// 3. Deadline (checked when deep_check=true, returns Reason::Timedout)

		// When ALLOC.is_beyond_threshold() returns true, done() will bail with
		// Error::QueryBeyondMemoryThreshold before checking the deadline.
		// This ensures memory violations are always detected before timeout errors.

		let ctx = Context::background();
		let ctx = ctx.freeze();

		// With no memory pressure, deadline not set, and no cancellation:
		let result = ctx.done(true);
		assert!(result.is_ok());
		assert_eq!(result.unwrap(), None);
	}

	/// Integration test that actually tests memory threshold detection.
	///
	/// This test requires:
	/// 1. The "allocation-tracking" feature to be enabled
	/// 2. The "allocator" feature to be enabled (for tracking to work)
	/// 3. Running with #[serial] to avoid interference from other tests
	///
	/// The test sets SURREAL_MEMORY_THRESHOLD environment variable, allocates memory
	/// to exceed the threshold, and verifies that context.done(true) detects the violation.
	#[tokio::test]
	#[cfg(all(feature = "allocation-tracking", feature = "allocator"))]
	#[serial_test::serial]
	async fn test_context_memory_threshold_integration() {
		use crate::err::Error;

		// Set a low memory threshold (1MB) before MEMORY_THRESHOLD is accessed
		// This must happen before any code accesses cnf::MEMORY_THRESHOLD
		// Safety: This test runs with #[serial] ensuring no other tests run concurrently,
		// so there's no risk of data races when modifying the environment variable.
		unsafe {
			std::env::set_var("SURREAL_MEMORY_THRESHOLD", "1MB");
		}

		// Force reinitialization by dropping and recreating (this won't work with LazyLock)
		// Instead, we rely on this test running in isolation with #[serial]
		// and being run in a fresh process where MEMORY_THRESHOLD hasn't been accessed yet

		// Note: This test may not work reliably if MEMORY_THRESHOLD was already accessed
		// elsewhere in the test suite. The #[serial] attribute ensures tests run one at a time,
		// but doesn't guarantee a fresh process. For reliable testing, this should be run
		// as a separate integration test binary.

		// Allocate a large vector (10MB) to exceed the threshold
		// Using Vec::with_capacity to ensure the memory is actually allocated
		let _large_allocation: Vec<u8> = Vec::with_capacity(20 * 1024 * 1024);

		// Give the allocator tracking time to register the allocation
		tokio::time::sleep(Duration::from_millis(10)).await;

		let ctx = Context::background();
		let ctx = ctx.freeze();

		// The memory threshold check should detect that we've exceeded the limit
		let result = ctx.done(true);

		// We expect either:
		// 1. An error if memory tracking properly detected the threshold violation
		// 2. Ok(None) if MEMORY_THRESHOLD was already initialized with default (0) before we set
		//    the environment variable
		match result {
			Err(e) => {
				// Verify it's the correct error type
				match e.downcast_ref::<Error>() {
					Some(Error::QueryBeyondMemoryThreshold) => {
						// Success! Memory threshold was properly detected
						println!("✓ Memory threshold violation detected as expected");
					}
					other => {
						panic!("Expected QueryBeyondMemoryThreshold error, got: {:?}", other);
					}
				}
			}
			Ok(None) => {
				// This means MEMORY_THRESHOLD was already initialized before we set the env var
				// This is expected behavior in the test suite - document it
				println!(
					"⚠ Memory threshold not enforced - MEMORY_THRESHOLD was already initialized"
				);
				println!("  This is expected when running as part of the full test suite.");
				println!(
					"  To properly test memory threshold enforcement, run this test in isolation:"
				);
				println!(
					"  cargo test --package surrealdb-core --features allocation-tracking,allocator test_context_memory_threshold_integration"
				);
				panic!("MEMORY_THRESHOLD was already initialized")
			}
			Ok(Some(reason)) => {
				panic!("Unexpected reason returned: {:?}", reason);
			}
		}

		// Clean up the environment variable
		// Safety: Same as above - #[serial] ensures no concurrent access
		unsafe {
			std::env::remove_var("SURREAL_MEMORY_THRESHOLD");
		}
	}
}
