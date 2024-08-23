use crate::cnf::PROTECTED_PARAM_NAMES;
use crate::ctx::canceller::Canceller;
use crate::ctx::reason::Reason;
#[cfg(feature = "http")]
use crate::dbs::capabilities::NetTarget;
use crate::dbs::{Capabilities, Notification};
use crate::err::Error;
use crate::idx::planner::executor::QueryExecutor;
use crate::idx::planner::{IterationStage, QueryPlanner};
use crate::idx::trees::store::IndexStores;
#[cfg(not(target_arch = "wasm32"))]
use crate::kvs::IndexBuilder;
use crate::kvs::Transaction;
use crate::sql::value::Value;
use channel::Sender;
use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt::{self, Debug};
#[cfg(any(
	feature = "kv-mem",
	feature = "kv-surrealkv",
	feature = "kv-rocksdb",
	feature = "kv-fdb",
	feature = "kv-tikv",
))]
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use trice::Instant;
#[cfg(feature = "http")]
use url::Url;

impl<'a> From<Value> for Cow<'a, Value> {
	fn from(v: Value) -> Cow<'a, Value> {
		Cow::Owned(v)
	}
}

impl<'a> From<&'a Value> for Cow<'a, Value> {
	fn from(v: &'a Value) -> Cow<'a, Value> {
		Cow::Borrowed(v)
	}
}

pub type Context = Arc<MutableContext>;

#[non_exhaustive]
pub struct MutableContext {
	// An optional parent context.
	parent: Option<Context>,
	// An optional deadline.
	deadline: Option<Instant>,
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
	// The index store
	index_stores: IndexStores,
	// The index concurrent builders
	#[cfg(not(target_arch = "wasm32"))]
	index_builder: Option<IndexBuilder>,
	// Capabilities
	capabilities: Arc<Capabilities>,
	#[cfg(any(
		feature = "kv-mem",
		feature = "kv-surrealkv",
		feature = "kv-rocksdb",
		feature = "kv-fdb",
		feature = "kv-tikv",
	))]
	// The temporary directory
	temporary_directory: Option<Arc<PathBuf>>,
	// An optional transaction
	transaction: Option<Arc<Transaction>>,
	// Does not read from parent `values`.
	isolated: bool,
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
	pub(crate) fn from_ds(
		time_out: Option<Duration>,
		capabilities: Capabilities,
		index_stores: IndexStores,
		#[cfg(not(target_arch = "wasm32"))] index_builder: IndexBuilder,
		#[cfg(any(
			feature = "kv-mem",
			feature = "kv-surrealkv",
			feature = "kv-rocksdb",
			feature = "kv-fdb",
			feature = "kv-tikv",
		))]
		temporary_directory: Option<Arc<PathBuf>>,
	) -> Result<MutableContext, Error> {
		let mut ctx = Self {
			values: HashMap::default(),
			parent: None,
			deadline: None,
			cancelled: Arc::new(AtomicBool::new(false)),
			notifications: None,
			query_planner: None,
			query_executor: None,
			iteration_stage: None,
			capabilities: Arc::new(capabilities),
			index_stores,
			#[cfg(not(target_arch = "wasm32"))]
			index_builder: Some(index_builder),
			#[cfg(any(
				feature = "kv-mem",
				feature = "kv-surrealkv",
				feature = "kv-rocksdb",
				feature = "kv-fdb",
				feature = "kv-tikv",
			))]
			temporary_directory,
			transaction: None,
			isolated: false,
		};
		if let Some(timeout) = time_out {
			ctx.add_timeout(timeout)?;
		}
		Ok(ctx)
	}
	/// Create an empty background context.
	pub fn background() -> Self {
		Self {
			values: HashMap::default(),
			parent: None,
			deadline: None,
			cancelled: Arc::new(AtomicBool::new(false)),
			notifications: None,
			query_planner: None,
			query_executor: None,
			iteration_stage: None,
			capabilities: Arc::new(Capabilities::default()),
			index_stores: IndexStores::default(),
			#[cfg(not(target_arch = "wasm32"))]
			index_builder: None,
			#[cfg(any(
				feature = "kv-mem",
				feature = "kv-surrealkv",
				feature = "kv-rocksdb",
				feature = "kv-fdb",
				feature = "kv-tikv",
			))]
			temporary_directory: None,
			transaction: None,
			isolated: false,
		}
	}

	/// Create a new child from a frozen context.
	pub fn new(parent: &Context) -> Self {
		MutableContext {
			values: HashMap::default(),
			deadline: parent.deadline,
			cancelled: Arc::new(AtomicBool::new(false)),
			notifications: parent.notifications.clone(),
			query_planner: parent.query_planner.clone(),
			query_executor: parent.query_executor.clone(),
			iteration_stage: parent.iteration_stage.clone(),
			capabilities: parent.capabilities.clone(),
			index_stores: parent.index_stores.clone(),
			#[cfg(not(target_arch = "wasm32"))]
			index_builder: parent.index_builder.clone(),
			#[cfg(any(
				feature = "kv-mem",
				feature = "kv-surrealkv",
				feature = "kv-rocksdb",
				feature = "kv-fdb",
				feature = "kv-tikv",
			))]
			temporary_directory: parent.temporary_directory.clone(),
			transaction: parent.transaction.clone(),
			isolated: false,
			parent: Some(parent.clone()),
		}
	}
	pub(crate) fn freeze(self) -> Context {
		Arc::new(self)
	}

	pub(crate) fn unfreeze(ctx: Context) -> Result<MutableContext, Error> {
		match Arc::try_unwrap(ctx) {
			Ok(inner) => Ok(inner),
			Err(_) => Err(Error::Unreachable("Context::unfreeze")),
		}
	}

	/// Create a new child from a frozen context.
	pub fn new_isolated(parent: &Context) -> Self {
		Self {
			values: HashMap::default(),
			deadline: parent.deadline,
			cancelled: Arc::new(AtomicBool::new(false)),
			notifications: parent.notifications.clone(),
			query_planner: parent.query_planner.clone(),
			query_executor: parent.query_executor.clone(),
			iteration_stage: parent.iteration_stage.clone(),
			capabilities: parent.capabilities.clone(),
			index_stores: parent.index_stores.clone(),
			#[cfg(not(target_arch = "wasm32"))]
			index_builder: parent.index_builder.clone(),
			#[cfg(any(
				feature = "kv-mem",
				feature = "kv-surrealkv",
				feature = "kv-rocksdb",
				feature = "kv-fdb",
				feature = "kv-tikv",
			))]
			temporary_directory: parent.temporary_directory.clone(),
			transaction: parent.transaction.clone(),
			isolated: true,
			parent: Some(parent.clone()),
		}
	}

	/// Create a new child from a frozen context.
	pub fn new_concurrent(from: &Context) -> Self {
		Self {
			values: HashMap::default(),
			deadline: None,
			cancelled: Arc::new(AtomicBool::new(false)),
			notifications: from.notifications.clone(),
			query_planner: from.query_planner.clone(),
			query_executor: from.query_executor.clone(),
			iteration_stage: from.iteration_stage.clone(),
			capabilities: from.capabilities.clone(),
			index_stores: from.index_stores.clone(),
			#[cfg(not(target_arch = "wasm32"))]
			index_builder: from.index_builder.clone(),
			#[cfg(any(
				feature = "kv-mem",
				feature = "kv-surrealkv",
				feature = "kv-rocksdb",
				feature = "kv-fdb",
				feature = "kv-tikv",
			))]
			temporary_directory: from.temporary_directory.clone(),
			transaction: None,
			isolated: false,
			parent: None,
		}
	}

	/// Add a value to the context. It overwrites any previously set values
	/// with the same key.
	pub fn add_value<K>(&mut self, key: K, value: Arc<Value>)
	where
		K: Into<Cow<'static, str>>,
	{
		self.values.insert(key.into(), value);
	}

	/// Add cancellation to the context. The value that is returned will cancel
	/// the context and it's children once called.
	pub fn add_cancel(&mut self) -> Canceller {
		let cancelled = self.cancelled.clone();
		Canceller::new(cancelled)
	}

	/// Add a deadline to the context. If the current deadline is sooner than
	/// the provided deadline, this method does nothing.
	pub fn add_deadline(&mut self, deadline: Instant) {
		match self.deadline {
			Some(current) if current < deadline => (),
			_ => self.deadline = Some(deadline),
		}
	}

	/// Add a timeout to the context. If the current timeout is sooner than
	/// the provided timeout, this method does nothing. If the result of the
	/// addition causes an overflow, this method returns an error.
	pub fn add_timeout(&mut self, timeout: Duration) -> Result<(), Error> {
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
	pub fn add_notifications(&mut self, chn: Option<&Sender<Notification>>) {
		self.notifications = chn.cloned()
	}

	pub(crate) fn set_query_planner(&mut self, qp: QueryPlanner) {
		self.query_planner = Some(Arc::new(qp));
	}

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
	pub fn timeout(&self) -> Option<Duration> {
		self.deadline.map(|v| v.saturating_duration_since(Instant::now()))
	}

	pub fn notifications(&self) -> Option<Sender<Notification>> {
		self.notifications.clone()
	}

	pub(crate) fn get_query_planner(&self) -> Option<&QueryPlanner> {
		self.query_planner.as_ref().map(|qp| qp.as_ref())
	}

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
	#[cfg(not(target_arch = "wasm32"))]
	pub(crate) fn get_index_builder(&self) -> Option<&IndexBuilder> {
		self.index_builder.as_ref()
	}

	/// Check if the context is done. If it returns `None` the operation may
	/// proceed, otherwise the operation should be stopped.
	pub fn done(&self) -> Option<Reason> {
		match self.deadline {
			Some(deadline) if deadline <= Instant::now() => Some(Reason::Timedout),
			_ if self.cancelled.load(Ordering::Relaxed) => Some(Reason::Canceled),
			_ => match &self.parent {
				Some(ctx) => ctx.done(),
				_ => None,
			},
		}
	}

	/// Check if the context is ok to continue.
	pub fn is_ok(&self) -> bool {
		self.done().is_none()
	}

	/// Check if the context is not ok to continue.
	pub fn is_done(&self) -> bool {
		self.done().is_some()
	}

	/// Check if the context is not ok to continue, because it timed out.
	pub fn is_timedout(&self) -> bool {
		matches!(self.done(), Some(Reason::Timedout))
	}

	#[cfg(any(
		feature = "kv-mem",
		feature = "kv-surrealkv",
		feature = "kv-rocksdb",
		feature = "kv-fdb",
		feature = "kv-tikv",
	))]
	/// Return the location of the temporary directory if any
	pub fn temporary_directory(&self) -> Option<&Arc<PathBuf>> {
		self.temporary_directory.as_ref()
	}

	/// Get a value from the context. If no value is stored under the
	/// provided key, then this will return None.
	pub fn value(&self, key: &str) -> Option<&Value> {
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
	pub fn cancellation(&self) -> crate::ctx::cancellation::Cancellation {
		crate::ctx::cancellation::Cancellation::new(
			self.deadline,
			std::iter::successors(Some(self), |ctx| ctx.parent.as_ref().map(|c| c.as_ref()))
				.map(|ctx| ctx.cancelled.clone())
				.collect(),
		)
	}

	//
	// Capabilities
	//

	/// Set the capabilities for this context
	pub fn add_capabilities(&mut self, caps: Capabilities) {
		self.capabilities = Arc::new(caps);
	}

	/// Get the capabilities for this context
	#[allow(dead_code)]
	pub fn get_capabilities(&self) -> Arc<Capabilities> {
		self.capabilities.clone()
	}

	/// Check if scripting is allowed
	#[allow(dead_code)]
	pub fn check_allowed_scripting(&self) -> Result<(), Error> {
		if !self.capabilities.allows_scripting() {
			return Err(Error::ScriptingNotAllowed);
		}
		Ok(())
	}

	/// Check if a function is allowed
	pub fn check_allowed_function(&self, target: &str) -> Result<(), Error> {
		if !self.capabilities.allows_function_name(target) {
			return Err(Error::FunctionNotAllowed(target.to_string()));
		}
		Ok(())
	}

	/// Check if a network target is allowed
	#[cfg(feature = "http")]
	pub fn check_allowed_net(&self, target: &Url) -> Result<(), Error> {
		match target.host() {
			Some(host)
				if self.capabilities.allows_network_target(&NetTarget::Host(
					host.to_owned(),
					target.port_or_known_default(),
				)) =>
			{
				Ok(())
			}
			_ => Err(Error::NetTargetNotAllowed(target.to_string())),
		}
	}
}
