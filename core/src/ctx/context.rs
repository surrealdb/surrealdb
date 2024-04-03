use crate::ctx::canceller::Canceller;
use crate::ctx::reason::Reason;
use crate::dbs::capabilities::FuncTarget;
#[cfg(feature = "http")]
use crate::dbs::capabilities::NetTarget;
use crate::dbs::{Capabilities, Notification};
use crate::err::Error;
use crate::idx::planner::executor::QueryExecutor;
use crate::idx::planner::{IterationStage, QueryPlanner};
use crate::idx::trees::store::IndexStores;
use crate::sql::value::Value;
use channel::Sender;
use std::borrow::Cow;
use std::collections::HashMap;
#[cfg(any(
	feature = "kv-surrealkv",
	feature = "kv-file",
	feature = "kv-rocksdb",
	feature = "kv-fdb",
	feature = "kv-tikv",
	feature = "kv-speedb"
))]
use std::env;
use std::fmt::{self, Debug};
#[cfg(any(
	feature = "kv-surrealkv",
	feature = "kv-file",
	feature = "kv-rocksdb",
	feature = "kv-fdb",
	feature = "kv-tikv",
	feature = "kv-speedb"
))]
use std::path::{Path, PathBuf};
use std::str::FromStr;
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
#[non_exhaustive]
pub struct Context<'a> {
	// An optional parent context.
	parent: Option<&'a Context<'a>>,
	// An optional deadline.
	deadline: Option<Instant>,
	// Whether or not this context is cancelled.
	cancelled: Arc<AtomicBool>,
	// A collection of read only values stored in this context.
	values: HashMap<Cow<'static, str>, Cow<'a, Value>>,
	// Stores the notification channel if available
	notifications: Option<Sender<Notification>>,
	// An optional query planner
	query_planner: Option<&'a QueryPlanner<'a>>,
	// An optional query executor
	query_executor: Option<QueryExecutor>,
	// An optional iteration stage
	iteration_stage: Option<IterationStage>,
	// The index store
	index_stores: IndexStores,
	// Capabilities
	capabilities: Arc<Capabilities>,
	#[cfg(any(
		feature = "kv-surrealkv",
		feature = "kv-file",
		feature = "kv-rocksdb",
		feature = "kv-fdb",
		feature = "kv-tikv",
		feature = "kv-speedb"
	))]
	// Is the datastore in memory? (KV-MEM, WASM)
	is_memory: bool,
	#[cfg(any(
		feature = "kv-surrealkv",
		feature = "kv-file",
		feature = "kv-rocksdb",
		feature = "kv-fdb",
		feature = "kv-tikv",
		feature = "kv-speedb"
	))]
	// The temporary directory
	temporary_directory: Arc<PathBuf>,
}

impl<'a> Default for Context<'a> {
	fn default() -> Self {
		Context::background()
	}
}

impl<'a> Debug for Context<'a> {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		f.debug_struct("Context")
			.field("parent", &self.parent)
			.field("deadline", &self.deadline)
			.field("cancelled", &self.cancelled)
			.field("values", &self.values)
			.finish()
	}
}

impl<'a> Context<'a> {
	pub(crate) fn from_ds(
		time_out: Option<Duration>,
		capabilities: Capabilities,
		index_stores: IndexStores,
		#[cfg(any(
			feature = "kv-surrealkv",
			feature = "kv-file",
			feature = "kv-rocksdb",
			feature = "kv-fdb",
			feature = "kv-tikv",
			feature = "kv-speedb"
		))]
		is_memory: bool,
		#[cfg(any(
			feature = "kv-surrealkv",
			feature = "kv-file",
			feature = "kv-rocksdb",
			feature = "kv-fdb",
			feature = "kv-tikv",
			feature = "kv-speedb"
		))]
		temporary_directory: Arc<PathBuf>,
	) -> Result<Context<'a>, Error> {
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
			#[cfg(any(
				feature = "kv-surrealkv",
				feature = "kv-file",
				feature = "kv-rocksdb",
				feature = "kv-fdb",
				feature = "kv-tikv",
				feature = "kv-speedb"
			))]
			is_memory,
			#[cfg(any(
				feature = "kv-surrealkv",
				feature = "kv-file",
				feature = "kv-rocksdb",
				feature = "kv-fdb",
				feature = "kv-tikv",
				feature = "kv-speedb"
			))]
			temporary_directory,
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
			#[cfg(any(
				feature = "kv-surrealkv",
				feature = "kv-file",
				feature = "kv-rocksdb",
				feature = "kv-fdb",
				feature = "kv-tikv",
				feature = "kv-speedb"
			))]
			is_memory: false,
			#[cfg(any(
				feature = "kv-surrealkv",
				feature = "kv-file",
				feature = "kv-rocksdb",
				feature = "kv-fdb",
				feature = "kv-tikv",
				feature = "kv-speedb"
			))]
			temporary_directory: Arc::new(env::temp_dir()),
		}
	}

	/// Create a new child from a frozen context.
	pub fn new(parent: &'a Context) -> Self {
		Context {
			values: HashMap::default(),
			parent: Some(parent),
			deadline: parent.deadline,
			cancelled: Arc::new(AtomicBool::new(false)),
			notifications: parent.notifications.clone(),
			query_planner: parent.query_planner,
			query_executor: parent.query_executor.clone(),
			iteration_stage: parent.iteration_stage.clone(),
			capabilities: parent.capabilities.clone(),
			index_stores: parent.index_stores.clone(),
			#[cfg(any(
				feature = "kv-surrealkv",
				feature = "kv-file",
				feature = "kv-rocksdb",
				feature = "kv-fdb",
				feature = "kv-tikv",
				feature = "kv-speedb"
			))]
			is_memory: parent.is_memory,
			#[cfg(any(
				feature = "kv-surrealkv",
				feature = "kv-file",
				feature = "kv-rocksdb",
				feature = "kv-fdb",
				feature = "kv-tikv",
				feature = "kv-speedb"
			))]
			temporary_directory: parent.temporary_directory.clone(),
		}
	}

	/// Add a value to the context. It overwrites any previously set values
	/// with the same key.
	pub fn add_value<K, V>(&mut self, key: K, value: V)
	where
		K: Into<Cow<'static, str>>,
		V: Into<Cow<'a, Value>>,
	{
		self.values.insert(key.into(), value.into());
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

	pub(crate) fn set_query_planner(&mut self, qp: &'a QueryPlanner) {
		self.query_planner = Some(qp);
	}

	pub(crate) fn set_query_executor(&mut self, qe: QueryExecutor) {
		self.query_executor = Some(qe);
	}

	pub(crate) fn set_iteration_stage(&mut self, is: IterationStage) {
		self.iteration_stage = Some(is);
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
		self.query_planner
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

	/// Check if the context is done. If it returns `None` the operation may
	/// proceed, otherwise the operation should be stopped.
	pub fn done(&self) -> Option<Reason> {
		match self.deadline {
			Some(deadline) if deadline <= Instant::now() => Some(Reason::Timedout),
			_ if self.cancelled.load(Ordering::Relaxed) => Some(Reason::Canceled),
			_ => match self.parent {
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
		feature = "kv-surrealkv",
		feature = "kv-file",
		feature = "kv-rocksdb",
		feature = "kv-fdb",
		feature = "kv-tikv",
		feature = "kv-speedb"
	))]
	/// Return true if the underlying Datastore is KV-MEM (Or WASM)
	pub fn is_memory(&self) -> bool {
		self.is_memory
	}

	#[cfg(any(
		feature = "kv-surrealkv",
		feature = "kv-file",
		feature = "kv-rocksdb",
		feature = "kv-fdb",
		feature = "kv-tikv",
		feature = "kv-speedb"
	))]
	/// Return the location of the temporary directory
	pub fn temporary_directory(&self) -> &Path {
		self.temporary_directory.as_ref()
	}

	/// Get a value from the context. If no value is stored under the
	/// provided key, then this will return None.
	pub fn value(&self, key: &str) -> Option<&Value> {
		match self.values.get(key) {
			Some(v) => match v {
				Cow::Borrowed(v) => Some(*v),
				Cow::Owned(v) => Some(v),
			},
			None => match self.parent {
				Some(p) => p.value(key),
				_ => None,
			},
		}
	}

	/// Get a 'static view into the cancellation status.
	#[cfg(feature = "scripting")]
	pub fn cancellation(&self) -> crate::ctx::cancellation::Cancellation {
		crate::ctx::cancellation::Cancellation::new(
			self.deadline,
			std::iter::successors(Some(self), |ctx| ctx.parent)
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
		let func_target = FuncTarget::from_str(target).map_err(|_| Error::InvalidFunction {
			name: target.to_string(),
			message: "Invalid function name".to_string(),
		})?;

		if !self.capabilities.allows_function(&func_target) {
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
