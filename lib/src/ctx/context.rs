use crate::ctx::canceller::Canceller;
use crate::ctx::reason::Reason;
use crate::dbs::capabilities::FuncTarget;
#[cfg(feature = "http")]
use crate::dbs::capabilities::NetTarget;
use crate::dbs::{Capabilities, Notification};
use crate::err::Error;
use crate::idx::planner::QueryPlanner;
use crate::sql::value::Value;
use channel::Sender;
use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt::{self, Debug};
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
	// Capabilities
	capabilities: Arc<Capabilities>,
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
	/// Create an empty background context.
	pub fn background() -> Self {
		Context {
			values: HashMap::default(),
			parent: None,
			deadline: None,
			cancelled: Arc::new(AtomicBool::new(false)),
			notifications: None,
			query_planner: None,
			capabilities: Arc::new(Capabilities::default()),
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
			capabilities: parent.capabilities.clone(),
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
	/// the provided timeout, this method does nothing.
	pub fn add_timeout(&mut self, timeout: Duration) {
		self.add_deadline(Instant::now() + timeout)
	}

	/// Add the LIVE query notification channel to the context, so that we
	/// can send notifications to any subscribers.
	pub fn add_notifications(&mut self, chn: Option<&Sender<Notification>>) {
		self.notifications = chn.cloned()
	}

	/// Set the query planner
	pub(crate) fn set_query_planner(&mut self, qp: &'a QueryPlanner) {
		self.query_planner = Some(qp);
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
