use crate::ctx::canceller::Canceller;
use crate::ctx::reason::Reason;
use crate::sql::value::Value;
use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

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
	values: HashMap<String, Cow<'a, Value>>,
}

impl<'a> Default for Context<'a> {
	fn default() -> Self {
		Context::background()
	}
}

impl<'a> Context<'a> {
	// Create an empty background context.
	pub fn background() -> Self {
		Context {
			values: HashMap::default(),
			parent: None,
			deadline: None,
			cancelled: Arc::new(AtomicBool::new(false)),
		}
	}

	// Create a new child from a frozen context.
	pub fn new(parent: &'a Context) -> Self {
		Context {
			values: HashMap::default(),
			parent: Some(parent),
			deadline: parent.deadline,
			cancelled: Arc::new(AtomicBool::new(false)),
		}
	}

	// Add cancellation to the context. The value that is returned will cancel
	// the context and it's children once called.
	pub fn add_cancel(&mut self) -> Canceller {
		let cancelled = self.cancelled.clone();
		Canceller::new(cancelled)
	}

	// Add a deadline to the context. If the current deadline is sooner than
	// the provided deadline, this method does nothing.
	pub fn add_deadline(&mut self, deadline: Instant) {
		match self.deadline {
			Some(current) if current < deadline => (),
			_ => self.deadline = Some(deadline),
		}
	}

	// Add a timeout to the context. If the current timeout is sooner than
	// the provided timeout, this method does nothing.
	pub fn add_timeout(&mut self, timeout: Duration) {
		self.add_deadline(Instant::now() + timeout)
	}

	// Add a value to the context. It overwrites any previously set values
	// with the same key.
	pub fn add_value<V>(&mut self, key: String, value: V)
	where
		V: Into<Cow<'a, Value>>,
	{
		self.values.insert(key, value.into());
	}

	// Get the deadline for this operation, if any. This is useful for
	// checking if a long job should be started or not.
	pub fn deadline(&self) -> Option<Instant> {
		self.deadline
	}

	// Check if the context is done. If it returns `None` the operation may
	// proceed, otherwise the operation should be stopped.
	pub fn done(&self) -> Option<Reason> {
		match self.deadline {
			Some(deadline) if deadline <= Instant::now() => Some(Reason::Timedout),
			// TODO: see if we can relax the ordering.
			_ if self.cancelled.load(Ordering::SeqCst) => Some(Reason::Canceled),
			_ => match self.parent {
				Some(ctx) => ctx.done(),
				_ => None,
			},
		}
	}

	// Check if the context is ok to continue.
	pub fn is_ok(&self) -> bool {
		self.done().is_none()
	}

	// Check if the context is not ok to continue.
	pub fn is_err(&self) -> bool {
		self.done().is_some()
	}

	// Check if the context is not ok to continue.
	pub fn is_done(&self) -> bool {
		self.done().is_some()
	}

	// Check if the context is not ok to continue, because it timed out.
	pub fn is_timedout(&self) -> bool {
		matches!(self.done(), Some(Reason::Timedout))
	}

	// Check if the context is not ok to continue, because it was cancelled.
	pub fn is_cancelled(&self) -> bool {
		matches!(self.done(), Some(Reason::Canceled))
	}

	// Check if the status of the context. This will return a Result, with an Ok
	// if the operation may proceed, and an Error if it should be stopped.
	pub fn check(&self) -> Result<(), Reason> {
		match self.done() {
			Some(reason) => Err(reason),
			None => Ok(()),
		}
	}

	// Get a value from the context. If no value is stored under the
	// provided key, then this will return None.
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
}

impl<'a> fmt::Debug for Context<'a> {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		f.debug_struct("Context")
			.field("parent", &self.parent)
			.field("deadline", &self.deadline)
			.field("cancelled", &self.cancelled)
			.field(
				"values",
				&if self.values.is_empty() {
					None
				} else {
					Some("values")
				},
			)
			.finish()
	}
}
