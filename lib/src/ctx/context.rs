use crate::ctx::canceller::Canceller;
use crate::ctx::reason::Reason;
use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

pub struct Context {
	// An optional parent context.
	parent: Option<Arc<Context>>,
	// An optional deadline.
	deadline: Option<Instant>,
	// Wether or not this context is cancelled.
	cancelled: Arc<AtomicBool>,
	// A collection of read only values stored in this context.
	values: Option<HashMap<String, Box<dyn Any + Send + Sync>>>,
}

impl Default for Context {
	fn default() -> Self {
		Context::background()
	}
}

impl Context {
	// Create an empty background context.
	pub fn background() -> Context {
		Context {
			values: None,
			parent: None,
			deadline: None,
			cancelled: Arc::new(AtomicBool::new(false)),
		}
	}

	// Create a new child from a frozen context.
	pub fn new(parent: &Arc<Context>) -> Context {
		Context {
			values: None,
			parent: Some(Arc::clone(parent)),
			deadline: parent.deadline,
			cancelled: Arc::new(AtomicBool::new(false)),
		}
	}

	// Freeze the context so it can be used to create child contexts. The
	// parent context can no longer be modified once it has been frozen.
	pub fn freeze(mut self) -> Arc<Context> {
		if let Some(ref mut values) = self.values {
			values.shrink_to_fit();
		}
		Arc::new(self)
	}

	// Add cancelation to the context. The value that is returned will cancel
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
		V: Any + Send + Sync + Sized,
	{
		if let Some(ref mut values) = self.values {
			values.insert(key, Box::new(value));
		} else {
			self.values = Some(HashMap::new());
			self.add_value(key, value);
		}
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
				Some(ref parent_ctx) => parent_ctx.done(),
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
	pub fn value<V>(&self, key: String) -> Option<&V>
	where
		V: Any + Send + Sync + Sized,
	{
		if let Some(ref values) = self.values {
			if let Some(value) = values.get(&key) {
				let value: &dyn Any = &**value;
				return value.downcast_ref::<V>();
			}
		}
		match self.parent {
			Some(ref parent) => parent.value(key),
			_ => None,
		}
	}
}

impl fmt::Debug for Context {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		f.debug_struct("Context")
			.field("parent", &self.parent)
			.field("deadline", &self.deadline)
			.field("cancelled", &self.cancelled)
			.field("values", &self.values.as_ref().map(|_| "values"))
			.finish()
	}
}
