#![cfg(feature = "scripting")]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use trice::Instant;

/// A 'static view into the cancellation status of a Context.
#[derive(Clone, Debug, Default)]
pub struct Cancellation {
	deadline: Option<Instant>,
	cancellations: Vec<Arc<AtomicBool>>,
}

impl Cancellation {
	pub fn new(deadline: Option<Instant>, cancellations: Vec<Arc<AtomicBool>>) -> Cancellation {
		Self {
			deadline,
			cancellations,
		}
	}

	pub fn is_done(&self) -> bool {
		self.deadline.map(|d| d <= Instant::now()).unwrap_or(false)
			|| self.cancellations.iter().any(|c| c.load(Ordering::Relaxed))
	}
}
