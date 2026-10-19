//! Counting the calls in flight through a gate that can be closed.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use tokio::sync::Notify;

/// Set in [`InFlight`]'s state once the gate is closed.
const CLOSED: usize = 1 << (usize::BITS - 1);

/// The calls in flight through a gate, each counted from when it enters
/// until its [`InFlightCall`] is dropped, and whether the gate has been
/// closed to further calls.
///
/// Admission and closing act on one atomic word, so a call admitted
/// concurrently with [`Self::close`] is either refused or counted before
/// the gate closes: once `close` returns, no call is admitted, and
/// [`Self::drained`] waits for every call admitted before it.
#[derive(Default)]
pub(super) struct InFlight {
	/// The calls in flight, with [`CLOSED`] set once the gate is closed.
	state: AtomicUsize,
	/// Notified whenever the count falls to zero.
	emptied: Notify,
}

/// Counts a call in its [`InFlight`] until dropped.
pub(super) struct InFlightCall(Arc<InFlight>);

impl InFlight {
	/// Admit a call, or refuse it once the gate is closed.
	pub(super) fn admit(self: &Arc<Self>) -> Option<InFlightCall> {
		if self.state.fetch_add(1, Ordering::AcqRel) & CLOSED != 0 {
			self.release();
			return None;
		}
		Some(InFlightCall(Arc::clone(self)))
	}

	/// Count a call whether or not the gate is closed, for a call that must
	/// be waited for once it has begun.
	pub(super) fn enter(self: &Arc<Self>) -> InFlightCall {
		self.state.fetch_add(1, Ordering::AcqRel);
		InFlightCall(Arc::clone(self))
	}

	/// Close the gate, refusing every later [`Self::admit`]. Calls already
	/// in flight run on.
	pub(super) fn close(&self) {
		self.state.fetch_or(CLOSED, Ordering::AcqRel);
	}

	/// How many calls are in flight.
	pub(super) fn count(&self) -> usize {
		self.state.load(Ordering::Acquire) & !CLOSED
	}

	/// Wait until no call is in flight. A call that enters while this waits
	/// is waited for too.
	pub(super) async fn drained(&self) {
		loop {
			// A `Notified` receives `notify_waiters` from the moment it is
			// created, so a call returning between here and the check below
			// still wakes it.
			let emptied = self.emptied.notified();
			if self.count() == 0 {
				return;
			}
			emptied.await;
		}
	}

	fn release(&self) {
		if self.state.fetch_sub(1, Ordering::AcqRel) & !CLOSED == 1 {
			self.emptied.notify_waiters();
		}
	}
}

impl Drop for InFlightCall {
	fn drop(&mut self) {
		self.0.release();
	}
}

#[cfg(test)]
mod tests {
	use std::time::Duration;

	use super::*;

	/// How long a wait that must not finish is given to finish anyway.
	const SETTLE: Duration = Duration::from_millis(200);

	#[test]
	fn a_call_is_in_flight_until_dropped() {
		let gate = Arc::new(InFlight::default());
		let (first, second) = (gate.admit().unwrap(), gate.enter());
		assert_eq!(gate.count(), 2);
		drop(first);
		assert_eq!(gate.count(), 1);
		drop(second);
		assert_eq!(gate.count(), 0);
	}

	/// Once closed, the gate admits nothing, and a refused call leaves no
	/// count behind, while calls admitted before it closed run on and a call
	/// entered afterwards still counts.
	#[test]
	fn a_closed_gate_refuses_calls_and_keeps_those_it_admitted() {
		let gate = Arc::new(InFlight::default());
		let admitted = gate.admit().unwrap();
		gate.close();
		assert!(gate.admit().is_none(), "a closed gate admitted a call");
		assert_eq!(gate.count(), 1, "a refused call counts");
		let entered = gate.enter();
		assert_eq!(gate.count(), 2, "an entered call does not count once the gate closed");
		drop((admitted, entered));
		assert_eq!(gate.count(), 0);
		assert!(gate.admit().is_none(), "a drained gate reopened");
	}

	#[tokio::test(flavor = "current_thread")]
	async fn draining_waits_for_every_call_in_flight() {
		let gate = Arc::new(InFlight::default());
		let (admitted, entered) = (gate.admit().unwrap(), gate.enter());
		gate.close();
		let drained = tokio::spawn({
			let gate = Arc::clone(&gate);
			async move { gate.drained().await }
		});
		drop(admitted);
		tokio::time::sleep(SETTLE).await;
		assert!(!drained.is_finished(), "draining ended while a call was in flight");
		drop(entered);
		tokio::time::timeout(Duration::from_secs(10), drained).await.unwrap().unwrap();
		// With nothing in flight, draining returns at once, refused calls
		// included.
		assert!(gate.admit().is_none());
		tokio::time::timeout(SETTLE, gate.drained()).await.unwrap();
	}
}
