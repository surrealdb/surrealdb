//! Fan-out dispatcher for composing multiple observers into one.
//!
//! The server wires both a metrics observer and, on enterprise builds, an
//! audit observer. [`FanOutObserver`] fans each event out to every attached
//! observer so consumers don't have to hand-roll composition every time.
//!
//! `needs_statement_text` returns `true` if *any* attached observer opts in,
//! so the executor populates the SQL text when any downstream observer needs
//! it.

use std::sync::Arc;

use super::events::{
	AuthEvent, HttpRequestEvent, HttpRequestStartEvent, NetworkBytesEvent, QueryEvent, RpcEvent,
	SessionEvent, StatementEvent, TransactionEvent,
};
use super::observer::ExecutionObserver;

/// Dispatches each event to every observer in `observers`, in order.
///
/// Observers run on the executor thread that produced the event. Any
/// observer that performs blocking or I/O work must hand it off to a
/// background task; a slow observer will back-pressure all downstream
/// observers.
pub struct FanOutObserver {
	observers: Vec<Arc<dyn ExecutionObserver>>,
	/// Cached value of [`ExecutionObserver::is_noop`] for the whole
	/// fan-out. Computed once in [`Self::new`] because the observer set
	/// is fixed for the lifetime of this struct and the result is read
	/// from emit hot paths to skip event construction.
	is_noop: bool,
}

impl FanOutObserver {
	/// Construct from an iterator of observers. Accepts zero observers (in
	/// which case the fan-out behaves identically to
	/// [`super::NoopObserver`]).
	pub fn new(observers: impl IntoIterator<Item = Arc<dyn ExecutionObserver>>) -> Self {
		let observers: Vec<_> = observers.into_iter().collect();
		let is_noop = observers.iter().all(|o| o.is_noop());
		Self {
			observers,
			is_noop,
		}
	}

	/// Number of attached observers.
	pub fn len(&self) -> usize {
		self.observers.len()
	}

	/// Returns `true` if no observers are attached.
	pub fn is_empty(&self) -> bool {
		self.observers.is_empty()
	}
}

impl ExecutionObserver for FanOutObserver {
	fn on_statement_complete(&self, event: &StatementEvent) {
		for o in &self.observers {
			o.on_statement_complete(event);
		}
	}

	fn on_query_complete(&self, event: &QueryEvent) {
		for o in &self.observers {
			o.on_query_complete(event);
		}
	}

	fn on_transaction_complete(&self, event: &TransactionEvent) {
		for o in &self.observers {
			o.on_transaction_complete(event);
		}
	}

	fn on_rpc_complete(&self, event: &RpcEvent) {
		for o in &self.observers {
			o.on_rpc_complete(event);
		}
	}

	fn on_auth_event(&self, event: &AuthEvent) {
		for o in &self.observers {
			o.on_auth_event(event);
		}
	}

	fn on_session_event(&self, event: &SessionEvent) {
		for o in &self.observers {
			o.on_session_event(event);
		}
	}

	fn on_network_bytes(&self, event: &NetworkBytesEvent) {
		for o in &self.observers {
			o.on_network_bytes(event);
		}
	}

	fn on_http_request_started(&self, event: &HttpRequestStartEvent) {
		for o in &self.observers {
			o.on_http_request_started(event);
		}
	}

	fn on_http_request_complete(&self, event: &HttpRequestEvent) {
		for o in &self.observers {
			o.on_http_request_complete(event);
		}
	}

	fn needs_statement_text(&self) -> bool {
		self.observers.iter().any(|o| o.needs_statement_text())
	}

	/// A fan-out is no-op only when every constituent observer is
	/// no-op. In practice the empty fan-out (no observers attached) is
	/// the case that benefits most: emit sites can skip event
	/// construction entirely.
	///
	/// Reads a cached value populated in [`Self::new`].
	fn is_noop(&self) -> bool {
		self.is_noop
	}
}

#[cfg(test)]
mod tests {
	use std::sync::Arc;
	use std::sync::atomic::{AtomicUsize, Ordering};

	use super::super::events::{Outcome, StatementEvent, StatementEventCtx, StatementEventSafe};
	use super::super::observer::ExecutionObserver;
	use super::FanOutObserver;
	use crate::observe::events::StatementType;

	struct Counting {
		statements: AtomicUsize,
		wants_sql: bool,
	}

	impl ExecutionObserver for Counting {
		fn on_statement_complete(&self, _event: &StatementEvent) {
			self.statements.fetch_add(1, Ordering::Relaxed);
		}

		fn needs_statement_text(&self) -> bool {
			self.wants_sql
		}
	}

	fn mk_event() -> StatementEvent {
		StatementEvent {
			safe: StatementEventSafe {
				kind: StatementType::Select,
				outcome: Outcome::Success,
				duration: std::time::Duration::from_millis(1),
				read_only: true,
				result_rows: 0,
				error_class: None,
			},
			ctx: StatementEventCtx::default(),
		}
	}

	#[test]
	fn empty_fan_out_is_noop() {
		let f = FanOutObserver::new([]);
		f.on_statement_complete(&mk_event());
		assert!(!f.needs_statement_text());
		assert!(f.is_empty());
	}

	#[test]
	fn dispatches_to_every_observer() {
		let a = Arc::new(Counting {
			statements: AtomicUsize::new(0),
			wants_sql: false,
		});
		let b = Arc::new(Counting {
			statements: AtomicUsize::new(0),
			wants_sql: false,
		});
		let f = FanOutObserver::new([
			Arc::clone(&a) as Arc<dyn ExecutionObserver>,
			Arc::clone(&b) as Arc<dyn ExecutionObserver>,
		]);
		f.on_statement_complete(&mk_event());
		assert_eq!(a.statements.load(Ordering::Relaxed), 1);
		assert_eq!(b.statements.load(Ordering::Relaxed), 1);
	}

	#[test]
	fn needs_statement_text_is_any() {
		let a = Arc::new(Counting {
			statements: AtomicUsize::new(0),
			wants_sql: false,
		});
		let b = Arc::new(Counting {
			statements: AtomicUsize::new(0),
			wants_sql: true,
		});
		let f =
			FanOutObserver::new([a as Arc<dyn ExecutionObserver>, b as Arc<dyn ExecutionObserver>]);
		assert!(f.needs_statement_text());
	}
}
