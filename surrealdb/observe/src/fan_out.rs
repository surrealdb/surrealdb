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
	AuthEvent, BucketOperationEvent, HttpRequestEvent, HttpRequestStartEvent, NetworkBytesEvent,
	QueryEvent, RpcEvent, SessionEvent, StatementEvent, TransactionEvent,
};
use super::observer::ExecutionObserver;

/// Dispatch one hook to every observer, containing a panic to the observer
/// that raised it.
///
/// Without this a panicking child — an audit observer hitting a poisoned mutex
/// or a closed channel — would both skip every observer registered after it and
/// unwind out of the emit site on the executor thread, turning an
/// observability fault into a failed query. Observation must not be able to
/// break the thing it observes.
macro_rules! fan_out {
	($self:ident, $hook:ident, $event:ident) => {
		for (index, observer) in $self.observers.iter().enumerate() {
			let call = std::panic::AssertUnwindSafe(|| observer.$hook($event));
			if std::panic::catch_unwind(call).is_err() {
				// The default panic hook has already reported it. This crate
				// deliberately has no logging dependency, so there is nothing
				// more to say here; `index` names which observer for a reader
				// stepping through.
				let _ = index;
			}
		}
	};
}

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
	/// Cached value of [`ExecutionObserver::needs_statement_text`] for the
	/// whole fan-out, for the same reason as `is_noop`.
	///
	/// Caching also keeps the capability query off the statement path, where
	/// it is read per statement and its result is not an event: the event
	/// hooks contain a panicking observer, but a panic raised while merely
	/// *asking* an observer what it wants would fail the query instead. Asking
	/// once, while the datastore is being built, turns that into a startup
	/// failure in the observer's own configuration.
	needs_statement_text: bool,
}

impl FanOutObserver {
	/// Construct from an iterator of observers. Accepts zero observers (in
	/// which case the fan-out behaves identically to
	/// [`super::NoopObserver`]).
	pub fn new(observers: impl IntoIterator<Item = Arc<dyn ExecutionObserver>>) -> Self {
		let observers: Vec<_> = observers.into_iter().collect();
		let is_noop = observers.iter().all(|o| o.is_noop());
		let needs_statement_text = observers.iter().any(|o| o.needs_statement_text());
		Self {
			observers,
			is_noop,
			needs_statement_text,
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
		fan_out!(self, on_statement_complete, event);
	}

	fn on_query_complete(&self, event: &QueryEvent) {
		fan_out!(self, on_query_complete, event);
	}

	fn on_transaction_complete(&self, event: &TransactionEvent) {
		fan_out!(self, on_transaction_complete, event);
	}

	fn on_rpc_complete(&self, event: &RpcEvent) {
		fan_out!(self, on_rpc_complete, event);
	}

	fn on_auth_event(&self, event: &AuthEvent) {
		fan_out!(self, on_auth_event, event);
	}

	fn on_session_event(&self, event: &SessionEvent) {
		fan_out!(self, on_session_event, event);
	}

	fn on_network_bytes(&self, event: &NetworkBytesEvent) {
		fan_out!(self, on_network_bytes, event);
	}

	fn on_bucket_operation(&self, event: &BucketOperationEvent) {
		fan_out!(self, on_bucket_operation, event);
	}

	fn on_http_request_started(&self, event: &HttpRequestStartEvent) {
		fan_out!(self, on_http_request_started, event);
	}

	fn on_http_request_complete(&self, event: &HttpRequestEvent) {
		fan_out!(self, on_http_request_complete, event);
	}

	/// Reads a cached value populated in [`Self::new`].
	fn needs_statement_text(&self) -> bool {
		self.needs_statement_text
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
	use std::sync::atomic::{AtomicUsize, Ordering};
	use std::sync::{Arc, Mutex};
	use std::time::Duration;

	use super::super::events::{Outcome, StatementEvent, StatementEventCtx, StatementEventSafe};
	use super::super::observer::ExecutionObserver;
	use super::FanOutObserver;
	use crate::events::{
		AuthAction, AuthEvent, AuthEventCtx, AuthEventSafe, AuthScope, BucketOp,
		BucketOperationEvent, BucketOperationEventCtx, BucketOperationEventSafe, HttpMethod,
		HttpRequestEvent, HttpRequestEventCtx, HttpRequestEventSafe, HttpRequestStartEvent,
		HttpRequestStartEventSafe, HttpVersion, NetworkBytesEvent, NetworkBytesEventCtx,
		NetworkBytesEventSafe, NetworkDirection, QueryCounters, QueryEvent, QueryEventCtx,
		QueryEventSafe, RpcEvent, RpcEventCtx, RpcEventSafe, SessionAction, SessionEvent,
		SessionEventCtx, SessionEventSafe, SessionProtocol, StatementType, TransactionEvent,
		TransactionEventCtx, TransactionEventSafe, TransactionMetricsSnapshot,
	};

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
				mutable_permission_writes: 0,
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

	/// Every event-carrying hook on [`ExecutionObserver`], in trait order.
	/// [`forwards_every_hook`] drives each one exactly once and requires the
	/// fan-out to have delivered all of them; a new hook must be added here
	/// and to the driving loop at the same time as it is added to the trait.
	const EVENT_HOOKS: [&str; 10] = [
		"on_statement_complete",
		"on_query_complete",
		"on_transaction_complete",
		"on_rpc_complete",
		"on_auth_event",
		"on_session_event",
		"on_network_bytes",
		"on_bucket_operation",
		"on_http_request_started",
		"on_http_request_complete",
	];

	/// Records the name of every hook it receives, so a test can assert on
	/// which hooks the fan-out actually forwarded.
	#[derive(Default)]
	struct Recorder {
		seen: Mutex<Vec<&'static str>>,
	}

	impl Recorder {
		fn record(&self, hook: &'static str) {
			self.seen.lock().unwrap().push(hook);
		}

		fn seen(&self) -> Vec<&'static str> {
			self.seen.lock().unwrap().clone()
		}
	}

	impl ExecutionObserver for Recorder {
		fn on_statement_complete(&self, _event: &StatementEvent) {
			self.record("on_statement_complete");
		}

		fn on_query_complete(&self, _event: &QueryEvent) {
			self.record("on_query_complete");
		}

		fn on_transaction_complete(&self, _event: &TransactionEvent) {
			self.record("on_transaction_complete");
		}

		fn on_rpc_complete(&self, _event: &RpcEvent) {
			self.record("on_rpc_complete");
		}

		fn on_auth_event(&self, _event: &AuthEvent) {
			self.record("on_auth_event");
		}

		fn on_session_event(&self, _event: &SessionEvent) {
			self.record("on_session_event");
		}

		fn on_network_bytes(&self, _event: &NetworkBytesEvent) {
			self.record("on_network_bytes");
		}

		fn on_bucket_operation(&self, _event: &BucketOperationEvent) {
			self.record("on_bucket_operation");
		}

		fn on_http_request_started(&self, _event: &HttpRequestStartEvent) {
			self.record("on_http_request_started");
		}

		fn on_http_request_complete(&self, _event: &HttpRequestEvent) {
			self.record("on_http_request_complete");
		}
	}

	/// An observer that panics.
	struct PanickingObserver;

	impl ExecutionObserver for PanickingObserver {
		fn on_statement_complete(&self, _event: &StatementEvent) {
			panic!("audit observer hit a poisoned lock");
		}
	}

	/// A panicking observer must not take the query down with it, nor silence
	/// the observers registered after it.
	///
	/// Both halves matter: the panic unwinding out of the emit site turns an
	/// observability fault into a failed query, and iterating in order without
	/// isolation means everything downstream of the failure stops receiving
	/// events with no indication.
	#[test]
	fn a_panicking_observer_is_contained() {
		let downstream = Arc::new(Recorder {
			seen: Mutex::new(Vec::new()),
		});
		let fan_out = FanOutObserver::new(vec![
			Arc::new(PanickingObserver),
			Arc::clone(&downstream) as Arc<dyn ExecutionObserver>,
		]);

		// The default hook prints the panic; keep the test output readable.
		let previous = std::panic::take_hook();
		std::panic::set_hook(Box::new(|_| {}));
		fan_out.on_statement_complete(&mk_event());
		std::panic::set_hook(previous);

		assert_eq!(
			downstream.seen(),
			vec!["on_statement_complete"],
			"an observer after the panicking one must still receive the event"
		);
	}

	#[test]
	fn forwards_every_hook() {
		let a = Arc::new(Recorder::default());
		let b = Arc::new(Recorder::default());
		let f = FanOutObserver::new([
			Arc::clone(&a) as Arc<dyn ExecutionObserver>,
			Arc::clone(&b) as Arc<dyn ExecutionObserver>,
		]);

		f.on_statement_complete(&mk_event());
		f.on_query_complete(&QueryEvent {
			safe: QueryEventSafe {
				outcome: Outcome::Success,
				duration: Duration::from_millis(1),
				counters: QueryCounters {
					total: 1,
					ok: 1,
					err: 0,
				},
				error_class: None,
			},
			ctx: QueryEventCtx::default(),
		});
		f.on_transaction_complete(&TransactionEvent {
			safe: TransactionEventSafe {
				outcome: Outcome::Success,
				write: true,
				duration: Duration::from_millis(1),
				metrics: TransactionMetricsSnapshot::default(),
				error_class: None,
			},
			ctx: TransactionEventCtx::default(),
		});
		f.on_rpc_complete(&RpcEvent {
			safe: RpcEventSafe {
				method: surrealdb_rpc::Method::Ping,
				outcome: Outcome::Success,
				duration: Duration::from_millis(1),
				error_class: None,
			},
			ctx: RpcEventCtx::default(),
		});
		f.on_auth_event(&AuthEvent {
			safe: AuthEventSafe {
				action: AuthAction::Signin,
				scope: AuthScope::Root,
				outcome: Outcome::Success,
				error_class: None,
			},
			ctx: AuthEventCtx::default(),
		});
		f.on_session_event(&SessionEvent {
			safe: SessionEventSafe {
				action: SessionAction::Connect,
				protocol: SessionProtocol::Http,
				duration: None,
			},
			ctx: SessionEventCtx::default(),
		});
		f.on_network_bytes(&NetworkBytesEvent {
			safe: NetworkBytesEventSafe {
				direction: NetworkDirection::Sent,
				protocol: SessionProtocol::Http,
				bytes: 64,
			},
			ctx: NetworkBytesEventCtx::default(),
		});
		f.on_bucket_operation(&BucketOperationEvent {
			safe: BucketOperationEventSafe {
				backend: "s3",
				op: BucketOp::Put,
				outcome: Outcome::Success,
				sent: 64,
				received: 0,
			},
			ctx: BucketOperationEventCtx::default(),
		});
		f.on_http_request_started(&HttpRequestStartEvent {
			safe: HttpRequestStartEventSafe {
				method: HttpMethod::Get,
				route: Some("/sql"),
				version: HttpVersion::Http11,
			},
			ctx: HttpRequestEventCtx::default(),
		});
		f.on_http_request_complete(&HttpRequestEvent {
			safe: HttpRequestEventSafe {
				method: HttpMethod::Get,
				route: Some("/sql"),
				status_code: Some(200),
				version: HttpVersion::Http11,
				outcome: Outcome::Success,
				duration: Duration::from_millis(1),
				request_size: Some(128),
				response_size: Some(256),
				error_class: None,
			},
			ctx: HttpRequestEventCtx::default(),
		});

		// Each hook was driven exactly once, so every observer must have seen
		// every hook, in the order they were dispatched. A hook the fan-out
		// silently drops is absent here.
		assert_eq!(a.seen(), EVENT_HOOKS);
		assert_eq!(b.seen(), EVENT_HOOKS);
	}

	/// The capability query must not reach the observers once the fan-out is
	/// built, so an observer that panics when asked cannot fail a statement.
	///
	/// `PanickingObserver` only overrides an event hook, so this asserts the
	/// weaker property that no observer is consulted at all: the answer comes
	/// from the value cached at construction. An observer that panicked in
	/// `needs_statement_text` itself would fail `FanOutObserver::new`, which is
	/// startup rather than query time.
	#[test]
	fn needs_statement_text_does_not_consult_observers() {
		let counting = Arc::new(Counting {
			statements: AtomicUsize::new(0),
			wants_sql: true,
		});
		let asked = Arc::new(AtomicUsize::new(0));
		let probe = Arc::new(AskCounting {
			asked: Arc::clone(&asked),
		});
		let fan_out = FanOutObserver::new([
			probe as Arc<dyn ExecutionObserver>,
			counting as Arc<dyn ExecutionObserver>,
		]);
		let after_construction = asked.load(Ordering::Relaxed);

		for _ in 0..3 {
			assert!(fan_out.needs_statement_text());
		}

		assert_eq!(
			asked.load(Ordering::Relaxed),
			after_construction,
			"the capability must be read from the cache, not re-asked per statement"
		);
	}

	/// Counts how many times it is asked whether it wants statement text.
	struct AskCounting {
		asked: Arc<AtomicUsize>,
	}

	impl ExecutionObserver for AskCounting {
		fn needs_statement_text(&self) -> bool {
			self.asked.fetch_add(1, Ordering::Relaxed);
			false
		}
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
