//! The [`ExecutionObserver`] trait and the no-op default implementation.

use super::events::{
	AuthEvent, HttpRequestEvent, HttpRequestStartEvent, NetworkBytesEvent, QueryEvent, RpcEvent,
	SessionEvent, StatementEvent, TransactionEvent,
};

/// Hook surface for receiving structured observability events from the core
/// during query execution, transactions, RPC calls, authentication, and
/// session lifecycle.
///
/// Implementations must be cheap to call from hot paths. They run on the
/// executor thread that produced the event. Any I/O must be handed off to a
/// background task or channel.
///
/// Events are split into `safe` (bounded, no customer data) and `ctx` (may
/// contain customer-identifiable data) sub-structs. Public-facing consumers
/// (e.g. the community Prometheus exporter feeding the unauthenticated
/// `/metrics` endpoint) MUST only read the `safe` half.
pub trait ExecutionObserver: Send + Sync + 'static {
	/// Called when a single top-level statement completes.
	fn on_statement_complete(&self, _event: &StatementEvent) {}

	/// Called when an entire query batch completes.
	fn on_query_complete(&self, _event: &QueryEvent) {}

	/// Called when a transaction is committed or cancelled.
	fn on_transaction_complete(&self, _event: &TransactionEvent) {}

	/// Called when an RPC method invocation completes.
	fn on_rpc_complete(&self, _event: &RpcEvent) {}

	/// Called when an authentication attempt (successful or failed) completes.
	fn on_auth_event(&self, _event: &AuthEvent) {}

	/// Called on session connect and disconnect.
	fn on_session_event(&self, _event: &SessionEvent) {}

	/// Called whenever the server records inbound or outbound bytes on a
	/// client-facing protocol. Recording sites pass the bounded `safe`
	/// half plus a `ctx` half that may carry tenant identity; community
	/// observers read only `safe`, enterprise observers read both.
	fn on_network_bytes(&self, _event: &NetworkBytesEvent) {}

	/// Called immediately after an HTTP request enters the tower stack,
	/// before the inner service runs. Pairs with
	/// [`Self::on_http_request_complete`] dispatched on completion.
	///
	/// Active-request gauges live here so the `+1` happens before any
	/// inner work, regardless of how the inner stack later returns.
	fn on_http_request_started(&self, _event: &HttpRequestStartEvent) {}

	/// Called once per HTTP request completion, after the inner stack
	/// returns. Carries the resolved status code, latency and wire sizes
	/// alongside the bounded request-shape fields.
	///
	/// Always paired with a prior [`Self::on_http_request_started`] for
	/// the same request.
	fn on_http_request_complete(&self, _event: &HttpRequestEvent) {}

	/// Returns `true` when the observer is a no-op shell that ignores
	/// every event. The only implementation that should override this
	/// is [`NoopObserver`]; downstream emit sites use it to short-circuit
	/// event allocation/dispatch when nothing is observing.
	///
	/// The default `false` keeps custom observers correct: even if they
	/// implement only some of the trait methods they will still receive
	/// every event the executor emits.
	fn is_noop(&self) -> bool {
		false
	}

	/// Returns `true` if the observer wants the full statement SQL text
	/// populated on [`StatementEvent`]s.
	///
	/// Defaults to `false`. The community metrics observer MUST always return
	/// `false`; enterprise audit observers may opt in.
	fn needs_statement_text(&self) -> bool {
		false
	}
}

/// An observer that does nothing. Used as the default before startup wires
/// anything more interesting in.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoopObserver;

impl ExecutionObserver for NoopObserver {
	/// Mark the no-op shell so downstream emit sites can short-circuit
	/// event allocation. See [`ExecutionObserver::is_noop`].
	fn is_noop(&self) -> bool {
		true
	}
}

#[cfg(test)]
mod tests {
	use std::time::Duration;

	use super::super::events::{
		AuthAction, AuthEvent, AuthEventCtx, AuthEventSafe, AuthScope, HttpMethod,
		HttpRequestEvent, HttpRequestEventCtx, HttpRequestEventSafe, HttpRequestStartEvent,
		HttpRequestStartEventSafe, HttpVersion, NetworkBytesEvent, NetworkBytesEventCtx,
		NetworkBytesEventSafe, NetworkDirection, Outcome, QueryCounters, QueryEvent, QueryEventCtx,
		QueryEventSafe, RpcEvent, RpcEventCtx, RpcEventSafe, SessionAction, SessionEvent,
		SessionEventCtx, SessionEventSafe, SessionProtocol, StatementEvent, StatementEventCtx,
		StatementEventSafe, StatementType, TransactionEvent, TransactionEventCtx,
		TransactionEventSafe, TransactionMetricsSnapshot,
	};
	use super::*;

	#[test]
	fn noop_observer_accepts_every_event_kind() {
		// Regression guard: adding a new event kind to `ExecutionObserver` without
		// a default impl would stop NoopObserver compiling, and this test still
		// ensures the default impl does not panic on any of them.
		let obs = NoopObserver;
		obs.on_statement_complete(&StatementEvent {
			safe: StatementEventSafe {
				kind: StatementType::Select,
				outcome: Outcome::Success,
				duration: Duration::from_millis(1),
				read_only: true,
				result_rows: 0,
				error_class: None,
			},
			ctx: StatementEventCtx::default(),
		});
		obs.on_query_complete(&QueryEvent {
			safe: QueryEventSafe {
				outcome: Outcome::Error,
				duration: Duration::from_millis(2),
				counters: QueryCounters {
					total: 1,
					ok: 0,
					err: 1,
				},
				error_class: None,
			},
			ctx: QueryEventCtx::default(),
		});
		obs.on_transaction_complete(&TransactionEvent {
			safe: TransactionEventSafe {
				outcome: Outcome::Success,
				write: true,
				duration: Duration::from_millis(3),
				metrics: TransactionMetricsSnapshot::default(),
				error_class: None,
			},
			ctx: TransactionEventCtx::default(),
		});
		obs.on_rpc_complete(&RpcEvent {
			safe: RpcEventSafe {
				method: crate::rpc::Method::Ping,
				outcome: Outcome::Success,
				duration: Duration::from_millis(4),
				error_class: None,
			},
			ctx: RpcEventCtx::default(),
		});
		obs.on_auth_event(&AuthEvent {
			safe: AuthEventSafe {
				action: AuthAction::Signin,
				scope: AuthScope::Root,
				outcome: Outcome::Success,
				error_class: None,
			},
			ctx: AuthEventCtx::default(),
		});
		obs.on_session_event(&SessionEvent {
			safe: SessionEventSafe {
				action: SessionAction::Connect,
				protocol: SessionProtocol::Http,
				duration: None,
			},
			ctx: SessionEventCtx::default(),
		});
		obs.on_network_bytes(&NetworkBytesEvent {
			safe: NetworkBytesEventSafe {
				direction: NetworkDirection::Sent,
				protocol: SessionProtocol::Http,
				bytes: 64,
			},
			ctx: NetworkBytesEventCtx::default(),
		});
		obs.on_http_request_started(&HttpRequestStartEvent {
			safe: HttpRequestStartEventSafe {
				method: HttpMethod::Get,
				route: Some("/sql"),
				version: HttpVersion::Http11,
			},
			ctx: HttpRequestEventCtx::default(),
		});
		obs.on_http_request_complete(&HttpRequestEvent {
			safe: HttpRequestEventSafe {
				method: HttpMethod::Get,
				route: Some("/sql"),
				status_code: Some(200),
				version: HttpVersion::Http11,
				outcome: Outcome::Success,
				duration: Duration::from_millis(5),
				request_size: Some(128),
				response_size: Some(256),
				error_class: None,
			},
			ctx: HttpRequestEventCtx::default(),
		});
		// Default answer for `needs_statement_text` is critical: enabling it on a
		// community build would cause the executor to populate `StatementEventCtx`
		// with the raw SQL text.
		assert!(!obs.needs_statement_text());
		// `NoopObserver` MUST advertise as no-op so emit sites can
		// short-circuit allocation and dispatch.
		assert!(obs.is_noop());
	}
}
