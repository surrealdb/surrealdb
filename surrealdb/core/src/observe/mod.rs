//! Execution-time observability hooks for SurrealDB.
//!
//! This module defines the [`ExecutionObserver`] trait and the event types it
//! receives, plus the [`ObservabilityProvider`] composer extension used by the
//! server to supply an observer at startup.
//!
//! # Data safety
//!
//! Every event type is split into two sub-structs: `*Safe` and `*Ctx`.
//!
//! - `*Safe` fields have bounded cardinality and never contain customer data, identifiers, SQL
//!   text, or values derived from user input. They are safe to use as attributes on metrics exposed
//!   to unauthenticated consumers.
//! - `*Ctx` fields may contain namespace/database/user identifiers or SQL text. They MUST NOT be
//!   emitted to any unauthenticated sink. Enterprise audit destinations may consume them.
//!
//! This split is the primary defence-in-depth mechanism for the public
//! `/metrics` endpoint.

pub mod error_class;
pub mod events;
pub mod fan_out;
pub mod observer;
pub mod process;
pub mod provider;

pub use error_class::{
	AUTH as ERROR_AUTH, CLIENT as ERROR_CLIENT, CTX_CANCELLED as ERROR_CTX_CANCELLED,
	CTX_TIMEOUT as ERROR_CTX_TIMEOUT, INTERNAL as ERROR_INTERNAL, PARSE as ERROR_PARSE,
	PERMISSION as ERROR_PERMISSION, STORAGE as ERROR_STORAGE, TIMEOUT as ERROR_TIMEOUT,
	TXN_CONFLICT as ERROR_TXN_CONFLICT, TXN_CREATE_FAILED as ERROR_TXN_CREATE_FAILED,
	TXN_TIMEOUT as ERROR_TXN_TIMEOUT,
};
pub use events::{
	AuthAction, AuthEvent, AuthEventCtx, AuthEventSafe, AuthScope, HttpMethod, HttpRequestEvent,
	HttpRequestEventCtx, HttpRequestEventSafe, HttpRequestStartEvent, HttpRequestStartEventSafe,
	HttpVersion, NetworkBytesEvent, NetworkBytesEventCtx, NetworkBytesEventSafe, NetworkDirection,
	Outcome, QueryCounters, QueryEvent, QueryEventCtx, QueryEventSafe, RpcEvent, RpcEventCtx,
	RpcEventSafe, SessionAction, SessionEvent, SessionEventCtx, SessionEventSafe, SessionProtocol,
	StatementEvent, StatementEventCtx, StatementEventSafe, StatementType, TenantIdentity,
	TransactionEvent, TransactionEventCtx, TransactionEventSafe, TransactionMetrics,
	TransactionMetricsSnapshot,
};
pub use fan_out::FanOutObserver;
pub use observer::{ExecutionObserver, NoopObserver};
pub use process::{ProcessSnapshot, process_snapshot, refresh_process_snapshot};
pub use provider::{ObservabilityProvider, requirements};
