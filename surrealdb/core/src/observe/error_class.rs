//! Bounded error-classification constants used as the `error_class` attribute
//! on the `surrealdb.*` metric families when an event's outcome is
//! [`super::Outcome::Error`].
//!
//! Values are `&'static str` so they are safe to use as metric attribute
//! values without inflating cardinality. Add new variants only when an
//! existing one would be misleading; do not derive from raw error messages.

/// Caller is not authenticated for the requested resource.
pub const AUTH: &str = "auth";
/// Caller is authenticated but lacks permission for the resource.
pub const PERMISSION: &str = "permission";
/// Statement parse / syntax error.
pub const PARSE: &str = "parse";
/// Caller-visible logical error (validation, type mismatch, missing record).
pub const CLIENT: &str = "client";
/// Transaction commit conflict (optimistic-lock / version / write-write).
pub const TXN_CONFLICT: &str = "txn_conflict";
/// Failed to construct or open a transaction.
pub const TXN_CREATE_FAILED: &str = "txn_create_failed";
/// Statement timed out (per-statement timeout, not ctx).
pub const TIMEOUT: &str = "timeout";
/// Transaction-scoped timeout fired (e.g. `BEGIN ... TIMEOUT 5s`).
pub const TXN_TIMEOUT: &str = "txn_timeout";
/// Operation aborted because the surrounding context was cancelled.
pub const CTX_CANCELLED: &str = "ctx_cancelled";
/// Operation aborted because the surrounding context timed out.
pub const CTX_TIMEOUT: &str = "ctx_timeout";
/// Storage-backend-level error (engine returned an error to the kvs layer).
pub const STORAGE: &str = "storage";
/// Catch-all for unexpected / internal errors.
pub const INTERNAL: &str = "internal";

/// Classify a [`surrealdb_types::Error`] into one of the bounded
/// `error_class` constants above.
///
/// Drives the `error_class` metric attribute on the
/// `surrealdb.{query,rpc,auth}.*` instrument families: every error path
/// that bottoms out in a `surrealdb_types::Error` (RPC handlers, query
/// batches, auth flows) goes through this single classifier so dashboards
/// see the same label set regardless of which producer dispatched the
/// event.
///
/// Mapping rules:
///
/// - `Validation` → [`PARSE`] (parse / invalid-shape errors).
/// - `Configuration` → [`CLIENT`] (caller asked for an unsupported feature).
/// - `Query(QueryError::TimedOut)` → [`TIMEOUT`].
/// - `Query(QueryError::Cancelled)` → [`CTX_CANCELLED`].
/// - `Query(QueryError::TransactionConflict)` → [`TXN_CONFLICT`].
/// - `Query(_)` (incl. `NotExecuted` and `None`) → [`CLIENT`].
/// - `Serialization` / `NotFound` / `AlreadyExists` / `Connection` / `Thrown` → [`CLIENT`].
/// - `NotAllowed` → [`PERMISSION`].
/// - `Internal` / `Context` → [`INTERNAL`].
pub fn classify_types_error(err: &surrealdb_types::Error) -> &'static str {
	use surrealdb_types::{ErrorDetails, QueryError};

	match err.details() {
		ErrorDetails::Validation(_) => PARSE,
		ErrorDetails::Configuration(_) => CLIENT,
		ErrorDetails::Query(detail) => match detail {
			Some(QueryError::TimedOut {
				..
			}) => TIMEOUT,
			Some(QueryError::Cancelled) => CTX_CANCELLED,
			Some(QueryError::TransactionConflict) => TXN_CONFLICT,
			// `NotExecuted` and the wire-form `None` collapse to the
			// generic client bucket: the executor's unexecuted-statement
			// emit path already records the more specific `txn_*` /
			// `ctx_*` classes on `surrealdb.statement.*`, so we don't
			// need to reach for those here.
			_ => CLIENT,
		},
		ErrorDetails::Serialization(_) => CLIENT,
		ErrorDetails::NotAllowed(_) => PERMISSION,
		ErrorDetails::NotFound(_) => CLIENT,
		ErrorDetails::AlreadyExists(_) => CLIENT,
		ErrorDetails::Connection(_) => CLIENT,
		ErrorDetails::Thrown => CLIENT,
		ErrorDetails::Internal => INTERNAL,
		ErrorDetails::Context => INTERNAL,
		// `ErrorDetails` is `#[non_exhaustive]`. New variants land in
		// the `internal` bucket by default so unclassified errors do
		// not silently collapse to the `-` sentinel; revisit when a
		// new variant appears in `surrealdb_types::error`.
		_ => INTERNAL,
	}
}

/// Classify an [`anyhow::Error`] into one of the bounded `error_class`
/// constants by downcasting to the well-known concrete error types
/// produced by the executor / kvs layers.
///
/// Mapping rules (first match wins):
///
/// - Downcasts to [`crate::kvs::Error`]: retryable variants → [`TXN_CONFLICT`]; everything else
///   from the kvs layer → [`STORAGE`].
/// - Downcasts to [`surrealdb_types::Error`]: delegates to [`classify_types_error`].
/// - Anything else: [`INTERNAL`].
pub fn classify_anyhow_error(err: &anyhow::Error) -> &'static str {
	if let Some(kvs_err) = err.downcast_ref::<crate::kvs::Error>() {
		if kvs_err.is_retryable() {
			return TXN_CONFLICT;
		}
		return STORAGE;
	}
	if let Some(types_err) = err.downcast_ref::<surrealdb_types::Error>() {
		return classify_types_error(types_err);
	}
	INTERNAL
}

/// Classify an HTTP response status code into one of the bounded
/// `error_class` constants. Returns `None` for 1xx / 2xx / 3xx, where
/// the metric attribute should remain unset.
///
/// `401 Unauthorized` → [`AUTH`]; `403 Forbidden` → [`PERMISSION`];
/// `408 Request Timeout` → [`TIMEOUT`]; everything else in the 4xx range
/// → [`CLIENT`]; the 5xx range → [`INTERNAL`].
pub fn classify_http_status(status: u16) -> Option<&'static str> {
	match status {
		401 => Some(AUTH),
		403 => Some(PERMISSION),
		408 => Some(TIMEOUT),
		400..=499 => Some(CLIENT),
		500..=599 => Some(INTERNAL),
		_ => None,
	}
}
