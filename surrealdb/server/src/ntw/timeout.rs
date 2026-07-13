//! Wall-clock request timeout helpers for the HTTP transports.
//!
//! These reuse the datastore's global `--query-timeout` / `SURREAL_QUERY_TIMEOUT`
//! value (default off) as a wall-clock guard around query / GraphQL execution.
//! The deadline plumbed into the executor via `Datastore::setup_ctx` only fires
//! at executor yield points and does not bound work such as the GraphQL engine
//! itself; this wall-clock backstop bounds the whole request. When no timeout is
//! configured the future is awaited directly, so there is no behaviour change
//! unless an operator opts in.

use std::future::Future;
use std::time::Duration;

use surrealdb_core::rpc::query_timeout_error;
use surrealdb_types::Error;

/// Run a fallible datastore future (`/sql`, `/gql`) under the configured
/// wall-clock query timeout. On elapse the future is dropped and a structured
/// [`QueryError::TimedOut`] error is returned so the transport renders the
/// standard SurrealDB error envelope rather than a bare 408.
pub(crate) async fn with_query_timeout<T, F>(timeout: Option<Duration>, fut: F) -> Result<T, Error>
where
	F: Future<Output = Result<T, Error>>,
{
	match timeout {
		Some(dur) => match tokio::time::timeout(dur, fut).await {
			Ok(inner) => inner,
			Err(_elapsed) => {
				warn!(
					target: "surrealdb::net",
					timeout = ?dur,
					"HTTP request exceeded the configured query timeout"
				);
				Err(query_timeout_error(dur))
			}
		},
		None => fut.await,
	}
}

/// Run an infallible future (the GraphQL engine returns a response envelope
/// rather than a `Result`) under the configured wall-clock query timeout.
/// Returns `Err(duration)` when the deadline elapses so the caller can build a
/// spec-compliant GraphQL error response and classify the metric as a timeout.
///
/// Only the `/graphql` service consumes this, so it is gated on the `graphql`
/// feature to avoid a `dead_code` warning in builds without it.
#[cfg(feature = "graphql")]
pub(crate) async fn with_request_timeout<T, F>(
	timeout: Option<Duration>,
	fut: F,
) -> Result<T, Duration>
where
	F: Future<Output = T>,
{
	match timeout {
		Some(dur) => match tokio::time::timeout(dur, fut).await {
			Ok(inner) => Ok(inner),
			Err(_elapsed) => {
				warn!(
					target: "surrealdb::net",
					timeout = ?dur,
					"HTTP request exceeded the configured query timeout"
				);
				Err(dur)
			}
		},
		None => Ok(fut.await),
	}
}

/// Message used for the GraphQL error envelope when a request trips the
/// wall-clock query-timeout guard. Kept consistent with [`query_timeout_error`]
/// (same wording and duration formatting as the deadline-based timeout) so the
/// timeout reads identically across transports.
///
/// Only the `/graphql` service consumes this, so it is gated on the `graphql`
/// feature to avoid a `dead_code` warning in builds without it.
#[cfg(feature = "graphql")]
pub(crate) fn graphql_timeout_message(duration: Duration) -> String {
	format!(
		"The GraphQL request was not executed because it exceeded the timeout: {}",
		surrealdb_types::Duration::from(duration)
	)
}

#[cfg(test)]
mod tests {
	use surrealdb_types::QueryError;

	use super::*;

	#[tokio::test]
	async fn passes_through_when_no_timeout_configured() {
		let out: Result<u32, Error> = with_query_timeout(None, async { Ok(7) }).await;
		assert_eq!(out.unwrap(), 7);
	}

	#[tokio::test]
	async fn returns_value_when_within_timeout() {
		let out: Result<u32, Error> =
			with_query_timeout(Some(Duration::from_secs(30)), async { Ok(9) }).await;
		assert_eq!(out.unwrap(), 9);
	}

	#[tokio::test]
	async fn returns_timed_out_error_when_exceeded() {
		let out: Result<u32, Error> = with_query_timeout(Some(Duration::from_millis(20)), async {
			tokio::time::sleep(Duration::from_secs(30)).await;
			Ok(1)
		})
		.await;
		let err = out.unwrap_err();
		assert!(
			matches!(err.query_details(), Some(QueryError::TimedOut { .. })),
			"expected a QueryError::TimedOut, got {err:?}"
		);
	}

	#[cfg(feature = "graphql")]
	#[tokio::test]
	async fn request_timeout_reports_duration_on_elapse() {
		let dur = Duration::from_millis(20);
		let out: Result<u32, Duration> = with_request_timeout(Some(dur), async {
			tokio::time::sleep(Duration::from_secs(30)).await;
			1
		})
		.await;
		assert_eq!(out.unwrap_err(), dur);
	}

	#[cfg(feature = "graphql")]
	#[tokio::test]
	async fn request_timeout_passes_through_without_config() {
		let out: Result<u32, Duration> = with_request_timeout(None, async { 3 }).await;
		assert_eq!(out.unwrap(), 3);
	}

	#[test]
	fn timeout_error_carries_structured_detail() {
		let dur = Duration::from_secs(5);
		let err = query_timeout_error(dur);
		assert!(matches!(
			err.query_details(),
			Some(QueryError::TimedOut { duration } ) if *duration == dur
		));
	}

	#[test]
	fn timeout_error_message_matches_deadline_wording() {
		// The wall-clock guard must read identically to the deadline-based
		// `Error::QueryTimedout`, using the SurrealQL duration format.
		let err = query_timeout_error(Duration::from_secs(5));
		assert_eq!(err.message(), "The query was not executed because it exceeded the timeout: 5s");
	}

	#[cfg(feature = "graphql")]
	#[test]
	fn graphql_timeout_message_uses_surrealql_duration() {
		assert_eq!(
			graphql_timeout_message(Duration::from_secs(5)),
			"The GraphQL request was not executed because it exceeded the timeout: 5s"
		);
	}
}
