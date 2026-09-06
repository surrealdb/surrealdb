//! Regression: the HTTP RPC client must not leak an attached server session on
//! each connect/disconnect.
//!
//! The SDK's HTTP engine attaches a server-side session on connect, promotes it
//! to a real principal via `signin`, and tears it down with `Detach` on drop.
//! The server routes `Detach` through the HTTP RPC ownership gate
//! (`verify_caller_for_session`), so the teardown must carry the session's auth.
//! Previously `handle_session_drop` sent `Detach` from a fresh, unauthenticated
//! `SessionState`, so the server rejected it with `session_not_found` and never
//! released the session — leaking one attached-session slot per connection until
//! `HTTP_MAX_ATTACHED_SESSIONS` wedged the HTTP transport with "Method not
//! allowed".
//!
//! This test pins the slot down to 1 via `SURREAL_HTTP_MAX_ATTACHED_SESSIONS`,
//! so a single leaked session makes the *next* connect fail. With the leak it
//! reliably fails on the second iteration; with the fix every iteration
//! succeeds.

mod common;

#[cfg(test)]
mod tests {
	use std::collections::HashMap;
	use std::time::Duration;

	use surrealdb::Surreal;
	use surrealdb::engine::remote::http::Http;
	use surrealdb::opt::auth::Root;
	use tokio::time::sleep;

	use super::common::{self, PASS, StartServerArguments, USER};

	#[test_log::test(tokio::test)]
	async fn http_detach_frees_attached_session() -> Result<(), Box<dyn std::error::Error>> {
		// Cap the attached-session pool low so that a per-connection leak
		// overflows it within a handful of iterations. The cap is kept well
		// above the small, constant baseline the server's own startup
		// readiness probe leaves behind, so a passing run reflects the test's
		// own connect/drop cycles rather than startup noise.
		const CAP: usize = 8;
		const ITERATIONS: usize = 20;

		let mut vars = HashMap::new();
		vars.insert("SURREAL_HTTP_MAX_ATTACHED_SESSIONS".to_string(), CAP.to_string());

		let (addr, _server) = common::start_server(StartServerArguments {
			vars: Some(vars),
			..Default::default()
		})
		.await
		.unwrap();

		// Each iteration is a fresh authenticated HTTP client that connects,
		// signs in (promoting its attached server session to root), runs a
		// query, and drops. If the teardown `Detach` does not free the
		// server-side session, every iteration leaks one slot; once the cap is
		// reached, further connects fail with "Method not allowed". With the
		// leak fixed the count stays flat and all iterations succeed.
		for i in 0..ITERATIONS {
			let db = Surreal::new::<Http>(addr.as_str())
				.await
				.unwrap_or_else(|e| panic!("connect #{i} failed (attached-session leak?): {e}"));
			db.signin(Root {
				username: USER.to_string(),
				password: PASS.to_string(),
			})
			.await
			.unwrap_or_else(|e| panic!("signin #{i} failed (attached-session leak?): {e}"));
			db.use_ns("test").use_db("test").await.unwrap_or_else(|e| panic!("use #{i}: {e}"));
			db.query("INFO FOR DB")
				.await
				.unwrap_or_else(|e| panic!("query #{i} failed: {e}"))
				.check()
				.unwrap_or_else(|e| panic!("query #{i} returned error: {e}"));

			// Dropping the client triggers the asynchronous teardown `Detach`.
			drop(db);
			// Give the fire-and-forget `Detach` time to reach the server before
			// the next connect consumes a session slot.
			sleep(Duration::from_millis(100)).await;
		}

		Ok(())
	}
}
