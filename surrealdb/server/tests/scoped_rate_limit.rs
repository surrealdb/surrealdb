//! The auth rate limiter applies to scoped credential routes.
//!
//! The limiter reads its configuration from the environment once per process,
//! so this test owns its own test binary and sets that configuration before
//! the first request.

// TODO: Remove once this crate no longer needs it; see `absolute_paths` in the workspace lints.
#![allow(clippy::absolute_paths)]
#![recursion_limit = "256"]

use std::net::SocketAddr;

use reqwest::{Client, StatusCode};
use surrealdb_core::CommunityComposer;
use surrealdb_core::kvs::Datastore;
use surrealdb_rpc::capabilities::Capabilities;
use surrealdb_server::ntw::{RouterOptions, SurrealRouter};
use tokio_util::sync::CancellationToken;

const BURST: u32 = 3;

#[tokio::test]
async fn scoped_signin_spends_the_auth_rate_limit() {
	// SAFETY: this is the only test in this binary, and it sets the variables
	// before the server starts or any other thread reads the environment.
	unsafe {
		std::env::set_var("SURREAL_HTTP_AUTH_RATE_LIMIT_ENABLED", "true");
		std::env::set_var("SURREAL_HTTP_AUTH_RATE_LIMIT_BURST", BURST.to_string());
		std::env::set_var("SURREAL_HTTP_AUTH_RATE_LIMIT_PER_MINUTE", "1");
	}

	let (send, recv) = surrealdb_core::channel::bounded(128);
	let datastore = Datastore::builder()
		.with_capabilities(Capabilities::all())
		.with_notify(send)
		.with_auth(true)
		.build_with_path("memory")
		.await
		.expect("datastore");
	let canceller = CancellationToken::new();
	let router = SurrealRouter::build::<CommunityComposer>(
		RouterOptions::default(),
		datastore,
		recv,
		canceller.clone(),
		(),
	)
	.await
	.unwrap_or_else(|e| panic!("router: {e}"));
	let handle = axum_server::Handle::<SocketAddr>::new();
	let app = router.into_router();
	tokio::spawn({
		let handle = handle.clone();
		async move {
			axum_server::bind("127.0.0.1:0".parse().expect("address"))
				.handle(handle)
				.serve(app.into_make_service_with_connect_info::<SocketAddr>())
				.await
				.ok();
		}
	});
	let address = handle.listening().await.expect("the server to bind");

	// Scoped and unscoped attempts from one client spend the same budget.
	let mut statuses = Vec::new();
	for path in
		["/ns/foo/db/bar/signin", "/signin", "/ns/foo/db/bar/ac/users/signin", "/ac/admins/signin"]
	{
		let status = Client::new()
			.post(format!("http://{address}{path}"))
			.header("Content-Type", "application/json")
			.body(r#"{"user":"nobody","pass":"wrong"}"#)
			.send()
			.await
			.expect("request")
			.status();
		statuses.push(status);
	}
	assert!(
		statuses[..BURST as usize].iter().all(|s| *s != StatusCode::TOO_MANY_REQUESTS),
		"{statuses:?}"
	);
	assert_eq!(statuses[BURST as usize], StatusCode::TOO_MANY_REQUESTS, "{statuses:?}");

	canceller.cancel();
	handle.shutdown();
}
