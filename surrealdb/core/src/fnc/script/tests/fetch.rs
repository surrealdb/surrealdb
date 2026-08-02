use std::str::FromStr;

use surrealdb_types::{Value, object};
use wiremock::matchers::{body_string, header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use crate::dbs::capabilities::{NetTarget, Targets};
use crate::dbs::{Capabilities, Session};
use crate::kvs::Datastore;

#[tokio::test]
async fn test_fetch_get() {
	// Prepare mock server
	let server = MockServer::start().await;
	Mock::given(method("GET"))
		.and(path("/hello"))
		.and(header("some-header", "some-value"))
		.respond_with(ResponseTemplate::new(200).set_body_string("some body once told me"))
		.expect(1)
		.mount(&server)
		.await;

	// Execute test
	let ds = Datastore::builder()
		.with_capabilities(Capabilities::all())
		.build_with_path("memory")
		.await
		.unwrap();
	let sess = Session::owner();
	let sql = format!(
		r#"
        RETURN function() {{
            let res = await fetch('{}/hello',{{
                headers: {{
                    "some-header": "some-value",
                }}
            }});
            let body = await res.text();

            return {{ status: res.status, body: body }};
        }}
    "#,
		server.uri()
	);
	let res = ds.execute(&sql, &sess, None).await;

	let res = res.unwrap().remove(0).output().unwrap();

	server.verify().await;

	assert_eq!(
		res,
		// "{ body: 'some body once told me', status: 200f }",
		Value::Object(object! {
			body: "some body once told me".to_string(),
			status: 200,
		}),
		"Unexpected result: {:?}",
		res
	);
}

#[tokio::test]
async fn test_fetch_put() {
	// Prepare mock server
	let server = MockServer::start().await;
	Mock::given(method("PUT"))
		.and(path("/hello"))
		.and(header("some-header", "some-value"))
		.and(body_string("some text"))
		.respond_with(ResponseTemplate::new(201).set_body_string("some body once told me"))
		.expect(1)
		.mount(&server)
		.await;

	// Execute test
	let ds = Datastore::builder()
		.with_capabilities(Capabilities::all())
		.build_with_path("memory")
		.await
		.unwrap();
	let sess = Session::owner();
	let sql = format!(
		r#"
        RETURN function() {{
            let res = await fetch('{}/hello',{{
                method: "PuT",
                headers: {{
                    "some-header": "some-value",
                }},
                body: "some text",
            }});
            let body = await res.text();

            return {{ status: res.status, body: body }};
        }}
    "#,
		server.uri()
	);
	let res = ds.execute(&sql, &sess, None).await;

	let res = res.unwrap().remove(0).output().unwrap();

	server.verify().await;

	assert_eq!(
		res,
		Value::Object(object! {
			body: "some body once told me".to_string(),
			status: 201,
		}),
		"Unexpected result: {res:?}"
	);
}

#[tokio::test]
async fn test_fetch_error() {
	// Prepare mock server
	let server = MockServer::start().await;
	Mock::given(method("PROPPATCH"))
		.and(path("/hello"))
		.and(header("some-header", "some-value"))
		.and(body_string("some text"))
		.respond_with(ResponseTemplate::new(500).set_body_json(serde_json::json!({
			"foo": "bar",
			"baz": 2,
		})))
		.expect(1)
		.mount(&server)
		.await;

	// Execute test
	let ds = Datastore::builder()
		.with_capabilities(Capabilities::all())
		.build_with_path("memory")
		.await
		.unwrap();
	let sess = Session::owner();
	let sql = format!(
		r#"
        RETURN function() {{
            let res = await fetch('{}/hello',{{
                method: "PROPPATCH",
                headers: {{
                    "some-header": "some-value",
                }},
                body: "some text",
            }});
            let body = await res.json();

            return {{ status: res.status, body: body }};
        }}
    "#,
		server.uri()
	);
	let res = ds.execute(&sql, &sess, None).await;

	let res = res.unwrap().remove(0).output().unwrap();

	server.verify().await;

	assert_eq!(
		res,
		Value::Object(object! {
			body: Value::Object(object! {
				baz: 2,
				foo: "bar".to_string(),
			}),
			status: 500,
		}),
		"Unexpected result: {res:?}",
	);
}

#[tokio::test]
async fn test_fetch_denied() {
	// Prepare mock server
	let server = MockServer::start().await;
	Mock::given(method("GET"))
		.and(path("/hello"))
		.and(header("some-header", "some-value"))
		.respond_with(ResponseTemplate::new(200).set_body_string("some body once told me"))
		.expect(0)
		.mount(&server)
		.await;

	// Execute test
	let ds = Datastore::builder()
		.with_capabilities(Capabilities::all().without_network_targets(Targets::Some(
			[NetTarget::from_str(&server.address().to_string()).unwrap()].into(),
		)))
		.build_with_path("memory")
		.await
		.unwrap();
	let sess = Session::owner();
	let sql = format!(
		r#"
        RETURN function() {{
            let res = await fetch('{}/hello',{{
                headers: {{
                    "some-header": "some-value",
                }}
            }});
            let body = await res.text();

            return {{ status: res.status, body: body }};
        }}
    "#,
		server.uri()
	);
	let res = ds.execute(&sql, &sess, None).await;

	let res = res.unwrap().remove(0).output().unwrap_err();

	server.verify().await;

	assert!(
		res.to_string()
			.contains(&format!("Access to network target '{}' is not allowed", server.address())),
		"Unexpected result: {:?}",
		res
	);
}

/// Runs `fetch(url, { redirect })` inside a scripting function and hands back
/// the status the script observed, or the error the statement failed with.
async fn fetch_status(
	capabilities: Capabilities,
	url: &str,
	redirect: &str,
) -> Result<Value, surrealdb_types::Error> {
	let ds = Datastore::builder()
		.with_capabilities(capabilities)
		.build_with_path("memory")
		.await
		.unwrap();
	let sql = format!(
		r#"
        RETURN function() {{
            let res = await fetch('{url}', {{ redirect: '{redirect}' }});
            return {{ status: res.status }};
        }}
    "#
	);
	ds.execute(&sql, &Session::owner(), None).await.unwrap().remove(0).output()
}

/// `redirect: "follow"` reuses the context's shared HTTP client, but `"error"`
/// and `"manual"` each build a client of their own. Those two clients install
/// their own [`crate::net::FilteringResolver`], which must be given the same
/// allow/deny pair as the shared client: a resolver whose deny rules are not
/// the operator's `deny_net` refuses every name it is asked to resolve.
///
/// The URL must name a host to reach that resolver. Hyper resolves IP-literal
/// hosts itself and never calls the custom resolver, so the IP-based
/// `server.uri()` the other tests use would not exercise it.
#[tokio::test]
async fn test_fetch_redirect_error_resolves_allowed_host() {
	// Prepare mock server
	let server = MockServer::start().await;
	Mock::given(method("GET"))
		.and(path("/hello"))
		.respond_with(ResponseTemplate::new(200).set_body_string("some body once told me"))
		.expect(1)
		.mount(&server)
		.await;

	// Execute test
	let url = format!("http://localhost:{}/hello", server.address().port());
	let res = fetch_status(Capabilities::all(), &url, "error").await;

	server.verify().await;

	let res = res.unwrap();
	assert_eq!(
		res,
		Value::Object(object! {
			status: 200,
		}),
		"Unexpected result: {res:?}"
	);
}

/// The `redirect: "manual"` client resolves names under the same contract as
/// the `"error"` client above.
#[tokio::test]
async fn test_fetch_redirect_manual_resolves_allowed_host() {
	// Prepare mock server
	let server = MockServer::start().await;
	Mock::given(method("GET"))
		.and(path("/hello"))
		.respond_with(ResponseTemplate::new(200).set_body_string("some body once told me"))
		.expect(1)
		.mount(&server)
		.await;

	// Execute test
	let url = format!("http://localhost:{}/hello", server.address().port());
	let res = fetch_status(Capabilities::all(), &url, "manual").await;

	server.verify().await;

	let res = res.unwrap();
	assert_eq!(
		res,
		Value::Object(object! {
			status: 200,
		}),
		"Unexpected result: {res:?}"
	);
}

/// Every hop of a redirect chain is re-checked against allow/deny before the
/// redirect policy runs. `redirect: "manual"` must still see an allowed hop as
/// allowed and hand the 3xx response back to the script instead of failing the
/// request.
#[tokio::test]
async fn test_fetch_redirect_manual_returns_redirect_response() {
	// Prepare mock server
	let server = MockServer::start().await;
	Mock::given(method("GET"))
		.and(path("/redirect"))
		.respond_with(
			ResponseTemplate::new(302).insert_header("location", format!("{}/hello", server.uri())),
		)
		.expect(1)
		.mount(&server)
		.await;

	// Execute test
	let url = format!("{}/redirect", server.uri());
	let res = fetch_status(Capabilities::all(), &url, "manual").await;

	server.verify().await;

	let res = res.unwrap();
	assert_eq!(
		res,
		Value::Object(object! {
			status: 302,
		}),
		"Unexpected result: {res:?}"
	);
}

/// A denied target stays denied whichever redirect mode the script selects.
#[tokio::test]
async fn test_fetch_denied_under_every_redirect_mode() {
	for redirect in ["follow", "error", "manual"] {
		// Prepare mock server
		let server = MockServer::start().await;
		Mock::given(method("GET"))
			.and(path("/hello"))
			.respond_with(ResponseTemplate::new(200))
			.expect(0)
			.mount(&server)
			.await;

		// Execute test
		let capabilities = Capabilities::all().without_network_targets(Targets::Some(
			[NetTarget::from_str(&server.address().to_string()).unwrap()].into(),
		));
		let url = format!("{}/hello", server.uri());
		let res = fetch_status(capabilities, &url, redirect).await;

		server.verify().await;

		let err = res.unwrap_err();
		assert!(
			err.to_string().contains(&format!(
				"Access to network target '{}' is not allowed",
				server.address()
			)),
			"Unexpected result for redirect: {redirect:?}: {err:?}"
		);
	}
}
