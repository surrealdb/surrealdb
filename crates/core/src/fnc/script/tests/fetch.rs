use std::str::FromStr;

use wiremock::matchers::{body_string, header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use crate::dbs::capabilities::{NetTarget, Targets};
use crate::dbs::{Capabilities, Session};
use crate::kvs::Datastore;
use crate::syn;

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
	let ds = Datastore::new("memory").await.unwrap().with_capabilities(Capabilities::all());
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
		res.to_string(),
		"{ body: 'some body once told me', status: 200f }",
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
	let ds = Datastore::new("memory").await.unwrap().with_capabilities(Capabilities::all());
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
		res.to_string(),
		"{ body: 'some body once told me', status: 201f }",
		"Unexpected result: {:?}",
		res
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
	let ds = Datastore::new("memory").await.unwrap().with_capabilities(Capabilities::all());
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
		syn::value("{ body: {baz:2, foo:\"bar\"}, status: 500f }").unwrap(),
		"Unexpected result: {:?}",
		res
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
	let ds = Datastore::new("memory").await.unwrap().with_capabilities(
		Capabilities::all().without_network_targets(Targets::Some(
			[NetTarget::from_str(&server.address().to_string()).unwrap()].into(),
		)),
	);
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
