#![allow(clippy::unwrap_used)]

// API integration tests for the `api` method (invoking `DEFINE API` endpoints)

use surrealdb::opt::Config;
use surrealdb::types::Value;
use ulid::Ulid;

use super::CreateDb;

define_include_tests!(api => {
	#[test_log::test(tokio::test)]
	api_get_returns_the_response,
	#[test_log::test(tokio::test)]
	api_post_sends_the_body_headers_and_query,
	#[test_log::test(tokio::test)]
	api_prefix_and_default_headers_apply_to_every_call,
	#[test_log::test(tokio::test)]
	api_value_decodes_a_successful_body,
	#[test_log::test(tokio::test)]
	api_value_fails_on_an_unsuccessful_status,
	#[test_log::test(tokio::test)]
	api_unknown_path_answers_not_found,
	#[test_log::test(tokio::test)]
	api_call_in_a_transaction_sees_its_writes,
});

async fn select_db(db: &surrealdb::Surreal<impl surrealdb::Connection>) {
	let ns = Ulid::new().to_string();
	let dbn = Ulid::new().to_string();
	db.use_ns(&ns).use_db(&dbn).await.unwrap();
}

pub async fn api_get_returns_the_response(new_db: impl CreateDb) {
	let (permit, db) = new_db.create_db(Config::new()).await;
	select_db(&db).await;

	db.query(
		r#"DEFINE API "/greeting" FOR get THEN {
			RETURN { status: 200, headers: { "x-greeting": "hello" }, body: "Hello, world!" };
		};"#,
	)
	.await
	.unwrap()
	.check()
	.unwrap();

	let response = db.api().get("/greeting").await.unwrap();
	assert_eq!(response.status, 200);
	assert!(response.is_success());
	assert_eq!(response.headers.get("x-greeting").map(String::as_str), Some("hello"));
	assert_eq!(response.body, Value::String("Hello, world!".to_string()));

	drop(permit);
}

pub async fn api_post_sends_the_body_headers_and_query(new_db: impl CreateDb) {
	let (permit, db) = new_db.create_db(Config::new()).await;
	select_db(&db).await;

	db.query(
		r#"DEFINE API "/echo" FOR post THEN {
			RETURN {
				status: 201,
				body: {
					body: $request.body,
					header: $request.headers["x-tenant"],
					query: $request.query.page,
				},
			};
		};"#,
	)
	.await
	.unwrap()
	.check()
	.unwrap();

	let response = db
		.api()
		.post("/echo", "payload")
		.header("x-tenant", "acme")
		.query("page", "2")
		.await
		.unwrap();
	assert_eq!(response.status, 201);
	let body = response.body.into_object().unwrap().into_inner();
	assert_eq!(body["body"], Value::String("payload".to_string()));
	assert_eq!(body["header"], Value::String("acme".to_string()));
	assert_eq!(body["query"], Value::String("2".to_string()));

	drop(permit);
}

pub async fn api_prefix_and_default_headers_apply_to_every_call(new_db: impl CreateDb) {
	let (permit, db) = new_db.create_db(Config::new()).await;
	select_db(&db).await;

	db.query(
		r#"DEFINE API "/users/:id" FOR get THEN {
			RETURN { status: 200, body: { id: $request.params.id, tenant: $request.headers["x-tenant"] } };
		};"#,
	)
	.await
	.unwrap()
	.check()
	.unwrap();

	let users = db.api().prefix("/users").header("x-tenant", "acme");
	for id in ["1", "2"] {
		let body = users.get(id).await.unwrap().body.into_object().unwrap().into_inner();
		assert_eq!(body["id"], Value::String(id.to_string()));
		assert_eq!(body["tenant"], Value::String("acme".to_string()));
	}

	drop(permit);
}

pub async fn api_value_decodes_a_successful_body(new_db: impl CreateDb) {
	let (permit, db) = new_db.create_db(Config::new()).await;
	select_db(&db).await;

	db.query(r#"DEFINE API "/numbers" FOR get THEN { RETURN { status: 200, body: [1, 2, 3] }; };"#)
		.await
		.unwrap()
		.check()
		.unwrap();

	let numbers: Vec<i64> = db.api().get("/numbers").value().await.unwrap();
	assert_eq!(numbers, vec![1, 2, 3]);

	drop(permit);
}

pub async fn api_value_fails_on_an_unsuccessful_status(new_db: impl CreateDb) {
	let (permit, db) = new_db.create_db(Config::new()).await;
	select_db(&db).await;

	db.query(
		r#"DEFINE API "/teapot" FOR get THEN { RETURN { status: 418, body: "I'm a teapot" }; };"#,
	)
	.await
	.unwrap()
	.check()
	.unwrap();

	let error = db.api().get("/teapot").value::<String>().await.unwrap_err();
	assert!(error.to_string().contains("418"), "{error}");

	let response = db.api().get("/teapot").await.unwrap();
	assert_eq!(response.status, 418);
	assert!(!response.is_success());

	drop(permit);
}

pub async fn api_unknown_path_answers_not_found(new_db: impl CreateDb) {
	let (permit, db) = new_db.create_db(Config::new()).await;
	select_db(&db).await;

	let response = db.api().get("/missing").await.unwrap();
	assert_eq!(response.status, 404);

	drop(permit);
}

#[cfg(not(feature = "protocol-http"))]
pub async fn api_call_in_a_transaction_sees_its_writes(new_db: impl CreateDb) {
	let (permit, db) = new_db.create_db(Config::new()).await;
	select_db(&db).await;

	// The table is defined outside the transaction, so it still exists once
	// the transaction's writes are rolled back.
	db.query(
		r#"DEFINE TABLE item;
		DEFINE API "/count" FOR get THEN { RETURN { status: 200, body: count(SELECT * FROM item) }; };"#,
	)
	.await
	.unwrap()
	.check()
	.unwrap();

	let txn = db.begin().await.unwrap();
	txn.query("CREATE item; CREATE item;").await.unwrap().check().unwrap();
	let inside: i64 = txn.api().get("/count").value().await.unwrap();
	assert_eq!(inside, 2);
	let db = txn.cancel().await.unwrap();

	let outside: i64 = db.api().get("/count").value().await.unwrap();
	assert_eq!(outside, 0);

	drop(permit);
}

#[cfg(feature = "protocol-http")]
pub async fn api_call_in_a_transaction_sees_its_writes(_new_db: impl CreateDb) {
	// Client-side transactions are not supported on HTTP
}
