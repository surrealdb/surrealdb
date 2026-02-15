// RUST_LOG=warn cargo make ci-ws-integration
mod common;

mod ws_integration {

	/// Tests for the empty protocol format
	mod none {
		use crate::common::Format;
		crate::include_tests!(None, Format::Json);
	}

	/// Tests for the JSON protocol format
	mod json {
		use crate::common::Format;
		crate::include_tests!(Some(Format::Json), Format::Json);
	}

	/// Tests for the CBOR protocol format
	mod cbor {
		use crate::common::Format;
		crate::include_tests!(Some(Format::Cbor), Format::Cbor);
	}
}

use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use assert_fs::TempDir;
use common::{DB, Format, NS, PASS, Socket, StartServerArguments, USER};
use http::header::{HeaderMap, HeaderValue};
use serde_json::json;

const HDR_SURREAL: &str = "surreal-id";
const HDR_REQUEST: &str = "x-request-id";

/// Helper function to ensure namespace and database exist before use
async fn ensure_namespace_and_database(
	socket: &mut Socket,
	ns: &str,
	db: &str,
) -> Result<(), Box<dyn std::error::Error>> {
	// Create namespace at ROOT level (before USE)
	socket.send_message_query(&format!("DEFINE NAMESPACE `{ns}`")).await?;

	// USE the namespace to create the database within it
	socket.send_message_use(Some(ns), None).await?;

	// Create database within the namespace
	socket.send_message_query(&format!("DEFINE DATABASE `{db}`")).await?;

	// Reset to ROOT level so the test can USE the namespace/database itself
	socket.send_message_use(None, None).await?;

	Ok(())
}

pub async fn ping(cfg_server: Option<Format>, cfg_format: Format) {
	// Setup database server
	let (addr, mut server) = common::start_server_with_defaults().await.unwrap();
	// Connect to WebSocket
	let socket = Socket::connect(&addr, cfg_server, cfg_format).await.unwrap();
	// Send INFO command
	let res = socket.send_request("ping", json!([])).await;
	assert!(res.is_ok(), "result: {res:?}");
	let res = res.unwrap();
	assert!(res.is_object(), "result: {res:?}");
	let res = res.as_object().unwrap();
	assert!(res.keys().all(|k| ["id", "result"].contains(&k.as_str())), "result: {res:?}");
	// Test passed
	server.finish().unwrap();
}

pub async fn info(cfg_server: Option<Format>, cfg_format: Format) {
	// Setup database server
	let (addr, mut server) = common::start_server_with_defaults().await.unwrap();
	// Connect to WebSocket
	let mut socket = Socket::connect(&addr, cfg_server, cfg_format).await.unwrap();
	// Authenticate the connection
	socket.send_message_signin(USER, PASS, None, None, None).await.unwrap();
	// Specify a namespace and database
	socket.send_message_use(Some(NS), Some(DB)).await.unwrap();
	// Define a user table
	socket.send_message_query("DEFINE TABLE user PERMISSIONS FULL").await.unwrap();
	// Define a user record access method
	socket
		.send_message_query(
			r#"
			DEFINE ACCESS user ON DATABASE TYPE RECORD
				SIGNUP ( CREATE user SET user = $user, pass = crypto::argon2::generate($pass) )
				SIGNIN ( SELECT * FROM user WHERE user = $user AND crypto::argon2::compare(pass, $pass) )
				DURATION FOR SESSION 24h
			;
			"#,
		)
		.await
		.unwrap();
	// Create a user record
	socket
		.send_message_query(
			r#"
			CREATE user CONTENT {
				user: 'user',
				pass: crypto::argon2::generate('pass')
			};
			"#,
		)
		.await
		.unwrap();
	// Sign in as record user
	socket.send_message_signin("user", "pass", Some(NS), Some(DB), Some("user")).await.unwrap();
	// Send INFO command
	let res = socket.send_request("info", json!([])).await.unwrap();
	// Expected result structure:
	// ```
	// Object {
	//   "id": String("user:yjdbdlx2mlciaxhsv8hp"),
	//   "pass": String("$argon2id$v=19$m=19456,t=2,p=1$DCt83IiOtOo3MW7lRB6PBg$kIUrYmZgysGzW+j5DOM7X4AsXlKK4fFkxW0vUVvMX2U"),
	//   "user": String("user")
	// }
	// ```
	assert!(res["result"].is_object(), "result: {res:?}");
	let res = res["result"].as_object().unwrap();
	assert!(res.contains_key("id"));
	assert!(res.contains_key("pass"));
	assert!(res.contains_key("user"));
	assert_eq!(res["user"], "user", "result: {res:?}");
	// Test passed
	server.finish().unwrap();
}

pub async fn signup(cfg_server: Option<Format>, cfg_format: Format) {
	// Setup database server
	let (addr, mut server) = common::start_server_with_defaults().await.unwrap();
	// Connect to WebSocket
	let mut socket = Socket::connect(&addr, cfg_server, cfg_format).await.unwrap();
	// Authenticate the connection
	socket.send_message_signin(USER, PASS, None, None, None).await.unwrap();
	// Specify a namespace and database
	socket.send_message_use(Some(NS), Some(DB)).await.unwrap();
	// Define a user record access method
	socket
		.send_message_query(
			r#"
			DEFINE ACCESS user ON DATABASE TYPE RECORD
				SIGNUP ( CREATE user SET email = $email, pass = crypto::argon2::generate($pass) )
				SIGNIN ( SELECT * FROM user WHERE email = $email AND crypto::argon2::compare(pass, $pass) )
				DURATION FOR SESSION 24h
			;"#,
		)
		.await
		.unwrap();
	// Send SIGNUP command
	let res = socket
		.send_request(
			"signup",
			json!([{
				"ns": NS,
				"db": DB,
				"ac": "user",
				"email": "email@email.com",
				"pass": "pass",
			}]),
		)
		.await;
	assert!(res.is_ok(), "result: {res:?}");
	let res = res.unwrap();
	assert!(res.is_object(), "result: {res:?}");
	let res = res.as_object().unwrap();
	// Verify response contains no error
	assert!(res.keys().all(|k| ["id", "result"].contains(&k.as_str())), "result: {res:?}");
	// Verify it returns a token
	assert!(res["result"].is_string(), "result: {res:?}");
	let res = res["result"].as_str().unwrap();
	assert!(res.starts_with("eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9"), "result: {res}");
	// Test passed
	server.finish().unwrap();
}

pub async fn signin(cfg_server: Option<Format>, cfg_format: Format) {
	// Setup database server
	let (addr, mut server) = common::start_server_with_defaults().await.unwrap();
	// Connect to WebSocket
	let mut socket = Socket::connect(&addr, cfg_server, cfg_format).await.unwrap();
	// Authenticate the connection
	socket.send_message_signin(USER, PASS, None, None, None).await.unwrap();
	// Specify a namespace and database
	socket.send_message_use(Some(NS), Some(DB)).await.unwrap();
	// Define a user record access method
	socket
		.send_message_query(
			r#"
			DEFINE ACCESS user ON DATABASE TYPE RECORD
				SIGNUP ( CREATE user SET email = $email, pass = crypto::argon2::generate($pass) )
				SIGNIN ( SELECT * FROM user WHERE email = $email AND crypto::argon2::compare(pass, $pass) )
				DURATION FOR SESSION 24h
			;"#,
		)
		.await
		.unwrap();
	// Send SIGNUP command
	let res = socket
		.send_request(
			"signup",
			json!(
				[{
					"ns": NS,
					"db": DB,
					"ac": "user",
					"email": "email@email.com",
					"pass": "pass",
				}]
			),
		)
		.await;
	assert!(res.is_ok(), "result: {res:?}");
	let res = res.unwrap();
	assert!(res.is_object(), "result: {res:?}");
	let res = res.as_object().unwrap();
	// Verify response contains no error
	assert!(res.keys().all(|k| ["id", "result"].contains(&k.as_str())), "result: {res:?}");
	// Verify it returns a token
	assert!(res["result"].is_string(), "result: {res:?}");
	let res = res["result"].as_str().unwrap();
	assert!(res.starts_with("eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9"), "result: {res}");
	// Send SIGNIN command
	let res = socket
		.send_request(
			"signin",
			json!(
			[{
				"ns": NS,
				"db": DB,
				"ac": "user",
				"email": "email@email.com",
				"pass": "pass",
			}]),
		)
		.await;
	assert!(res.is_ok(), "result: {res:?}");
	let res = res.unwrap();
	assert!(res.is_object(), "result: {res:?}");
	let res = res.as_object().unwrap();
	// Verify response contains no error
	assert!(res.keys().all(|k| ["id", "result"].contains(&k.as_str())), "result: {res:?}");
	// Verify it returns a token
	assert!(res["result"].is_string(), "result: {res:?}");
	let res = res["result"].as_str().unwrap();
	assert!(res.starts_with("eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9"), "result: {res}");
	// Test passed
	server.finish().unwrap();
}

pub async fn invalidate(cfg_server: Option<Format>, cfg_format: Format) {
	// Setup database server
	let (addr, mut server) = common::start_server_with_defaults().await.unwrap();
	// Connect to WebSocket
	let mut socket = Socket::connect(&addr, cfg_server, cfg_format).await.unwrap();
	// Authenticate the connection
	socket.send_message_signin(USER, PASS, None, None, None).await.unwrap();
	// Specify a namespace and database
	socket.send_message_use(Some(NS), Some(DB)).await.unwrap();
	// Verify we have an authenticated session
	let res = socket.send_message_query("DEFINE NAMESPACE test").await.unwrap();
	assert_eq!(res[0]["status"], "OK", "result: {res:?}");
	// Send INVALIDATE command
	socket.send_request("invalidate", json!([])).await.unwrap();
	// Verify we have an invalidated session
	let res = socket.send_request("query", json!(["DEFINE NAMESPACE test"])).await.unwrap();
	assert_eq!(
		res["error"]["message"],
		"Anonymous access not allowed: Not enough permissions to perform this action",
		"result: {res:?}"
	);
	// Test passed
	server.finish().unwrap();
}

pub async fn authenticate(cfg_server: Option<Format>, cfg_format: Format) {
	// Setup database server
	let (addr, mut server) = common::start_server_with_defaults().await.unwrap();
	// Connect to WebSocket
	let mut socket = Socket::connect(&addr, cfg_server, cfg_format).await.unwrap();
	// Authenticate the connection
	let token = socket.send_message_signin(USER, PASS, None, None, None).await.unwrap();
	// Disconnect the connection
	socket.close().await.unwrap();
	// Connect to WebSocket
	let mut socket = Socket::connect(&addr, cfg_server, cfg_format).await.unwrap();
	// Send AUTHENTICATE command
	socket.send_request("authenticate", json!([token,])).await.unwrap();
	// Verify we have an authenticated session
	let res = socket.send_message_query("DEFINE NAMESPACE test").await.unwrap();
	assert_eq!(res[0]["status"], "OK", "result: {res:?}");
	// Test passed
	server.finish().unwrap();
}

pub async fn letset(cfg_server: Option<Format>, cfg_format: Format) {
	// Setup database server
	let (addr, mut server) = common::start_server_with_defaults().await.unwrap();
	// Connect to WebSocket
	let mut socket = Socket::connect(&addr, cfg_server, cfg_format).await.unwrap();
	// Authenticate the connection
	socket.send_message_signin(USER, PASS, None, None, None).await.unwrap();
	// Specify a namespace and database
	socket.send_message_use(Some(NS), Some(DB)).await.unwrap();
	// Send LET command
	socket.send_request("let", json!(["let_var", "let_value",])).await.unwrap();
	// Send SET command
	socket.send_request("set", json!(["set_var", "set_value",])).await.unwrap();
	// Verify the variables are set
	let res = socket.send_message_query("SELECT * FROM $let_var, $set_var").await.unwrap();
	assert_eq!(res[0]["result"], json!(["let_value", "set_value"]), "result: {res:?}");
	server.finish().unwrap();
}

pub async fn unset(cfg_server: Option<Format>, cfg_format: Format) {
	// Setup database server
	let (addr, mut server) = common::start_server_with_defaults().await.unwrap();
	// Connect to WebSocket
	let mut socket = Socket::connect(&addr, cfg_server, cfg_format).await.unwrap();
	// Authenticate the connection
	socket.send_message_signin(USER, PASS, None, None, None).await.unwrap();
	// Specify a namespace and database
	socket.send_message_use(Some(NS), Some(DB)).await.unwrap();
	// Send LET command
	socket.send_request("let", json!(["let_var", "let_value",])).await.unwrap();
	// Verify the variable is set
	let res = socket.send_message_query("RETURN $let_var").await.unwrap();
	assert_eq!(res[0]["result"], json!("let_value"), "result: {res:?}");
	// Send UNSET command
	socket.send_request("unset", json!(["let_var"])).await.unwrap();
	// Verify the variable is unset
	let res = socket.send_message_query("RETURN $let_var").await.unwrap();
	assert_eq!(res[0]["result"], json!(null), "result: {res:?}");
	// Test passed
	server.finish().unwrap();
}

pub async fn select(cfg_server: Option<Format>, cfg_format: Format) {
	// Setup database server
	let (addr, mut server) = common::start_server_with_defaults().await.unwrap();
	// Connect to WebSocket
	let mut socket = Socket::connect(&addr, cfg_server, cfg_format).await.unwrap();
	// Authenticate the connection
	socket.send_message_signin(USER, PASS, None, None, None).await.unwrap();
	// Specify a namespace and database
	socket.send_message_use(Some(NS), Some(DB)).await.unwrap();
	// Create a test record
	socket.send_message_query("CREATE tester SET name = 'foo', value = 'bar'").await.unwrap();
	// Send SELECT command
	let res = socket.send_request("select", json!(["tester",])).await.unwrap();
	assert!(res.is_object(), "result: {res:?}");
	assert!(res["result"].is_array(), "result: {res:?}");
	let res = res["result"].as_array().unwrap();
	assert_eq!(res.len(), 1, "result: {res:?}");
	assert_eq!(res[0]["name"], "foo", "result: {res:?}");
	assert_eq!(res[0]["value"], "bar", "result: {res:?}");
	// Test passed
	server.finish().unwrap();
}

pub async fn insert(cfg_server: Option<Format>, cfg_format: Format) {
	// Setup database server
	let (addr, mut server) = common::start_server_with_defaults().await.unwrap();
	// Connect to WebSocket
	let mut socket = Socket::connect(&addr, cfg_server, cfg_format).await.unwrap();
	// Authenticate the connection
	socket.send_message_signin(USER, PASS, None, None, None).await.unwrap();
	// Specify a namespace and database
	socket.send_message_use(Some(NS), Some(DB)).await.unwrap();
	// Send INSERT command
	let res = socket
		.send_request(
			"insert",
			json!([
				"tester",
				{
					"name": "foo",
					"value": "bar",
				}
			]),
		)
		.await
		.unwrap();
	assert!(res.is_object(), "result: {res:?}");
	assert!(res["result"].is_array(), "result: {res:?}");
	let res = res["result"].as_array().unwrap();
	assert_eq!(res.len(), 1, "result: {res:?}");
	assert_eq!(res[0]["name"], "foo", "result: {res:?}");
	assert_eq!(res[0]["value"], "bar", "result: {res:?}");
	// Send INSERT command trying to create multiple records
	let res = socket
		.send_request(
			"insert",
			json!([
				"tester",
				[
					{
						"name": "foo",
						"value": "bar",
					},
					{
						"name": "foo",
						"value": "bar",
					}
				]
			]),
		)
		.await
		.unwrap();
	assert!(res.is_object(), "result: {res:?}");
	assert!(res["result"].is_array(), "result: {res:?}");
	let res = res["result"].as_array().unwrap();
	assert_eq!(res.len(), 2, "result: {res:?}");
	assert_eq!(res[0]["name"], "foo", "result: {res:?}");
	assert_eq!(res[0]["value"], "bar", "result: {res:?}");
	assert_eq!(res[1]["name"], "foo", "result: {res:?}");
	assert_eq!(res[1]["value"], "bar", "result: {res:?}");
	// Verify the data was inserted and can be queried
	let res = socket.send_message_query("SELECT * FROM tester").await.unwrap();
	assert!(res[0]["result"].is_array(), "result: {res:?}");
	let res = res[0]["result"].as_array().unwrap();
	assert_eq!(res.len(), 3, "result: {res:?}");
	assert_eq!(res[0]["name"], "foo", "result: {res:?}");
	assert_eq!(res[0]["value"], "bar", "result: {res:?}");
	assert_eq!(res[1]["name"], "foo", "result: {res:?}");
	assert_eq!(res[1]["value"], "bar", "result: {res:?}");
	assert_eq!(res[2]["name"], "foo", "result: {res:?}");
	assert_eq!(res[2]["value"], "bar", "result: {res:?}");
	// Test passed
	server.finish().unwrap();
}

pub async fn create(cfg_server: Option<Format>, cfg_format: Format) {
	// Setup database server
	let (addr, mut server) = common::start_server_with_defaults().await.unwrap();
	// Connect to WebSocket
	let mut socket = Socket::connect(&addr, cfg_server, cfg_format).await.unwrap();
	// Authenticate the connection
	socket.send_message_signin(USER, PASS, None, None, None).await.unwrap();
	// Specify a namespace and database
	socket.send_message_use(Some(NS), Some(DB)).await.unwrap();
	// Send CREATE command
	let res = socket
		.send_request(
			"create",
			json!([
				"tester",
				{
					"value": "bar",
				}
			]),
		)
		.await
		.unwrap();
	assert!(res.is_object(), "result: {res:?}");
	assert!(res["result"].is_object(), "result: {res:?}");
	let res = res["result"].as_object().unwrap();
	assert_eq!(res["value"], "bar", "result: {res:?}");
	// Verify the data was created
	let res = socket.send_message_query("SELECT * FROM tester").await.unwrap();
	assert!(res[0]["result"].is_array(), "result: {res:?}");
	let res = res[0]["result"].as_array().unwrap();
	assert_eq!(res.len(), 1, "result: {res:?}");
	assert_eq!(res[0]["value"], "bar", "result: {res:?}");
	// Test passed
	server.finish().unwrap();
}

pub async fn update(cfg_server: Option<Format>, cfg_format: Format) {
	// Setup database server
	let (addr, mut server) = common::start_server_with_defaults().await.unwrap();
	// Connect to WebSocket
	let mut socket = Socket::connect(&addr, cfg_server, cfg_format).await.unwrap();
	// Authenticate the connection
	socket.send_message_signin(USER, PASS, None, None, None).await.unwrap();
	// Specify a namespace and database
	socket.send_message_use(Some(NS), Some(DB)).await.unwrap();
	// Create a test record
	socket.send_message_query("CREATE tester SET name = 'foo'").await.unwrap();
	// Send UPDATE command
	let res = socket
		.send_request(
			"update",
			json!([
				"tester",
				{
					"value": "bar",
				}
			]),
		)
		.await
		.unwrap();
	assert!(res.is_object(), "result: {res:?}");
	assert!(res["result"].is_array(), "result: {res:?}");
	let res = res["result"].as_array().unwrap();
	assert_eq!(res.len(), 1, "result: {res:?}");
	assert_eq!(res[0]["value"], "bar", "result: {res:?}");
	// Verify the data was updated
	let res = socket.send_message_query("SELECT * FROM tester").await.unwrap();
	assert!(res[0]["result"].is_array(), "result: {res:?}");
	let res = res[0]["result"].as_array().unwrap();
	assert_eq!(res.len(), 1, "result: {res:?}");
	assert_eq!(res[0]["name"], json!(null), "result: {res:?}");
	assert_eq!(res[0]["value"], "bar", "result: {res:?}");
	// Test passed
	server.finish().unwrap();
}

pub async fn merge(cfg_server: Option<Format>, cfg_format: Format) {
	// Setup database server
	let (addr, mut server) = common::start_server_with_defaults().await.unwrap();
	// Connect to WebSocket
	let mut socket = Socket::connect(&addr, cfg_server, cfg_format).await.unwrap();
	// Authenticate the connection
	socket.send_message_signin(USER, PASS, None, None, None).await.unwrap();
	// Specify a namespace and database
	socket.send_message_use(Some(NS), Some(DB)).await.unwrap();
	// Create a test record
	socket.send_message_query("CREATE tester SET name = 'foo'").await.unwrap();
	// Send UPDATE command
	let res = socket
		.send_request(
			"merge",
			json!([
				"tester",
				{
					"value": "bar",
				}
			]),
		)
		.await
		.unwrap();
	assert!(res.is_object(), "result: {res:?}");
	assert!(res["result"].is_array(), "result: {res:?}");
	let res = res["result"].as_array().unwrap();
	assert_eq!(res.len(), 1, "result: {res:?}");
	assert_eq!(res[0]["name"], "foo", "result: {res:?}");
	assert_eq!(res[0]["value"], "bar", "result: {res:?}");
	// Verify the data was merged
	let res = socket.send_message_query("SELECT * FROM tester").await.unwrap();
	assert!(res[0]["result"].is_array(), "result: {res:?}");
	let res = res[0]["result"].as_array().unwrap();
	assert_eq!(res.len(), 1, "result: {res:?}");
	assert_eq!(res[0]["name"], "foo", "result: {res:?}");
	assert_eq!(res[0]["value"], "bar", "result: {res:?}");
	// Test passed
	server.finish().unwrap();
}

pub async fn patch(cfg_server: Option<Format>, cfg_format: Format) {
	// Setup database server
	let (addr, mut server) = common::start_server_with_defaults().await.unwrap();
	// Connect to WebSocket
	let mut socket = Socket::connect(&addr, cfg_server, cfg_format).await.unwrap();
	// Authenticate the connection
	socket.send_message_signin(USER, PASS, None, None, None).await.unwrap();
	// Specify a namespace and database
	socket.send_message_use(Some(NS), Some(DB)).await.unwrap();
	// Create a test record
	socket.send_message_query("CREATE tester:id SET name = 'foo'").await.unwrap();
	// Send PATCH command
	let res = socket
		.send_request(
			"patch",
			json!([
				"tester:id",
				[
					{
						"op": "add",
						"path": "value",
						"value": "bar"
					},
					{
						"op": "remove",
						"path": "name",
					}
				]
			]),
		)
		.await
		.unwrap();
	assert!(res["result"].is_object(), "result: {res:?}");
	let res = res["result"].as_object().unwrap();
	assert_eq!(res.get("value"), Some(json!("bar")).as_ref(), "result: {res:?}");
	// Verify the data was patched
	let res = socket.send_message_query("SELECT * FROM tester").await.unwrap();
	assert!(res[0]["result"].is_array(), "result: {res:?}");
	let res = res[0]["result"].as_array().unwrap();
	assert_eq!(res.len(), 1, "result: {res:?}");
	assert_eq!(res[0]["name"], json!(null), "result: {res:?}");
	assert_eq!(res[0]["value"], "bar", "result: {res:?}");
	// Test passed
	server.finish().unwrap();
}

pub async fn delete(cfg_server: Option<Format>, cfg_format: Format) {
	// Setup database server
	let (addr, mut server) = common::start_server_with_defaults().await.unwrap();
	// Connect to WebSocket
	let mut socket = Socket::connect(&addr, cfg_server, cfg_format).await.unwrap();
	// Authenticate the connection
	socket.send_message_signin(USER, PASS, None, None, None).await.unwrap();
	// Specify a namespace and database
	socket.send_message_use(Some(NS), Some(DB)).await.unwrap();
	// Create a test record
	socket.send_message_query("CREATE tester:id").await.unwrap();
	// Send DELETE command
	let res = socket.send_request("delete", json!(["tester"])).await.unwrap();
	assert!(res.is_object(), "result: {res:?}");
	assert!(res["result"].is_array(), "result: {res:?}");
	let res = res["result"].as_array().unwrap();
	assert_eq!(res.len(), 1, "result: {res:?}");
	assert_eq!(res[0]["id"], "tester:id", "result: {res:?}");
	// Create a test record
	socket.send_message_query("CREATE tester:id").await.unwrap();
	// Send DELETE command
	let res = socket.send_request("delete", json!(["tester:id"])).await.unwrap();
	assert!(res.is_object(), "result: {res:?}");
	assert!(res["result"].is_object(), "result: {res:?}");
	let res = res["result"].as_object().unwrap();
	assert_eq!(res["id"], "tester:id", "result: {res:?}");
	// Verify the data was merged
	let res = socket.send_message_query("SELECT * FROM tester").await.unwrap();
	assert!(res[0]["result"].is_array(), "result: {res:?}");
	let res = res[0]["result"].as_array().unwrap();
	assert_eq!(res.len(), 0, "result: {res:?}");
	// Test passed
	server.finish().unwrap();
}

pub async fn query(cfg_server: Option<Format>, cfg_format: Format) {
	// Setup database server
	let (addr, mut server) = common::start_server_with_defaults().await.unwrap();
	// Connect to WebSocket
	let mut socket = Socket::connect(&addr, cfg_server, cfg_format).await.unwrap();
	// Authenticate the connection
	socket.send_message_signin(USER, PASS, None, None, None).await.unwrap();
	// Specify a namespace and database
	socket.send_message_use(Some(NS), Some(DB)).await.unwrap();
	// Send QUERY command
	let res = socket
		.send_request("query", json!(["CREATE tester; SELECT * FROM tester;",]))
		.await
		.unwrap();
	assert!(res.is_object(), "result: {res:?}");
	assert!(res["result"].is_array(), "result: {res:?}");
	let res = res["result"].as_array().unwrap();
	assert_eq!(res.len(), 2, "result: {res:?}");
	// Verify the data was created
	let res = socket.send_message_query("SELECT * FROM tester").await.unwrap();
	assert!(res[0]["result"].is_array(), "result: {res:?}");
	let res = res[0]["result"].as_array().unwrap();
	assert_eq!(res.len(), 1, "result: {res:?}");
	// Test passed
	server.finish().unwrap();
}

pub async fn version(cfg_server: Option<Format>, cfg_format: Format) {
	// Setup database server
	let (addr, mut server) = common::start_server_with_defaults().await.unwrap();
	// Connect to WebSocket
	let socket = Socket::connect(&addr, cfg_server, cfg_format).await.unwrap();
	// Send version command
	let res = socket.send_request("version", json!([])).await.unwrap();
	assert!(res["result"].is_string(), "result: {res:?}");
	let res = res["result"].as_str().unwrap();
	assert!(res.starts_with("surrealdb-"), "result: {res}");
	// Test passed
	server.finish().unwrap();
}

// Validate that the WebSocket is able to process multiple queries concurrently
pub async fn concurrency(cfg_server: Option<Format>, cfg_format: Format) {
	// Setup database server
	let (addr, mut server) = common::start_server_with_defaults().await.unwrap();
	// Connect to WebSocket
	let mut socket = Socket::connect(&addr, cfg_server, cfg_format).await.unwrap();
	// Authenticate the connection
	socket.send_message_signin(USER, PASS, None, None, None).await.unwrap();
	// Specify a namespace and database
	socket.send_message_use(Some(NS), Some(DB)).await.unwrap();
	// Send 5 long-running queries and verify they run concurrently

	let mut futures = Vec::<Pin<Box<dyn Future<Output = _>>>>::new();
	for i in 0..5 {
		let future = socket.send_request("query", json!([format!("SLEEP 3s; RETURN {i};")]));
		futures.push(Box::pin(future))
	}

	let res =
		tokio::time::timeout(Duration::from_secs(5), futures::future::join_all(futures)).await;
	let Ok(res) = res else {
		panic!("future timed-out");
	};

	let res = res
		.into_iter()
		.try_fold(Vec::new(), |mut acc, x| -> Result<_, Box<dyn std::error::Error>> {
			acc.push(x?);
			Ok(acc)
		})
		.unwrap();

	assert!(res.iter().all(|v| v["error"].is_null()), "Unexpected error received: {res:#?}");
	// Test passed
	server.finish().unwrap();
}

pub async fn live_query(cfg_server: Option<Format>, cfg_format: Format) {
	// Setup database server
	let (addr, mut server) = common::start_server_with_defaults().await.unwrap();
	// Connect to WebSocket
	let mut socket = Socket::connect(&addr, cfg_server, cfg_format).await.unwrap();
	// Authenticate the connection
	socket.send_message_signin(USER, PASS, None, None, None).await.unwrap();
	// Create namespace and database
	ensure_namespace_and_database(&mut socket, NS, DB).await.unwrap();
	// Specify a namespace and database
	socket.send_message_use(Some(NS), Some(DB)).await.unwrap();
	// Create the table before using it
	socket.send_message_query("DEFINE TABLE tester").await.unwrap();
	// Send LIVE command
	let res = socket.send_request("query", json!(["LIVE SELECT * FROM tester"])).await.unwrap();
	assert!(res.is_object(), "result: {res:?}");
	assert!(res["result"].is_array(), "result: {res:?}");
	let res = res["result"].as_array().unwrap();
	assert_eq!(res.len(), 1, "result: {res:?}");
	assert!(res[0]["result"].is_string(), "result: {res:?}");
	assert!(res[0]["type"].is_string(), "type: {res:?}");
	let live1 = res[0]["result"].as_str().unwrap();
	// Send LIVE command
	let res = socket.send_request("query", json!(["LIVE SELECT * FROM tester"])).await.unwrap();
	assert!(res.is_object(), "result: {res:?}");
	assert!(res["result"].is_array(), "result: {res:?}");
	let res = res["result"].as_array().unwrap();
	assert_eq!(res.len(), 1, "result: {res:?}");
	assert!(res[0]["result"].is_string(), "result: {res:?}");
	assert!(res[0]["type"].is_string(), "type: {res:?}");
	let live2 = res[0]["result"].as_str().unwrap();
	// Create a new test record
	let res =
		socket.send_request("query", json!(["CREATE tester:id SET name = 'foo'"])).await.unwrap();
	assert!(res.is_object(), "result: {res:?}");
	assert!(res["result"].is_array(), "result: {res:?}");
	let res = res["result"].as_array().unwrap();
	assert_eq!(res.len(), 1, "result: {res:?}");
	// Wait some time for all messages to arrive, and then search for the
	// notification message
	let msgs: Result<_, Box<dyn std::error::Error>> =
		tokio::time::timeout(Duration::from_secs(1), async {
			Ok(vec![
				socket.receive_other_message().await.unwrap(),
				socket.receive_other_message().await.unwrap(),
			])
		})
		.await
		.unwrap();
	let msgs = msgs.unwrap();
	assert!(msgs.iter().all(|v| v["error"].is_null()), "Unexpected error received: {msgs:?}");
	// Check for first live query notifcation
	let res = msgs.iter().find(|v| common::is_notification_from_lq(v, live1));
	assert!(res.is_some(), "Expected to find a notification for LQ id {live1}: {msgs:?}");
	let res = res.unwrap();
	assert!(res.is_object(), "result: {res:?}");
	let res = res.as_object().unwrap();
	assert!(res["result"].is_object(), "result: {res:?}");
	let res = res["result"].as_object().unwrap();
	assert!(res["action"].is_string(), "result: {res:?}");
	assert_eq!(res["action"], "CREATE", "result: {res:?}");
	assert!(res["result"].is_object(), "result: {res:?}");
	let res = res["result"].as_object().unwrap();
	assert_eq!(res["id"], "tester:id", "result: {res:?}");
	// Check for second live query notifcation
	let res = msgs.iter().find(|v| common::is_notification_from_lq(v, live2));
	assert!(res.is_some(), "Expected to find a notification for LQ id {live2}: {msgs:?}");
	let res = res.unwrap();
	assert!(res.is_object(), "result: {res:?}");
	let res = res.as_object().unwrap();
	assert!(res["result"].is_object(), "result: {res:?}");
	let res = res["result"].as_object().unwrap();
	assert_eq!(res["action"], "CREATE", "result: {res:?}");
	assert!(res["result"].is_object(), "result: {res:?}");
	let res = res["result"].as_object().unwrap();
	assert_eq!(res["id"], "tester:id", "result: {res:?}");
	// Test passed
	server.finish().unwrap();
}

/// Same as live but uses the RPC for both methods.
pub async fn live_rpc(cfg_server: Option<Format>, cfg_format: Format) {
	// Setup database server
	let (addr, mut server) = common::start_server_with_defaults().await.unwrap();
	// Connect to WebSocket
	let mut socket = Socket::connect(&addr, cfg_server, cfg_format).await.unwrap();
	// Authenticate the connection
	socket.send_message_signin(USER, PASS, None, None, None).await.unwrap();
	// Create namespace and database
	ensure_namespace_and_database(&mut socket, NS, DB).await.unwrap();
	// Specify a namespace and database
	socket.send_message_use(Some(NS), Some(DB)).await.unwrap();
	// Create the table before using it
	socket.send_message_query("DEFINE TABLE tester").await.unwrap();
	// Send LIVE command
	let res = socket.send_request("live", json!(["tester"])).await.unwrap();
	assert!(res.is_object(), "result: {res:?}");
	assert!(res["result"].is_string(), "result: {res:?}");
	let live1 = res["result"].as_str().unwrap();

	// Send LIVE command
	let res = socket.send_request("live", json!(["tester"])).await.unwrap();
	assert!(res.is_object(), "result: {res:?}");
	assert!(res["result"].is_string(), "result: {res:?}");
	let live2 = res["result"].as_str().unwrap();

	// Create a new test record
	let res =
		socket.send_request("query", json!(["CREATE tester:id SET name = 'foo'"])).await.unwrap();
	assert!(res.is_object(), "result: {res:?}");
	assert!(res["result"].is_array(), "result: {res:?}");
	let res = res["result"].as_array().unwrap();
	assert_eq!(res.len(), 1, "result: {res:?}");
	// Wait some time for all messages to arrive, and then search for the
	// notification message
	let msgs: Result<_, Box<dyn std::error::Error>> =
		tokio::time::timeout(Duration::from_secs(1), async {
			Ok(vec![
				socket.receive_other_message().await.unwrap(),
				socket.receive_other_message().await.unwrap(),
			])
		})
		.await
		.unwrap();
	let msgs = msgs.unwrap();
	assert!(msgs.iter().all(|v| v["error"].is_null()), "Unexpected error received: {msgs:?}");
	// Check for first live query notifcation
	let res = msgs.iter().find(|v| common::is_notification_from_lq(v, live1));
	assert!(res.is_some(), "Expected to find a notification for LQ id {live1}: {msgs:?}");
	let res = res.unwrap();
	assert!(res.is_object(), "result: {res:?}");
	let res = res.as_object().unwrap();
	assert!(res["result"].is_object(), "result: {res:?}");
	let res = res["result"].as_object().unwrap();
	assert!(res["action"].is_string(), "result: {res:?}");
	assert_eq!(res["action"], "CREATE", "result: {res:?}");
	assert!(res["result"].is_object(), "result: {res:?}");
	let res = res["result"].as_object().unwrap();
	assert_eq!(res["id"], "tester:id", "result: {res:?}");
	// Check for second live query notifcation
	let res = msgs.iter().find(|v| common::is_notification_from_lq(v, live2));
	assert!(res.is_some(), "Expected to find a notification for LQ id {live2}: {msgs:?}");
	let res = res.unwrap();
	assert!(res.is_object(), "result: {res:?}");
	let res = res.as_object().unwrap();
	assert!(res["result"].is_object(), "result: {res:?}");
	let res = res["result"].as_object().unwrap();
	assert_eq!(res["action"], "CREATE", "result: {res:?}");
	assert!(res["result"].is_object(), "result: {res:?}");
	let res = res["result"].as_object().unwrap();
	assert_eq!(res["id"], "tester:id", "result: {res:?}");
	// Test passed
	server.finish().unwrap();
}

pub async fn live_query_diff(cfg_server: Option<Format>, cfg_format: Format) {
	// Setup database server
	let (addr, mut server) = common::start_server_with_defaults().await.unwrap();
	// Connect to WebSocket
	let mut socket = Socket::connect(&addr, cfg_server, cfg_format).await.unwrap();
	// Authenticate the connection
	socket.send_message_signin(USER, PASS, None, None, None).await.unwrap();
	// Create namespace and database
	ensure_namespace_and_database(&mut socket, NS, DB).await.unwrap();
	// Specify a namespace and database
	socket.send_message_use(Some(NS), Some(DB)).await.unwrap();
	// Create the table before using it
	socket.send_message_query("DEFINE TABLE tester").await.unwrap();
	// Send LIVE DIFF command
	let res = socket.send_request("query", json!(["LIVE SELECT DIFF FROM tester"])).await.unwrap();
	assert!(res.is_object(), "result: {res:?}");
	assert!(res["result"].is_array(), "result: {res:?}");
	let res = res["result"].as_array().unwrap();
	assert_eq!(res.len(), 1, "result: {res:?}");
	assert!(res[0]["result"].is_string(), "result: {res:?}");
	let live_id = res[0]["result"].as_str().unwrap();

	// Create a new test record
	let res = socket
		.send_request("query", json!(["CREATE tester:id SET name = 'foo', value = 42"]))
		.await
		.unwrap();
	assert!(res.is_object(), "result: {res:?}");
	assert!(res["result"].is_array(), "result: {res:?}");

	// Wait for the CREATE notification
	let msgs = socket.receive_all_other_messages(1, Duration::from_secs(1)).await.unwrap();
	assert!(msgs.iter().all(|v| v["error"].is_null()), "Unexpected error received: {msgs:?}");
	let res = msgs.iter().find(|v| common::is_notification_from_lq(v, live_id));
	assert!(res.is_some(), "Expected to find a notification for LQ id {live_id}: {msgs:?}");
	let res = res.unwrap();
	assert!(res.is_object(), "result: {res:?}");
	let res = res["result"].as_object().unwrap();
	assert_eq!(res["action"], "CREATE", "result: {res:?}");
	// For CREATE with DIFF, the result should be an array of operations
	assert!(res["result"].is_array(), "Expected diff operations array, got: {res:?}");
	let operations = res["result"].clone();
	// Check that we have proper JSON patch operations
	assert_eq!(
		operations,
		json!([
			{
				"op": "replace",
				"path": "",
				"value": {
					"id": "tester:id",
					"name": "foo",
					"value": 42
				}
			}
		])
	);

	// Update the record
	let res = socket
		.send_request("query", json!(["UPDATE tester:id SET name = 'bar', value = 100"]))
		.await
		.unwrap();
	assert!(res.is_object(), "result: {res:?}");

	// Wait for the UPDATE notification
	let msgs = socket.receive_all_other_messages(1, Duration::from_secs(1)).await.unwrap();
	assert!(msgs.iter().all(|v| v["error"].is_null()), "Unexpected error received: {msgs:?}");
	let res = msgs.iter().find(|v| common::is_notification_from_lq(v, live_id));
	assert!(res.is_some(), "Expected to find a notification for LQ id {live_id}: {msgs:?}");
	let res = res.unwrap();
	assert!(res.is_object(), "result: {res:?}");
	let res = res["result"].as_object().unwrap();
	assert_eq!(res["action"], "UPDATE", "result: {res:?}");
	// For UPDATE with DIFF, the result should be an array of operations
	assert!(res["result"].is_array(), "Expected diff operations array, got: {res:?}");
	let operations = res["result"].clone();
	assert_eq!(
		operations,
		json!([
			{
				"op": "change",
				"path": "/name",
				"value": "@@ -1,3 +1,3 @@\n-foo\n+bar\n",
			},
			{
				"op": "replace",
				"path": "/value",
				"value": 100
			}
		])
	);

	// Delete the record
	let res = socket.send_request("query", json!(["DELETE tester:id"])).await.unwrap();
	assert!(res.is_object(), "result: {res:?}");

	// Wait for the DELETE notification
	let msgs = socket.receive_all_other_messages(1, Duration::from_secs(1)).await.unwrap();
	assert!(msgs.iter().all(|v| v["error"].is_null()), "Unexpected error received: {msgs:?}");
	let res = msgs.iter().find(|v| common::is_notification_from_lq(v, live_id));
	assert!(res.is_some(), "Expected to find a notification for LQ id {live_id}: {msgs:?}");
	let res = res.unwrap();
	assert!(res.is_object(), "result: {res:?}");
	let res = res["result"].as_object().unwrap();
	assert_eq!(res["action"], "DELETE", "result: {res:?}");
	// For DELETE with DIFF, the result should be an array of operations
	let operations = res["result"].clone();
	assert_eq!(
		operations,
		json!([
			{
				"op": "replace",
				"path": "",
				"value": null
			}
		])
	);

	// Test passed
	server.finish().unwrap();
}

pub async fn kill(cfg_server: Option<Format>, cfg_format: Format) {
	// Setup database server
	let (addr, mut server) = common::start_server_with_defaults().await.unwrap();
	// Connect to WebSocket
	let mut socket = Socket::connect(&addr, cfg_server, cfg_format).await.unwrap();
	// Authenticate the connection
	socket.send_message_signin(USER, PASS, None, None, None).await.unwrap();
	// Create namespace and database
	ensure_namespace_and_database(&mut socket, NS, DB).await.unwrap();
	// Specify a namespace and database
	socket.send_message_use(Some(NS), Some(DB)).await.unwrap();
	// Create the table before using it
	socket.send_message_query("DEFINE TABLE tester").await.unwrap();
	// Send LIVE command
	let res = socket.send_request("live", json!(["tester"])).await.unwrap();
	assert!(res.is_object(), "result: {res:?}");
	assert!(res["result"].is_string(), "result: {res:?}");
	let live1 = res["result"].as_str().unwrap();
	// Send QUERY command
	let res = socket.send_request("query", json!(["LIVE SELECT * FROM tester"])).await.unwrap();
	assert!(res.is_object(), "result: {res:?}");
	assert!(res["result"].is_array(), "result: {res:?}");
	let res = res["result"].as_array().unwrap();
	assert_eq!(res.len(), 1, "result: {res:?}");
	assert!(res[0]["result"].is_string(), "result: {res:?}");
	assert!(res[0]["type"].is_string(), "type: {res:?}");
	let live2 = res[0]["result"].as_str().unwrap();
	// Create a new test record
	let res =
		socket.send_request("query", json!(["CREATE tester:one SET name = 'one'"])).await.unwrap();
	assert!(res.is_object(), "result: {res:?}");
	assert!(res["result"].is_array(), "result: {res:?}");
	let res = res["result"].as_array().unwrap();
	assert_eq!(res.len(), 1, "result: {res:?}");
	// Wait some time for all messages to arrive, and then search for the
	// notification message
	let msgs = socket.receive_all_other_messages(2, Duration::from_secs(1)).await.unwrap();
	assert!(msgs.iter().all(|v| v["error"].is_null()), "Unexpected error received: {msgs:?}");
	// Check for first live query notifcation
	let res = msgs.iter().find(|v| common::is_notification_from_lq(v, live1));
	assert!(res.is_some(), "Expected to find a notification for LQ id {live1}: {msgs:?}");
	let res = res.unwrap();
	assert!(res.is_object(), "result: {res:?}");
	let res = res.as_object().unwrap();
	assert!(res["result"].is_object(), "result: {res:?}");
	let res = res["result"].as_object().unwrap();
	assert!(res["action"].is_string(), "result: {res:?}");
	assert_eq!(res["action"], "CREATE", "result: {res:?}");
	assert!(res["result"].is_object(), "result: {res:?}");
	let res = res["result"].as_object().unwrap();
	assert_eq!(res["id"], "tester:one", "result: {res:?}");
	// Check for second live query notifcation
	let res = msgs.iter().find(|v| common::is_notification_from_lq(v, live2));
	assert!(res.is_some(), "Expected to find a notification for LQ id {live2}: {msgs:?}");
	let res = res.unwrap();
	assert!(res.is_object(), "result: {res:?}");
	let res = res.as_object().unwrap();
	assert!(res["result"].is_object(), "result: {res:?}");
	let res = res["result"].as_object().unwrap();
	assert_eq!(res["action"], "CREATE", "result: {res:?}");
	assert!(res["result"].is_object(), "result: {res:?}");
	let res = res["result"].as_object().unwrap();
	assert_eq!(res["id"], "tester:one", "result: {res:?}");
	// Send KILL command
	let res = socket.send_request("kill", json!([live1])).await.unwrap();
	assert!(res.is_object(), "result: {res:?}");
	assert!(res["result"].is_null(), "result: {res:?}");
	// Wait some time for all messages to arrive, and then search for the
	// notification message
	let msgs = socket.receive_all_other_messages(1, Duration::from_secs(1)).await.unwrap();
	assert!(msgs.iter().all(|v| v["error"].is_null()), "Unexpected error received: {msgs:?}");
	// Check for second live query notifcation
	let res = msgs.iter().find(|v| common::is_notification_from_lq(v, live1));
	assert!(res.is_some(), "Expected to find a notification for LQ id {live1}: {msgs:?}");
	let res = res.unwrap();
	assert!(res.is_object(), "result: {res:?}");
	let res = res.as_object().unwrap();
	assert!(res["result"].is_object(), "result: {res:?}");
	let res = res["result"].as_object().unwrap();
	assert_eq!(res["action"], "KILLED", "result: {res:?}");
	// Create a new test record
	let res =
		socket.send_request("query", json!(["CREATE tester:two SET name = 'two'"])).await.unwrap();
	assert!(res.is_object(), "result: {res:?}");
	assert!(res["result"].is_array(), "result: {res:?}");
	let res = res["result"].as_array().unwrap();
	assert_eq!(res.len(), 1, "result: {res:?}");
	// Wait some time for all messages to arrive, and then search for the
	// notification message
	let msgs = socket.receive_all_other_messages(1, Duration::from_secs(1)).await.unwrap();
	assert!(msgs.iter().all(|v| v["error"].is_null()), "Unexpected error received: {msgs:?}");
	// Check for second live query notifcation
	let res = msgs.iter().find(|v| common::is_notification_from_lq(v, live2));
	assert!(res.is_some(), "Expected to find a notification for LQ id {live2}: {msgs:?}");
	let res = res.unwrap();
	assert!(res.is_object(), "result: {res:?}");
	let res = res.as_object().unwrap();
	assert!(res["result"].is_object(), "result: {res:?}");
	let res = res["result"].as_object().unwrap();
	assert_eq!(res["action"], "CREATE", "result: {res:?}");
	assert!(res["result"].is_object(), "result: {res:?}");
	let res = res["result"].as_object().unwrap();
	assert_eq!(res["id"], "tester:two", "result: {res:?}");
	// Send QUERY command
	let res = socket.send_request("query", json!([format!("KILL u'{live2}'")])).await.unwrap();
	assert!(res.is_object(), "result: {res:?}");
	assert!(res["result"].is_array(), "result: {res:?}");
	let res = res["result"].as_array().unwrap();
	assert_eq!(res.len(), 1, "result: {res:?}");
	assert!(res[0]["result"].is_null(), "result: {res:?}");
	assert!(res[0]["type"].is_string(), "type: {res:?}");
	// Wait some time for all messages to arrive, and then search for the
	// notification message
	let msgs = socket.receive_all_other_messages(1, Duration::from_secs(1)).await.unwrap();
	assert!(msgs.iter().all(|v| v["error"].is_null()), "Unexpected error received: {msgs:?}");
	// Check for second live query notifcation
	let res = msgs.iter().find(|v| common::is_notification_from_lq(v, live2));
	assert!(res.is_some(), "Expected to find a notification for LQ id {live2}: {msgs:?}");
	let res = res.unwrap();
	assert!(res.is_object(), "result: {res:?}");
	let res = res.as_object().unwrap();
	assert!(res["result"].is_object(), "result: {res:?}");
	let res = res["result"].as_object().unwrap();
	assert_eq!(res["action"], "KILLED", "result: {res:?}");
	// Create a new test record
	let res =
		socket.send_request("query", json!(["CREATE tester:tre SET name = 'two'"])).await.unwrap();
	assert!(res.is_object(), "result: {res:?}");
	assert!(res["result"].is_array(), "result: {res:?}");
	let res = res["result"].as_array().unwrap();
	assert_eq!(res.len(), 1, "result: {res:?}");
	// Wait some time for all messages to arrive, and then search for the
	// notification message
	let msgs = socket.receive_all_other_messages(0, Duration::from_secs(1)).await.unwrap();
	assert!(msgs.iter().all(|v| v["error"].is_null()), "Unexpected error received: {msgs:?}");
	// Test passed
	server.finish().unwrap();
}

pub async fn live_table_removal(cfg_server: Option<Format>, cfg_format: Format) {
	// Setup database server
	let (addr, mut server) = common::start_server_with_defaults().await.unwrap();
	// Connect to WebSocket
	let mut socket = Socket::connect(&addr, cfg_server, cfg_format).await.unwrap();
	// Authenticate the connection
	socket.send_message_signin(USER, PASS, None, None, None).await.unwrap();
	// Create namespace and database
	ensure_namespace_and_database(&mut socket, NS, DB).await.unwrap();
	// Specify a namespace and database
	socket.send_message_use(Some(NS), Some(DB)).await.unwrap();
	// Create the table before using it
	socket.send_message_query("DEFINE TABLE tester").await.unwrap();
	// Send LIVE command
	let res = socket.send_request("live", json!(["tester"])).await.unwrap();
	assert!(res.is_object(), "result: {res:?}");
	assert!(res["result"].is_string(), "result: {res:?}");
	let live1 = res["result"].as_str().unwrap();
	// Remove table
	let res = socket.send_request("query", json!(["REMOVE TABLE tester"])).await.unwrap();
	assert!(res.is_object(), "result: {res:?}");
	assert!(res["result"].is_array(), "result: {res:?}");
	let res = res["result"].as_array().unwrap();
	assert_eq!(res.len(), 1, "result: {res:?}");
	// Wait some time for all messages to arrive, and then search for the
	// notification message
	let msgs = socket.receive_all_other_messages(1, Duration::from_secs(1)).await.unwrap();
	assert!(msgs.iter().all(|v| v["error"].is_null()), "Unexpected error received: {msgs:?}");
	// Check for second live query notifcation
	let res = msgs.iter().find(|v| common::is_notification_from_lq(v, live1));
	assert!(res.is_some(), "Expected to find a notification for LQ id {live1}: {msgs:?}");
	let res = res.unwrap();
	assert!(res.is_object(), "result: {res:?}");
	let res = res.as_object().unwrap();
	assert!(res["result"].is_object(), "result: {res:?}");
	let res = res["result"].as_object().unwrap();
	assert_eq!(res["action"], "KILLED", "result: {res:?}");
	// Test passed
	server.finish().unwrap();
}

pub async fn live_second_connection(cfg_server: Option<Format>, cfg_format: Format) {
	// Setup database server
	let (addr, mut server) = common::start_server_with_defaults().await.unwrap();
	// Connect to WebSocket
	let mut socket1 = Socket::connect(&addr, cfg_server, cfg_format).await.unwrap();
	// Authenticate the connection
	socket1.send_message_signin(USER, PASS, None, None, None).await.unwrap();
	// Create namespace and database
	ensure_namespace_and_database(&mut socket1, NS, DB).await.unwrap();
	// Specify a namespace and database
	socket1.send_message_use(Some(NS), Some(DB)).await.unwrap();
	// Create the table before using it
	socket1.send_message_query("DEFINE TABLE tester").await.unwrap();
	// Send LIVE command
	let res = socket1.send_request("live", json!(["tester"])).await.unwrap();
	assert!(res.is_object(), "result: {res:?}");
	assert!(res["result"].is_string(), "result: {res:?}");
	let liveid = res["result"].as_str().unwrap();
	// Connect to WebSocket
	let mut socket2 = Socket::connect(&addr, cfg_server, cfg_format).await.unwrap();
	// Authenticate the connection
	socket2.send_message_signin(USER, PASS, None, None, None).await.unwrap();
	// Create namespace and database (already exists, but ensure it's set up)
	ensure_namespace_and_database(&mut socket2, NS, DB).await.unwrap();
	// Specify a namespace and database
	socket2.send_message_use(Some(NS), Some(DB)).await.unwrap();
	// Create a new test record
	let res =
		socket2.send_request("query", json!(["CREATE tester:id SET name = 'foo'"])).await.unwrap();
	assert!(res.is_object(), "result: {res:?}");
	assert!(res["result"].is_array(), "result: {res:?}");
	let res = res["result"].as_array().unwrap();
	assert_eq!(res.len(), 1, "result: {res:?}");
	// Wait some time for all messages to arrive, and then search for the
	// notification message
	let msgs = socket1.receive_all_other_messages(1, Duration::from_secs(1)).await.unwrap();
	assert!(msgs.iter().all(|v| v["error"].is_null()), "Unexpected error received: {msgs:?}");
	// Check for live query notifcation
	let res = msgs.iter().find(|v| common::is_notification_from_lq(v, liveid));
	assert!(res.is_some(), "Expected to find a notification for LQ id {liveid}: {msgs:?}");
	let res = res.unwrap();
	assert!(res.is_object(), "result: {res:?}");
	let res = res.as_object().unwrap();
	assert!(res["result"].is_object(), "result: {res:?}");
	let res = res["result"].as_object().unwrap();
	assert!(res["action"].is_string(), "result: {res:?}");
	assert_eq!(res["action"], "CREATE", "result: {res:?}");
	assert!(res["result"].is_object(), "result: {res:?}");
	let res = res["result"].as_object().unwrap();
	assert_eq!(res["id"], "tester:id", "result: {res:?}");
	// Test passed
	server.finish().unwrap();
}

pub async fn variable_auth_live_query(cfg_server: Option<Format>, cfg_format: Format) {
	// Setup database server
	let (addr, mut server) = common::start_server_with_defaults().await.unwrap();
	// Connect to WebSocket
	let mut socket_permanent = Socket::connect(&addr, cfg_server, cfg_format).await.unwrap();
	// Authenticate the connection
	socket_permanent.send_message_signin(USER, PASS, None, None, None).await.unwrap();
	// Create namespace and database
	ensure_namespace_and_database(&mut socket_permanent, NS, DB).await.unwrap();
	// Specify a namespace and database
	socket_permanent.send_message_use(Some(NS), Some(DB)).await.unwrap();
	// Create the table before using it
	socket_permanent.send_message_query("DEFINE TABLE tester").await.unwrap();
	// Define a user record access method
	socket_permanent
		.send_message_query(
			r#"
			DEFINE ACCESS user ON DATABASE TYPE RECORD
				SIGNUP ( CREATE user SET email = $email, pass = crypto::argon2::generate($pass) )
				SIGNIN ( SELECT * FROM user WHERE email = $email AND crypto::argon2::compare(pass, $pass) )
				DURATION FOR SESSION 1s, FOR TOKEN 24h
			;"#,
		)
		.await
		.unwrap();
	// Send SIGNUP command
	let mut socket_expiring_auth = Socket::connect(&addr, cfg_server, cfg_format).await.unwrap();

	let res = socket_expiring_auth
		.send_request(
			"signup",
			json!([{
				"ns": NS,
				"db": DB,
				"ac": "user",
				"email": "email@email.com",
				"pass": "pass",
			}]),
		)
		.await;
	assert!(res.is_ok(), "result: {res:?}");
	let res = res.unwrap();
	assert!(res.is_object(), "result: {res:?}");
	let res = res.as_object().unwrap();
	// Verify response contains no error
	assert!(res.keys().all(|k| ["id", "result"].contains(&k.as_str())), "result: {res:?}");
	// Authenticate the connection
	socket_expiring_auth.send_message_signin(USER, PASS, None, None, None).await.unwrap();
	// Send LIVE command
	let res = socket_expiring_auth.send_request("live", json!(["tester"])).await.unwrap();
	assert!(res.is_object(), "result: {res:?}");
	assert!(res["result"].is_string(), "result: {res:?}");
	// Wait 2 seconds for auth to expire
	tokio::time::sleep(Duration::from_secs(1)).await;
	// Create a new test record
	let res = socket_permanent
		.send_request("query", json!(["CREATE tester:id SET name = 'foo'"]))
		.await
		.unwrap();
	assert!(res.is_object(), "result: {res:?}");
	assert!(res["result"].is_array(), "result: {res:?}");
	let res = res["result"].as_array().unwrap();
	assert_eq!(res.len(), 1, "result: {res:?}");
	// Wait some time for all messages to arrive, and then search for the
	// notification message
	let msgs =
		socket_expiring_auth.receive_all_other_messages(0, Duration::from_secs(1)).await.unwrap();
	assert!(msgs.iter().all(|v| v["error"].is_null()), "Unexpected error received: {msgs:?}");
	// Test passed
	server.finish().unwrap();
}

pub async fn session_expiration(cfg_server: Option<Format>, cfg_format: Format) {
	// Setup database server
	let (addr, mut server) = common::start_server_with_defaults().await.unwrap();
	// Connect to WebSocket
	let mut socket = Socket::connect(&addr, cfg_server, cfg_format).await.unwrap();
	// Authenticate the connection
	socket.send_message_signin(USER, PASS, None, None, None).await.unwrap();
	// Specify a namespace and database
	socket.send_message_use(Some(NS), Some(DB)).await.unwrap();
	// Define a user record access method
	socket
		.send_message_query(
			r#"
			DEFINE ACCESS user ON DATABASE TYPE RECORD
				SIGNUP ( CREATE user SET email = $email, pass = crypto::argon2::generate($pass) )
				SIGNIN ( SELECT * FROM user WHERE email = $email AND crypto::argon2::compare(pass, $pass) )
				DURATION FOR SESSION 1s, FOR TOKEN 1d
			;"#,
		)
		.await
		.unwrap();
	// Create resource that requires a session with the access method to query
	socket
		.send_message_query(
			r#"
			DEFINE TABLE test SCHEMALESS
				PERMISSIONS FOR select, create, update, delete WHERE $access = "user"
			;"#,
		)
		.await
		.unwrap();
	socket
		.send_message_query(
			r#"
			CREATE test:1 SET working = "yes"
			;"#,
		)
		.await
		.unwrap();
	// Send SIGNUP command
	let res = socket
		.send_request(
			"signup",
			json!(
				[{
					"ns": NS,
					"db": DB,
					"ac": "user",
					"email": "email@email.com",
					"pass": "pass",
				}]
			),
		)
		.await;
	assert!(res.is_ok(), "result: {res:?}");
	let res = res.unwrap();
	assert!(res.is_object(), "result: {res:?}");
	let res = res.as_object().unwrap();
	// Verify response contains no error
	assert!(res.keys().all(|k| ["id", "result"].contains(&k.as_str())), "result: {res:?}");
	// Verify it returns a token
	assert!(res["result"].is_string(), "result: {res:?}");
	let res = res["result"].as_str().unwrap();
	assert!(res.starts_with("eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9"), "result: {res}");
	// Authenticate using the token, which expires in a day
	socket.send_request("authenticate", json!([res,])).await.unwrap();
	// Check if the session is now authenticated
	let res = socket.send_message_query("SELECT VALUE working FROM test:1").await.unwrap();
	assert_eq!(res[0]["result"], json!(["yes"]), "result: {res:?}");
	// Wait two seconds for the session to expire
	tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
	// Check that the session has expired and queries fail
	let res = socket.send_request("query", json!(["SELECT VALUE working FROM test:1",])).await;
	assert!(res.is_ok(), "result: {res:?}");
	let res = res.unwrap();
	assert!(res.is_object(), "result: {res:?}");
	let res = res.as_object().unwrap();
	assert_eq!(res["error"], json!({"code": -32000, "message": "The session has expired"}));
	// Sign in again using the same session
	let res = socket
		.send_request(
			"signin",
			json!(
				[{
					"ns": NS,
					"db": DB,
					"ac": "user",
					"email": "email@email.com",
					"pass": "pass",
				}]
			),
		)
		.await;
	assert!(res.is_ok(), "result: {res:?}");
	let res = res.unwrap();
	assert!(res.is_object(), "result: {res:?}");
	let res = res.as_object().unwrap();
	// Verify response contains no error
	assert!(res.keys().all(|k| ["id", "result"].contains(&k.as_str())), "result: {res:?}");
	// Check that the session is now valid again and queries succeed
	let res = socket.send_message_query("SELECT VALUE working FROM test:1").await.unwrap();
	assert_eq!(res[0]["result"], json!(["yes"]), "result: {res:?}");
	// Test passed
	server.finish().unwrap();
}

pub async fn session_expiration_operations(cfg_server: Option<Format>, cfg_format: Format) {
	// Setup database server
	let (addr, mut server) = common::start_server_with_defaults().await.unwrap();
	// Connect to WebSocket
	let mut socket = Socket::connect(&addr, cfg_server, cfg_format).await.unwrap();
	// Authenticate the connection
	// We store the root token to test reauthentication later
	let root_token = socket.send_message_signin(USER, PASS, None, None, None).await.unwrap();
	// Specify a namespace and database
	socket.send_message_use(Some(NS), Some(DB)).await.unwrap();
	// Define a user record access method
	socket
		.send_message_query(
			r#"
			DEFINE ACCESS user ON DATABASE TYPE RECORD
				SIGNUP ( CREATE user SET email = $email, pass = crypto::argon2::generate($pass) )
				SIGNIN ( SELECT * FROM user WHERE email = $email AND crypto::argon2::compare(pass, $pass) )
				DURATION FOR SESSION 1s, FOR TOKEN 1d
			;"#,
		)
		.await
		.unwrap();
	// Create resource that requires a session with the access method to query
	socket
		.send_message_query(
			r#"
			DEFINE TABLE test SCHEMALESS
				PERMISSIONS FOR select, create, update, delete WHERE $access = "user"
			;"#,
		)
		.await
		.unwrap();
	socket
		.send_message_query(
			r#"
			CREATE test:1 SET working = "yes"
			;"#,
		)
		.await
		.unwrap();
	// Send SIGNUP command
	let res = socket
		.send_request(
			"signup",
			json!(
				[{
					"ns": NS,
					"db": DB,
					"ac": "user",
					"email": "email@email.com",
					"pass": "pass",
				}]
			),
		)
		.await;
	assert!(res.is_ok(), "result: {res:?}");
	let res = res.unwrap();
	assert!(res.is_object(), "result: {res:?}");
	let res = res.as_object().unwrap();
	// Verify response contains no error
	assert!(res.keys().all(|k| ["id", "result"].contains(&k.as_str())), "result: {res:?}");
	// Verify it returns a token
	assert!(res["result"].is_string(), "result: {res:?}");
	let res = res["result"].as_str().unwrap();
	assert!(res.starts_with("eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9"), "result: {res}");
	// Authenticate using the token, which expires in a day
	socket.send_request("authenticate", json!([res,])).await.unwrap();
	// Check if the session is now authenticated
	let res = socket.send_message_query("SELECT VALUE working FROM test:1").await.unwrap();
	assert_eq!(res[0]["result"], json!(["yes"]), "result: {res:?}");
	// Wait two seconds for the session to expire
	tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
	// Check if the session is now expired
	let res = socket.send_request("query", json!(["SELECT VALUE working FROM test:1",])).await;
	assert!(res.is_ok(), "result: {res:?}");
	let res = res.unwrap();
	assert!(res.is_object(), "result: {res:?}");
	let res = res.as_object().unwrap();
	assert_eq!(res["error"], json!({"code": -32000, "message": "The session has expired"}));
	// Test operations that SHOULD NOT work with an expired session
	let operations_ko = vec![
		socket.send_request("let", json!(["let_var", "let_value",])),
		socket.send_request("set", json!(["set_var", "set_value",])),
		socket.send_request("info", json!([])),
		socket.send_request("select", json!(["tester",])),
		socket.send_request(
			"insert",
			json!([
				"tester",
				{
					"name": "foo",
					"value": "bar",
				}
			]),
		),
		socket.send_request(
			"create",
			json!([
				"tester",
				{
					"value": "bar",
				}
			]),
		),
		socket.send_request(
			"update",
			json!([
				"tester",
				{
					"value": "bar",
				}
			]),
		),
		socket.send_request(
			"merge",
			json!([
				"tester",
				{
					"value": "bar",
				}
			]),
		),
		socket.send_request(
			"patch",
			json!([
				"tester:id",
				[
					{
						"op": "add",
						"path": "value",
						"value": "bar"
					},
					{
						"op": "remove",
						"path": "name",
					}
				]
			]),
		),
		socket.send_request("delete", json!(["tester"])),
		socket.send_request("live", json!(["tester"])),
		socket.send_request("kill", json!(["tester"])),
	];
	// Futures are executed sequentially as some operations rely on the previous
	// state
	for (idx, operation) in operations_ko.into_iter().enumerate() {
		println!("Operation: {idx}");
		let res = operation.await;
		println!("res: {res:?}");
		assert!(res.is_ok(), "result: {res:?}");
		let res = res.unwrap();
		assert!(res.is_object(), "result: {res:?}");
		let res = res.as_object().unwrap();
		assert_eq!(res["error"]["message"], "The session has expired");
	}

	// Test operations that SHOULD work with an expired session
	let operations_ok = vec![
		socket.send_request("use", json!([NS, DB])),
		socket.send_request("ping", json!([])),
		socket.send_request("version", json!([])),
		socket.send_request("invalidate", json!([])),
	];
	// Futures are executed sequentially as some operations rely on the previous
	// state
	for (idx, operation) in operations_ok.into_iter().enumerate() {
		println!("operation: {idx}");
		let res = operation.await;
		assert!(res.is_ok(), "result: {res:?}");
		let res = res.unwrap();
		assert!(res.is_object(), "result: {res:?}");
		let res = res.as_object().unwrap();
		// Verify response contains no error
		assert!(res.keys().all(|k| ["id", "result"].contains(&k.as_str())), "result: {res:?}");
	}

	// Test operations that SHOULD work with an expired session
	// These operations will refresh the session expiration
	let res = socket
		.send_request(
			"signup",
			json!([{
				"ns": NS,
				"db": DB,
				"ac": "user",
				"email": "another@email.com",
				"pass": "pass",
			}]),
		)
		.await;
	assert!(res.is_ok(), "result: {res:?}");
	let res = res.unwrap();
	assert!(res.is_object(), "result: {res:?}");
	let res = res.as_object().unwrap();
	// Verify response contains no error
	assert!(res.keys().all(|k| ["id", "result"].contains(&k.as_str())), "result: {res:?}");
	// Wait two seconds for the session to expire
	tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
	// The session must be expired now or we fail the test
	let res = socket.send_request("query", json!(["SELECT VALUE working FROM test:1",])).await;
	assert!(res.is_ok(), "result: {res:?}");
	let res = res.unwrap();
	assert!(res.is_object(), "result: {res:?}");
	let res = res.as_object().unwrap();
	assert_eq!(res["error"], json!({"code": -32000, "message": "The session has expired"}));
	let res = socket
		.send_request(
			"signin",
			json!(
				[{
					"ns": NS,
					"db": DB,
					"ac": "user",
					"email": "another@email.com",
					"pass": "pass",
				}]
			),
		)
		.await;
	assert!(res.is_ok(), "result: {res:?}");
	let res = res.unwrap();
	assert!(res.is_object(), "result: {res:?}");
	let res = res.as_object().unwrap();
	// Verify response contains no error
	assert!(res.keys().all(|k| ["id", "result"].contains(&k.as_str())), "result: {res:?}");
	// Wait two seconds for the session to expire
	tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
	// The session must be expired now or we fail the test
	let res = socket.send_request("query", json!(["SELECT VALUE working FROM test:1",])).await;
	assert!(res.is_ok(), "result: {res:?}");
	let res = res.unwrap();
	assert!(res.is_object(), "result: {res:?}");
	let res = res.as_object().unwrap();
	assert_eq!(res["error"], json!({"code": -32000, "message": "The session has expired"}));

	// This needs to be last operation as the session will no longer expire
	// afterwards
	let res = socket.send_request("authenticate", json!([root_token,])).await;
	assert!(res.is_ok(), "result: {res:?}");
	let res = res.unwrap();
	assert!(res.is_object(), "result: {res:?}");
	let res = res.as_object().unwrap();
	// Verify response contains no error
	assert!(res.keys().all(|k| ["id", "result"].contains(&k.as_str())), "result: {res:?}");

	// Test passed
	server.finish().unwrap();
}

pub async fn session_reauthentication(cfg_server: Option<Format>, cfg_format: Format) {
	// Setup database server
	let (addr, mut server) = common::start_server_with_defaults().await.unwrap();
	// Connect to WebSocket
	let mut socket = Socket::connect(&addr, cfg_server, cfg_format).await.unwrap();
	// Authenticate the connection and store the root level token
	let root_token = socket.send_message_signin(USER, PASS, None, None, None).await.unwrap();
	// Check that we have root access
	socket.send_message_query("INFO FOR ROOT").await.unwrap();
	// Specify a namespace and database
	socket.send_message_use(Some(NS), Some(DB)).await.unwrap();
	// Define a user record access method
	socket
		.send_message_query(
			r#"
			DEFINE ACCESS user ON DATABASE TYPE RECORD
				SIGNUP ( CREATE user SET email = $email, pass = crypto::argon2::generate($pass) )
				SIGNIN ( SELECT * FROM user WHERE email = $email AND crypto::argon2::compare(pass, $pass) )
				DURATION FOR SESSION 1s, FOR TOKEN 24h
			;"#,
		)
		.await
		.unwrap();
	// Create resource that requires a session with the access method to query
	socket
		.send_message_query(
			r#"
			DEFINE TABLE test SCHEMALESS
				PERMISSIONS FOR select, create, update, delete WHERE $access = "user"
			;"#,
		)
		.await
		.unwrap();
	socket
		.send_message_query(
			r#"
			CREATE test:1 SET working = "yes"
			;"#,
		)
		.await
		.unwrap();
	// Send SIGNUP command
	let res = socket
		.send_request(
			"signup",
			json!(
				[{
					"ns": NS,
					"db": DB,
					"ac": "user",
					"email": "email@email.com",
					"pass": "pass",
				}]
			),
		)
		.await;
	assert!(res.is_ok(), "result: {res:?}");
	let res = res.unwrap();
	assert!(res.is_object(), "result: {res:?}");
	let res = res.as_object().unwrap();
	// Verify response contains no error
	assert!(res.keys().all(|k| ["id", "result"].contains(&k.as_str())), "result: {res:?}");
	// Verify it returns a token
	assert!(res["result"].is_string(), "result: {res:?}");
	let res = res["result"].as_str().unwrap();
	assert!(res.starts_with("eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9"), "result: {res}");
	// Authenticate using the token
	socket.send_request("authenticate", json!([res,])).await.unwrap();
	// Check that we do not have root access
	let res = socket.send_message_query("INFO FOR ROOT").await.unwrap();
	assert_eq!(res[0]["status"], "ERR", "result: {res:?}");
	assert_eq!(
		res[0]["result"], "IAM error: Not enough permissions to perform this action",
		"result: {res:?}"
	);
	// Check if the session is authenticated
	let res = socket.send_message_query("SELECT VALUE working FROM test:1").await.unwrap();
	assert_eq!(res[0]["result"], json!(["yes"]), "result: {res:?}");
	// Authenticate using the root token
	socket.send_request("authenticate", json!([root_token,])).await.unwrap();
	// Check that we have root access again
	let res = socket.send_message_query("INFO FOR ROOT").await.unwrap();
	assert_eq!(res[0]["status"], "OK", "result: {res:?}");
	// Test passed
	server.finish().unwrap();
}

pub async fn session_reauthentication_expired(cfg_server: Option<Format>, cfg_format: Format) {
	// Setup database server
	let (addr, mut server) = common::start_server_with_defaults().await.unwrap();
	// Connect to WebSocket
	let mut socket = Socket::connect(&addr, cfg_server, cfg_format).await.unwrap();
	// Authenticate the connection and store the root level token
	let root_token = socket.send_message_signin(USER, PASS, None, None, None).await.unwrap();
	// Check that we have root access
	socket.send_message_query("INFO FOR ROOT").await.unwrap();
	// Specify a namespace and database
	socket.send_message_use(Some(NS), Some(DB)).await.unwrap();
	// Define a user record access method
	socket
		.send_message_query(
			r#"
			DEFINE ACCESS user ON DATABASE TYPE RECORD
				SIGNUP ( CREATE user SET email = $email, pass = crypto::argon2::generate($pass) )
				SIGNIN ( SELECT * FROM user WHERE email = $email AND crypto::argon2::compare(pass, $pass) )
				DURATION FOR SESSION 1s, FOR TOKEN 24h
			;"#,
		)
		.await
		.unwrap();
	// Create resource that requires a session with the access method to query
	socket
		.send_message_query(
			r#"
			DEFINE TABLE test SCHEMALESS
				PERMISSIONS FOR select, create, update, delete WHERE $access = "user"
			;"#,
		)
		.await
		.unwrap();
	socket
		.send_message_query(
			r#"
			CREATE test:1 SET working = "yes"
			;"#,
		)
		.await
		.unwrap();
	// Send SIGNUP command
	let res = socket
		.send_request(
			"signup",
			json!(
				[{
					"ns": NS,
					"db": DB,
					"ac": "user",
					"email": "email@email.com",
					"pass": "pass",
				}]
			),
		)
		.await;
	assert!(res.is_ok(), "result: {res:?}");
	let res = res.unwrap();
	assert!(res.is_object(), "result: {res:?}");
	let res = res.as_object().unwrap();
	// Verify response contains no error
	assert!(res.keys().all(|k| ["id", "result"].contains(&k.as_str())), "result: {res:?}");
	// Verify it returns a token
	assert!(res["result"].is_string(), "result: {res:?}");
	let res = res["result"].as_str().unwrap();
	assert!(res.starts_with("eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9"), "result: {res}");
	// Authenticate using the token, which will expire soon
	socket.send_request("authenticate", json!([res,])).await.unwrap();
	// Wait two seconds for token to expire
	tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
	// Verify that the session has expired
	let res = socket.send_request("query", json!(["SELECT VALUE working FROM test:1",])).await;
	assert!(res.is_ok(), "result: {res:?}");
	let res = res.unwrap();
	assert!(res.is_object(), "result: {res:?}");
	let res = res.as_object().unwrap();
	assert_eq!(res["error"], json!({"code": -32000, "message": "The session has expired"}));
	// Authenticate using the root token, which has not expired yet
	socket.send_request("authenticate", json!([root_token,])).await.unwrap();
	// Check that we have root access and the session is not expired
	let res = socket.send_message_query("INFO FOR ROOT").await.unwrap();
	assert_eq!(res[0]["status"], "OK", "result: {res:?}");
	// Test passed
	server.finish().unwrap();
}

pub async fn session_failed_reauthentication(cfg_server: Option<Format>, cfg_format: Format) {
	// Setup database server without authentication
	let (addr, mut server) = common::start_server_without_auth().await.unwrap();
	// Connect to WebSocket
	let mut socket = Socket::connect(&addr, cfg_server, cfg_format).await.unwrap();
	// Specify a namespace and database to use
	socket.send_message_use(Some(NS), Some(DB)).await.unwrap();
	// Check that we have are have a database and namespace selected
	socket.send_message_query("INFO FOR DB").await.unwrap();
	// Authenticate using an invalid token
	socket.send_request("authenticate", json!(["invalid",])).await.unwrap();
	// Check to see if we still have a namespace and database selected
	let res = socket.send_message_query("INFO FOR DB").await.unwrap();
	assert_eq!(res[0]["status"], "OK", "result: {res:?}");
	// Test passed
	server.finish().unwrap();
}

pub async fn session_use_change_database(cfg_server: Option<Format>, cfg_format: Format) {
	// Setup database server
	let (addr, mut server) = common::start_server_with_defaults().await.unwrap();
	// Connect to WebSocket
	let mut socket = Socket::connect(&addr, cfg_server, cfg_format).await.unwrap();
	// Authenticate the connection as a root level system user
	let _ = socket.send_message_signin(USER, PASS, None, None, None).await.unwrap();
	// Check that we have root access
	socket.send_message_query("INFO FOR ROOT").await.unwrap();
	// Specify a namespace and database
	socket.send_message_use(Some(NS), Some("original")).await.unwrap();
	// Define a scope on the original database
	socket
		.send_message_query(
			r#"
                       DEFINE USER user ON DATABASE PASSWORD "secret" ROLES VIEWER
                       ;"#,
		)
		.await
		.unwrap();
	// Create resource that requires an authenticated record user to query
	socket
		.send_message_query(
			r#"
                       DEFINE TABLE user SCHEMALESS
                               PERMISSIONS FOR select, create, update, delete NONE
                       ;"#,
		)
		.await
		.unwrap();
	socket
               .send_message_query(
                       r#"
                       CREATE user:1 CONTENT { name: "original", pass: crypto::argon2::generate("original") }
                       ;"#,
               )
               .await
               .unwrap();
	// Change to a different database
	socket.send_message_use(Some(NS), Some("different")).await.unwrap();
	// Create the same user table with a user record with the same identifier
	socket
		.send_message_query(
			r#"
                       DEFINE TABLE user SCHEMALESS
                               PERMISSIONS FOR select, create, update, delete NONE
                       ;"#,
		)
		.await
		.unwrap();
	socket
               .send_message_query(
                       r#"
                       CREATE user:1 CONTENT { name: "different", pass: crypto::argon2::generate("different") }
                       ;"#,
               )
               .await
               .unwrap();
	// Sign in to original database as user
	let res = socket
		.send_request(
			"signin",
			json!(
					[{
							"ns": NS,
							"db": "original",
							"user": "user",
							"pass": "secret",
					}]
			),
		)
		.await;
	assert!(res.is_ok(), "result: {:?}", res);
	let res = res.unwrap();
	assert!(res.is_object(), "result: {:?}", res);
	let res = res.as_object().unwrap();
	// Verify response contains no error
	assert!(res.keys().all(|k| ["id", "result"].contains(&k.as_str())), "result: {:?}", res);
	// Verify that the authenticated session corresponds with the original user
	let res = socket.send_message_query("SELECT VALUE name FROM user:1").await.unwrap();
	assert_eq!(res[0]["result"], json!(["original"]), "result: {:?}", res);
	// Swtich to the different database without signing in again
	socket.send_message_use(Some(NS), Some("different")).await.unwrap();
	// Verify that the authenticated session is unable to query data
	let res = socket.send_message_query("SELECT VALUE name FROM user:1").await.unwrap();
	// The query succeeds but the results does not contain the value with
	// permissions
	assert_eq!(res[0]["status"], "OK", "result: {:?}", res);
	assert_eq!(res[0]["result"], json!([]), "result: {:?}", res);
	// Test passed
	server.finish().unwrap();
}

pub async fn session_use_change_database_scope(cfg_server: Option<Format>, cfg_format: Format) {
	// Setup database server
	let (addr, mut server) = common::start_server_with_defaults().await.unwrap();
	// Connect to WebSocket
	let mut socket = Socket::connect(&addr, cfg_server, cfg_format).await.unwrap();
	// Authenticate the connection as a root level system user
	let _ = socket.send_message_signin(USER, PASS, None, None, None).await.unwrap();
	// Check that we have root access
	socket.send_message_query("INFO FOR ROOT").await.unwrap();
	// Specify a namespace and database
	socket.send_message_use(Some(NS), Some("original")).await.unwrap();
	// Define a user record access method on the original database
	socket
		.send_message_query(
			r#"
			DEFINE ACCESS user ON DATABASE TYPE RECORD
				SIGNIN ( SELECT * FROM user WHERE name = $name AND crypto::argon2::compare(pass, $pass) )
				DURATION FOR SESSION 24h, FOR TOKEN 24h
			;"#,
		)
		.await
		.unwrap();
	// Create resource that requires an authenticated record user to query
	socket
		.send_message_query(
			r#"
			DEFINE TABLE user SCHEMALESS
				PERMISSIONS FOR select, create, update, delete WHERE id = $auth
			;"#,
		)
		.await
		.unwrap();
	socket
		.send_message_query(
			r#"
			CREATE user:1 CONTENT { name: "original", pass: crypto::argon2::generate("original") }
			;"#,
		)
		.await
		.unwrap();
	// Change to a different database
	socket.send_message_use(Some(NS), Some("different")).await.unwrap();
	// Create the same user table with a user record with the same identifier
	socket
		.send_message_query(
			r#"
			DEFINE TABLE user SCHEMALESS
				PERMISSIONS FOR select, create, update, delete WHERE id = $auth
			;"#,
		)
		.await
		.unwrap();
	socket
		.send_message_query(
			r#"
			CREATE user:1 CONTENT { name: "different", pass: crypto::argon2::generate("different") }
			;"#,
		)
		.await
		.unwrap();
	// Sign in to original database as user
	let res = socket
		.send_request(
			"signin",
			json!(
				[{
					"ns": NS,
					"db": "original",
					"ac": "user",
					"name": "original",
					"pass": "original",
				}]
			),
		)
		.await;
	assert!(res.is_ok(), "result: {:?}", res);
	let res = res.unwrap();
	assert!(res.is_object(), "result: {:?}", res);
	let res = res.as_object().unwrap();
	// Verify response contains no error
	assert!(res.keys().all(|k| ["id", "result"].contains(&k.as_str())), "result: {:?}", res);
	// Verify that the authenticated session corresponds with the original user
	let res = socket.send_message_query("SELECT VALUE name FROM $auth").await.unwrap();
	assert_eq!(res[0]["result"], json!(["original"]), "result: {:?}", res);
	// Swtich to the different database without signing in again
	socket.send_message_use(Some(NS), Some("different")).await.unwrap();
	// Verify that the authenticated session is unable to query data
	let res = socket.send_message_query("SELECT VALUE name FROM $auth").await.unwrap();
	// The following statement would be true when the bug was present:
	// assert_eq!(res[0]["result"], json!(["different"]), "result: {:?}", res);
	assert_eq!(res[0]["status"], "ERR", "result: {:?}", res);
	assert_eq!(
		res[0]["result"], "You don't have permission to change to the different database",
		"result: {:?}",
		res
	);
	// Test passed
	server.finish().unwrap();
}

pub async fn run_functions(cfg_server: Option<Format>, cfg_format: Format) {
	// Setup database server
	let (addr, mut server) = common::start_server_with_defaults().await.unwrap();
	// Connect to WebSocket
	let mut socket = Socket::connect(&addr, cfg_server, cfg_format).await.unwrap();
	// Authenticate the connection
	socket.send_message_signin(USER, PASS, None, None, None).await.unwrap();
	// Specify a namespace and database
	socket.send_message_use(Some(NS), Some(DB)).await.unwrap();
	// Define function
	socket
		.send_message_query("DEFINE FUNCTION fn::foo() {RETURN 'fn::foo called';}")
		.await
		.unwrap();
	socket
		.send_message_query(
			"DEFINE FUNCTION fn::bar($val: string) {RETURN 'fn::bar called with: ' + $val;}",
		)
		.await
		.unwrap();
	// call functions
	let res = socket.send_message_run("fn::foo", None, vec![]).await.unwrap();
	assert!(matches!(res, serde_json::Value::String(s) if &s == "fn::foo called"));
	let res = socket.send_message_run("fn::bar", None, vec![]).await;
	assert!(res.is_err());
	let res = socket.send_message_run("fn::bar", None, vec![42.into()]).await;
	assert!(res.is_err());
	let res = socket.send_message_run("fn::bar", None, vec!["first".into(), "second".into()]).await;
	assert!(res.is_err());
	let res = socket.send_message_run("fn::bar", None, vec!["string_val".into()]).await.unwrap();
	assert!(matches!(res, serde_json::Value::String(s) if &s == "fn::bar called with: string_val"));

	// normal functions
	let res = socket.send_message_run("math::abs", None, vec![42.into()]).await.unwrap();
	assert!(matches!(res, serde_json::Value::Number(n) if n.as_u64() == Some(42)));
	let res = socket
		.send_message_run("math::max", None, vec![vec![1, 2, 3, 4, 5, 6].into()])
		.await
		.unwrap();
	assert!(matches!(res, serde_json::Value::Number(n) if n.as_u64() == Some(6)));

	// Test passed
	server.finish().unwrap();
}

pub async fn relate_rpc(cfg_server: Option<Format>, cfg_format: Format) {
	// Setup database server
	let (addr, mut server) = common::start_server_with_defaults().await.unwrap();
	// Connect to WebSocket
	let mut socket = Socket::connect(&addr, cfg_server, cfg_format).await.unwrap();
	// Authenticate the connection
	socket.send_message_signin(USER, PASS, None, None, None).await.unwrap();
	// Specify a namespace and database
	socket.send_message_use(Some(NS), Some(DB)).await.unwrap();
	// create records and relate
	socket.send_message_query("CREATE foo:a, foo:b").await.unwrap();
	socket
		.send_message_relate("foo:a".into(), "bar".into(), "foo:b".into(), Some(json!({"val": 42})))
		.await
		.unwrap();
	// test

	let mut res = socket.send_message_query("RETURN foo:a->bar.val").await.unwrap();
	let expected = json!([42]);
	assert_eq!(res.remove(0)["result"], expected);

	let mut res = socket.send_message_query("RETURN foo:a->bar->foo").await.unwrap();
	let expected = json!(["foo:b"]);
	assert_eq!(res.remove(0)["result"], expected);

	// Test passed
	server.finish().unwrap();
}

pub async fn temporary_directory(cfg_server: Option<Format>, cfg_format: Format) {
	// Setup database server
	let temp_dir = TempDir::new().unwrap();
	let (addr, mut server) =
		common::start_server_with_temporary_directory(temp_dir.to_string_lossy().as_ref())
			.await
			.unwrap();
	// Connect to WebSocket
	let mut socket = Socket::connect(&addr, cfg_server, cfg_format).await.unwrap();
	// Authenticate the connection
	socket.send_message_signin(USER, PASS, None, None, None).await.unwrap();
	// Specify a namespace and database
	socket.send_message_use(Some(NS), Some(DB)).await.unwrap();
	// create records
	socket.send_message_query("CREATE test:a, test:b").await.unwrap();
	// ORDER BY id DESC is satisfied by a backward TableScan (no explicit sort needed)
	let mut res =
		socket.send_message_query("SELECT * FROM test ORDER BY id DESC EXPLAIN").await.unwrap();
	let result = &res.remove(0)["result"];
	// New executor EXPLAIN produces a JSON plan tree
	assert_eq!(result["operator"], "Project");
	assert_eq!(result["children"][0]["operator"], "TableScan");
	assert_eq!(result["children"][0]["attributes"]["direction"], "Backward");
	// And return the correct result
	let mut res = socket.send_message_query("SELECT * FROM test ORDER BY id DESC").await.unwrap();
	let expected = json!([{"id": "test:b" }, { "id": "test:a" }]);
	assert_eq!(res.remove(0)["result"], expected);
	// TEMPFILES requests file-backed sort, but the planner eliminates the sort
	// entirely since a backward TableScan already satisfies ORDER BY id DESC.
	let mut res = socket
		.send_message_query("SELECT * FROM test ORDER BY id DESC TEMPFILES EXPLAIN")
		.await
		.unwrap();
	let result = &res.remove(0)["result"];
	assert_eq!(result["operator"], "Project");
	assert_eq!(result["children"][0]["operator"], "TableScan");
	assert_eq!(result["children"][0]["attributes"]["direction"], "Backward");
	// And return the correct result
	let mut res =
		socket.send_message_query("SELECT * FROM test ORDER BY id DESC TEMPFILES").await.unwrap();
	let expected = json!([{"id": "test:b" }, { "id": "test:a" }]);
	assert_eq!(res.remove(0)["result"], expected);
	// Test passed
	server.finish().unwrap();
	// Cleanup
	temp_dir.close().unwrap();
}

pub async fn session_id_defined(cfg_server: Option<Format>, cfg_format: Format) {
	// Setup database server
	let (addr, mut server) = common::start_server_with_defaults().await.unwrap();
	// We specify a request identifier via a specific SurrealDB header
	let mut headers = HeaderMap::new();
	headers.insert(HDR_SURREAL, HeaderValue::from_static("00000000-0000-0000-0000-000000000000"));
	// Connect to WebSocket
	let mut socket =
		Socket::connect_with_headers(&addr, cfg_server, cfg_format, headers).await.unwrap();
	// Authenticate the connection
	socket.send_message_signin(USER, PASS, None, None, None).await.unwrap();
	// Specify a namespace and database
	socket.send_message_use(Some(NS), Some(DB)).await.unwrap();

	let mut res = socket.send_message_query("SELECT VALUE id FROM $session").await.unwrap();
	let expected = json!(["00000000-0000-0000-0000-000000000000"]);
	assert_eq!(res.remove(0)["result"], expected);

	// Test passed
	server.finish().unwrap();
}

pub async fn session_id_defined_generic(cfg_server: Option<Format>, cfg_format: Format) {
	// Setup database server
	let (addr, mut server) = common::start_server_with_defaults().await.unwrap();
	// We specify a request identifier via a generic header
	let mut headers = HeaderMap::new();
	headers.insert(HDR_REQUEST, HeaderValue::from_static("00000000-0000-0000-0000-000000000000"));
	// Connect to WebSocket
	let mut socket =
		Socket::connect_with_headers(&addr, cfg_server, cfg_format, headers).await.unwrap();
	// Authenticate the connection
	socket.send_message_signin(USER, PASS, None, None, None).await.unwrap();
	// Specify a namespace and database
	socket.send_message_use(Some(NS), Some(DB)).await.unwrap();

	let mut res = socket.send_message_query("SELECT VALUE id FROM $session").await.unwrap();
	let expected = json!(["00000000-0000-0000-0000-000000000000"]);
	assert_eq!(res.remove(0)["result"], expected);

	// Test passed
	server.finish().unwrap();
}

pub async fn session_id_defined_both(cfg_server: Option<Format>, cfg_format: Format) {
	// Setup database server
	let (addr, mut server) = common::start_server_with_defaults().await.unwrap();
	// We specify a request identifier via both headers
	let mut headers = HeaderMap::new();
	headers.insert(HDR_SURREAL, HeaderValue::from_static("00000000-0000-0000-0000-000000000000"));
	headers.insert(HDR_REQUEST, HeaderValue::from_static("aaaaaaaa-aaaa-0000-0000-000000000000"));
	// Connect to WebSocket
	let mut socket =
		Socket::connect_with_headers(&addr, cfg_server, cfg_format, headers).await.unwrap();
	// Authenticate the connection
	socket.send_message_signin(USER, PASS, None, None, None).await.unwrap();
	// Specify a namespace and database
	socket.send_message_use(Some(NS), Some(DB)).await.unwrap();

	let mut res = socket.send_message_query("SELECT VALUE id FROM $session").await.unwrap();
	// The specific header should be used
	let expected = json!(["00000000-0000-0000-0000-000000000000"]);
	assert_eq!(res.remove(0)["result"], expected);

	// Test passed
	server.finish().unwrap();
}

pub async fn session_id_invalid(cfg_server: Option<Format>, cfg_format: Format) {
	// Setup database server
	let (addr, mut server) = common::start_server_with_defaults().await.unwrap();
	// We specify a request identifier via a specific SurrealDB header
	let mut headers = HeaderMap::new();
	// Not a valid UUIDv4
	headers.insert(HDR_SURREAL, HeaderValue::from_static("123"));
	// Connect to WebSocket
	let socket = Socket::connect_with_headers(&addr, cfg_server, cfg_format, headers).await;
	assert!(socket.is_err(), "unexpected success using connecting with invalid id header");

	// Test passed
	server.finish().unwrap();
}

pub async fn session_id_undefined(cfg_server: Option<Format>, cfg_format: Format) {
	// Setup database server
	let (addr, mut server) = common::start_server_with_defaults().await.unwrap();
	// Connect to WebSocket
	let mut socket = Socket::connect(&addr, cfg_server, cfg_format).await.unwrap();
	// Authenticate the connection
	socket.send_message_signin(USER, PASS, None, None, None).await.unwrap();
	// Specify a namespace and database
	socket.send_message_use(Some(NS), Some(DB)).await.unwrap();

	let mut res = socket.send_message_query("SELECT VALUE id FROM $session").await.unwrap();
	// The field is expected to be present even when not provided in the header
	let unexpected = json!([null]);
	assert_ne!(res.remove(0)["result"], unexpected);

	// Test passed
	server.finish().unwrap();
}

pub async fn rpc_capability(cfg_server: Option<Format>, cfg_format: Format) {
	// Deny some
	{
		// Start server disallowing some RPC methods
		let (addr, mut server) = common::start_server(StartServerArguments {
			// Deny all routes except for RPC
			args: "--deny-rpc info".to_string(),
			// Auth disabled to ensure unauthorized errors are due to capabilities
			auth: false,
			..Default::default()
		})
		.await
		.unwrap();
		// Connect to WebSocket
		let mut socket = Socket::connect(&addr, cfg_server, cfg_format).await.unwrap();
		// Specify a namespace and database
		socket.send_message_use(Some(NS), Some(DB)).await.unwrap();

		// Test operations that SHOULD NOT with the provided capabilities
		let operations_ko = vec![socket.send_request("info", json!([]))];
		for operation in operations_ko {
			let res = operation.await;
			assert!(res.is_ok(), "result: {res:?}");
			let res = res.unwrap();
			assert!(res.is_object(), "result: {res:?}");
			let res = res.as_object().unwrap();
			assert_eq!(res["error"], json!({"code": -32602, "message": "Method not allowed"}));
		}

		// Test operations that SHOULD work with the provided capabilities
		let operations_ok = vec![
			socket.send_request("use", json!([NS, DB])),
			socket.send_request("ping", json!([])),
			socket.send_request("version", json!([])),
			socket.send_request("let", json!(["let_var", "let_value",])),
			socket.send_request("set", json!(["set_var", "set_value",])),
			socket.send_request("query", json!(["DEFINE TABLE tester"])),
			socket.send_request("select", json!(["tester",])),
			socket.send_request(
				"insert",
				json!([
					"tester",
					{
						"name": "foo",
						"value": "bar",
					}
				]),
			),
			socket.send_request(
				"create",
				json!([
					"tester",
					{
						"value": "bar",
					}
				]),
			),
			socket.send_request(
				"update",
				json!([
					"tester",
					{
						"value": "bar",
					}
				]),
			),
			socket.send_request(
				"merge",
				json!([
					"tester",
					{
						"value": "bar",
					}
				]),
			),
			socket.send_request("delete", json!(["tester"])),
			socket.send_request("invalidate", json!([])),
		];
		for (idx, operation) in operations_ok.into_iter().enumerate() {
			let res = operation.await;
			assert!(res.is_ok(), "result: {res:?}");
			let res = res.unwrap();
			assert!(res.is_object(), "result: {res:?}");
			let res = res.as_object().unwrap();
			// Verify response contains no error
			assert!(
				res.keys().all(|k| ["id", "result"].contains(&k.as_str())),
				"[{idx}] result: {res:?}"
			);
		}

		// Test passed
		server.finish().unwrap();
	}
	// Deny all
	{
		// Start server disallowing all RPC methods except for version, use, and attach
		// attach is required for the SDK to establish a session
		let (addr, mut server) = common::start_server(StartServerArguments {
			// Deny all routes except for RPC
			args: "--deny-rpc --allow-rpc version,use,attach".to_string(),
			// Auth disabled to ensure unauthorized errors are due to capabilities
			auth: false,
			..Default::default()
		})
		.await
		.unwrap();
		// Connect to WebSocket
		let mut socket = Socket::connect(&addr, cfg_server, cfg_format).await.unwrap();
		// Specify a namespace and database
		socket.send_message_use(Some(NS), Some(DB)).await.unwrap();

		// Test operations that SHOULD NOT with the provided capabilities
		let operations_ko = vec![
			socket.send_request("query", json!(["SELECT * FROM 1"])),
			socket.send_request("ping", json!([])),
			socket.send_request("info", json!([])),
			socket.send_request("let", json!(["let_var", "let_value",])),
			socket.send_request("set", json!(["set_var", "set_value",])),
			socket.send_request("select", json!(["tester",])),
			socket.send_request(
				"insert",
				json!([
					"tester",
					{
						"name": "foo",
						"value": "bar",
					}
				]),
			),
			socket.send_request(
				"create",
				json!([
					"tester",
					{
						"value": "bar",
					}
				]),
			),
			socket.send_request(
				"update",
				json!([
					"tester",
					{
						"value": "bar",
					}
				]),
			),
			socket.send_request(
				"merge",
				json!([
					"tester",
					{
						"value": "bar",
					}
				]),
			),
			socket.send_request(
				"patch",
				json!([
					"tester:id",
					[
						{
							"op": "add",
							"path": "value",
							"value": "bar"
						},
						{
							"op": "remove",
							"path": "name",
						}
					]
				]),
			),
			socket.send_request("delete", json!(["tester"])),
			socket.send_request("invalidate", json!([])),
		];
		for operation in operations_ko {
			let res = operation.await;
			assert!(res.is_ok(), "result: {res:?}");
			let res = res.unwrap();
			assert!(res.is_object(), "result: {res:?}");
			let res = res.as_object().unwrap();
			assert_eq!(res["error"], json!({"code": -32602, "message": "Method not allowed"}));
		}

		// Test operations that SHOULD work with the provided capabilities
		let operations_ok = vec![
			socket.send_request("version", json!([])),
			socket.send_request("use", json!([NS, DB])),
		];
		for operation in operations_ok {
			let res = operation.await;
			assert!(res.is_ok(), "result: {res:?}");
			let res = res.unwrap();
			assert!(res.is_object(), "result: {res:?}");
			let res = res.as_object().unwrap();
			// Verify response contains no error
			assert!(res.keys().all(|k| ["id", "result"].contains(&k.as_str())), "result: {res:?}");
		}

		// Test passed
		server.finish().unwrap();
	}
}

/// A macro which defines a macro which can be used to define tests running the
/// above functions with a set of given paramenters.
macro_rules! define_include_tests {
	( $( $( #[$m:meta] )* $test_name:ident),* $(,)? ) => {
		macro_rules! include_tests {
			($server:expr_2021, $format:expr_2021) => {
				$(
					$(#[$m])*
					async fn $test_name(){
						crate::$test_name($server,$format).await
					}
				)*

			};
		}
		pub(crate) use include_tests;
	};
}

pub async fn multi_session_isolation(cfg_server: Option<Format>, cfg_format: Format) {
	// Setup database server
	let (addr, mut server) = common::start_server_with_defaults().await.unwrap();
	// Connect to WebSocket
	let mut socket = Socket::connect(&addr, cfg_server, cfg_format).await.unwrap();
	// Authenticate the connection
	socket.send_message_signin(USER, PASS, None, None, None).await.unwrap();
	// Create namespace and database
	ensure_namespace_and_database(&mut socket, NS, DB).await.unwrap();
	// Specify a namespace and database
	socket.send_message_use(Some(NS), Some(DB)).await.unwrap();

	// Define session IDs
	let session1 = "11111111-1111-1111-1111-111111111111";
	let session2 = "22222222-2222-2222-2222-222222222222";

	socket.send_request_with_session("attach", json!([]), session1).await.unwrap();
	socket.send_request_with_session("attach", json!([]), session2).await.unwrap();

	// Test 1: Variable isolation between named sessions
	// Setup session1 with auth and namespace/database
	socket
		.send_request_with_session("signin", json!([{"user": USER, "pass": PASS}]), session1)
		.await
		.unwrap();
	socket.send_request_with_session("use", json!([NS, DB]), session1).await.unwrap();
	socket
		.send_request_with_session("set", json!(["my_var", "value_from_session1"]), session1)
		.await
		.unwrap();

	// Setup session2 with auth and namespace/database
	socket
		.send_request_with_session("signin", json!([{"user": USER, "pass": PASS}]), session2)
		.await
		.unwrap();
	socket.send_request_with_session("use", json!([NS, DB]), session2).await.unwrap();
	socket
		.send_request_with_session("set", json!(["my_var", "value_from_session2"]), session2)
		.await
		.unwrap();

	// Verify each named session has its own value
	let res = socket
		.send_request_with_session("query", json!(["RETURN $my_var"]), session1)
		.await
		.unwrap();
	assert_eq!(res["result"][0]["result"], "value_from_session1", "result: {res:?}");

	let res = socket
		.send_request_with_session("query", json!(["RETURN $my_var"]), session2)
		.await
		.unwrap();
	assert_eq!(res["result"][0]["result"], "value_from_session2", "result: {res:?}");

	// Test 2: Default session isolation
	socket.send_request("set", json!(["my_var", "default_session_value"])).await.unwrap();

	// Verify default session has its own value
	let res = socket.send_request("query", json!(["RETURN $my_var"])).await.unwrap();
	assert_eq!(res["result"][0]["result"], "default_session_value", "result: {res:?}");

	// Verify named sessions still have their own values
	let res = socket
		.send_request_with_session("query", json!(["RETURN $my_var"]), session1)
		.await
		.unwrap();
	assert_eq!(res["result"][0]["result"], "value_from_session1", "result: {res:?}");

	// Test 3: Namespace/database isolation
	// Create namespace and database for session1
	socket
		.send_request_with_session("query", json!(["DEFINE NAMESPACE `test_ns1`"]), session1)
		.await
		.unwrap();
	socket
		.send_request_with_session("use", json!(["test_ns1", None::<String>]), session1)
		.await
		.unwrap();
	socket
		.send_request_with_session("query", json!(["DEFINE DATABASE `test_db1`"]), session1)
		.await
		.unwrap();
	socket
		.send_request_with_session("use", json!(["test_ns1", "test_db1"]), session1)
		.await
		.unwrap();
	// Create the table before using it
	socket
		.send_request_with_session("query", json!(["DEFINE TABLE test"]), session1)
		.await
		.unwrap();

	socket
		.send_request_with_session(
			"query",
			json!(["CREATE test:one SET source = 'session1'"]),
			session1,
		)
		.await
		.unwrap();

	// Default session should not see the record (different ns/db)
	// The default session is still using NS/DB, so querying test:one will fail because
	// the table doesn't exist in that namespace/database
	let res = socket.send_request("query", json!(["SELECT * FROM test:one"])).await.unwrap();
	// When table doesn't exist in the namespace/database, we get an error
	// Check for either empty result or error about table not existing
	if res["result"][0]["status"] == "ERR" {
		assert!(
			res["result"][0]["result"].as_str().unwrap().contains("does not exist"),
			"result: {res:?}"
		);
	} else {
		assert_eq!(res["result"][0]["result"], json!([]), "result: {res:?}");
	}

	// Session 1 can see its own record
	let res = socket
		.send_request_with_session("query", json!(["SELECT * FROM test:one"]), session1)
		.await
		.unwrap();
	assert_eq!(res["result"][0]["result"][0]["source"], "session1", "result: {res:?}");

	// Test passed
	server.finish().unwrap();
}

pub async fn multi_session_authentication(cfg_server: Option<Format>, cfg_format: Format) {
	// Setup database server
	let (addr, mut server) = common::start_server_with_defaults().await.unwrap();
	// Connect to WebSocket
	let socket = Socket::connect(&addr, cfg_server, cfg_format).await.unwrap();

	// Define session IDs
	let session1 = "11111111-1111-1111-1111-111111111111";
	let session2 = "22222222-2222-2222-2222-222222222222";

	socket.send_request_with_session("attach", json!([]), session1).await.unwrap();
	socket.send_request_with_session("attach", json!([]), session2).await.unwrap();

	// Authenticate session 1 as root user
	let res = socket
		.send_request_with_session(
			"signin",
			json!([{
				"user": USER,
				"pass": PASS,
			}]),
			session1,
		)
		.await
		.unwrap();
	assert!(res["result"].is_string(), "result: {res:?}");

	// Session 2 remains unauthenticated
	// Try to define a namespace with session 2 (should fail with auth error)
	let res = socket
		.send_request_with_session("query", json!(["DEFINE NAMESPACE test"]), session2)
		.await
		.unwrap();
	// Should get an RPC-level error due to lack of authentication
	assert!(res["error"].is_object(), "Expected error for unauthenticated query: {res:?}");

	// Try to define a namespace with session 1 (should succeed)
	socket.send_request_with_session("use", json!(["test_ns", "test_db"]), session1).await.unwrap();
	let res = socket
		.send_request_with_session("query", json!(["DEFINE TABLE test"]), session1)
		.await
		.unwrap();
	assert!(res["result"].is_array(), "result: {res:?}");
	let result = res["result"].as_array().unwrap();
	assert_eq!(result[0]["status"], "OK", "result: {res:?}");

	// Test passed
	server.finish().unwrap();
}

pub async fn multi_session_management(cfg_server: Option<Format>, cfg_format: Format) {
	// Setup database server
	let (addr, mut server) = common::start_server_with_defaults().await.unwrap();
	// Connect to WebSocket
	let mut socket = Socket::connect(&addr, cfg_server, cfg_format).await.unwrap();
	// Authenticate the connection
	socket.send_message_signin(USER, PASS, None, None, None).await.unwrap();
	// Specify a namespace and database
	socket.send_message_use(Some(NS), Some(DB)).await.unwrap();

	// Define session IDs
	let session1 = "11111111-1111-1111-1111-111111111111";
	let session2 = "22222222-2222-2222-2222-222222222222";
	let session3 = "33333333-3333-3333-3333-333333333333";

	// Test 1: List sessions - should be empty initially
	let res = socket.send_request("sessions", json!([])).await.unwrap();
	assert_eq!(res["result"].as_array().unwrap().len(), 0, "Expected no sessions initially");

	// Test 2: Create sessions with proper authentication and namespace/database setup
	socket.send_request_with_session("attach", json!([]), session1).await.unwrap();
	socket.send_request_with_session("attach", json!([]), session2).await.unwrap();
	socket.send_request_with_session("attach", json!([]), session3).await.unwrap();
	let res = socket.send_request("sessions", json!([])).await.unwrap();
	let sessions = res["result"].as_array().unwrap();
	assert_eq!(sessions.len(), 3, "Expected 3 sessions");

	let session_ids: Vec<String> =
		sessions.iter().filter_map(|v| v.as_str()).map(|s| s.to_string()).collect();
	assert!(session_ids.contains(&session1.to_string()), "Session 1 not found");
	assert!(session_ids.contains(&session2.to_string()), "Session 2 not found");
	assert!(session_ids.contains(&session3.to_string()), "Session 3 not found");

	socket
		.send_request_with_session("signin", json!([{"user": USER, "pass": PASS}]), session1)
		.await
		.unwrap();
	socket.send_request_with_session("use", json!([NS, DB]), session1).await.unwrap();
	socket.send_request_with_session("set", json!(["var1", "value1"]), session1).await.unwrap();

	socket
		.send_request_with_session("signin", json!([{"user": USER, "pass": PASS}]), session2)
		.await
		.unwrap();
	socket.send_request_with_session("use", json!([NS, DB]), session2).await.unwrap();
	socket.send_request_with_session("set", json!(["var2", "value2"]), session2).await.unwrap();

	socket
		.send_request_with_session("signin", json!([{"user": USER, "pass": PASS}]), session3)
		.await
		.unwrap();
	socket.send_request_with_session("use", json!([NS, DB]), session3).await.unwrap();
	socket.send_request_with_session("set", json!(["var3", "value3"]), session3).await.unwrap();

	// Test 3: Verify session variables work
	let res =
		socket.send_request_with_session("query", json!(["RETURN $var1"]), session1).await.unwrap();
	assert_eq!(res["result"][0]["result"], "value1");

	// Test 4: Detach with session ID removes the session completely
	socket.send_request_with_session("detach", json!([]), session1).await.unwrap();

	let res = socket.send_request("sessions", json!([])).await.unwrap();
	let sessions = res["result"].as_array().unwrap();
	assert_eq!(sessions.len(), 2, "Expected 2 sessions after removing one");

	let session_ids: Vec<String> =
		sessions.iter().filter_map(|v| v.as_str()).map(|s| s.to_string()).collect();
	assert!(!session_ids.contains(&session1.to_string()), "Session 1 should be removed");

	// Detach another session
	socket.send_request_with_session("detach", json!([]), session2).await.unwrap();

	let res = socket.send_request("sessions", json!([])).await.unwrap();
	let sessions = res["result"].as_array().unwrap();
	assert_eq!(sessions.len(), 1, "Expected 1 session after removing two");

	let session_ids: Vec<String> =
		sessions.iter().filter_map(|v| v.as_str()).map(|s| s.to_string()).collect();
	assert!(!session_ids.contains(&session2.to_string()), "Session 2 should be removed");

	// Verify other sessions still work
	let res =
		socket.send_request_with_session("query", json!(["RETURN $var3"]), session3).await.unwrap();
	assert_eq!(res["result"][0]["result"], "value3");

	// Test passed
	server.finish().unwrap();
}

define_include_tests! {
	#[test_log::test(tokio::test)]
	ping,
	#[test_log::test(tokio::test)]
	info,
	#[test_log::test(tokio::test)]
	signup,
	#[test_log::test(tokio::test)]
	signin,
	#[test_log::test(tokio::test)]
	invalidate,
	#[test_log::test(tokio::test)]
	authenticate,
	#[test_log::test(tokio::test)]
	letset,
	#[test_log::test(tokio::test)]
	unset,
	#[test_log::test(tokio::test)]
	select,
	#[test_log::test(tokio::test)]
	insert,
	#[test_log::test(tokio::test)]
	create,
	#[test_log::test(tokio::test)]
	update,
	#[test_log::test(tokio::test)]
	merge,
	#[test_log::test(tokio::test)]
	patch,
	#[test_log::test(tokio::test)]
	delete,
	#[test_log::test(tokio::test)]
	query,
	#[test_log::test(tokio::test)]
	version,
	#[test_log::test(tokio::test)]
	concurrency,
	#[test_log::test(tokio::test)]
	live_query,
	#[test_log::test(tokio::test)]
	live_rpc,
	#[test_log::test(tokio::test)]
	live_query_diff,
	#[test_log::test(tokio::test)]
	kill,
	#[test_log::test(tokio::test)]
	live_table_removal,
	#[test_log::test(tokio::test)]
	live_second_connection,
	#[test_log::test(tokio::test)]
	variable_auth_live_query,
	#[test_log::test(tokio::test)]
	session_expiration,
	#[test_log::test(tokio::test)]
	session_expiration_operations,
	#[test_log::test(tokio::test)]
	session_reauthentication,
	#[test_log::test(tokio::test)]
	session_reauthentication_expired,
	#[test_log::test(tokio::test)]
	session_failed_reauthentication,
	#[test_log::test(tokio::test)]
	session_use_change_database,
	#[test_log::test(tokio::test)]
	session_use_change_database_scope,
	#[test_log::test(tokio::test)]
	run_functions,
	#[test_log::test(tokio::test)]
	relate_rpc,
	#[test_log::test(tokio::test)]
	temporary_directory,
	#[test_log::test(tokio::test)]
	session_id_defined,
	#[test_log::test(tokio::test)]
	session_id_defined_generic,
	#[test_log::test(tokio::test)]
	session_id_defined_both,
	#[test_log::test(tokio::test)]
	session_id_invalid,
	#[test_log::test(tokio::test)]
	session_id_undefined,
	#[test_log::test(tokio::test)]
	rpc_capability,
	#[test_log::test(tokio::test)]
	multi_session_isolation,
	#[test_log::test(tokio::test)]
	multi_session_authentication,
	#[test_log::test(tokio::test)]
	multi_session_management,
}
