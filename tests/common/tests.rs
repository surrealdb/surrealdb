use super::common::{self, Format, Socket, DB, NS, PASS, USER};
use serde_json::json;
use std::time::Duration;
use test_log::test;
use ulid::Ulid;

#[test(tokio::test)]
async fn ping() -> Result<(), Box<dyn std::error::Error>> {
	// Setup database server
	let (addr, _server) = common::start_server_with_defaults().await.unwrap();
	// Connect to WebSocket
	let mut socket = Socket::connect(&addr, SERVER).await?;
	// Send INFO command
	let res = socket
		.send_and_receive_message(
			FORMAT,
			json!({
				"id": Ulid::new(),
				"method": "ping",
			}),
		)
		.await;
	assert!(res.is_ok(), "result: {:?}", res);
	let res = res.unwrap();
	assert!(res.is_object(), "result: {:?}", res);
	let res = res.as_object().unwrap();
	assert!(res.keys().all(|k| ["id", "result"].contains(&k.as_str())), "result: {:?}", res);
	// Test passed
	Ok(())
}

#[test(tokio::test)]
async fn info() -> Result<(), Box<dyn std::error::Error>> {
	// Setup database server
	let (addr, _server) = common::start_server_with_defaults().await.unwrap();
	// Connect to WebSocket
	let mut socket = Socket::connect(&addr, SERVER).await?;
	// Authenticate the connection
	socket.send_message_signin(FORMAT, USER, PASS, None, None, None).await?;
	// Specify a namespace and database
	socket.send_message_use(FORMAT, Some(NS), Some(DB)).await?;
	// Define a user table
	socket.send_message_query(FORMAT, "DEFINE TABLE user PERMISSIONS FULL").await?;
	// Define a user scope
	socket
		.send_message_query(
			FORMAT,
			r#"
			DEFINE SCOPE scope SESSION 24h
				SIGNUP ( CREATE user SET user = $user, pass = crypto::argon2::generate($pass) )
				SIGNIN ( SELECT * FROM user WHERE user = $user AND crypto::argon2::compare(pass, $pass) )
			;
			"#,
		)
		.await?;
	// Create a user record
	socket
		.send_message_query(
			FORMAT,
			r#"
			CREATE user CONTENT {
				user: 'user',
				pass: crypto::argon2::generate('pass')
			};
			"#,
		)
		.await?;
	// Sign in as scope user
	socket.send_message_signin(FORMAT, "user", "pass", Some(NS), Some(DB), Some("scope")).await?;
	// Send INFO command
	let res = socket
		.send_and_receive_message(
			FORMAT,
			json!({
				"id": Ulid::new(),
				"method": "info",
			}),
		)
		.await?;
	assert!(res["result"].is_object(), "result: {:?}", res);
	let res = res["result"].as_object().unwrap();
	assert_eq!(res["user"], "user", "result: {:?}", res);
	// Test passed
	Ok(())
}

#[test(tokio::test)]
async fn signup() -> Result<(), Box<dyn std::error::Error>> {
	// Setup database server
	let (addr, _server) = common::start_server_with_defaults().await.unwrap();
	// Connect to WebSocket
	let mut socket = Socket::connect(&addr, SERVER).await?;
	// Authenticate the connection
	socket.send_message_signin(FORMAT, USER, PASS, None, None, None).await?;
	// Specify a namespace and database
	socket.send_message_use(FORMAT, Some(NS), Some(DB)).await?;
	// Setup the scope
	socket
		.send_message_query(
			FORMAT,
			r#"
			DEFINE SCOPE scope SESSION 24h
				SIGNUP ( CREATE user SET email = $email, pass = crypto::argon2::generate($pass) )
				SIGNIN ( SELECT * FROM user WHERE email = $email AND crypto::argon2::compare(pass, $pass) )
			;"#,
		)
		.await?;
	// Send SIGNUP command
	let res = socket
		.send_and_receive_message(
			FORMAT,
			json!({
				"id": Ulid::new(),
				"method": "signup",
				"params": [{
					"ns": NS,
					"db": DB,
					"sc": "scope",
					"email": "email@email.com",
					"pass": "pass",
				}],
			}),
		)
		.await;
	assert!(res.is_ok(), "result: {:?}", res);
	let res = res.unwrap();
	assert!(res.is_object(), "result: {:?}", res);
	let res = res.as_object().unwrap();
	// Verify response contains no error
	assert!(res.keys().all(|k| ["id", "result"].contains(&k.as_str())), "result: {:?}", res);
	// Verify it returns a token
	assert!(res["result"].is_string(), "result: {:?}", res);
	let res = res["result"].as_str().unwrap();
	assert!(res.starts_with("eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9"), "result: {}", res);
	// Test passed
	Ok(())
}

#[test(tokio::test)]
async fn signin() -> Result<(), Box<dyn std::error::Error>> {
	// Setup database server
	let (addr, _server) = common::start_server_with_defaults().await.unwrap();
	// Connect to WebSocket
	let mut socket = Socket::connect(&addr, SERVER).await?;
	// Authenticate the connection
	socket.send_message_signin(FORMAT, USER, PASS, None, None, None).await?;
	// Specify a namespace and database
	socket.send_message_use(FORMAT, Some(NS), Some(DB)).await?;
	// Setup the scope
	socket
		.send_message_query(
			FORMAT,
			r#"
			DEFINE SCOPE scope SESSION 24h
				SIGNUP ( CREATE user SET email = $email, pass = crypto::argon2::generate($pass) )
				SIGNIN ( SELECT * FROM user WHERE email = $email AND crypto::argon2::compare(pass, $pass) )
			;"#,
		)
		.await?;
	// Send SIGNUP command
	let res = socket
		.send_and_receive_message(
			FORMAT,
			json!({
				"id": Ulid::new(),
				"method": "signup",
				"params": [{
					"ns": NS,
					"db": DB,
					"sc": "scope",
					"email": "email@email.com",
					"pass": "pass",
				}],
			}),
		)
		.await;
	assert!(res.is_ok(), "result: {:?}", res);
	let res = res.unwrap();
	assert!(res.is_object(), "result: {:?}", res);
	let res = res.as_object().unwrap();
	// Verify response contains no error
	assert!(res.keys().all(|k| ["id", "result"].contains(&k.as_str())), "result: {:?}", res);
	// Verify it returns a token
	assert!(res["result"].is_string(), "result: {:?}", res);
	let res = res["result"].as_str().unwrap();
	assert!(res.starts_with("eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9"), "result: {}", res);
	// Send SIGNIN command
	let res = socket
		.send_and_receive_message(
			FORMAT,
			json!({
				"id": Ulid::new(),
				"method": "signin",
				"params": [{
					"ns": NS,
					"db": DB,
					"sc": "scope",
					"email": "email@email.com",
					"pass": "pass",
				}],
			}),
		)
		.await;
	assert!(res.is_ok(), "result: {:?}", res);
	let res = res.unwrap();
	assert!(res.is_object(), "result: {:?}", res);
	let res = res.as_object().unwrap();
	// Verify response contains no error
	assert!(res.keys().all(|k| ["id", "result"].contains(&k.as_str())), "result: {:?}", res);
	// Verify it returns a token
	assert!(res["result"].is_string(), "result: {:?}", res);
	let res = res["result"].as_str().unwrap();
	assert!(res.starts_with("eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9"), "result: {}", res);
	// Test passed
	Ok(())
}

#[test(tokio::test)]
async fn invalidate() -> Result<(), Box<dyn std::error::Error>> {
	// Setup database server
	let (addr, _server) = common::start_server_with_defaults().await.unwrap();
	// Connect to WebSocket
	let mut socket = Socket::connect(&addr, SERVER).await?;
	// Authenticate the connection
	socket.send_message_signin(FORMAT, USER, PASS, None, None, None).await?;
	// Specify a namespace and database
	socket.send_message_use(FORMAT, Some(NS), Some(DB)).await?;
	// Verify we have an authenticated session
	let res = socket.send_message_query(FORMAT, "DEFINE NAMESPACE test").await?;
	assert_eq!(res[0]["status"], "OK", "result: {:?}", res);
	// Send INVALIDATE command
	socket
		.send_and_receive_message(
			FORMAT,
			json!({
				"id": Ulid::new(),
				"method": "invalidate",
			}),
		)
		.await?;
	// Verify we have an invalidated session
	let res = socket.send_message_query(FORMAT, "DEFINE NAMESPACE test").await?;
	assert_eq!(res[0]["status"], "ERR", "result: {:?}", res);
	assert_eq!(
		res[0]["result"], "IAM error: Not enough permissions to perform this action",
		"result: {:?}",
		res
	);
	// Test passed
	Ok(())
}

#[test(tokio::test)]
async fn authenticate() -> Result<(), Box<dyn std::error::Error>> {
	// Setup database server
	let (addr, _server) = common::start_server_with_defaults().await.unwrap();
	// Connect to WebSocket
	let mut socket = Socket::connect(&addr, SERVER).await?;
	// Authenticate the connection
	let token = socket.send_message_signin(FORMAT, USER, PASS, None, None, None).await?;
	// Disconnect the connection
	socket.close().await?;
	// Connect to WebSocket
	let mut socket = Socket::connect(&addr, SERVER).await?;
	// Send AUTHENTICATE command
	socket
		.send_and_receive_message(
			FORMAT,
			json!({
				"id": Ulid::new(),
				"method": "authenticate",
				"params": [
					token,
				],
			}),
		)
		.await?;
	// Verify we have an authenticated session
	let res = socket.send_message_query(FORMAT, "DEFINE NAMESPACE test").await?;
	assert_eq!(res[0]["status"], "OK", "result: {:?}", res);
	// Test passed
	Ok(())
}

#[test(tokio::test)]
async fn letset() -> Result<(), Box<dyn std::error::Error>> {
	// Setup database server
	let (addr, _server) = common::start_server_with_defaults().await.unwrap();
	// Connect to WebSocket
	let mut socket = Socket::connect(&addr, SERVER).await?;
	// Authenticate the connection
	socket.send_message_signin(FORMAT, USER, PASS, None, None, None).await?;
	// Specify a namespace and database
	socket.send_message_use(FORMAT, Some(NS), Some(DB)).await?;
	// Send LET command
	socket
		.send_and_receive_message(
			FORMAT,
			json!({
				"id": Ulid::new(),
				"method": "let",
				"params": [
					"let_var", "let_value",
				],
			}),
		)
		.await?;
	// Send SET command
	socket
		.send_and_receive_message(
			FORMAT,
			json!({
				"id": Ulid::new(),
				"method": "set",
				"params": [
					"set_var", "set_value",
				],
			}),
		)
		.await?;
	// Verify the variables are set
	let res = socket.send_message_query(FORMAT, "SELECT * FROM $let_var, $set_var").await?;
	assert_eq!(res[0]["result"], json!(["let_value", "set_value"]), "result: {:?}", res);
	Ok(())
}

#[test(tokio::test)]
async fn unset() -> Result<(), Box<dyn std::error::Error>> {
	// Setup database server
	let (addr, _server) = common::start_server_with_defaults().await.unwrap();
	// Connect to WebSocket
	let mut socket = Socket::connect(&addr, SERVER).await?;
	// Authenticate the connection
	socket.send_message_signin(FORMAT, USER, PASS, None, None, None).await?;
	// Specify a namespace and database
	socket.send_message_use(FORMAT, Some(NS), Some(DB)).await?;
	// Send LET command
	socket
		.send_and_receive_message(
			FORMAT,
			json!({
				"id": Ulid::new(),
				"method": "let",
				"params": [
					"let_var", "let_value",
				],
			}),
		)
		.await?;
	// Verify the variable is set
	let res = socket.send_message_query(FORMAT, "SELECT * FROM $let_var").await?;
	assert_eq!(res[0]["result"], json!(["let_value"]), "result: {:?}", res);
	// Send UNSET command
	socket
		.send_and_receive_message(
			FORMAT,
			json!({
				"id": Ulid::new(),
				"method": "unset",
				"params": [
					"let_var",
				],
			}),
		)
		.await?;
	// Verify the variable is unset
	let res = socket.send_message_query(FORMAT, "SELECT * FROM $let_var").await?;
	assert_eq!(res[0]["result"], json!([null]), "result: {:?}", res);
	// Test passed
	Ok(())
}

#[test(tokio::test)]
async fn select() -> Result<(), Box<dyn std::error::Error>> {
	// Setup database server
	let (addr, _server) = common::start_server_with_defaults().await.unwrap();
	// Connect to WebSocket
	let mut socket = Socket::connect(&addr, SERVER).await?;
	// Authenticate the connection
	socket.send_message_signin(FORMAT, USER, PASS, None, None, None).await?;
	// Specify a namespace and database
	socket.send_message_use(FORMAT, Some(NS), Some(DB)).await?;
	// Create a test record
	socket.send_message_query(FORMAT, "CREATE tester SET name = 'foo', value = 'bar'").await?;
	// Send SELECT command
	let res = socket
		.send_and_receive_message(
			FORMAT,
			json!({
				"id": Ulid::new(),
				"method": "select",
				"params": [
					"tester",
				],
			}),
		)
		.await?;
	assert!(res.is_object(), "result: {:?}", res);
	assert!(res["result"].is_array(), "result: {:?}", res);
	let res = res["result"].as_array().unwrap();
	assert_eq!(res.len(), 1, "result: {:?}", res);
	assert_eq!(res[0]["name"], "foo", "result: {:?}", res);
	assert_eq!(res[0]["value"], "bar", "result: {:?}", res);
	// Test passed
	Ok(())
}

#[test(tokio::test)]
async fn insert() -> Result<(), Box<dyn std::error::Error>> {
	// Setup database server
	let (addr, _server) = common::start_server_with_defaults().await.unwrap();
	// Connect to WebSocket
	let mut socket = Socket::connect(&addr, SERVER).await?;
	// Authenticate the connection
	socket.send_message_signin(FORMAT, USER, PASS, None, None, None).await?;
	// Specify a namespace and database
	socket.send_message_use(FORMAT, Some(NS), Some(DB)).await?;
	// Send INSERT command
	let res = socket
		.send_and_receive_message(
			FORMAT,
			json!({
				"id": Ulid::new(),
				"method": "insert",
				"params": [
					"tester",
					{
						"name": "foo",
						"value": "bar",
					}
				],
			}),
		)
		.await?;
	assert!(res.is_object(), "result: {:?}", res);
	assert!(res["result"].is_array(), "result: {:?}", res);
	let res = res["result"].as_array().unwrap();
	assert_eq!(res.len(), 1, "result: {:?}", res);
	assert_eq!(res[0]["name"], "foo", "result: {:?}", res);
	assert_eq!(res[0]["value"], "bar", "result: {:?}", res);
	// Verify the data was inserted and can be queried
	let res = socket.send_message_query(FORMAT, "SELECT * FROM tester").await?;
	assert!(res[0]["result"].is_array(), "result: {:?}", res);
	let res = res[0]["result"].as_array().unwrap();
	assert_eq!(res.len(), 1, "result: {:?}", res);
	assert_eq!(res[0]["name"], "foo", "result: {:?}", res);
	assert_eq!(res[0]["value"], "bar", "result: {:?}", res);
	// Test passed
	Ok(())
}

#[test(tokio::test)]
async fn create() -> Result<(), Box<dyn std::error::Error>> {
	// Setup database server
	let (addr, _server) = common::start_server_with_defaults().await.unwrap();
	// Connect to WebSocket
	let mut socket = Socket::connect(&addr, SERVER).await?;
	// Authenticate the connection
	socket.send_message_signin(FORMAT, USER, PASS, None, None, None).await?;
	// Specify a namespace and database
	socket.send_message_use(FORMAT, Some(NS), Some(DB)).await?;
	// Send CREATE command
	let res = socket
		.send_and_receive_message(
			FORMAT,
			json!({
				"id": Ulid::new(),
				"method": "create",
				"params": [
					"tester",
					{
						"value": "bar",
					}
				],
			}),
		)
		.await?;
	assert!(res.is_object(), "result: {:?}", res);
	assert!(res["result"].is_array(), "result: {:?}", res);
	let res = res["result"].as_array().unwrap();
	assert_eq!(res.len(), 1, "result: {:?}", res);
	assert_eq!(res[0]["value"], "bar", "result: {:?}", res);
	// Verify the data was created
	let res = socket.send_message_query(FORMAT, "SELECT * FROM tester").await?;
	assert!(res[0]["result"].is_array(), "result: {:?}", res);
	let res = res[0]["result"].as_array().unwrap();
	assert_eq!(res.len(), 1, "result: {:?}", res);
	assert_eq!(res[0]["value"], "bar", "result: {:?}", res);
	// Test passed
	Ok(())
}

#[test(tokio::test)]
async fn update() -> Result<(), Box<dyn std::error::Error>> {
	// Setup database server
	let (addr, _server) = common::start_server_with_defaults().await.unwrap();
	// Connect to WebSocket
	let mut socket = Socket::connect(&addr, SERVER).await?;
	// Authenticate the connection
	socket.send_message_signin(FORMAT, USER, PASS, None, None, None).await?;
	// Specify a namespace and database
	socket.send_message_use(FORMAT, Some(NS), Some(DB)).await?;
	// Create a test record
	socket.send_message_query(FORMAT, "CREATE tester SET name = 'foo'").await?;
	// Send UPDATE command
	let res = socket
		.send_and_receive_message(
			FORMAT,
			json!({
				"id": Ulid::new(),
				"method": "update",
				"params": [
					"tester",
					{
						"value": "bar",
					}
				],
			}),
		)
		.await?;
	assert!(res.is_object(), "result: {:?}", res);
	assert!(res["result"].is_array(), "result: {:?}", res);
	let res = res["result"].as_array().unwrap();
	assert_eq!(res.len(), 1, "result: {:?}", res);
	assert_eq!(res[0]["value"], "bar", "result: {:?}", res);
	// Verify the data was updated
	let res = socket.send_message_query(FORMAT, "SELECT * FROM tester").await?;
	assert!(res[0]["result"].is_array(), "result: {:?}", res);
	let res = res[0]["result"].as_array().unwrap();
	assert_eq!(res.len(), 1, "result: {:?}", res);
	assert_eq!(res[0]["name"], json!(null), "result: {:?}", res);
	assert_eq!(res[0]["value"], "bar", "result: {:?}", res);
	// Test passed
	Ok(())
}

#[test(tokio::test)]
async fn merge() -> Result<(), Box<dyn std::error::Error>> {
	// Setup database server
	let (addr, _server) = common::start_server_with_defaults().await.unwrap();
	// Connect to WebSocket
	let mut socket = Socket::connect(&addr, SERVER).await?;
	// Authenticate the connection
	socket.send_message_signin(FORMAT, USER, PASS, None, None, None).await?;
	// Specify a namespace and database
	socket.send_message_use(FORMAT, Some(NS), Some(DB)).await?;
	// Create a test record
	socket.send_message_query(FORMAT, "CREATE tester SET name = 'foo'").await?;
	// Send UPDATE command
	let res = socket
		.send_and_receive_message(
			FORMAT,
			json!({
				"id": Ulid::new(),
				"method": "merge",
				"params": [
					"tester",
					{
						"value": "bar",
					}
				],
			}),
		)
		.await?;
	assert!(res.is_object(), "result: {:?}", res);
	assert!(res["result"].is_array(), "result: {:?}", res);
	let res = res["result"].as_array().unwrap();
	assert_eq!(res.len(), 1, "result: {:?}", res);
	assert_eq!(res[0]["name"], "foo", "result: {:?}", res);
	assert_eq!(res[0]["value"], "bar", "result: {:?}", res);
	// Verify the data was merged
	let res = socket.send_message_query(FORMAT, "SELECT * FROM tester").await?;
	assert!(res[0]["result"].is_array(), "result: {:?}", res);
	let res = res[0]["result"].as_array().unwrap();
	assert_eq!(res.len(), 1, "result: {:?}", res);
	assert_eq!(res[0]["name"], "foo", "result: {:?}", res);
	assert_eq!(res[0]["value"], "bar", "result: {:?}", res);
	// Test passed
	Ok(())
}

#[test(tokio::test)]
async fn patch() -> Result<(), Box<dyn std::error::Error>> {
	// Setup database server
	let (addr, _server) = common::start_server_with_defaults().await.unwrap();
	// Connect to WebSocket
	let mut socket = Socket::connect(&addr, SERVER).await?;
	// Authenticate the connection
	socket.send_message_signin(FORMAT, USER, PASS, None, None, None).await?;
	// Specify a namespace and database
	socket.send_message_use(FORMAT, Some(NS), Some(DB)).await?;
	// Create a test record
	socket.send_message_query(FORMAT, "CREATE tester:id SET name = 'foo'").await?;
	// Send PATCH command
	let res = socket
		.send_and_receive_message(
			FORMAT,
			json!({
				"id": Ulid::new(),
				"method": "patch",
				"params": [
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
				]
			}),
		)
		.await?;
	assert!(res["result"].is_object(), "result: {:?}", res);
	let res = res["result"].as_object().unwrap();
	assert_eq!(res.get("value"), Some(json!("bar")).as_ref(), "result: {:?}", res);
	// Verify the data was patched
	let res = socket.send_message_query(FORMAT, "SELECT * FROM tester").await?;
	assert!(res[0]["result"].is_array(), "result: {:?}", res);
	let res = res[0]["result"].as_array().unwrap();
	assert_eq!(res.len(), 1, "result: {:?}", res);
	assert_eq!(res[0]["name"], json!(null), "result: {:?}", res);
	assert_eq!(res[0]["value"], "bar", "result: {:?}", res);
	// Test passed
	Ok(())
}

#[test(tokio::test)]
async fn delete() -> Result<(), Box<dyn std::error::Error>> {
	// Setup database server
	let (addr, _server) = common::start_server_with_defaults().await.unwrap();
	// Connect to WebSocket
	let mut socket = Socket::connect(&addr, SERVER).await?;
	// Authenticate the connection
	socket.send_message_signin(FORMAT, USER, PASS, None, None, None).await?;
	// Specify a namespace and database
	socket.send_message_use(FORMAT, Some(NS), Some(DB)).await?;
	// Create a test record
	socket.send_message_query(FORMAT, "CREATE tester:id").await?;
	// Send DELETE command
	let res = socket
		.send_and_receive_message(
			FORMAT,
			json!({
				"id": Ulid::new(),
				"method": "delete",
				"params": [
					"tester"
				]
			}),
		)
		.await?;
	assert!(res.is_object(), "result: {:?}", res);
	assert!(res["result"].is_array(), "result: {:?}", res);
	let res = res["result"].as_array().unwrap();
	assert_eq!(res.len(), 1, "result: {:?}", res);
	assert_eq!(res[0]["id"], "tester:id", "result: {:?}", res);
	// Create a test record
	socket.send_message_query(FORMAT, "CREATE tester:id").await?;
	// Send DELETE command
	let res = socket
		.send_and_receive_message(
			FORMAT,
			json!({
				"id": Ulid::new(),
				"method": "delete",
				"params": [
					"tester:id"
				]
			}),
		)
		.await?;
	assert!(res.is_object(), "result: {:?}", res);
	assert!(res["result"].is_object(), "result: {:?}", res);
	let res = res["result"].as_object().unwrap();
	assert_eq!(res["id"], "tester:id", "result: {:?}", res);
	// Verify the data was merged
	let res = socket.send_message_query(FORMAT, "SELECT * FROM tester").await?;
	assert!(res[0]["result"].is_array(), "result: {:?}", res);
	let res = res[0]["result"].as_array().unwrap();
	assert_eq!(res.len(), 0, "result: {:?}", res);
	// Test passed
	Ok(())
}

#[test(tokio::test)]
async fn query() -> Result<(), Box<dyn std::error::Error>> {
	// Setup database server
	let (addr, _server) = common::start_server_with_defaults().await.unwrap();
	// Connect to WebSocket
	let mut socket = Socket::connect(&addr, SERVER).await?;
	// Authenticate the connection
	socket.send_message_signin(FORMAT, USER, PASS, None, None, None).await?;
	// Specify a namespace and database
	socket.send_message_use(FORMAT, Some(NS), Some(DB)).await?;
	// Send QUERY command
	let res = socket
		.send_and_receive_message(
			FORMAT,
			json!({
				"id": Ulid::new(),
				"method": "query",
				"params": [
					"CREATE tester; SELECT * FROM tester;",
				]
			}),
		)
		.await?;
	assert!(res.is_object(), "result: {:?}", res);
	assert!(res["result"].is_array(), "result: {:?}", res);
	let res = res["result"].as_array().unwrap();
	assert_eq!(res.len(), 2, "result: {:?}", res);
	// Verify the data was created
	let res = socket.send_message_query(FORMAT, "SELECT * FROM tester").await?;
	assert!(res[0]["result"].is_array(), "result: {:?}", res);
	let res = res[0]["result"].as_array().unwrap();
	assert_eq!(res.len(), 1, "result: {:?}", res);
	// Test passed
	Ok(())
}

#[test(tokio::test)]
async fn version() -> Result<(), Box<dyn std::error::Error>> {
	// Setup database server
	let (addr, _server) = common::start_server_with_defaults().await.unwrap();
	// Connect to WebSocket
	let mut socket = Socket::connect(&addr, SERVER).await?;
	// Send version command
	let res = socket
		.send_and_receive_message(
			FORMAT,
			json!({
				"id": Ulid::new(),
				"method": "version",
			}),
		)
		.await?;
	assert!(res["result"].is_string(), "result: {:?}", res);
	let res = res["result"].as_str().unwrap();
	assert!(res.starts_with("surrealdb-"), "result: {}", res);
	// Test passed
	Ok(())
}

// Validate that the WebSocket is able to process multiple queries concurrently
#[test(tokio::test)]
async fn concurrency() -> Result<(), Box<dyn std::error::Error>> {
	// Setup database server
	let (addr, _server) = common::start_server_with_defaults().await.unwrap();
	// Connect to WebSocket
	let mut socket = Socket::connect(&addr, SERVER).await?;
	// Authenticate the connection
	socket.send_message_signin(FORMAT, USER, PASS, None, None, None).await?;
	// Specify a namespace and database
	socket.send_message_use(FORMAT, Some(NS), Some(DB)).await?;
	// Send 5 long-running queries and verify they run concurrently
	for i in 0..5 {
		socket
			.send_message(
				FORMAT,
				json!({
					"id": Ulid::new(),
					"method": "query",
					"params": [format!("SLEEP 3s; RETURN {i};")],
				}),
			)
			.await?;
	}
	// Verify the queries all completed concurrently within 5 seconds
	let res = socket.receive_all_messages(FORMAT, 5, Duration::from_secs(5)).await?;
	assert!(res.iter().all(|v| v["error"].is_null()), "Unexpected error received: {:#?}", res);
	// Test passed
	Ok(())
}

#[test(tokio::test)]
async fn live() -> Result<(), Box<dyn std::error::Error>> {
	// Setup database server
	let (addr, _server) = common::start_server_with_defaults().await.unwrap();
	// Connect to WebSocket
	let mut socket = Socket::connect(&addr, SERVER).await?;
	// Authenticate the connection
	socket.send_message_signin(FORMAT, USER, PASS, None, None, None).await?;
	// Specify a namespace and database
	socket.send_message_use(FORMAT, Some(NS), Some(DB)).await?;
	// Send LIVE command
	let res = socket
		.send_and_receive_message(
			FORMAT,
			json!({
				"id": Ulid::new(),
				"method": "live",
				"params": ["tester"],
			}),
		)
		.await?;
	assert!(res.is_object(), "result: {:?}", res);
	assert!(res["result"].is_string(), "result: {:?}", res);
	let live1 = res["result"].as_str().unwrap();
	// Send QUERY command
	let res = socket
		.send_and_receive_message(
			FORMAT,
			json!({
				"id": Ulid::new(),
				"method": "query",
				"params": ["LIVE SELECT * FROM tester"],
			}),
		)
		.await?;
	assert!(res.is_object(), "result: {:?}", res);
	assert!(res["result"].is_array(), "result: {:?}", res);
	let res = res["result"].as_array().unwrap();
	assert_eq!(res.len(), 1, "result: {:?}", res);
	assert!(res[0]["result"].is_string(), "result: {:?}", res);
	let live2 = res[0]["result"].as_str().unwrap();
	// Create a new test record
	let res = socket
		.send_and_receive_message(
			FORMAT,
			json!({
				"id": Ulid::new(),
				"method": "query",
				"params": ["CREATE tester:id SET name = 'foo'"],
			}),
		)
		.await?;
	assert!(res.is_object(), "result: {:?}", res);
	assert!(res["result"].is_array(), "result: {:?}", res);
	let res = res["result"].as_array().unwrap();
	assert_eq!(res.len(), 1, "result: {:?}", res);
	// Wait some time for all messages to arrive, and then search for the notification message
	let msgs = socket.receive_all_messages(FORMAT, 2, Duration::from_secs(1)).await?;
	assert!(msgs.iter().all(|v| v["error"].is_null()), "Unexpected error received: {:?}", msgs);
	// Check for first live query notifcation
	let res = msgs.iter().find(|v| common::is_notification_from_lq(v, live1));
	assert!(res.is_some(), "Expected to find a notification for LQ id {}: {:?}", live1, msgs);
	let res = res.unwrap();
	assert!(res.is_object(), "result: {:?}", res);
	let res = res.as_object().unwrap();
	assert!(res["result"].is_object(), "result: {:?}", res);
	let res = res["result"].as_object().unwrap();
	assert!(res["action"].is_string(), "result: {:?}", res);
	assert_eq!(res["action"], "CREATE", "result: {:?}", res);
	assert!(res["result"].is_object(), "result: {:?}", res);
	let res = res["result"].as_object().unwrap();
	assert_eq!(res["id"], "tester:id", "result: {:?}", res);
	// Check for second live query notifcation
	let res = msgs.iter().find(|v| common::is_notification_from_lq(v, live2));
	assert!(res.is_some(), "Expected to find a notification for LQ id {}: {:?}", live2, msgs);
	let res = res.unwrap();
	assert!(res.is_object(), "result: {:?}", res);
	let res = res.as_object().unwrap();
	assert!(res["result"].is_object(), "result: {:?}", res);
	let res = res["result"].as_object().unwrap();
	assert_eq!(res["action"], "CREATE", "result: {:?}", res);
	assert!(res["result"].is_object(), "result: {:?}", res);
	let res = res["result"].as_object().unwrap();
	assert_eq!(res["id"], "tester:id", "result: {:?}", res);
	// Test passed
	Ok(())
}

#[test(tokio::test)]
async fn kill() -> Result<(), Box<dyn std::error::Error>> {
	// Setup database server
	let (addr, _server) = common::start_server_with_defaults().await.unwrap();
	// Connect to WebSocket
	let mut socket = Socket::connect(&addr, SERVER).await?;
	// Authenticate the connection
	socket.send_message_signin(FORMAT, USER, PASS, None, None, None).await?;
	// Specify a namespace and database
	socket.send_message_use(FORMAT, Some(NS), Some(DB)).await?;
	// Send LIVE command
	let res = socket
		.send_and_receive_message(
			FORMAT,
			json!({
				"id": Ulid::new(),
				"method": "live",
				"params": ["tester"],
			}),
		)
		.await?;
	assert!(res.is_object(), "result: {:?}", res);
	assert!(res["result"].is_string(), "result: {:?}", res);
	let live1 = res["result"].as_str().unwrap();
	// Send QUERY command
	let res = socket
		.send_and_receive_message(
			FORMAT,
			json!({
				"id": Ulid::new(),
				"method": "query",
				"params": ["LIVE SELECT * FROM tester"],
			}),
		)
		.await?;
	assert!(res.is_object(), "result: {:?}", res);
	assert!(res["result"].is_array(), "result: {:?}", res);
	let res = res["result"].as_array().unwrap();
	assert_eq!(res.len(), 1, "result: {:?}", res);
	assert!(res[0]["result"].is_string(), "result: {:?}", res);
	let live2 = res[0]["result"].as_str().unwrap();
	// Create a new test record
	let res = socket
		.send_and_receive_message(
			FORMAT,
			json!({
				"id": Ulid::new(),
				"method": "query",
				"params": ["CREATE tester:one SET name = 'one'"],
			}),
		)
		.await?;
	assert!(res.is_object(), "result: {:?}", res);
	assert!(res["result"].is_array(), "result: {:?}", res);
	let res = res["result"].as_array().unwrap();
	assert_eq!(res.len(), 1, "result: {:?}", res);
	// Wait some time for all messages to arrive, and then search for the notification message
	let msgs = socket.receive_all_messages(FORMAT, 2, Duration::from_secs(1)).await?;
	assert!(msgs.iter().all(|v| v["error"].is_null()), "Unexpected error received: {:?}", msgs);
	// Check for first live query notifcation
	let res = msgs.iter().find(|v| common::is_notification_from_lq(v, live1));
	assert!(res.is_some(), "Expected to find a notification for LQ id {}: {:?}", live1, msgs);
	let res = res.unwrap();
	assert!(res.is_object(), "result: {:?}", res);
	let res = res.as_object().unwrap();
	assert!(res["result"].is_object(), "result: {:?}", res);
	let res = res["result"].as_object().unwrap();
	assert!(res["action"].is_string(), "result: {:?}", res);
	assert_eq!(res["action"], "CREATE", "result: {:?}", res);
	assert!(res["result"].is_object(), "result: {:?}", res);
	let res = res["result"].as_object().unwrap();
	assert_eq!(res["id"], "tester:one", "result: {:?}", res);
	// Check for second live query notifcation
	let res = msgs.iter().find(|v| common::is_notification_from_lq(v, live2));
	assert!(res.is_some(), "Expected to find a notification for LQ id {}: {:?}", live2, msgs);
	let res = res.unwrap();
	assert!(res.is_object(), "result: {:?}", res);
	let res = res.as_object().unwrap();
	assert!(res["result"].is_object(), "result: {:?}", res);
	let res = res["result"].as_object().unwrap();
	assert_eq!(res["action"], "CREATE", "result: {:?}", res);
	assert!(res["result"].is_object(), "result: {:?}", res);
	let res = res["result"].as_object().unwrap();
	assert_eq!(res["id"], "tester:one", "result: {:?}", res);
	// Send KILL command
	let res = socket
		.send_and_receive_message(
			FORMAT,
			json!({
				"id": Ulid::new(),
				"method": "kill",
				"params": [live1],
			}),
		)
		.await?;
	assert!(res.is_object(), "result: {:?}", res);
	assert!(res["result"].is_null(), "result: {:?}", res);
	// Create a new test record
	let res = socket
		.send_and_receive_message(
			FORMAT,
			json!({
				"id": Ulid::new(),
				"method": "query",
				"params": ["CREATE tester:two SET name = 'two'"],
			}),
		)
		.await?;
	assert!(res.is_object(), "result: {:?}", res);
	assert!(res["result"].is_array(), "result: {:?}", res);
	let res = res["result"].as_array().unwrap();
	assert_eq!(res.len(), 1, "result: {:?}", res);
	// Wait some time for all messages to arrive, and then search for the notification message
	let msgs = socket.receive_all_messages(FORMAT, 1, Duration::from_secs(1)).await?;
	assert!(msgs.iter().all(|v| v["error"].is_null()), "Unexpected error received: {:?}", msgs);
	// Check for second live query notifcation
	let res = msgs.iter().find(|v| common::is_notification_from_lq(v, live2));
	assert!(res.is_some(), "Expected to find a notification for LQ id {}: {:?}", live2, msgs);
	let res = res.unwrap();
	assert!(res.is_object(), "result: {:?}", res);
	let res = res.as_object().unwrap();
	assert!(res["result"].is_object(), "result: {:?}", res);
	let res = res["result"].as_object().unwrap();
	assert_eq!(res["action"], "CREATE", "result: {:?}", res);
	assert!(res["result"].is_object(), "result: {:?}", res);
	let res = res["result"].as_object().unwrap();
	assert_eq!(res["id"], "tester:two", "result: {:?}", res);
	// Send QUERY command
	let res = socket
		.send_and_receive_message(
			FORMAT,
			json!({
				"id": Ulid::new(),
				"method": "query",
				"params": [format!("KILL '{live2}'")],
			}),
		)
		.await?;
	assert!(res.is_object(), "result: {:?}", res);
	assert!(res["result"].is_array(), "result: {:?}", res);
	let res = res["result"].as_array().unwrap();
	assert_eq!(res.len(), 1, "result: {:?}", res);
	assert!(res[0]["result"].is_null(), "result: {:?}", res);
	// Create a new test record
	let res = socket
		.send_and_receive_message(
			FORMAT,
			json!({
				"id": Ulid::new(),
				"method": "query",
				"params": ["CREATE tester:tre SET name = 'two'"],
			}),
		)
		.await?;
	assert!(res.is_object(), "result: {:?}", res);
	assert!(res["result"].is_array(), "result: {:?}", res);
	let res = res["result"].as_array().unwrap();
	assert_eq!(res.len(), 1, "result: {:?}", res);
	// Wait some time for all messages to arrive, and then search for the notification message
	let msgs = socket.receive_all_messages(FORMAT, 0, Duration::from_secs(1)).await?;
	assert!(msgs.iter().all(|v| v["error"].is_null()), "Unexpected error received: {:?}", msgs);
	// Test passed
	Ok(())
}

#[test(tokio::test)]
async fn live_second_connection() -> Result<(), Box<dyn std::error::Error>> {
	// Setup database server
	let (addr, _server) = common::start_server_with_defaults().await.unwrap();
	// Connect to WebSocket
	let mut socket1 = Socket::connect(&addr, SERVER).await?;
	// Authenticate the connection
	socket1.send_message_signin(FORMAT, USER, PASS, None, None, None).await?;
	// Specify a namespace and database
	socket1.send_message_use(FORMAT, Some(NS), Some(DB)).await?;
	// Send LIVE command
	let res = socket1
		.send_and_receive_message(
			FORMAT,
			json!({
				"id": Ulid::new(),
				"method": "live",
				"params": ["tester"],
			}),
		)
		.await?;
	assert!(res.is_object(), "result: {:?}", res);
	assert!(res["result"].is_string(), "result: {:?}", res);
	let liveid = res["result"].as_str().unwrap();
	// Connect to WebSocket
	let mut socket2 = Socket::connect(&addr, SERVER).await?;
	// Authenticate the connection
	socket2.send_message_signin(FORMAT, USER, PASS, None, None, None).await?;
	// Specify a namespace and database
	socket2.send_message_use(FORMAT, Some(NS), Some(DB)).await?;
	// Create a new test record
	let res = socket2
		.send_and_receive_message(
			FORMAT,
			json!({
				"id": Ulid::new(),
				"method": "query",
				"params": ["CREATE tester:id SET name = 'foo'"],
			}),
		)
		.await?;
	assert!(res.is_object(), "result: {:?}", res);
	assert!(res["result"].is_array(), "result: {:?}", res);
	let res = res["result"].as_array().unwrap();
	assert_eq!(res.len(), 1, "result: {:?}", res);
	// Wait some time for all messages to arrive, and then search for the notification message
	let msgs = socket1.receive_all_messages(FORMAT, 1, Duration::from_secs(1)).await?;
	assert!(msgs.iter().all(|v| v["error"].is_null()), "Unexpected error received: {:?}", msgs);
	// Check for live query notifcation
	let res = msgs.iter().find(|v| common::is_notification_from_lq(v, liveid));
	assert!(res.is_some(), "Expected to find a notification for LQ id {}: {:?}", liveid, msgs);
	let res = res.unwrap();
	assert!(res.is_object(), "result: {:?}", res);
	let res = res.as_object().unwrap();
	assert!(res["result"].is_object(), "result: {:?}", res);
	let res = res["result"].as_object().unwrap();
	assert!(res["action"].is_string(), "result: {:?}", res);
	assert_eq!(res["action"], "CREATE", "result: {:?}", res);
	assert!(res["result"].is_object(), "result: {:?}", res);
	let res = res["result"].as_object().unwrap();
	assert_eq!(res["id"], "tester:id", "result: {:?}", res);
	// Test passed
	Ok(())
}

#[test(tokio::test)]
async fn variable_auth_live_query() -> Result<(), Box<dyn std::error::Error>> {
	// Setup database server
	let (addr, _server) = common::start_server_with_defaults().await.unwrap();
	// Connect to WebSocket
	let mut socket = Socket::connect(&addr, SERVER).await?;
	// Authenticate the connection
	socket.send_message_signin(FORMAT, USER, PASS, None, None, None).await?;
	// Specify a namespace and database
	socket.send_message_use(FORMAT, Some(NS), Some(DB)).await?;
	// Setup the scope
	socket
		.send_message_query(
			FORMAT,
			r#"
			DEFINE SCOPE scope SESSION 1s
				SIGNUP ( CREATE user SET email = $email, pass = crypto::argon2::generate($pass) )
				SIGNIN ( SELECT * FROM user WHERE email = $email AND crypto::argon2::compare(pass, $pass) )
			;"#,
		)
		.await?;
	// Send SIGNUP command
	let res = socket
		.send_and_receive_message(
			FORMAT,
			json!({
				"id": Ulid::new(),
				"method": "signup",
				"params": [{
					"ns": NS,
					"db": DB,
					"sc": "scope",
					"email": "email@email.com",
					"pass": "pass",
				}],
			}),
		)
		.await;
	assert!(res.is_ok(), "result: {:?}", res);
	let res = res.unwrap();
	assert!(res.is_object(), "result: {:?}", res);
	let res = res.as_object().unwrap();
	// Verify response contains no error
	assert!(res.keys().all(|k| ["id", "result"].contains(&k.as_str())), "result: {:?}", res);
	// Send LIVE command
	let res = socket
		.send_and_receive_message(
			FORMAT,
			json!({
				"id": Ulid::new(),
				"method": "live",
				"params": ["tester"],
			}),
		)
		.await?;
	assert!(res.is_object(), "result: {:?}", res);
	assert!(res["result"].is_string(), "result: {:?}", res);
	// Wait 2 seconds for auth to expire
	tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
	// Create a new test record
	let res = socket
		.send_and_receive_message(
			FORMAT,
			json!({
				"id": Ulid::new(),
				"method": "query",
				"params": ["CREATE tester:id SET name = 'foo'"],
			}),
		)
		.await?;
	assert!(res.is_object(), "result: {:?}", res);
	assert!(res["result"].is_array(), "result: {:?}", res);
	let res = res["result"].as_array().unwrap();
	assert_eq!(res.len(), 1, "result: {:?}", res);
	// Wait some time for all messages to arrive, and then search for the notification message
	let msgs = socket.receive_all_messages(FORMAT, 0, Duration::from_secs(1)).await?;
	assert!(msgs.iter().all(|v| v["error"].is_null()), "Unexpected error received: {:?}", msgs);
	// Test passed
	Ok(())
}
