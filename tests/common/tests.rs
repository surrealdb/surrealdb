use super::common::{self, Format, Socket, DB, NS, PASS, USER};
use assert_fs::TempDir;
use serde_json::json;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;
use test_log::test;

#[test(tokio::test)]
async fn ping() -> Result<(), Box<dyn std::error::Error>> {
	// Setup database server
	let (addr, mut server) = common::start_server_with_defaults().await.unwrap();
	// Connect to WebSocket
	let socket = Socket::connect(&addr, SERVER, FORMAT).await?;
	// Send INFO command
	let res = socket.send_request("ping", json!([])).await;
	assert!(res.is_ok(), "result: {:?}", res);
	let res = res.unwrap();
	assert!(res.is_object(), "result: {:?}", res);
	let res = res.as_object().unwrap();
	assert!(res.keys().all(|k| ["id", "result"].contains(&k.as_str())), "result: {:?}", res);
	// Test passed
	server.finish().unwrap();
	Ok(())
}

#[test(tokio::test)]
async fn info() -> Result<(), Box<dyn std::error::Error>> {
	// Setup database server
	let (addr, mut server) = common::start_server_with_defaults().await.unwrap();
	// Connect to WebSocket
	let mut socket = Socket::connect(&addr, SERVER, FORMAT).await?;
	// Authenticate the connection
	socket.send_message_signin(USER, PASS, None, None, None).await?;
	// Specify a namespace and database
	socket.send_message_use(Some(NS), Some(DB)).await?;
	// Define a user table
	socket.send_message_query("DEFINE TABLE user PERMISSIONS FULL").await?;
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
		.await?;
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
		.await?;
	// Sign in as record user
	socket.send_message_signin("user", "pass", Some(NS), Some(DB), Some("user")).await?;
	// Send INFO command
	let res = socket.send_request("info", json!([])).await?;
	assert!(res["result"].is_object(), "result: {:?}", res);
	let res = res["result"].as_object().unwrap();
	assert_eq!(res["user"], "user", "result: {:?}", res);
	// Test passed
	server.finish().unwrap();
	Ok(())
}

#[test(tokio::test)]
async fn signup() -> Result<(), Box<dyn std::error::Error>> {
	// Setup database server
	let (addr, mut server) = common::start_server_with_defaults().await.unwrap();
	// Connect to WebSocket
	let mut socket = Socket::connect(&addr, SERVER, FORMAT).await?;
	// Authenticate the connection
	socket.send_message_signin(USER, PASS, None, None, None).await?;
	// Specify a namespace and database
	socket.send_message_use(Some(NS), Some(DB)).await?;
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
		.await?;
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
	server.finish().unwrap();
	Ok(())
}

#[test(tokio::test)]
async fn signin() -> Result<(), Box<dyn std::error::Error>> {
	// Setup database server
	let (addr, mut server) = common::start_server_with_defaults().await.unwrap();
	// Connect to WebSocket
	let mut socket = Socket::connect(&addr, SERVER, FORMAT).await?;
	// Authenticate the connection
	socket.send_message_signin(USER, PASS, None, None, None).await?;
	// Specify a namespace and database
	socket.send_message_use(Some(NS), Some(DB)).await?;
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
		.await?;
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
	server.finish().unwrap();
	Ok(())
}

#[test(tokio::test)]
async fn invalidate() -> Result<(), Box<dyn std::error::Error>> {
	// Setup database server
	let (addr, mut server) = common::start_server_with_defaults().await.unwrap();
	// Connect to WebSocket
	let mut socket = Socket::connect(&addr, SERVER, FORMAT).await?;
	// Authenticate the connection
	socket.send_message_signin(USER, PASS, None, None, None).await?;
	// Specify a namespace and database
	socket.send_message_use(Some(NS), Some(DB)).await?;
	// Verify we have an authenticated session
	let res = socket.send_message_query("DEFINE NAMESPACE test").await?;
	assert_eq!(res[0]["status"], "OK", "result: {:?}", res);
	// Send INVALIDATE command
	socket.send_request("invalidate", json!([])).await?;
	// Verify we have an invalidated session
	let res = socket.send_message_query("DEFINE NAMESPACE test").await?;
	assert_eq!(res[0]["status"], "ERR", "result: {:?}", res);
	assert_eq!(
		res[0]["result"], "IAM error: Not enough permissions to perform this action",
		"result: {:?}",
		res
	);
	// Test passed
	server.finish().unwrap();
	Ok(())
}

#[test(tokio::test)]
async fn authenticate() -> Result<(), Box<dyn std::error::Error>> {
	// Setup database server
	let (addr, mut server) = common::start_server_with_defaults().await.unwrap();
	// Connect to WebSocket
	let mut socket = Socket::connect(&addr, SERVER, FORMAT).await?;
	// Authenticate the connection
	let token = socket.send_message_signin(USER, PASS, None, None, None).await?;
	// Disconnect the connection
	socket.close().await?;
	// Connect to WebSocket
	let mut socket = Socket::connect(&addr, SERVER, FORMAT).await?;
	// Send AUTHENTICATE command
	socket.send_request("authenticate", json!([token,])).await?;
	// Verify we have an authenticated session
	let res = socket.send_message_query("DEFINE NAMESPACE test").await?;
	assert_eq!(res[0]["status"], "OK", "result: {:?}", res);
	// Test passed
	server.finish().unwrap();
	Ok(())
}

#[test(tokio::test)]
async fn letset() -> Result<(), Box<dyn std::error::Error>> {
	// Setup database server
	let (addr, mut server) = common::start_server_with_defaults().await.unwrap();
	// Connect to WebSocket
	let mut socket = Socket::connect(&addr, SERVER, FORMAT).await?;
	// Authenticate the connection
	socket.send_message_signin(USER, PASS, None, None, None).await?;
	// Specify a namespace and database
	socket.send_message_use(Some(NS), Some(DB)).await?;
	// Send LET command
	socket.send_request("let", json!(["let_var", "let_value",])).await?;
	// Send SET command
	socket.send_request("set", json!(["set_var", "set_value",])).await?;
	// Verify the variables are set
	let res = socket.send_message_query("SELECT * FROM $let_var, $set_var").await?;
	assert_eq!(res[0]["result"], json!(["let_value", "set_value"]), "result: {:?}", res);
	server.finish().unwrap();
	Ok(())
}

#[test(tokio::test)]
async fn unset() -> Result<(), Box<dyn std::error::Error>> {
	// Setup database server
	let (addr, mut server) = common::start_server_with_defaults().await.unwrap();
	// Connect to WebSocket
	let mut socket = Socket::connect(&addr, SERVER, FORMAT).await?;
	// Authenticate the connection
	socket.send_message_signin(USER, PASS, None, None, None).await?;
	// Specify a namespace and database
	socket.send_message_use(Some(NS), Some(DB)).await?;
	// Send LET command
	socket.send_request("let", json!(["let_var", "let_value",])).await?;
	// Verify the variable is set
	let res = socket.send_message_query("SELECT * FROM $let_var").await?;
	assert_eq!(res[0]["result"], json!(["let_value"]), "result: {:?}", res);
	// Send UNSET command
	socket.send_request("unset", json!(["let_var",])).await?;
	// Verify the variable is unset
	let res = socket.send_message_query("SELECT * FROM $let_var").await?;
	assert_eq!(res[0]["result"], json!([null]), "result: {:?}", res);
	// Test passed
	server.finish().unwrap();
	Ok(())
}

#[test(tokio::test)]
async fn select() -> Result<(), Box<dyn std::error::Error>> {
	// Setup database server
	let (addr, mut server) = common::start_server_with_defaults().await.unwrap();
	// Connect to WebSocket
	let mut socket = Socket::connect(&addr, SERVER, FORMAT).await?;
	// Authenticate the connection
	socket.send_message_signin(USER, PASS, None, None, None).await?;
	// Specify a namespace and database
	socket.send_message_use(Some(NS), Some(DB)).await?;
	// Create a test record
	socket.send_message_query("CREATE tester SET name = 'foo', value = 'bar'").await?;
	// Send SELECT command
	let res = socket.send_request("select", json!(["tester",])).await?;
	assert!(res.is_object(), "result: {:?}", res);
	assert!(res["result"].is_array(), "result: {:?}", res);
	let res = res["result"].as_array().unwrap();
	assert_eq!(res.len(), 1, "result: {:?}", res);
	assert_eq!(res[0]["name"], "foo", "result: {:?}", res);
	assert_eq!(res[0]["value"], "bar", "result: {:?}", res);
	// Test passed
	server.finish().unwrap();
	Ok(())
}

#[test(tokio::test)]
async fn insert() -> Result<(), Box<dyn std::error::Error>> {
	// Setup database server
	let (addr, mut server) = common::start_server_with_defaults().await.unwrap();
	// Connect to WebSocket
	let mut socket = Socket::connect(&addr, SERVER, FORMAT).await?;
	// Authenticate the connection
	socket.send_message_signin(USER, PASS, None, None, None).await?;
	// Specify a namespace and database
	socket.send_message_use(Some(NS), Some(DB)).await?;
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
		.await?;
	assert!(res.is_object(), "result: {:?}", res);
	assert!(res["result"].is_array(), "result: {:?}", res);
	let res = res["result"].as_array().unwrap();
	assert_eq!(res.len(), 1, "result: {:?}", res);
	assert_eq!(res[0]["name"], "foo", "result: {:?}", res);
	assert_eq!(res[0]["value"], "bar", "result: {:?}", res);
	// Verify the data was inserted and can be queried
	let res = socket.send_message_query("SELECT * FROM tester").await?;
	assert!(res[0]["result"].is_array(), "result: {:?}", res);
	let res = res[0]["result"].as_array().unwrap();
	assert_eq!(res.len(), 1, "result: {:?}", res);
	assert_eq!(res[0]["name"], "foo", "result: {:?}", res);
	assert_eq!(res[0]["value"], "bar", "result: {:?}", res);
	// Test passed
	server.finish().unwrap();
	Ok(())
}

#[test(tokio::test)]
async fn create() -> Result<(), Box<dyn std::error::Error>> {
	// Setup database server
	let (addr, mut server) = common::start_server_with_defaults().await.unwrap();
	// Connect to WebSocket
	let mut socket = Socket::connect(&addr, SERVER, FORMAT).await?;
	// Authenticate the connection
	socket.send_message_signin(USER, PASS, None, None, None).await?;
	// Specify a namespace and database
	socket.send_message_use(Some(NS), Some(DB)).await?;
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
		.await?;
	assert!(res.is_object(), "result: {:?}", res);
	assert!(res["result"].is_array(), "result: {:?}", res);
	let res = res["result"].as_array().unwrap();
	assert_eq!(res.len(), 1, "result: {:?}", res);
	assert_eq!(res[0]["value"], "bar", "result: {:?}", res);
	// Verify the data was created
	let res = socket.send_message_query("SELECT * FROM tester").await?;
	assert!(res[0]["result"].is_array(), "result: {:?}", res);
	let res = res[0]["result"].as_array().unwrap();
	assert_eq!(res.len(), 1, "result: {:?}", res);
	assert_eq!(res[0]["value"], "bar", "result: {:?}", res);
	// Test passed
	server.finish().unwrap();
	Ok(())
}

#[test(tokio::test)]
async fn update() -> Result<(), Box<dyn std::error::Error>> {
	// Setup database server
	let (addr, mut server) = common::start_server_with_defaults().await.unwrap();
	// Connect to WebSocket
	let mut socket = Socket::connect(&addr, SERVER, FORMAT).await?;
	// Authenticate the connection
	socket.send_message_signin(USER, PASS, None, None, None).await?;
	// Specify a namespace and database
	socket.send_message_use(Some(NS), Some(DB)).await?;
	// Create a test record
	socket.send_message_query("CREATE tester SET name = 'foo'").await?;
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
		.await?;
	assert!(res.is_object(), "result: {:?}", res);
	assert!(res["result"].is_array(), "result: {:?}", res);
	let res = res["result"].as_array().unwrap();
	assert_eq!(res.len(), 1, "result: {:?}", res);
	assert_eq!(res[0]["value"], "bar", "result: {:?}", res);
	// Verify the data was updated
	let res = socket.send_message_query("SELECT * FROM tester").await?;
	assert!(res[0]["result"].is_array(), "result: {:?}", res);
	let res = res[0]["result"].as_array().unwrap();
	assert_eq!(res.len(), 1, "result: {:?}", res);
	assert_eq!(res[0]["name"], json!(null), "result: {:?}", res);
	assert_eq!(res[0]["value"], "bar", "result: {:?}", res);
	// Test passed
	server.finish().unwrap();
	Ok(())
}

#[test(tokio::test)]
async fn merge() -> Result<(), Box<dyn std::error::Error>> {
	// Setup database server
	let (addr, mut server) = common::start_server_with_defaults().await.unwrap();
	// Connect to WebSocket
	let mut socket = Socket::connect(&addr, SERVER, FORMAT).await?;
	// Authenticate the connection
	socket.send_message_signin(USER, PASS, None, None, None).await?;
	// Specify a namespace and database
	socket.send_message_use(Some(NS), Some(DB)).await?;
	// Create a test record
	socket.send_message_query("CREATE tester SET name = 'foo'").await?;
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
		.await?;
	assert!(res.is_object(), "result: {:?}", res);
	assert!(res["result"].is_array(), "result: {:?}", res);
	let res = res["result"].as_array().unwrap();
	assert_eq!(res.len(), 1, "result: {:?}", res);
	assert_eq!(res[0]["name"], "foo", "result: {:?}", res);
	assert_eq!(res[0]["value"], "bar", "result: {:?}", res);
	// Verify the data was merged
	let res = socket.send_message_query("SELECT * FROM tester").await?;
	assert!(res[0]["result"].is_array(), "result: {:?}", res);
	let res = res[0]["result"].as_array().unwrap();
	assert_eq!(res.len(), 1, "result: {:?}", res);
	assert_eq!(res[0]["name"], "foo", "result: {:?}", res);
	assert_eq!(res[0]["value"], "bar", "result: {:?}", res);
	// Test passed
	server.finish().unwrap();
	Ok(())
}

#[test(tokio::test)]
async fn patch() -> Result<(), Box<dyn std::error::Error>> {
	// Setup database server
	let (addr, mut server) = common::start_server_with_defaults().await.unwrap();
	// Connect to WebSocket
	let mut socket = Socket::connect(&addr, SERVER, FORMAT).await?;
	// Authenticate the connection
	socket.send_message_signin(USER, PASS, None, None, None).await?;
	// Specify a namespace and database
	socket.send_message_use(Some(NS), Some(DB)).await?;
	// Create a test record
	socket.send_message_query("CREATE tester:id SET name = 'foo'").await?;
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
		.await?;
	assert!(res["result"].is_object(), "result: {:?}", res);
	let res = res["result"].as_object().unwrap();
	assert_eq!(res.get("value"), Some(json!("bar")).as_ref(), "result: {:?}", res);
	// Verify the data was patched
	let res = socket.send_message_query("SELECT * FROM tester").await?;
	assert!(res[0]["result"].is_array(), "result: {:?}", res);
	let res = res[0]["result"].as_array().unwrap();
	assert_eq!(res.len(), 1, "result: {:?}", res);
	assert_eq!(res[0]["name"], json!(null), "result: {:?}", res);
	assert_eq!(res[0]["value"], "bar", "result: {:?}", res);
	// Test passed
	server.finish().unwrap();
	Ok(())
}

#[test(tokio::test)]
async fn delete() -> Result<(), Box<dyn std::error::Error>> {
	// Setup database server
	let (addr, mut server) = common::start_server_with_defaults().await.unwrap();
	// Connect to WebSocket
	let mut socket = Socket::connect(&addr, SERVER, FORMAT).await?;
	// Authenticate the connection
	socket.send_message_signin(USER, PASS, None, None, None).await?;
	// Specify a namespace and database
	socket.send_message_use(Some(NS), Some(DB)).await?;
	// Create a test record
	socket.send_message_query("CREATE tester:id").await?;
	// Send DELETE command
	let res = socket.send_request("delete", json!(["tester"])).await?;
	assert!(res.is_object(), "result: {:?}", res);
	assert!(res["result"].is_array(), "result: {:?}", res);
	let res = res["result"].as_array().unwrap();
	assert_eq!(res.len(), 1, "result: {:?}", res);
	assert_eq!(res[0]["id"], "tester:id", "result: {:?}", res);
	// Create a test record
	socket.send_message_query("CREATE tester:id").await?;
	// Send DELETE command
	let res = socket.send_request("delete", json!(["tester:id"])).await?;
	assert!(res.is_object(), "result: {:?}", res);
	assert!(res["result"].is_object(), "result: {:?}", res);
	let res = res["result"].as_object().unwrap();
	assert_eq!(res["id"], "tester:id", "result: {:?}", res);
	// Verify the data was merged
	let res = socket.send_message_query("SELECT * FROM tester").await?;
	assert!(res[0]["result"].is_array(), "result: {:?}", res);
	let res = res[0]["result"].as_array().unwrap();
	assert_eq!(res.len(), 0, "result: {:?}", res);
	// Test passed
	server.finish().unwrap();
	Ok(())
}

#[test(tokio::test)]
async fn query() -> Result<(), Box<dyn std::error::Error>> {
	// Setup database server
	let (addr, mut server) = common::start_server_with_defaults().await.unwrap();
	// Connect to WebSocket
	let mut socket = Socket::connect(&addr, SERVER, FORMAT).await?;
	// Authenticate the connection
	socket.send_message_signin(USER, PASS, None, None, None).await?;
	// Specify a namespace and database
	socket.send_message_use(Some(NS), Some(DB)).await?;
	// Send QUERY command
	let res =
		socket.send_request("query", json!(["CREATE tester; SELECT * FROM tester;",])).await?;
	assert!(res.is_object(), "result: {:?}", res);
	assert!(res["result"].is_array(), "result: {:?}", res);
	let res = res["result"].as_array().unwrap();
	assert_eq!(res.len(), 2, "result: {:?}", res);
	// Verify the data was created
	let res = socket.send_message_query("SELECT * FROM tester").await?;
	assert!(res[0]["result"].is_array(), "result: {:?}", res);
	let res = res[0]["result"].as_array().unwrap();
	assert_eq!(res.len(), 1, "result: {:?}", res);
	// Test passed
	server.finish().unwrap();
	Ok(())
}

#[test(tokio::test)]
async fn version() -> Result<(), Box<dyn std::error::Error>> {
	// Setup database server
	let (addr, mut server) = common::start_server_with_defaults().await.unwrap();
	// Connect to WebSocket
	let socket = Socket::connect(&addr, SERVER, FORMAT).await?;
	// Send version command
	let res = socket.send_request("version", json!([])).await?;
	assert!(res["result"].is_string(), "result: {:?}", res);
	let res = res["result"].as_str().unwrap();
	assert!(res.starts_with("surrealdb-"), "result: {}", res);
	// Test passed
	server.finish().unwrap();
	Ok(())
}

// Validate that the WebSocket is able to process multiple queries concurrently
#[test(tokio::test)]
async fn concurrency() -> Result<(), Box<dyn std::error::Error>> {
	// Setup database server
	let (addr, mut server) = common::start_server_with_defaults().await.unwrap();
	// Connect to WebSocket
	let mut socket = Socket::connect(&addr, SERVER, FORMAT).await?;
	// Authenticate the connection
	socket.send_message_signin(USER, PASS, None, None, None).await?;
	// Specify a namespace and database
	socket.send_message_use(Some(NS), Some(DB)).await?;
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

	let res = res.into_iter().try_fold(
		Vec::new(),
		|mut acc, x| -> Result<_, Box<dyn std::error::Error>> {
			acc.push(x?);
			Ok(acc)
		},
	)?;

	assert!(res.iter().all(|v| v["error"].is_null()), "Unexpected error received: {:#?}", res);
	// Test passed
	server.finish().unwrap();
	Ok(())
}

#[test(tokio::test)]
async fn live() -> Result<(), Box<dyn std::error::Error>> {
	// Setup database server
	let (addr, mut server) = common::start_server_with_defaults().await.unwrap();
	// Connect to WebSocket
	let mut socket = Socket::connect(&addr, SERVER, FORMAT).await?;
	// Authenticate the connection
	socket.send_message_signin(USER, PASS, None, None, None).await?;
	// Specify a namespace and database
	socket.send_message_use(Some(NS), Some(DB)).await?;
	// Send LIVE command
	let res = socket.send_request("live", json!(["tester"])).await?;
	assert!(res.is_object(), "result: {:?}", res);
	assert!(res["result"].is_string(), "result: {:?}", res);
	let live1 = res["result"].as_str().unwrap();
	// Send QUERY command
	let res = socket.send_request("query", json!(["LIVE SELECT * FROM tester"])).await?;
	assert!(res.is_object(), "result: {:?}", res);
	assert!(res["result"].is_array(), "result: {:?}", res);
	let res = res["result"].as_array().unwrap();
	assert_eq!(res.len(), 1, "result: {:?}", res);
	assert!(res[0]["result"].is_string(), "result: {:?}", res);
	let live2 = res[0]["result"].as_str().unwrap();
	// Create a new test record
	let res = socket.send_request("query", json!(["CREATE tester:id SET name = 'foo'"])).await?;
	assert!(res.is_object(), "result: {:?}", res);
	assert!(res["result"].is_array(), "result: {:?}", res);
	let res = res["result"].as_array().unwrap();
	assert_eq!(res.len(), 1, "result: {:?}", res);
	// Wait some time for all messages to arrive, and then search for the notification message
	let msgs: Result<_, Box<dyn std::error::Error>> =
		tokio::time::timeout(Duration::from_secs(1), async {
			Ok(vec![socket.receive_other_message().await?, socket.receive_other_message().await?])
		})
		.await?;
	let msgs = msgs?;
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
	server.finish().unwrap();
	Ok(())
}

#[test(tokio::test)]
async fn kill() -> Result<(), Box<dyn std::error::Error>> {
	// Setup database server
	let (addr, mut server) = common::start_server_with_defaults().await.unwrap();
	// Connect to WebSocket
	let mut socket = Socket::connect(&addr, SERVER, FORMAT).await?;
	// Authenticate the connection
	socket.send_message_signin(USER, PASS, None, None, None).await?;
	// Specify a namespace and database
	socket.send_message_use(Some(NS), Some(DB)).await?;
	// Send LIVE command
	let res = socket.send_request("live", json!(["tester"])).await?;
	assert!(res.is_object(), "result: {:?}", res);
	assert!(res["result"].is_string(), "result: {:?}", res);
	let live1 = res["result"].as_str().unwrap();
	// Send QUERY command
	let res = socket.send_request("query", json!(["LIVE SELECT * FROM tester"])).await?;
	assert!(res.is_object(), "result: {:?}", res);
	assert!(res["result"].is_array(), "result: {:?}", res);
	let res = res["result"].as_array().unwrap();
	assert_eq!(res.len(), 1, "result: {:?}", res);
	assert!(res[0]["result"].is_string(), "result: {:?}", res);
	let live2 = res[0]["result"].as_str().unwrap();
	// Create a new test record
	let res = socket.send_request("query", json!(["CREATE tester:one SET name = 'one'"])).await?;
	assert!(res.is_object(), "result: {:?}", res);
	assert!(res["result"].is_array(), "result: {:?}", res);
	let res = res["result"].as_array().unwrap();
	assert_eq!(res.len(), 1, "result: {:?}", res);
	// Wait some time for all messages to arrive, and then search for the notification message
	let msgs = socket.receive_all_other_messages(2, Duration::from_secs(1)).await?;
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
	let res = socket.send_request("kill", json!([live1])).await?;
	assert!(res.is_object(), "result: {:?}", res);
	assert!(res["result"].is_null(), "result: {:?}", res);
	// Create a new test record
	let res = socket.send_request("query", json!(["CREATE tester:two SET name = 'two'"])).await?;
	assert!(res.is_object(), "result: {:?}", res);
	assert!(res["result"].is_array(), "result: {:?}", res);
	let res = res["result"].as_array().unwrap();
	assert_eq!(res.len(), 1, "result: {:?}", res);
	// Wait some time for all messages to arrive, and then search for the notification message
	let msgs = socket.receive_all_other_messages(1, Duration::from_secs(1)).await?;
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
	let res = socket.send_request("query", json!([format!("KILL u'{live2}'")])).await?;
	assert!(res.is_object(), "result: {:?}", res);
	assert!(res["result"].is_array(), "result: {:?}", res);
	let res = res["result"].as_array().unwrap();
	assert_eq!(res.len(), 1, "result: {:?}", res);
	assert!(res[0]["result"].is_null(), "result: {:?}", res);
	// Create a new test record
	let res = socket.send_request("query", json!(["CREATE tester:tre SET name = 'two'"])).await?;
	assert!(res.is_object(), "result: {:?}", res);
	assert!(res["result"].is_array(), "result: {:?}", res);
	let res = res["result"].as_array().unwrap();
	assert_eq!(res.len(), 1, "result: {:?}", res);
	// Wait some time for all messages to arrive, and then search for the notification message
	let msgs = socket.receive_all_other_messages(0, Duration::from_secs(1)).await?;
	assert!(msgs.iter().all(|v| v["error"].is_null()), "Unexpected error received: {:?}", msgs);
	// Test passed
	server.finish().unwrap();
	Ok(())
}

#[test(tokio::test)]
async fn live_second_connection() -> Result<(), Box<dyn std::error::Error>> {
	// Setup database server
	let (addr, mut server) = common::start_server_with_defaults().await.unwrap();
	// Connect to WebSocket
	let mut socket1 = Socket::connect(&addr, SERVER, FORMAT).await?;
	// Authenticate the connection
	socket1.send_message_signin(USER, PASS, None, None, None).await?;
	// Specify a namespace and database
	socket1.send_message_use(Some(NS), Some(DB)).await?;
	// Send LIVE command
	let res = socket1.send_request("live", json!(["tester"])).await?;
	assert!(res.is_object(), "result: {:?}", res);
	assert!(res["result"].is_string(), "result: {:?}", res);
	let liveid = res["result"].as_str().unwrap();
	// Connect to WebSocket
	let mut socket2 = Socket::connect(&addr, SERVER, FORMAT).await?;
	// Authenticate the connection
	socket2.send_message_signin(USER, PASS, None, None, None).await?;
	// Specify a namespace and database
	socket2.send_message_use(Some(NS), Some(DB)).await?;
	// Create a new test record
	let res = socket2.send_request("query", json!(["CREATE tester:id SET name = 'foo'"])).await?;
	assert!(res.is_object(), "result: {:?}", res);
	assert!(res["result"].is_array(), "result: {:?}", res);
	let res = res["result"].as_array().unwrap();
	assert_eq!(res.len(), 1, "result: {:?}", res);
	// Wait some time for all messages to arrive, and then search for the notification message
	let msgs = socket1.receive_all_other_messages(1, Duration::from_secs(1)).await?;
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
	server.finish().unwrap();
	Ok(())
}

#[test(tokio::test)]
async fn variable_auth_live_query() -> Result<(), Box<dyn std::error::Error>> {
	// Setup database server
	let (addr, mut server) = common::start_server_with_defaults().await.unwrap();
	// Connect to WebSocket
	let mut socket_permanent = Socket::connect(&addr, SERVER, FORMAT).await?;
	// Authenticate the connection
	socket_permanent.send_message_signin(USER, PASS, None, None, None).await?;
	// Specify a namespace and database
	socket_permanent.send_message_use(Some(NS), Some(DB)).await?;
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
		.await?;
	// Send SIGNUP command
	let mut socket_expiring_auth = Socket::connect(&addr, SERVER, FORMAT).await?;

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
	assert!(res.is_ok(), "result: {:?}", res);
	let res = res.unwrap();
	assert!(res.is_object(), "result: {:?}", res);
	let res = res.as_object().unwrap();
	// Verify response contains no error
	assert!(res.keys().all(|k| ["id", "result"].contains(&k.as_str())), "result: {:?}", res);
	// Authenticate the connection
	socket_expiring_auth.send_message_signin(USER, PASS, None, None, None).await?;
	// Send LIVE command
	let res = socket_expiring_auth.send_request("live", json!(["tester"])).await?;
	assert!(res.is_object(), "result: {:?}", res);
	assert!(res["result"].is_string(), "result: {:?}", res);
	// Wait 2 seconds for auth to expire
	tokio::time::sleep(Duration::from_secs(1)).await;
	// Create a new test record
	let res = socket_permanent
		.send_request("query", json!(["CREATE tester:id SET name = 'foo'"]))
		.await?;
	assert!(res.is_object(), "result: {:?}", res);
	assert!(res["result"].is_array(), "result: {:?}", res);
	let res = res["result"].as_array().unwrap();
	assert_eq!(res.len(), 1, "result: {:?}", res);
	// Wait some time for all messages to arrive, and then search for the notification message
	let msgs = socket_expiring_auth.receive_all_other_messages(0, Duration::from_secs(1)).await?;
	assert!(msgs.iter().all(|v| v["error"].is_null()), "Unexpected error received: {:?}", msgs);
	// Test passed
	server.finish().unwrap();
	Ok(())
}

#[test(tokio::test)]
async fn session_expiration() {
	// Setup database server
	let (addr, mut server) = common::start_server_with_defaults().await.unwrap();
	// Connect to WebSocket
	let mut socket = Socket::connect(&addr, SERVER, FORMAT).await.unwrap();
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
	// Authenticate using the token, which will expire soon
	socket.send_request("authenticate", json!([res,])).await.unwrap();
	// Check if the session is now authenticated
	let res = socket.send_message_query("SELECT VALUE working FROM test:1").await.unwrap();
	assert_eq!(res[0]["result"], json!(["yes"]), "result: {:?}", res);
	// Wait two seconds for token to expire
	tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
	// Check that the session has expired and queries fail
	let res = socket.send_request("query", json!(["SELECT VALUE working FROM test:1",])).await;
	assert!(res.is_ok(), "result: {:?}", res);
	let res = res.unwrap();
	assert!(res.is_object(), "result: {:?}", res);
	let res = res.as_object().unwrap();
	assert_eq!(
		res["error"],
		json!({"code": -32000, "message": "There was a problem with the database: The session has expired"})
	);
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
	assert!(res.is_ok(), "result: {:?}", res);
	let res = res.unwrap();
	assert!(res.is_object(), "result: {:?}", res);
	let res = res.as_object().unwrap();
	// Verify response contains no error
	assert!(res.keys().all(|k| ["id", "result"].contains(&k.as_str())), "result: {:?}", res);
	// Check that the session is now valid again and queries succeed
	let res = socket.send_message_query("SELECT VALUE working FROM test:1").await.unwrap();
	assert_eq!(res[0]["result"], json!(["yes"]), "result: {:?}", res);
	// Test passed
	server.finish().unwrap();
}

#[test(tokio::test)]
async fn session_expiration_operations() {
	// Setup database server
	let (addr, mut server) = common::start_server_with_defaults().await.unwrap();
	// Connect to WebSocket
	let mut socket = Socket::connect(&addr, SERVER, FORMAT).await.unwrap();
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
	// Authenticate using the token, which will expire soon
	socket.send_request("authenticate", json!([res,])).await.unwrap();
	// Check if the session is now authenticated
	let res = socket.send_message_query("SELECT VALUE working FROM test:1").await.unwrap();
	assert_eq!(res[0]["result"], json!(["yes"]), "result: {:?}", res);
	// Wait two seconds for the session to expire
	tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
	// Check if the session is now expired
	let res = socket.send_request("query", json!(["SELECT VALUE working FROM test:1",])).await;
	assert!(res.is_ok(), "result: {:?}", res);
	let res = res.unwrap();
	assert!(res.is_object(), "result: {:?}", res);
	let res = res.as_object().unwrap();
	assert_eq!(
		res["error"],
		json!({"code": -32000, "message": "There was a problem with the database: The session has expired"})
	);
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
	// Futures are executed sequentially as some operations rely on the previous state
	for operation in operations_ko {
		let res = operation.await;
		assert!(res.is_ok(), "result: {:?}", res);
		let res = res.unwrap();
		assert!(res.is_object(), "result: {:?}", res);
		let res = res.as_object().unwrap();
		assert_eq!(
			res["error"],
			json!({"code": -32000, "message": "There was a problem with the database: The session has expired"})
		);
	}

	// Test operations that SHOULD work with an expired session
	let operations_ok = vec![
		socket.send_request("use", json!([NS, DB])),
		socket.send_request("ping", json!([])),
		socket.send_request("version", json!([])),
		socket.send_request("invalidate", json!([])),
	];
	// Futures are executed sequentially as some operations rely on the previous state
	for operation in operations_ok {
		let res = operation.await;
		assert!(res.is_ok(), "result: {:?}", res);
		let res = res.unwrap();
		assert!(res.is_object(), "result: {:?}", res);
		let res = res.as_object().unwrap();
		// Verify response contains no error
		assert!(res.keys().all(|k| ["id", "result"].contains(&k.as_str())), "result: {:?}", res);
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
	assert!(res.is_ok(), "result: {:?}", res);
	let res = res.unwrap();
	assert!(res.is_object(), "result: {:?}", res);
	let res = res.as_object().unwrap();
	// Verify response contains no error
	assert!(res.keys().all(|k| ["id", "result"].contains(&k.as_str())), "result: {:?}", res);
	// Wait two seconds for the session to expire
	tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
	// The session must be expired now or we fail the test
	let res = socket.send_request("query", json!(["SELECT VALUE working FROM test:1",])).await;
	assert!(res.is_ok(), "result: {:?}", res);
	let res = res.unwrap();
	assert!(res.is_object(), "result: {:?}", res);
	let res = res.as_object().unwrap();
	assert_eq!(
		res["error"],
		json!({"code": -32000, "message": "There was a problem with the database: The session has expired"})
	);
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
	assert!(res.is_ok(), "result: {:?}", res);
	let res = res.unwrap();
	assert!(res.is_object(), "result: {:?}", res);
	let res = res.as_object().unwrap();
	// Verify response contains no error
	assert!(res.keys().all(|k| ["id", "result"].contains(&k.as_str())), "result: {:?}", res);
	// Wait two seconds for the session to expire
	tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
	// The session must be expired now or we fail the test
	let res = socket.send_request("query", json!(["SELECT VALUE working FROM test:1",])).await;
	assert!(res.is_ok(), "result: {:?}", res);
	let res = res.unwrap();
	assert!(res.is_object(), "result: {:?}", res);
	let res = res.as_object().unwrap();
	assert_eq!(
		res["error"],
		json!({"code": -32000, "message": "There was a problem with the database: The session has expired"})
	);

	// This needs to be last operation as the session will no longer expire afterwards
	let res = socket.send_request("authenticate", json!([root_token,])).await;
	assert!(res.is_ok(), "result: {:?}", res);
	let res = res.unwrap();
	assert!(res.is_object(), "result: {:?}", res);
	let res = res.as_object().unwrap();
	// Verify response contains no error
	assert!(res.keys().all(|k| ["id", "result"].contains(&k.as_str())), "result: {:?}", res);

	// Test passed
	server.finish().unwrap();
}

#[test(tokio::test)]
async fn session_reauthentication() {
	// Setup database server
	let (addr, mut server) = common::start_server_with_defaults().await.unwrap();
	// Connect to WebSocket
	let mut socket = Socket::connect(&addr, SERVER, FORMAT).await.unwrap();
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
	// Authenticate using the token
	socket.send_request("authenticate", json!([res,])).await.unwrap();
	// Check that we do not have root access
	let res = socket.send_message_query("INFO FOR ROOT").await.unwrap();
	assert_eq!(res[0]["status"], "ERR", "result: {:?}", res);
	assert_eq!(
		res[0]["result"], "IAM error: Not enough permissions to perform this action",
		"result: {:?}",
		res
	);
	// Check if the session is authenticated
	let res = socket.send_message_query("SELECT VALUE working FROM test:1").await.unwrap();
	assert_eq!(res[0]["result"], json!(["yes"]), "result: {:?}", res);
	// Authenticate using the root token
	socket.send_request("authenticate", json!([root_token,])).await.unwrap();
	// Check that we have root access again
	let res = socket.send_message_query("INFO FOR ROOT").await.unwrap();
	assert_eq!(res[0]["status"], "OK", "result: {:?}", res);
	// Test passed
	server.finish().unwrap();
}

#[test(tokio::test)]
async fn session_reauthentication_expired() {
	// Setup database server
	let (addr, mut server) = common::start_server_with_defaults().await.unwrap();
	// Connect to WebSocket
	let mut socket = Socket::connect(&addr, SERVER, FORMAT).await.unwrap();
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
	// Authenticate using the token, which will expire soon
	socket.send_request("authenticate", json!([res,])).await.unwrap();
	// Wait two seconds for token to expire
	tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
	// Verify that the session has expired
	let res = socket.send_request("query", json!(["SELECT VALUE working FROM test:1",])).await;
	assert!(res.is_ok(), "result: {:?}", res);
	let res = res.unwrap();
	assert!(res.is_object(), "result: {:?}", res);
	let res = res.as_object().unwrap();
	assert_eq!(
		res["error"],
		json!({"code": -32000, "message": "There was a problem with the database: The session has expired"})
	);
	// Authenticate using the root token, which has not expired yet
	socket.send_request("authenticate", json!([root_token,])).await.unwrap();
	// Check that we have root access and the session is not expired
	let res = socket.send_message_query("INFO FOR ROOT").await.unwrap();
	assert_eq!(res[0]["status"], "OK", "result: {:?}", res);
	// Test passed
	server.finish().unwrap();
}

#[test(tokio::test)]
async fn run_functions() {
	// Setup database server
	let (addr, mut server) = common::start_server_with_defaults().await.unwrap();
	// Connect to WebSocket
	let mut socket = Socket::connect(&addr, SERVER, FORMAT).await.unwrap();
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

#[test(tokio::test)]
async fn relate_rpc() {
	// Setup database server
	let (addr, mut server) = common::start_server_with_defaults().await.unwrap();
	// Connect to WebSocket
	let mut socket = Socket::connect(&addr, SERVER, FORMAT).await.unwrap();
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
	let expected = json!(42);
	assert_eq!(res.remove(0)["result"], expected);

	let mut res = socket.send_message_query("RETURN foo:a->bar->foo").await.unwrap();
	let expected = json!(["foo:b"]);
	assert_eq!(res.remove(0)["result"], expected);

	// Test passed
	server.finish().unwrap();
}

#[test(tokio::test)]
async fn temporary_directory() {
	// Setup database server
	let temp_dir = TempDir::new().unwrap();
	let (addr, mut server) =
		common::start_server_with_temporary_directory(temp_dir.to_string_lossy().as_ref())
			.await
			.unwrap();
	// Connect to WebSocket
	let mut socket = Socket::connect(&addr, SERVER, FORMAT).await.unwrap();
	// Authenticate the connection
	socket.send_message_signin(USER, PASS, None, None, None).await.unwrap();
	// Specify a namespace and database
	socket.send_message_use(Some(NS), Some(DB)).await.unwrap();
	// create records
	socket.send_message_query("CREATE test:a, test:b").await.unwrap();
	// These selects use the memory collector
	let mut res =
		socket.send_message_query("SELECT * FROM test ORDER BY id DESC EXPLAIN").await.unwrap();
	let expected = json!([{"detail": { "table": "test" }, "operation": "Iterate Table" }, { "detail": { "type": "Memory" }, "operation": "Collector" }]);
	assert_eq!(res.remove(0)["result"], expected);
	// And return the correct result
	let mut res = socket.send_message_query("SELECT * FROM test ORDER BY id DESC").await.unwrap();
	let expected = json!([{"id": "test:b" }, { "id": "test:a" }]);
	assert_eq!(res.remove(0)["result"], expected);
	// This one should the file collector
	let mut res = socket
		.send_message_query("SELECT * FROM test ORDER BY id DESC TEMPFILES EXPLAIN")
		.await
		.unwrap();
	let expected = json!([{"detail": { "table": "test" }, "operation": "Iterate Table" }, { "detail": { "type": "TempFiles" }, "operation": "Collector" }]);
	assert_eq!(res.remove(0)["result"], expected);
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
