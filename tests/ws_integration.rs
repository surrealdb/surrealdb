// cargo test --package surreal --bin surreal --no-default-features --features storage-mem --test ws_integration -- --nocapture

mod common;

use serde_json::json;
use serial_test::serial;
use test_log::test;

use crate::common::error::TestError;
use crate::common::{PASS, USER};

#[test(tokio::test)]
#[serial]
async fn ping() -> Result<(), Box<dyn std::error::Error>> {
	let (addr, _server) = common::start_server(true, false, true).await.unwrap();
	let socket = &mut common::connect_ws(&addr).await?;

	// Send command
	let res = common::ws_send_msg(
		socket,
		serde_json::to_string(&json!({
			"id": "1",
			"method": "ping",
		}))
		.unwrap(),
	)
	.await;
	assert!(res.is_ok(), "result: {:?}", res);
	let res = res.unwrap();
	assert!(res.is_object(), "result: {:?}", res);
	let res = res.as_object().unwrap();
	assert!(res.keys().all(|k| ["id", "result"].contains(&k.as_str())), "result: {:?}", res);

	Ok(())
}

#[test(tokio::test)]
#[serial]
async fn info() -> Result<(), Box<dyn std::error::Error>> {
	let (addr, _server) = common::start_server(true, false, true).await.unwrap();
	let socket = &mut common::connect_ws(&addr).await?;

	//
	// Prepare the connection
	//
	let res = common::ws_signin(socket, USER, PASS, None, None, None).await;
	assert!(res.is_ok(), "result: {:?}", res);
	let res = common::ws_use(socket, Some("N"), Some("D")).await;
	assert!(res.is_ok(), "result: {:?}", res);

	//
	// Setup operations
	//
	let res = common::ws_query(socket, "DEFINE TABLE user PERMISSIONS FULL").await;
	assert!(res.is_ok(), "result: {:?}", res);
	let res = common::ws_query(
		socket,
		r#"
		DEFINE SCOPE scope SESSION 24h
			SIGNUP ( CREATE user SET user = $user, pass = crypto::argon2::generate($pass) )
			SIGNIN ( SELECT * FROM user WHERE user = $user AND crypto::argon2::compare(pass, $pass) )
		;
		"#,
	)
	.await;
	assert!(res.is_ok(), "result: {:?}", res);
	let res = common::ws_query(
		socket,
		r#"
		CREATE user CONTENT {
			user: 'user',
			pass: crypto::argon2::generate('pass')
		};
		"#,
	)
	.await;
	assert!(res.is_ok(), "result: {:?}", res);

	// Sign in
	let res = common::ws_signin(socket, "user", "pass", Some("N"), Some("D"), Some("scope")).await;
	assert!(res.is_ok(), "result: {:?}", res);

	// Send the info command
	let res = common::ws_send_msg(
		socket,
		serde_json::to_string(&json!({
			"id": "1",
			"method": "info",
		}))
		.unwrap(),
	)
	.await;
	assert!(res.is_ok(), "result: {:?}", res);

	// Verify the response contains the expected info
	let res = res.unwrap();
	assert!(res["result"].is_object(), "result: {:?}", res);
	let res = res["result"].as_object().unwrap();
	assert_eq!(res["user"], "user", "result: {:?}", res);

	Ok(())
}

#[test(tokio::test)]
#[serial]
async fn signup() -> Result<(), Box<dyn std::error::Error>> {
	let (addr, _server) = common::start_server(true, false, true).await.unwrap();
	let socket = &mut common::connect_ws(&addr).await?;

	//
	// Prepare the connection
	//
	let res = common::ws_signin(socket, USER, PASS, None, None, None).await;
	assert!(res.is_ok(), "result: {:?}", res);
	let res = common::ws_use(socket, Some("N"), Some("D")).await;
	assert!(res.is_ok(), "result: {:?}", res);

	// Setup scope
	let res = common::ws_query(socket, r#"
        DEFINE SCOPE scope SESSION 24h
            SIGNUP ( CREATE user SET email = $email, pass = crypto::argon2::generate($pass) )
            SIGNIN ( SELECT * FROM user WHERE email = $email AND crypto::argon2::compare(pass, $pass) )
        ;"#).await;
	assert!(res.is_ok(), "result: {:?}", res);

	// Signup
	let res = common::ws_send_msg(
		socket,
		serde_json::to_string(&json!({
			"id": "1",
			"method": "signup",
			"params": [{
				"ns": "N",
				"db": "D",
				"sc": "scope",
				"email": "email@email.com",
				"pass": "pass",
			}],
		}))
		.unwrap(),
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
	Ok(())
}

#[test(tokio::test)]
#[serial]
async fn signin() -> Result<(), Box<dyn std::error::Error>> {
	let (addr, _server) = common::start_server(true, false, true).await.unwrap();
	let socket = &mut common::connect_ws(&addr).await?;

	//
	// Prepare the connection
	//
	let res = common::ws_signin(socket, USER, PASS, None, None, None).await;
	assert!(res.is_ok(), "result: {:?}", res);
	let res = common::ws_use(socket, Some("N"), Some("D")).await;
	assert!(res.is_ok(), "result: {:?}", res);

	// Setup scope
	let res = common::ws_query(socket, r#"
        DEFINE SCOPE scope SESSION 24h
            SIGNUP ( CREATE user SET email = $email, pass = crypto::argon2::generate($pass) )
            SIGNIN ( SELECT * FROM user WHERE email = $email AND crypto::argon2::compare(pass, $pass) )
        ;"#).await;
	assert!(res.is_ok(), "result: {:?}", res);

	// Signup
	let res = common::ws_send_msg(
		socket,
		serde_json::to_string(&json!({
			"id": "1",
			"method": "signup",
			"params": [{
				"ns": "N",
				"db": "D",
				"sc": "scope",
				"email": "email@email.com",
				"pass": "pass",
			}],
		}))
		.unwrap(),
	)
	.await;
	assert!(res.is_ok(), "result: {:?}", res);

	// Sign in
	let res = common::ws_send_msg(
		socket,
		serde_json::to_string(&json!({
			"id": "1",
			"method": "signin",
			"params": [{
				"ns": "N",
				"db": "D",
				"sc": "scope",
				"email": "email@email.com",
				"pass": "pass",
			}],
		}))
		.unwrap(),
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

	Ok(())
}

#[test(tokio::test)]
#[serial]
async fn invalidate() -> Result<(), Box<dyn std::error::Error>> {
	let (addr, _server) = common::start_server(true, false, true).await.unwrap();
	let socket = &mut common::connect_ws(&addr).await?;

	//
	// Prepare the connection
	//
	let res = common::ws_signin(socket, USER, PASS, None, None, None).await;
	assert!(res.is_ok(), "result: {:?}", res);

	// Verify we have a ROOT session
	let res = common::ws_query(socket, "DEFINE NAMESPACE NS").await;
	assert!(res.is_ok(), "result: {:?}", res);

	// Invalidate session
	let res = common::ws_send_msg(
		socket,
		serde_json::to_string(&json!({
			"id": "1",
			"method": "invalidate",
		}))
		.unwrap(),
	)
	.await;
	assert!(res.is_ok(), "result: {:?}", res);

	// Verify we invalidated the root session
	let res = common::ws_query(socket, "DEFINE NAMESPACE NS2").await;
	assert!(res.is_ok(), "result: {:?}", res);
	let res = res.unwrap();

	assert_eq!(res[0]["status"], "ERR", "result: {:?}", res);
	assert_eq!(
		res[0]["result"], "IAM error: Not enough permissions to perform this action",
		"result: {:?}",
		res
	);
	Ok(())
}

#[test(tokio::test)]
#[serial]
async fn authenticate() -> Result<(), Box<dyn std::error::Error>> {
	let (addr, _server) = common::start_server(true, false, true).await.unwrap();
	let socket = &mut common::connect_ws(&addr).await?;

	//
	// Prepare the connection
	//
	let res = common::ws_signin(socket, USER, PASS, None, None, None).await;
	assert!(res.is_ok(), "result: {:?}", res);
	let token = res.unwrap();

	// Reconnect so we start with an empty session
	socket.close(None).await?;
	let socket = &mut common::connect_ws(&addr).await?;

	//
	// Authenticate with the token
	//

	// Send command
	let res = common::ws_send_msg(
		socket,
		serde_json::to_string(&json!({
			"id": "1",
			"method": "authenticate",
			"params": [
				token,

			],
		}))
		.unwrap(),
	)
	.await;
	assert!(res.is_ok(), "result: {:?}", res);

	// Verify we have a ROOT session
	let res = common::ws_query(socket, "DEFINE NAMESPACE D2").await;
	assert!(res.is_ok(), "result: {:?}", res);
	let res = res.unwrap();
	assert_eq!(res[0]["status"], "OK", "result: {:?}", res);
	Ok(())
}

#[test(tokio::test)]
#[serial]
async fn kill() -> Result<(), Box<dyn std::error::Error>> {
	// TODO: implement
	Ok(())
}

#[test(tokio::test)]
#[serial]
async fn live_live_endpoint() -> Result<(), Box<dyn std::error::Error>> {
	let (addr, _server) = common::start_server(false, false, true).await.unwrap();
	let table_name = "table_FD40A9A361884C56B5908A934164884A".to_string();

	let socket = &mut common::connect_ws(&addr).await?;

	let ns = "3498b03b44b5452a9d3f15252b454db1";
	let db = "2cf93e52ff0a42f39d271412404a01f6";
	let _ = common::ws_signin(socket, USER, PASS, None, None, None).await?;
	let _ = common::ws_use(socket, Some(ns), Some(db)).await?;

	// LIVE query via live endpoint
	let live_id = common::ws_send_msg(
		socket,
		serde_json::to_string(&json!({
				"id": "1",
				"method": "live",
				"params": [
					table_name
				],
		}))
		.unwrap(),
	)
	.await?;

	// Create some data for notification
	let id = "an-id-goes-here";
	let query = format!(r#"INSERT INTO {} {{"id": "{}", "name": "ok"}};"#, table_name, id);
	let created = common::ws_query(socket, query.as_str()).await.unwrap();
	assert_eq!(created.len(), 1);

	// Receive notification
	let res = common::ws_recv_msg(socket).await.unwrap();

	// Verify response contains no error
	assert!(
		res.as_object()
			.ok_or(TestError::AssertionError {
				message: format!("Unable to retrieve object from result: {}", res)
			})
			.unwrap()
			.keys()
			.eq(["result"]),
		"result: {}",
		res
	);

	// Unwrap
	let notification = &res
		.as_object()
		.ok_or(TestError::NetworkError {
			message: format!("missing json object, res: {:?}", res).to_string(),
		})
		.unwrap()["result"];
	assert_eq!(
		&notification["id"],
		live_id["result"].as_str().unwrap(),
		"expected a notification id to match the live query id: {} but was {}",
		&notification,
		live_id
	);
	let action = notification["action"].as_str().unwrap();
	let result = notification["result"].as_object().unwrap();

	// Verify message on individual keys since the notification ID is random
	assert_eq!(action, &serde_json::to_value("CREATE").unwrap(), "result: {:?}", res);
	assert_eq!(
		result["id"].as_str().ok_or(TestError::AssertionError {
			message: format!("missing id, res: {:?}", res).to_string(),
		})?,
		format!("{}:⟨{}⟩", table_name, id),
		"result: {:?}",
		res
	);
	assert_eq!(
		result["name"].as_str().unwrap(),
		serde_json::to_value("ok").unwrap(),
		"result: {:?}",
		res
	);

	Ok(())
}

#[test(tokio::test)]
#[serial]
async fn live_query_endpoint() -> Result<(), Box<dyn std::error::Error>> {
	let (addr, _server) = common::start_server(false, false, true).await.unwrap();
	let table_name = "table_FD40A9A361884C56B5908A934164884A".to_string();

	let socket = &mut common::connect_ws(&addr).await?;

	let ns = "3498b03b44b5452a9d3f15252b454db1";
	let db = "2cf93e52ff0a42f39d271412404a01f6";
	let _ = common::ws_signin(socket, USER, PASS, None, None, None).await?;
	let _ = common::ws_use(socket, Some(ns), Some(db)).await?;

	// LIVE query via query endpoint
	let lq_res =
		common::ws_query(socket, format!("LIVE SELECT * FROM {};", table_name).as_str()).await?;
	assert_eq!(lq_res.len(), 1);
	let live_id = lq_res
		.get(0)
		.ok_or(TestError::AssertionError {
			message: "Expected 1 result after len check".to_string(),
		})
		.unwrap();

	// Create some data for notification
	let id = "an-id-goes-here";
	let query = format!(r#"INSERT INTO {} {{"id": "{}", "name": "ok"}};"#, table_name, id);
	let created = common::ws_query(socket, query.as_str()).await.unwrap();
	assert_eq!(created.len(), 1);

	// Receive notification
	let res = common::ws_recv_msg(socket).await.unwrap();

	// Verify response contains no error
	assert!(
		res.as_object()
			.ok_or(TestError::AssertionError {
				message: format!("Unable to retrieve object from result: {}", res)
			})
			.unwrap()
			.keys()
			.eq(["result"]),
		"result: {}",
		res
	);

	// Unwrap
	let notification = &res
		.as_object()
		.ok_or(TestError::NetworkError {
			message: format!("missing json object, res: {:?}", res).to_string(),
		})
		.unwrap()["result"];
	assert_eq!(
		&notification["id"],
		live_id["result"].as_str().unwrap(),
		"expected a notification id to match the live query id: {} but was {}",
		&notification,
		live_id
	);
	let action = notification["action"].as_str().unwrap();
	let result = notification["result"].as_object().unwrap();

	// Verify message on individual keys since the notification ID is random
	assert_eq!(action, &serde_json::to_value("CREATE").unwrap(), "result: {:?}", res);
	assert_eq!(
		result["id"].as_str().ok_or(TestError::AssertionError {
			message: format!("missing id, res: {:?}", res).to_string(),
		})?,
		format!("{}:⟨{}⟩", table_name, id),
		"result: {:?}",
		res
	);
	assert_eq!(
		result["name"].as_str().unwrap(),
		serde_json::to_value("ok").unwrap(),
		"result: {:?}",
		res
	);

	Ok(())
}

#[test(tokio::test)]
#[serial]
async fn let_and_set() -> Result<(), Box<dyn std::error::Error>> {
	let (addr, _server) = common::start_server(true, false, true).await.unwrap();
	let socket = &mut common::connect_ws(&addr).await?;

	//
	// Prepare the connection
	//
	let res = common::ws_use(socket, Some("N"), Some("D")).await;
	assert!(res.is_ok(), "result: {:?}", res);

	// Define variable using let
	let res = common::ws_send_msg(
		socket,
		serde_json::to_string(&json!({
			"id": "1",
			"method": "let",
			"params": [
				"let_var", "let_value",
			],
		}))
		.unwrap(),
	)
	.await;
	assert!(res.is_ok(), "result: {:?}", res);

	// Define variable using set
	let res = common::ws_send_msg(
		socket,
		serde_json::to_string(&json!({
			"id": "1",
			"method": "set",
			"params": [
				"set_var", "set_value",
			],
		}))
		.unwrap(),
	)
	.await;
	assert!(res.is_ok(), "result: {:?}", res);

	// Verify the variables are set
	let res = common::ws_query(socket, "SELECT * FROM $let_var, $set_var").await?;
	assert_eq!(
		res[0]["result"],
		serde_json::to_value(["let_value", "set_value"]).unwrap(),
		"result: {:?}",
		res
	);
	Ok(())
}

#[test(tokio::test)]
#[serial]
async fn unset() -> Result<(), Box<dyn std::error::Error>> {
	let (addr, _server) = common::start_server(true, false, true).await.unwrap();
	let socket = &mut common::connect_ws(&addr).await?;

	//
	// Prepare the connection
	//
	let res = common::ws_use(socket, Some("N"), Some("D")).await;
	assert!(res.is_ok(), "result: {:?}", res);

	// Define variable
	let res = common::ws_send_msg(
		socket,
		serde_json::to_string(&json!({
			"id": "1",
			"method": "let",
			"params": [
				"let_var", "let_value",
			],
		}))
		.unwrap(),
	)
	.await;
	assert!(res.is_ok(), "result: {:?}", res);

	// Verify the variable is set
	let res = common::ws_query(socket, "SELECT * FROM $let_var").await;
	assert!(res.is_ok(), "result: {:?}", res);
	let res = res.unwrap();
	assert!(res[0]["result"].is_array(), "result: {:?}", res);
	let res = res[0]["result"].as_array().unwrap();

	assert_eq!(res[0], "let_value", "result: {:?}", res);

	// Unset variable
	let res = common::ws_send_msg(
		socket,
		serde_json::to_string(&json!({
			"id": "1",
			"method": "unset",
			"params": [
				"let_var",
			],
		}))
		.unwrap(),
	)
	.await;
	assert!(res.is_ok(), "result: {:?}", res);

	// Verify the variable is unset
	let res = common::ws_query(socket, "SELECT * FROM $let_var").await;
	assert!(res.is_ok(), "result: {:?}", res);
	let res = res.unwrap();
	assert!(res[0]["result"].is_array(), "result: {:?}", res);
	let res = res[0]["result"].as_array().unwrap();

	assert!(res[0].is_null(), "result: {:?}", res);

	Ok(())
}

#[test(tokio::test)]
#[serial]
async fn select() -> Result<(), Box<dyn std::error::Error>> {
	let (addr, _server) = common::start_server(true, false, true).await.unwrap();
	let socket = &mut common::connect_ws(&addr).await?;

	//
	// Prepare the connection
	//
	let res = common::ws_signin(socket, USER, PASS, None, None, None).await;
	assert!(res.is_ok(), "result: {:?}", res);
	let res = common::ws_use(socket, Some("N"), Some("D")).await;
	assert!(res.is_ok(), "result: {:?}", res);

	//
	// Setup the database
	//
	let res = common::ws_query(socket, "CREATE foo").await;
	assert!(res.is_ok(), "result: {:?}", res);

	// Select data
	let res = common::ws_send_msg(
		socket,
		serde_json::to_string(&json!({
			"id": "1",
			"method": "select",
			"params": [
				"foo",
			],
		}))
		.unwrap(),
	)
	.await;
	assert!(res.is_ok(), "result: {:?}", res);
	let res = res.unwrap();
	assert!(res["result"].is_array(), "result: {:?}", res);
	let res = res["result"].as_array().unwrap();

	// Verify the response contains the output of the select
	assert_eq!(res.len(), 1, "result: {:?}", res);

	Ok(())
}

#[test(tokio::test)]
#[serial]
async fn insert() -> Result<(), Box<dyn std::error::Error>> {
	let (addr, _server) = common::start_server(true, false, true).await.unwrap();
	let socket = &mut common::connect_ws(&addr).await?;

	//
	// Prepare the connection
	//
	let res = common::ws_signin(socket, USER, PASS, None, None, None).await;
	assert!(res.is_ok(), "result: {:?}", res);
	let res = common::ws_use(socket, Some("N"), Some("D")).await;
	assert!(res.is_ok(), "result: {:?}", res);

	// Insert data
	let res = common::ws_send_msg(
		socket,
		serde_json::to_string(&json!({
			"id": "1",
			"method": "insert",
			"params": [
				"table",
				{
					"name": "foo",
					"value": "bar",
				}
			],
		}))
		.unwrap(),
	)
	.await;
	assert!(res.is_ok(), "result: {:?}", res);

	// Verify the data was inserted and can be queried
	let res = common::ws_query(socket, "SELECT * FROM table").await;
	assert!(res.is_ok(), "result: {:?}", res);
	let res = res.unwrap();
	assert!(res[0]["result"].is_array(), "result: {:?}", res);
	let res = res[0]["result"].as_array().unwrap();

	assert_eq!(res[0]["name"], "foo", "result: {:?}", res);
	assert_eq!(res[0]["value"], "bar", "result: {:?}", res);

	Ok(())
}

#[test(tokio::test)]
#[serial]
async fn create() -> Result<(), Box<dyn std::error::Error>> {
	let (addr, _server) = common::start_server(true, false, true).await.unwrap();
	let socket = &mut common::connect_ws(&addr).await?;

	//
	// Prepare the connection
	//
	let res = common::ws_signin(socket, USER, PASS, None, None, None).await;
	assert!(res.is_ok(), "result: {:?}", res);
	let res = common::ws_use(socket, Some("N"), Some("D")).await;
	assert!(res.is_ok(), "result: {:?}", res);

	// Insert data
	let res = common::ws_send_msg(
		socket,
		serde_json::to_string(&json!({
			"id": "1",
			"method": "create",
			"params": [
				"table",
			],
		}))
		.unwrap(),
	)
	.await;
	assert!(res.is_ok(), "result: {:?}", res);

	// Verify the data was created and can be queried
	let res = common::ws_query(socket, "SELECT * FROM table").await;
	assert!(res.is_ok(), "result: {:?}", res);
	let res = res.unwrap();
	assert!(res[0]["result"].is_array(), "result: {:?}", res);
	let res = res[0]["result"].as_array().unwrap();
	assert_eq!(res.len(), 1, "result: {:?}", res);

	Ok(())
}

#[test(tokio::test)]
#[serial]
async fn update() -> Result<(), Box<dyn std::error::Error>> {
	let (addr, _server) = common::start_server(true, false, true).await.unwrap();
	let socket = &mut common::connect_ws(&addr).await?;

	//
	// Prepare the connection
	//
	let res = common::ws_signin(socket, USER, PASS, None, None, None).await;
	assert!(res.is_ok(), "result: {:?}", res);
	let res = common::ws_use(socket, Some("N"), Some("D")).await;
	assert!(res.is_ok(), "result: {:?}", res);

	//
	// Setup the database
	//
	let res = common::ws_query(socket, r#"CREATE table SET name = "foo""#).await;
	assert!(res.is_ok(), "result: {:?}", res);

	// Insert data
	let res = common::ws_send_msg(
		socket,
		serde_json::to_string(&json!({
			"id": "1",
			"method": "update",
			"params": [
				"table",
				{
					"value": "bar",
				}
			],
		}))
		.unwrap(),
	)
	.await;
	assert!(res.is_ok(), "result: {:?}", res);

	// Verify the data was updated
	let res = common::ws_query(socket, "SELECT * FROM table").await;
	assert!(res.is_ok(), "result: {:?}", res);
	let res = res.unwrap();
	assert!(res[0]["result"].is_array(), "result: {:?}", res);
	let res = &res[0]["result"].as_array().unwrap()[0];
	assert!(res["name"].is_null(), "result: {:?}", res);
	assert_eq!(res["value"], "bar", "result: {:?}", res);

	Ok(())
}

#[test(tokio::test)]
#[serial]
async fn change_and_merge() -> Result<(), Box<dyn std::error::Error>> {
	let (addr, _server) = common::start_server(true, false, true).await.unwrap();
	let socket = &mut common::connect_ws(&addr).await?;

	//
	// Prepare the connection
	//
	let res = common::ws_signin(socket, USER, PASS, None, None, None).await;
	assert!(res.is_ok(), "result: {:?}", res);
	let res = common::ws_use(socket, Some("N"), Some("D")).await;
	assert!(res.is_ok(), "result: {:?}", res);

	//
	// Setup the database
	//
	let res = common::ws_query(socket, "CREATE foo").await;
	assert!(res.is_ok(), "result: {:?}", res);
	let res = res.unwrap();
	assert!(res[0]["result"].is_array(), "result: {:?}", res);
	let res = &res[0]["result"].as_array().unwrap()[0];

	assert!(res["id"].is_string(), "result: {:?}", res);
	let what = res["id"].as_str().unwrap();

	//
	// Change / Marge data
	//

	let res = common::ws_send_msg(
		socket,
		serde_json::to_string(&json!({
		"id": "1",
		"method": "change",
		"params": [
			what, {
				"name_for_change": "foo",
				"value_for_change": "bar",
			}
			]
		}))
		.unwrap(),
	)
	.await;
	assert!(res.is_ok(), "result: {:?}", res);

	let res = common::ws_send_msg(
		socket,
		serde_json::to_string(&json!({
			"id": "1",
			"method": "merge",
			"params": [
				what, {
					"name_for_merge": "foo",
					"value_for_merge": "bar",
				}
			]
		}))
		.unwrap(),
	)
	.await;
	assert!(res.is_ok(), "result: {:?}", res);

	//
	// Get the data and verify the output
	//
	let res = common::ws_query(socket, &format!("SELECT * FROM foo WHERE id = {what}")).await;
	assert!(res.is_ok(), "result: {:?}", res);
	let res = res.unwrap();
	assert!(res[0]["result"].is_array(), "result: {:?}", res);
	let res = &res[0]["result"].as_array().unwrap()[0];

	assert_eq!(res["id"], what);
	assert_eq!(res["name_for_change"], "foo");
	assert_eq!(res["value_for_change"], "bar");
	assert_eq!(res["name_for_merge"], "foo");
	assert_eq!(res["value_for_merge"], "bar");

	Ok(())
}

#[test(tokio::test)]
#[serial]
async fn modify_and_patch() -> Result<(), Box<dyn std::error::Error>> {
	let (addr, _server) = common::start_server(true, false, true).await.unwrap();
	let socket = &mut common::connect_ws(&addr).await?;

	//
	// Prepare the connection
	//
	let res = common::ws_signin(socket, USER, PASS, None, None, None).await;
	assert!(res.is_ok(), "result: {:?}", res);
	let res = common::ws_use(socket, Some("N"), Some("D")).await;
	assert!(res.is_ok(), "result: {:?}", res);

	//
	// Setup the database
	//
	let res =
		common::ws_query(socket, r#"CREATE table SET original_name = "oritinal_value""#).await;
	assert!(res.is_ok(), "result: {:?}", res);
	let res = res.unwrap();
	assert!(res[0]["result"].is_array(), "result: {:?}", res);
	let res = &res[0]["result"].as_array().unwrap()[0];

	let what = res["id"].as_str().unwrap();

	//
	// Modify data
	//

	let ops = json!([
		{
			"op": "add",
			"path": "modify_name",
			"value": "modify_value"
		},
		{
			"op": "remove",
			"path": "original_name",
		}
	]);
	let res = common::ws_send_msg(
		socket,
		serde_json::to_string(&json!({
			"id": "1",
			"method": "modify",
			"params": [
				what, ops
			]
		}))
		.unwrap(),
	)
	.await;
	assert!(res.is_ok(), "result: {:?}", res);
	let res = res.unwrap();
	assert!(res["result"].is_array(), "result: {:?}", res);
	let res = res["result"].as_array().unwrap();
	assert_eq!(res.len(), ops.as_array().unwrap().len(), "result: {:?}", res);

	//
	// Patch data
	//

	let ops = json!([
		{
			"op": "add",
			"path": "patch_name",
			"value": "patch_value"
		}
	]);
	let res = common::ws_send_msg(
		socket,
		serde_json::to_string(&json!({
			"id": "1",
			"method": "patch",
			"params": [
				what, ops
			]
		}))
		.unwrap(),
	)
	.await;
	assert!(res.is_ok(), "result: {:?}", res);
	let res = res.unwrap();
	assert!(res["result"].is_array(), "result: {:?}", res);
	let res = res["result"].as_array().unwrap();
	assert_eq!(res.len(), ops.as_array().unwrap().len(), "result: {:?}", res);

	//
	// Get the data and verify the output
	//
	let res = common::ws_query(socket, &format!("SELECT * FROM table WHERE id = {what}")).await;
	assert!(res.is_ok(), "result: {:?}", res);
	let res = res.unwrap();
	assert!(res[0]["result"].is_array(), "result: {:?}", res);
	let res = &res[0]["result"].as_array().unwrap()[0];

	assert_eq!(res["id"], what);
	assert!(res["original_name"].is_null());
	assert_eq!(res["modify_name"], "modify_value");
	assert_eq!(res["patch_name"], "patch_value");

	Ok(())
}

#[test(tokio::test)]
#[serial]
async fn delete() -> Result<(), Box<dyn std::error::Error>> {
	let (addr, _server) = common::start_server(true, false, true).await.unwrap();
	let socket = &mut common::connect_ws(&addr).await?;

	//
	// Prepare the connection
	//
	let res = common::ws_signin(socket, USER, PASS, None, None, None).await;
	assert!(res.is_ok(), "result: {:?}", res);
	let res = common::ws_use(socket, Some("N"), Some("D")).await;
	assert!(res.is_ok(), "result: {:?}", res);

	//
	// Setup the database
	//
	let res = common::ws_query(socket, "CREATE table:id").await;
	assert!(res.is_ok(), "result: {:?}", res);

	//
	// Verify the data was created and can be queried
	//
	let res = common::ws_query(socket, "SELECT * FROM table:id").await;
	assert!(res.is_ok(), "result: {:?}", res);
	let res = res.unwrap();
	assert!(res[0]["result"].is_array(), "result: {:?}", res);
	let res = res[0]["result"].as_array().unwrap();

	assert_eq!(res.len(), 1, "result: {:?}", res);

	//
	// Delete data
	//
	let res = common::ws_send_msg(
		socket,
		serde_json::to_string(&json!({
			"id": "1",
			"method": "delete",
			"params": [
				"table:id"
			]
		}))
		.unwrap(),
	)
	.await;
	assert!(res.is_ok(), "result: {:?}", res);

	//
	// Verify the data was deleted
	//
	let res = common::ws_query(socket, "SELECT * FROM table:id").await;
	assert!(res.is_ok(), "result: {:?}", res);
	let res = res.unwrap();
	assert!(res[0]["result"].is_array(), "result: {:?}", res);
	let res = res[0]["result"].as_array().unwrap();

	assert_eq!(res.len(), 0, "result: {:?}", res);

	Ok(())
}

#[test(tokio::test)]
#[serial]
async fn format_json() -> Result<(), Box<dyn std::error::Error>> {
	let (addr, _server) = common::start_server(true, false, true).await.unwrap();
	let socket = &mut common::connect_ws(&addr).await?;

	//
	// Prepare the connection
	//
	let res = common::ws_signin(socket, USER, PASS, None, None, None).await;
	assert!(res.is_ok(), "result: {:?}", res);
	let res = common::ws_use(socket, Some("N"), Some("D")).await;
	assert!(res.is_ok(), "result: {:?}", res);

	//
	// Setup the database
	//
	let res = common::ws_query(socket, "CREATE table:id").await;
	assert!(res.is_ok(), "result: {:?}", res);

	//
	// Test JSON format
	//

	// Change format
	let msg = json!({
		"id": "1",
		"method": "format",
		"params": [
			"json"
		]
	});
	let res = common::ws_send_msg(socket, serde_json::to_string(&msg).unwrap()).await;
	assert!(res.is_ok(), "result: {:?}", res);

	// Query data
	let res = common::ws_query(socket, "SELECT * FROM table:id").await;
	assert!(res.is_ok(), "result: {:?}", res);
	let res = res.unwrap();
	assert!(res[0]["result"].is_array(), "result: {:?}", res);
	let res = res[0]["result"].as_array().unwrap();
	assert_eq!(res.len(), 1, "result: {:?}", res);

	Ok(())
}

#[test(tokio::test)]
#[serial]
async fn format_cbor() -> Result<(), Box<dyn std::error::Error>> {
	let (addr, _server) = common::start_server(true, false, true).await.unwrap();
	let socket = &mut common::connect_ws(&addr).await?;

	//
	// Prepare the connection
	//
	let res = common::ws_signin(socket, USER, PASS, None, None, None).await;
	assert!(res.is_ok(), "result: {:?}", res);
	let res = common::ws_use(socket, Some("N"), Some("D")).await;
	assert!(res.is_ok(), "result: {:?}", res);

	//
	// Setup the database
	//
	let res = common::ws_query(socket, "CREATE table:id").await;
	assert!(res.is_ok(), "result: {:?}", res);

	//
	// Test CBOR format
	//

	// Change format
	let msg = serde_json::to_string(&json!({
		"id": "1",
		"method": "format",
		"params": [
			"cbor"
		]
	}))
	.unwrap();
	let res = common::ws_send_msg(socket, msg).await;
	assert!(res.is_ok(), "result: {:?}", res);

	// Query data
	let msg = serde_json::to_string(&json!({
		"id": "1",
		"method": "query",
		"params": [
			"SELECT * FROM table:id"
		]
	}))
	.unwrap();

	let res = common::ws_send_msg_with_fmt(socket, msg, common::Format::Cbor).await;
	assert!(res.is_ok(), "result: {:?}", res);
	let res = res.unwrap();
	assert!(res["result"].is_array(), "result: {:?}", res);
	let res = res["result"].as_array().unwrap();
	assert!(res[0]["result"].is_array(), "result: {:?}", res);
	let res = res[0]["result"].as_array().unwrap();
	assert_eq!(res.len(), 1, "result: {:?}", res);

	Ok(())
}

#[test(tokio::test)]
#[serial]
async fn format_pack() -> Result<(), Box<dyn std::error::Error>> {
	let (addr, _server) = common::start_server(true, false, true).await.unwrap();
	let socket = &mut common::connect_ws(&addr).await?;

	//
	// Prepare the connection
	//
	let res = common::ws_signin(socket, USER, PASS, None, None, None).await;
	assert!(res.is_ok(), "result: {:?}", res);
	let res = common::ws_use(socket, Some("N"), Some("D")).await;
	assert!(res.is_ok(), "result: {:?}", res);

	//
	// Setup the database
	//
	let res = common::ws_query(socket, "CREATE table:id").await;
	assert!(res.is_ok(), "result: {:?}", res);

	//
	// Test PACK format
	//

	// Change format
	let msg = serde_json::to_string(&json!({
		"id": "1",
		"method": "format",
		"params": [
			"pack"
		]
	}))
	.unwrap();
	let res = common::ws_send_msg(socket, msg).await;
	assert!(res.is_ok(), "result: {:?}", res);

	// Query data
	let msg = serde_json::to_string(&json!({
		"id": "1",
		"method": "query",
		"params": [
			"SELECT * FROM table:id"
		]
	}))
	.unwrap();

	let res = common::ws_send_msg_with_fmt(socket, msg, common::Format::Pack).await;
	assert!(res.is_ok(), "result: {:?}", res);
	let res = res.unwrap();
	assert!(res["result"].is_array(), "result: {:?}", res);
	let res = res["result"].as_array().unwrap();
	assert!(res[0]["result"].is_array(), "result: {:?}", res);
	let res = res[0]["result"].as_array().unwrap();
	assert_eq!(res.len(), 1, "result: {:?}", res);

	Ok(())
}

#[test(tokio::test)]
#[serial]
async fn query() -> Result<(), Box<dyn std::error::Error>> {
	let (addr, _server) = common::start_server(true, false, true).await.unwrap();
	let socket = &mut common::connect_ws(&addr).await?;

	//
	// Prepare the connection
	//
	let res = common::ws_signin(socket, USER, PASS, None, None, None).await;
	assert!(res.is_ok(), "result: {:?}", res);
	let res = common::ws_use(socket, Some("N"), Some("D")).await;
	assert!(res.is_ok(), "result: {:?}", res);

	//
	// Run a CREATE query
	//
	let res = common::ws_send_msg(
		socket,
		serde_json::to_string(&json!({
			"id": "1",
			"method": "query",
			"params": [
				"CREATE foo",
			]
		}))
		.unwrap(),
	)
	.await;
	assert!(res.is_ok(), "result: {:?}", res);

	//
	// Verify the data was created and can be queried
	//
	let res = common::ws_query(socket, "SELECT * FROM foo").await;
	assert!(res.is_ok(), "result: {:?}", res);
	let res = res.unwrap();
	assert!(res[0]["result"].is_array(), "result: {:?}", res);
	let res = res[0]["result"].as_array().unwrap();
	assert_eq!(res.len(), 1, "result: {:?}", res);

	Ok(())
}

#[test(tokio::test)]
#[serial]
async fn version() -> Result<(), Box<dyn std::error::Error>> {
	let (addr, _server) = common::start_server(true, false, true).await.unwrap();
	let socket = &mut common::connect_ws(&addr).await?;

	// Send command
	let res = common::ws_send_msg(
		socket,
		serde_json::to_string(&json!({
			"id": "1",
			"method": "version",
			"params": [],
		}))
		.unwrap(),
	)
	.await;
	assert!(res.is_ok(), "result: {:?}", res);
	let res = res.unwrap();
	assert!(res["result"].is_string(), "result: {:?}", res);
	let res = res["result"].as_str().unwrap();

	// Verify response
	assert!(res.starts_with("surrealdb-"), "result: {}", res);
	Ok(())
}
