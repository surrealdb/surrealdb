// cargo test --package surreal --bin surreal --no-default-features --features storage-mem --test ws_integration -- --nocapture

mod common;

use serde_json::json;
use serial_test::serial;
use tokio_tungstenite::tungstenite::Message;

use crate::common::{PASS, USER};

#[tokio::test]
#[serial]
async fn ping() -> Result<(), Box<dyn std::error::Error>> {
	let (addr, _server) = common::start_server(false, true).await.unwrap();
	let socket = &mut common::connect_ws(&addr).await?;

	// Send command
	let res = common::ws_send_msg(
		socket,
		Message::Text(
			serde_json::to_string(&json!({
				"id": "1",
				"method": "ping",
			}))
			.unwrap(),
		),
	)
	.await?;
	assert!(res.as_object().unwrap().keys().eq(["id", "result"]), "result: {}", res);

	Ok(())
}

#[tokio::test]
#[serial]
async fn info() -> Result<(), Box<dyn std::error::Error>> {
	let (addr, _server) = common::start_server(false, true).await.unwrap();
	let socket = &mut common::connect_ws(&addr).await?;

	//
	// Prepare the connection
	//
	let _ = common::ws_signin(socket, USER, PASS, None, None, None).await?;
	let _ = common::ws_use(socket, Some("N"), Some("D")).await?;

	// Send command
	let res = common::ws_send_msg(
		socket,
		Message::Text(
			serde_json::to_string(&json!({
				"id": "1",
				"method": "info",
			}))
			.unwrap(),
		),
	)
	.await?;

	todo!("verify response");
}

#[tokio::test]
#[serial]
async fn signup() -> Result<(), Box<dyn std::error::Error>> {
	let (addr, _server) = common::start_server(false, true).await.unwrap();
	let socket = &mut common::connect_ws(&addr).await?;

	//
	// Prepare the connection
	//
	let _ = common::ws_signin(socket, USER, PASS, None, None, None).await?;
	let _ = common::ws_use(socket, Some("N"), Some("D")).await?;

	// Setup scope
	let _ = common::ws_query(socket, r#"
        DEFINE SCOPE scope SESSION 24h
            SIGNUP ( CREATE user SET email = $email, pass = crypto::argon2::generate($pass) )
            SIGNIN ( SELECT * FROM user WHERE email = $email AND crypto::argon2::compare(pass, $pass) )
        ;"#).await?;

	// Signup
	let res = common::ws_send_msg(
		socket,
		Message::Text(
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
		),
	)
	.await?;

	// Verify response contains no error
	assert!(res.as_object().unwrap().keys().eq(["id", "result"]), "result: {}", res);
	// Verify it returns a token
	assert!(
		res["result"].as_str().unwrap().starts_with("eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9"),
		"result: {}",
		res
	);
	Ok(())
}

#[tokio::test]
#[serial]
async fn signin() -> Result<(), Box<dyn std::error::Error>> {
	let (addr, _server) = common::start_server(false, true).await.unwrap();
	let socket = &mut common::connect_ws(&addr).await?;

	//
	// Prepare the connection
	//
	let _ = common::ws_signin(socket, USER, PASS, None, None, None).await?;
	let _ = common::ws_use(socket, Some("N"), Some("D")).await?;

	// Setup scope
	let _ = common::ws_query(socket, r#"
        DEFINE SCOPE scope SESSION 24h
            SIGNUP ( CREATE user SET email = $email, pass = crypto::argon2::generate($pass) )
            SIGNIN ( SELECT * FROM user WHERE email = $email AND crypto::argon2::compare(pass, $pass) )
        ;"#).await?;

	// Signup
	let res = common::ws_send_msg(
		socket,
		Message::Text(
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
		),
	)
	.await?;
	assert!(res.as_object().unwrap().keys().eq(["id", "result"]), "result: {}", res);

	// Verify response contains no error
	let res = common::ws_send_msg(
		socket,
		Message::Text(
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
		),
	)
	.await?;

	// Verify it returns a token
	assert!(
		res["result"].as_str().unwrap().starts_with("eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9"),
		"result: {}",
		res
	);

	Ok(())
}

#[tokio::test]
#[serial]
async fn invalidate() -> Result<(), Box<dyn std::error::Error>> {
	let (addr, _server) = common::start_server(false, true).await.unwrap();
	let socket = &mut common::connect_ws(&addr).await?;

	//
	// Prepare the connection
	//
	let _ = common::ws_signin(socket, USER, PASS, None, None, None).await?;

	// Verify we have a ROOT session
	let _ = common::ws_query(socket, "DEFINE NAMESPACE NS").await?;

	// Invalidate session
	let res = common::ws_send_msg(
		socket,
		Message::Text(
			serde_json::to_string(&json!({
				"id": "1",
				"method": "invalidate",
			}))
			.unwrap(),
		),
	)
	.await?;
	assert!(res.as_object().unwrap().keys().eq(["id", "result"]), "result: {}", res);

	// Verify we invalidated the root session
	let res = common::ws_query(socket, "DEFINE NAMESPACE NS2").await?;
	assert_eq!(
		res.first().unwrap().as_object().unwrap()["result"],
		"You don't have permission to perform this query type",
		"result: {:?}",
		res
	);
	Ok(())
}

#[tokio::test]
#[serial]
async fn authenticate() -> Result<(), Box<dyn std::error::Error>> {
	let (addr, _server) = common::start_server(false, true).await.unwrap();
	let socket = &mut common::connect_ws(&addr).await?;

	//
	// Authenticate with the user and password
	//
	let token = common::ws_signin(socket, USER, PASS, None, None, None).await?;
	// Reconnect so we start with an empty session
	socket.close(None).await?;
	let socket = &mut common::connect_ws(&addr).await?;

	//
	// Authenticate with the token
	//

	// Send command
	let _ = common::ws_send_msg(
		socket,
		Message::Text(
			serde_json::to_string(&json!({
				"id": "1",
				"method": "authenticate",
				"params": {
					"token": token,
				},
			}))
			.unwrap(),
		),
	)
	.await?;

	todo!("verify response");
}

#[tokio::test]
#[serial]
async fn kill() -> Result<(), Box<dyn std::error::Error>> {
	todo!()
}

#[tokio::test]
#[serial]
async fn live() -> Result<(), Box<dyn std::error::Error>> {
	todo!()
}

#[tokio::test]
#[serial]
async fn let_and_set() -> Result<(), Box<dyn std::error::Error>> {
	let (addr, _server) = common::start_server(false, true).await.unwrap();
	let socket = &mut common::connect_ws(&addr).await?;

	//
	// Prepare the connection
	//
	let _ = common::ws_use(socket, Some("N"), Some("D")).await?;

	// Define variable using let
	let res = common::ws_send_msg(
		socket,
		Message::Text(
			serde_json::to_string(&json!({
				"id": "1",
				"method": "let",
				"params": [
					"let_var", "let_value",
				],
			}))
			.unwrap(),
		),
	)
	.await?;
	assert!(res.as_object().unwrap().keys().eq(["id", "result"]), "result: {}", res);

	// Define variable using set
	let res = common::ws_send_msg(
		socket,
		Message::Text(
			serde_json::to_string(&json!({
				"id": "1",
				"method": "set",
				"params": [
					"set_var", "set_value",
				],
			}))
			.unwrap(),
		),
	)
	.await?;
	assert!(res.as_object().unwrap().keys().eq(["id", "result"]), "result: {}", res);

	// Verify the variables are set
	let res = common::ws_query(socket, "SELECT * FROM $let_var, $set_var").await?;
	assert_eq!(
		res[0].as_object().unwrap()["result"],
		serde_json::to_value(["let_value", "set_value"]).unwrap(),
		"result: {:?}",
		res
	);
	Ok(())
}

#[tokio::test]
#[serial]
async fn unset() -> Result<(), Box<dyn std::error::Error>> {
	let (addr, _server) = common::start_server(false, true).await.unwrap();
	let socket = &mut common::connect_ws(&addr).await?;

	//
	// Prepare the connection
	//
	let _ = common::ws_use(socket, Some("N"), Some("D")).await?;

	// Define variable
	let res = common::ws_send_msg(
		socket,
		Message::Text(
			serde_json::to_string(&json!({
				"id": "1",
				"method": "let",
				"params": [
					"let_var", "let_value",
				],
			}))
			.unwrap(),
		),
	)
	.await?;
	assert!(res.as_object().unwrap().keys().eq(["id", "result"]), "result: {}", res);

	// Verify the variable is set
	let res = common::ws_query(socket, "SELECT * FROM $let_var").await?;
	assert_eq!(
		res[0].as_object().unwrap()["result"].as_array().unwrap()[0],
		"let_value",
		"result: {:?}",
		res
	);

	// Unset variable
	let res = common::ws_send_msg(
		socket,
		Message::Text(
			serde_json::to_string(&json!({
				"id": "1",
				"method": "unset",
				"params": [
					"let_var",
				],
			}))
			.unwrap(),
		),
	)
	.await?;
	assert!(res.as_object().unwrap().keys().eq(["id", "result"]), "result: {}", res);

	// Verify the variable is unset
	let res = common::ws_query(socket, "SELECT * FROM $let_var").await?;
	assert!(
		res[0].as_object().unwrap()["result"].as_array().unwrap()[0].is_null(),
		"result: {:?}",
		res
	);

	Ok(())
}

#[tokio::test]
#[serial]
async fn select() -> Result<(), Box<dyn std::error::Error>> {
	let (addr, _server) = common::start_server(false, true).await.unwrap();
	let socket = &mut common::connect_ws(&addr).await?;

	//
	// Prepare the connection
	//
	let _ = common::ws_signin(socket, USER, PASS, None, None, None).await?;
	let _ = common::ws_use(socket, Some("N"), Some("D")).await?;

	//
	// Setup the database
	//
	let _ = common::ws_query(socket, "CREATE foo").await?;

	// Select data
	let res = common::ws_send_msg(
		socket,
		Message::Text(
			serde_json::to_string(&json!({
				"id": "1",
				"method": "select",
				"params": [
					"foo",
				],
			}))
			.unwrap(),
		),
	)
	.await?;
	assert!(res.as_object().unwrap().keys().eq(["id", "result"]), "result: {}", res);

	// Verify the response contains the output of the select
	assert_eq!(
		res.as_object().unwrap()["result"].as_array().unwrap().len(),
		1,
		"result: {:?}",
		res
	);

	Ok(())
}

#[tokio::test]
#[serial]
async fn insert() -> Result<(), Box<dyn std::error::Error>> {
	let (addr, _server) = common::start_server(false, true).await.unwrap();
	let socket = &mut common::connect_ws(&addr).await?;

	//
	// Prepare the connection
	//
	let _ = common::ws_signin(socket, USER, PASS, None, None, None).await?;
	let _ = common::ws_use(socket, Some("N"), Some("D")).await?;

	// Insert data
	let res = common::ws_send_msg(
		socket,
		Message::Text(
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
		),
	)
	.await?;
	assert!(res.as_object().unwrap().keys().eq(["id", "result"]), "result: {}", res);

	// Verify the data was inserted and can be queried
	let res = common::ws_query(socket, "SELECT * FROM table").await?;
	let res_obj = res[0].as_object().unwrap()["result"].as_array().unwrap()[0].clone();
	assert_eq!(res_obj["name"], "foo", "result: {:?}", res);
	assert_eq!(res_obj["value"], "bar", "result: {:?}", res);

	Ok(())
}

#[tokio::test]
#[serial]
async fn create() -> Result<(), Box<dyn std::error::Error>> {
	let (addr, _server) = common::start_server(false, true).await.unwrap();
	let socket = &mut common::connect_ws(&addr).await?;

	//
	// Prepare the connection
	//
	let _ = common::ws_signin(socket, USER, PASS, None, None, None).await?;
	let _ = common::ws_use(socket, Some("N"), Some("D")).await?;

	// Insert data
	let res = common::ws_send_msg(
		socket,
		Message::Text(
			serde_json::to_string(&json!({
				"id": "1",
				"method": "create",
				"params": [
					"table",
				],
			}))
			.unwrap(),
		),
	)
	.await?;
	assert!(res.as_object().unwrap().keys().eq(["id", "result"]), "result: {}", res);

	// Verify the data was created and can be queried
	let res = common::ws_query(socket, "SELECT * FROM table").await?;
	assert_eq!(
		res[0].as_object().unwrap()["result"].as_array().unwrap().len(),
		1,
		"result: {:?}",
		res
	);

	Ok(())
}

#[tokio::test]
#[serial]
async fn update() -> Result<(), Box<dyn std::error::Error>> {
	let (addr, _server) = common::start_server(false, true).await.unwrap();
	let socket = &mut common::connect_ws(&addr).await?;

	//
	// Prepare the connection
	//
	let _ = common::ws_signin(socket, USER, PASS, None, None, None).await?;
	let _ = common::ws_use(socket, Some("N"), Some("D")).await?;

	//
	// Setup the database
	//
	let _ = common::ws_query(socket, r#"CREATE table SET name = "foo""#).await?;

	// Insert data
	let res = common::ws_send_msg(
		socket,
		Message::Text(
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
		),
	)
	.await?;
	assert!(res.as_object().unwrap().keys().eq(["id", "result"]), "result: {}", res);

	// Verify the data was updated
	let res = common::ws_query(socket, "SELECT * FROM table").await?;
	let res_obj = res[0].as_object().unwrap()["result"].as_array().unwrap()[0].clone();
	assert!(res_obj["name"].is_null(), "result: {:?}", res);
	assert_eq!(res_obj["value"], "bar", "result: {:?}", res);

	Ok(())
}

#[tokio::test]
#[serial]
async fn change_and_merge() -> Result<(), Box<dyn std::error::Error>> {
	let (addr, _server) = common::start_server(false, true).await.unwrap();
	let socket = &mut common::connect_ws(&addr).await?;

	//
	// Prepare the connection
	//
	let _ = common::ws_signin(socket, USER, PASS, None, None, None).await?;
	let _ = common::ws_use(socket, Some("N"), Some("D")).await?;

	//
	// Setup the database
	//
	let result = common::ws_query(socket, "CREATE foo").await?;
	let result = &result.last().unwrap().as_object().unwrap()["result"].as_array().unwrap();
	let what = result[0]["id"].as_str().unwrap();

	//
	// Change / Marge data
	//

	let _ = common::ws_send_msg(
		socket,
		Message::Text(
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
		),
	)
	.await?;

	let _ = common::ws_send_msg(
		socket,
		Message::Text(
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
		),
	)
	.await?;

	//
	// Get the data and verify the output
	//
	let result = common::ws_query(socket, &format!("SELECT * FROM foo WHERE id = {what}")).await?;
	let result = &result.last().unwrap().as_object().unwrap()["result"].as_array().unwrap()[0]
		.as_object()
		.unwrap();
	assert_eq!(result["id"], what);
	assert_eq!(result["name_for_change"], "foo");
	assert_eq!(result["value_for_change"], "bar");
	assert_eq!(result["name_for_merge"], "foo");
	assert_eq!(result["value_for_merge"], "bar");

	Ok(())
}

#[tokio::test]
#[serial]
async fn modify_and_patch() -> Result<(), Box<dyn std::error::Error>> {
	let (addr, _server) = common::start_server(false, true).await.unwrap();
	let socket = &mut common::connect_ws(&addr).await?;

	//
	// Prepare the connection
	//
	let _ = common::ws_signin(socket, USER, PASS, None, None, None).await?;
	let _ = common::ws_use(socket, Some("N"), Some("D")).await?;

	//
	// Setup the database
	//
	let result =
		common::ws_query(socket, r#"CREATE table SET original_name = "oritinal_value""#).await?;
	let result = &result.last().unwrap().as_object().unwrap()["result"].as_array().unwrap();
	let what = result[0]["id"].as_str().unwrap();

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
		Message::Text(
			serde_json::to_string(&json!({
				"id": "1",
				"method": "modify",
				"params": [
					what, ops
				]
			}))
			.unwrap(),
		),
	)
	.await?;
	assert_eq!(
		res.as_object().unwrap()["result"].as_array().unwrap().len(),
		ops.as_array().unwrap().len(),
		"result: {}",
		res
	);

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
		Message::Text(
			serde_json::to_string(&json!({
				"id": "1",
				"method": "patch",
				"params": [
					what, ops
				]
			}))
			.unwrap(),
		),
	)
	.await?;
	assert_eq!(
		res.as_object().unwrap()["result"].as_array().unwrap().len(),
		ops.as_array().unwrap().len(),
		"result: {}",
		res
	);

	//
	// Get the data and verify the output
	//
	let result =
		common::ws_query(socket, &format!("SELECT * FROM table WHERE id = {what}")).await?;
	let result = &result.last().unwrap().as_object().unwrap()["result"].as_array().unwrap()[0]
		.as_object()
		.unwrap();
	assert_eq!(result["id"], what);
	assert!(!result.contains_key("original_name"));
	assert_eq!(result["modify_name"], "modify_value");
	assert_eq!(result["patch_name"], "patch_value");

	Ok(())
}

#[tokio::test]
#[serial]
async fn delete() -> Result<(), Box<dyn std::error::Error>> {
	let (addr, _server) = common::start_server(false, true).await.unwrap();
	let socket = &mut common::connect_ws(&addr).await?;

	//
	// Prepare the connection
	//
	let _ = common::ws_signin(socket, USER, PASS, None, None, None).await?;
	let _ = common::ws_use(socket, Some("N"), Some("D")).await?;

	//
	// Setup the database
	//
	let _ = common::ws_query(socket, "CREATE table:id").await?;

	//
	// Verify the data was created and can be queried
	//
	let res = common::ws_query(socket, "SELECT * FROM table:id").await?;
	assert_eq!(
		res[0].as_object().unwrap()["result"].as_array().unwrap().len(),
		1,
		"result: {:?}",
		res
	);

	//
	// Delete data
	//
	let _res = common::ws_send_msg(
		socket,
		Message::Text(
			serde_json::to_string(&json!({
				"id": "1",
				"method": "delete",
				"params": [
					"table:id"
				]
			}))
			.unwrap(),
		),
	)
	.await?;

	//
	// Verify the data was deleted
	//
	let res = common::ws_query(socket, "SELECT * FROM table:id").await?;
	assert_eq!(
		res[0].as_object().unwrap()["result"].as_array().unwrap().len(),
		0,
		"result: {:?}",
		res
	);

	Ok(())
}

#[tokio::test]
#[serial]
async fn format() -> Result<(), Box<dyn std::error::Error>> {
	todo!()
}

#[tokio::test]
#[serial]
async fn query() -> Result<(), Box<dyn std::error::Error>> {
	let (addr, _server) = common::start_server(false, true).await.unwrap();
	let socket = &mut common::connect_ws(&addr).await?;

	//
	// Prepare the connection
	//
	let _ = common::ws_signin(socket, USER, PASS, None, None, None).await?;
	let _ = common::ws_use(socket, Some("N"), Some("D")).await?;

	//
	// Run a CREATE query
	//
	let _ = common::ws_send_msg(
		socket,
		Message::Text(
			serde_json::to_string(&json!({
				"id": "1",
				"method": "query",
				"params": [
					"CREATE foo",
				]
			}))
			.unwrap(),
		),
	)
	.await?;

	//
	// Verify the data was created and can be queried
	//
	let res = common::ws_query(socket, "SELECT * FROM foo").await?;
	assert_eq!(
		res[0].as_object().unwrap()["result"].as_array().unwrap().len(),
		1,
		"result: {:?}",
		res
	);

	Ok(())
}

#[tokio::test]
#[serial]
async fn version() -> Result<(), Box<dyn std::error::Error>> {
	let (addr, _server) = common::start_server(false, true).await.unwrap();
	let socket = &mut common::connect_ws(&addr).await?;

	// Send command
	let result = common::ws_send_msg(
		socket,
		Message::Text(
			serde_json::to_string(&json!({
				"id": "1",
				"method": "version",
				"params": [],
			}))
			.unwrap(),
		),
	)
	.await?;

	// Verify response
	assert!(result["result"].as_str().unwrap().starts_with("surrealdb-"), "result: {}", result);
	Ok(())
}
