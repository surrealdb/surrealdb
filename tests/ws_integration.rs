// RUST_LOG=warn cargo make ci-ws-integration
mod common;

mod ws_integration {
	use std::time::Duration;

	use serde_json::json;
	use test_log::test;
	use ulid::Ulid;

	use super::common::{self, PASS, USER};
	use crate::common::error::TestError;

	#[test(tokio::test)]
	async fn ping() -> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = common::start_server_with_defaults().await.unwrap();
		let socket = &mut common::connect_ws(&addr).await?;

		// Send command
		let res = common::ws_send_msg_and_wait_response(
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
	async fn info() -> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = common::start_server_with_defaults().await.unwrap();
		let socket = &mut common::connect_ws(&addr).await?;

		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();

		//
		// Prepare the connection
		//
		let res = common::ws_signin(socket, USER, PASS, None, None, None).await;
		assert!(res.is_ok(), "result: {:?}", res);
		let res = common::ws_use(socket, Some(&ns), Some(&db)).await;
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
		let res =
			common::ws_signin(socket, "user", "pass", Some(&ns), Some(&db), Some("scope")).await;
		assert!(res.is_ok(), "result: {:?}", res);

		// Send the info command
		let res = common::ws_send_msg_and_wait_response(
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
	async fn signup() -> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = common::start_server_with_defaults().await.unwrap();
		let socket = &mut common::connect_ws(&addr).await?;

		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();

		//
		// Prepare the connection
		//
		let res = common::ws_signin(socket, USER, PASS, None, None, None).await;
		assert!(res.is_ok(), "result: {:?}", res);
		let res = common::ws_use(socket, Some(&ns), Some(&db)).await;
		assert!(res.is_ok(), "result: {:?}", res);

		// Setup scope
		let res = common::ws_query(
			socket,
			r#"
			DEFINE SCOPE scope SESSION 24h
				SIGNUP ( CREATE user SET email = $email, pass = crypto::argon2::generate($pass) )
				SIGNIN ( SELECT * FROM user WHERE email = $email AND crypto::argon2::compare(pass, $pass) )
			;"#,
		)
		.await;
		assert!(res.is_ok(), "result: {:?}", res);

		// Signup
		let res = common::ws_send_msg_and_wait_response(
			socket,
			serde_json::to_string(&json!({
				"id": "1",
				"method": "signup",
				"params": [{
					"ns": ns,
					"db": db,
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
	async fn signin() -> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = common::start_server_with_defaults().await.unwrap();
		let socket = &mut common::connect_ws(&addr).await?;

		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();

		//
		// Prepare the connection
		//
		let res = common::ws_signin(socket, USER, PASS, None, None, None).await;
		assert!(res.is_ok(), "result: {:?}", res);
		let res = common::ws_use(socket, Some(&ns), Some(&db)).await;
		assert!(res.is_ok(), "result: {:?}", res);

		// Setup scope
		let res = common::ws_query(
			socket,
			r#"
			DEFINE SCOPE scope SESSION 24h
				SIGNUP ( CREATE user SET email = $email, pass = crypto::argon2::generate($pass) )
				SIGNIN ( SELECT * FROM user WHERE email = $email AND crypto::argon2::compare(pass, $pass) )
			;"#,
		)
		.await;
		assert!(res.is_ok(), "result: {:?}", res);

		// Signup
		let res = common::ws_send_msg_and_wait_response(
			socket,
			serde_json::to_string(&json!({
				"id": "1",
				"method": "signup",
				"params": [{
					"ns": ns,
					"db": db,
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
		let res = common::ws_send_msg_and_wait_response(
			socket,
			serde_json::to_string(&json!({
				"id": "1",
				"method": "signin",
				"params": [{
					"ns": ns,
					"db": db,
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
	async fn variable_auth_live_query() -> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = common::start_server_with_defaults().await.unwrap();
		let socket = &mut common::connect_ws(&addr).await?;

		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();

		//
		// Prepare the connection
		//
		let res = common::ws_signin(socket, USER, PASS, None, None, None).await;
		assert!(res.is_ok(), "result: {:?}", res);
		let res = common::ws_use(socket, Some(&ns), Some(&db)).await;
		assert!(res.is_ok(), "result: {:?}", res);

		// Setup scope
		let res = common::ws_query(socket, r#"
        DEFINE SCOPE scope SESSION 2s
            SIGNUP ( CREATE user SET user = $user, pass = crypto::argon2::generate($pass) )
            SIGNIN ( SELECT * FROM user WHERE user = $user AND crypto::argon2::compare(pass, $pass) )
        ;"#).await;
		assert!(res.is_ok(), "result: {:?}", res);

		// Signup
		let res = common::ws_send_msg_and_wait_response(
			socket,
			serde_json::to_string(&json!({
				"id": "1",
				"method": "signup",
				"params": [{
					"ns": ns,
					"db": db,
					"sc": "scope",
					"user": "user",
					"pass": "pass",
				}],
			}))
			.unwrap(),
		)
		.await;
		assert!(res.is_ok(), "result: {:?}", res);

		// Sign in
		let res =
			common::ws_signin(socket, "user", "pass", Some(&ns), Some(&db), Some("scope")).await;
		assert!(res.is_ok(), "result: {:?}", res);
		let res = res.unwrap();

		// Start Live Query
		let table_name = "test_tableBB4B0A788C7E46E798720AEF938CBCF6";
		let _live_query_response = common::ws_send_msg_and_wait_response(
			socket,
			serde_json::to_string(&json!({
					"id": "66BB05C8-EF4B-4338-BCCD-8F8A19223CB1",
					"method": "live",
					"params": [
						table_name
					],
			}))
			.unwrap(),
		)
		.await
		.unwrap_or_else(|e| panic!("Error sending message: {}", e))
		.as_object()
		.unwrap_or_else(|| panic!("Expected object, got {:?}", res));

		// Wait 2 seconds for auth to expire
		tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

		// Start second connection
		let socket2 = &mut common::connect_ws(&addr).await?;

		// Signin
		let res =
			common::ws_signin(socket2, "user", "pass", Some(&ns), Some(&db), Some("scope")).await;
		assert!(res.is_ok(), "result: {:?}", res);

		// Insert
		let id = "A23A05ABC15C420E9A7E13D2C8657890";
		let query = format!(r#"INSERT INTO {} {{"id": "{}", "name": "ok"}};"#, table_name, id);
		let created = common::ws_query(socket2, query.as_str()).await.unwrap();
		assert_eq!(created.len(), 1);

		// Validate live query from first session didnt produce a result
		let res = common::ws_recv_msg(socket).await;
		match &res {
			Err(e) => {
				if let Some(TestError::NetworkError {
					..
				}) = e.downcast_ref::<TestError>()
				{
				} else {
					panic!("Expected a network error, but got: {:?}", e)
				}
			}
			Ok(v) => {
				panic!("Expected a network error, but got: {:?}", v)
			}
		}
		Ok(())
	}

	#[test(tokio::test)]
	async fn invalidate() -> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = common::start_server_with_defaults().await.unwrap();
		let socket = &mut common::connect_ws(&addr).await?;

		//
		// Prepare the connection
		//
		let res = common::ws_signin(socket, USER, PASS, None, None, None).await;
		assert!(res.is_ok(), "result: {:?}", res);

		// Verify we have a ROOT session
		let res = common::ws_query(
			socket,
			&format!("DEFINE NAMESPACE {throwaway}", throwaway = Ulid::new()),
		)
		.await;
		assert!(res.is_ok(), "result: {:?}", res);

		// Invalidate session
		let res = common::ws_send_msg_and_wait_response(
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
		let res = common::ws_query(
			socket,
			&format!("DEFINE NAMESPACE {throwaway}", throwaway = Ulid::new()),
		)
		.await;
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
	async fn authenticate() -> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = common::start_server_with_defaults().await.unwrap();
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
		let res = common::ws_send_msg_and_wait_response(
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
		let res = common::ws_query(
			socket,
			&format!("DEFINE NAMESPACE {throwaway}", throwaway = Ulid::new()),
		)
		.await;
		assert!(res.is_ok(), "result: {:?}", res);
		let res = res.unwrap();
		assert_eq!(res[0]["status"], "OK", "result: {:?}", res);
		Ok(())
	}

	#[test(tokio::test)]
	async fn kill_kill_endpoint() -> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = common::start_server_with_defaults().await.unwrap();
		let table_name = "table_D250F804BC244558982DB7D8712F6BE3".to_string();

		let socket = &mut common::connect_ws(&addr).await?;

		let _ = common::ws_signin(socket, USER, PASS, None, None, None).await?;
		let _ =
			common::ws_use(socket, Some(&Ulid::new().to_string()), Some(&Ulid::new().to_string()))
				.await?;

		// LIVE query via live endpoint
		let live_res = common::ws_send_msg_and_wait_response(
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
		let live_id = live_res["result"].as_str().unwrap();

		// KILL query via kill endpoint
		common::ws_send_msg(
			socket,
			serde_json::to_string(&json!({
					"id": "1",
					"method": "kill",
					"params": [
						live_id
					],
			}))
			.unwrap(),
		)
		.await?;

		// Verify we killed the query
		let msgs = common::ws_recv_all_msgs(socket, 1, Duration::from_millis(1000)).await?;
		assert!(
			msgs.iter().all(|v| v["error"].is_null()),
			"Unexpected error received: {:#?}",
			msgs
		);
		let msg = msgs.get(0).unwrap();
		assert!(msg["status"].is_null(), "unexpected status: {:?}", msg);

		// Create some data for notification
		let id = "an-id-goes-here";
		let query = format!(r#"INSERT INTO {} {{"id": "{}", "name": "ok"}};"#, table_name, id);
		let _ = common::ws_query(socket, query.as_str()).await.unwrap();
		let json = json!({
			"id": "1",
			"method": "query",
			"params": [query],
		});

		common::ws_send_msg(socket, serde_json::to_string(&json).unwrap()).await?;

		// Wait some time for all messages to arrive, and then verify we didn't get any notification
		let msgs = common::ws_recv_all_msgs(socket, 1, Duration::from_millis(500)).await?;
		assert!(
			msgs.iter().all(|v| v["error"].is_null()),
			"Unexpected error received: {:#?}",
			msgs
		);
		let lq_notif = msgs.iter().find(|v| common::ws_msg_is_notification_from_lq(v, live_id));
		assert!(lq_notif.is_none(), "Expected to find no notifications, found 1: {:#?}", msgs);

		Ok(())
	}

	#[test(tokio::test)]
	async fn kill_query_endpoint() -> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = common::start_server_with_defaults().await.unwrap();
		let table_name = "table_8B5E5635869E4FF2A35C94E8FC2CAE9A".to_string();

		let socket = &mut common::connect_ws(&addr).await?;

		let _ = common::ws_signin(socket, USER, PASS, None, None, None).await?;
		let _ =
			common::ws_use(socket, Some(&Ulid::new().to_string()), Some(&Ulid::new().to_string()))
				.await?;

		// LIVE query via live endpoint
		let live_res = common::ws_send_msg_and_wait_response(
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
		let live_id = live_res["result"].as_str().unwrap();

		// KILL query via kill endpoint
		let kill_query = format!("KILL '{live_id}'");
		common::ws_send_msg(
			socket,
			serde_json::to_string(&json!({
					"id": "1",
					"method": "query",
					"params": [
						kill_query
					],
			}))
			.unwrap(),
		)
		.await?;

		// Verify we killed the query
		let msgs = common::ws_recv_all_msgs(socket, 1, Duration::from_millis(1000)).await?;
		assert!(
			msgs.iter().all(|v| v["error"].is_null()),
			"Unexpected error received: {:#?}",
			msgs
		);
		let msg = msgs.get(0).unwrap();
		assert!(msg["status"].is_null(), "unexpected status: {:?}", msg);

		// Create some data for notification
		let id = "an-id-goes-here";
		let query = format!(r#"INSERT INTO {} {{"id": "{}", "name": "ok"}};"#, table_name, id);
		let _ = common::ws_query(socket, query.as_str()).await.unwrap();
		let json = json!({
			"id": "1",
			"method": "query",
			"params": [query],
		});

		common::ws_send_msg(socket, serde_json::to_string(&json).unwrap()).await?;

		// Wait some time for all messages to arrive, and then verify we didn't get any notification
		let msgs = common::ws_recv_all_msgs(socket, 1, Duration::from_millis(500)).await?;
		assert!(
			msgs.iter().all(|v| v["error"].is_null()),
			"Unexpected error received: {:#?}",
			msgs
		);
		let lq_notif = msgs.iter().find(|v| common::ws_msg_is_notification_from_lq(v, live_id));
		assert!(lq_notif.is_none(), "Expected to find no notifications, found 1: {:#?}", msgs);

		Ok(())
	}

	#[test(tokio::test)]
	async fn live_live_endpoint() -> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = common::start_server_without_auth().await.unwrap();
		let table_name = "table_FD40A9A361884C56B5908A934164884A".to_string();

		let socket = &mut common::connect_ws(&addr).await?;

		let _ = common::ws_signin(socket, USER, PASS, None, None, None).await?;
		let _ =
			common::ws_use(socket, Some(&Ulid::new().to_string()), Some(&Ulid::new().to_string()))
				.await?;

		// LIVE query via live endpoint
		let live_res = common::ws_send_msg_and_wait_response(
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
		let live_id = live_res["result"].as_str().unwrap();

		// Create some data for notification
		// Manually send the query and wait for multiple messages. Ordering of the messages is not guaranteed, so we could receive the notification before the query result.
		let id = "an-id-goes-here";
		let query = format!(r#"INSERT INTO {} {{"id": "{}", "name": "ok"}};"#, table_name, id);
		let json = json!({
			"id": "1",
			"method": "query",
			"params": [query],
		});
		common::ws_send_msg(socket, serde_json::to_string(&json).unwrap()).await?;

		// Wait some time for all messages to arrive, and then search for the notification message
		let msgs = common::ws_recv_all_msgs(socket, 2, Duration::from_millis(500)).await;
		assert!(msgs.is_ok(), "Error waiting for messages: {:?}", msgs.err());
		let msgs = msgs.unwrap();
		assert!(
			msgs.iter().all(|v| v["error"].is_null()),
			"Unexpected error received: {:#?}",
			msgs
		);

		let lq_notif = msgs.iter().find(|v| common::ws_msg_is_notification_from_lq(v, live_id));
		assert!(
			lq_notif.is_some(),
			"Expected to find a notification for LQ id {}: {:#?}",
			live_id,
			msgs
		);
		// Extract the notification object
		let lq_notif = lq_notif.unwrap();
		let lq_notif = lq_notif["result"].as_object().unwrap();

		// Verify message on individual keys since the notification ID is random
		let action = lq_notif["action"].as_str().unwrap();
		let result = lq_notif["result"].as_object().unwrap();
		assert_eq!(action, "CREATE", "expected notification to be `CREATE`: {:?}", lq_notif);
		let expected_id = format!("{}:⟨{}⟩", table_name, id);
		assert_eq!(
			result["id"].as_str(),
			Some(expected_id.as_str()),
			"expected notification to have id {:?}: {:?}",
			expected_id,
			lq_notif
		);
		assert_eq!(
			result["name"].as_str(),
			Some("ok"),
			"expected notification to have name `ok`: {:?}",
			lq_notif
		);

		Ok(())
	}

	#[test(tokio::test)]
	async fn live_query_endpoint() -> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = common::start_server_without_auth().await.unwrap();
		let table_name = "table_FD40A9A361884C56B5908A934164884A".to_string();

		let socket = &mut common::connect_ws(&addr).await?;

		let _ = common::ws_signin(socket, USER, PASS, None, None, None).await?;
		let _ =
			common::ws_use(socket, Some(&Ulid::new().to_string()), Some(&Ulid::new().to_string()))
				.await?;

		// LIVE query via query endpoint
		let lq_res =
			common::ws_query(socket, format!("LIVE SELECT * FROM {};", table_name).as_str())
				.await?;
		assert_eq!(lq_res.len(), 1, "Expected 1 result got: {:?}", lq_res);
		let live_id = lq_res[0]["result"].as_str().unwrap();

		// Create some data for notification
		// Manually send the query and wait for multiple messages. Ordering of the messages is not guaranteed, so we could receive the notification before the query result.
		let id = "an-id-goes-here";
		let query = format!(r#"INSERT INTO {} {{"id": "{}", "name": "ok"}};"#, table_name, id);
		let json = json!({
			"id": "1",
			"method": "query",
			"params": [query],
		});

		common::ws_send_msg(socket, serde_json::to_string(&json).unwrap()).await?;

		// Wait some time for all messages to arrive, and then search for the notification message
		let msgs = common::ws_recv_all_msgs(socket, 2, Duration::from_millis(500)).await;
		assert!(msgs.is_ok(), "Error waiting for messages: {:?}", msgs.err());
		let msgs = msgs.unwrap();
		assert!(
			msgs.iter().all(|v| v["error"].is_null()),
			"Unexpected error received: {:#?}",
			msgs
		);

		let lq_notif = msgs.iter().find(|v| common::ws_msg_is_notification_from_lq(v, live_id));
		assert!(
			lq_notif.is_some(),
			"Expected to find a notification for LQ id {}: {:#?}",
			live_id,
			msgs
		);

		// Extract the notification object
		let lq_notif = lq_notif.unwrap();
		let lq_notif = lq_notif["result"].as_object().unwrap();

		// Verify message on individual keys since the notification ID is random
		let action = lq_notif["action"].as_str().unwrap();
		let result = lq_notif["result"].as_object().unwrap();
		assert_eq!(action, "CREATE", "expected notification to be `CREATE`: {:?}", lq_notif);
		let expected_id = format!("{}:⟨{}⟩", table_name, id);
		assert_eq!(
			result["id"].as_str(),
			Some(expected_id.as_str()),
			"expected notification to have id {:?}: {:?}",
			expected_id,
			lq_notif
		);
		assert_eq!(
			result["name"].as_str(),
			Some("ok"),
			"expected notification to have name `ok`: {:?}",
			lq_notif
		);

		Ok(())
	}

	#[test(tokio::test)]
	async fn let_and_set() -> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = common::start_server_with_defaults().await.unwrap();
		let socket = &mut common::connect_ws(&addr).await?;

		//
		// Prepare the connection
		//
		let res =
			common::ws_use(socket, Some(&Ulid::new().to_string()), Some(&Ulid::new().to_string()))
				.await;
		assert!(res.is_ok(), "result: {:?}", res);

		// Define variable using let
		let res = common::ws_send_msg_and_wait_response(
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
		let res = common::ws_send_msg_and_wait_response(
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
	async fn unset() -> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = common::start_server_with_defaults().await.unwrap();
		let socket = &mut common::connect_ws(&addr).await?;

		//
		// Prepare the connection
		//
		let res =
			common::ws_use(socket, Some(&Ulid::new().to_string()), Some(&Ulid::new().to_string()))
				.await;
		assert!(res.is_ok(), "result: {:?}", res);

		// Define variable
		let res = common::ws_send_msg_and_wait_response(
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
		let res = common::ws_send_msg_and_wait_response(
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
	async fn select() -> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = common::start_server_with_defaults().await.unwrap();
		let socket = &mut common::connect_ws(&addr).await?;

		//
		// Prepare the connection
		//
		let res = common::ws_signin(socket, USER, PASS, None, None, None).await;
		assert!(res.is_ok(), "result: {:?}", res);
		let res =
			common::ws_use(socket, Some(&Ulid::new().to_string()), Some(&Ulid::new().to_string()))
				.await;
		assert!(res.is_ok(), "result: {:?}", res);

		//
		// Setup the database
		//
		let res = common::ws_query(socket, "CREATE foo").await;
		assert!(res.is_ok(), "result: {:?}", res);

		// Select data
		let res = common::ws_send_msg_and_wait_response(
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
	async fn insert() -> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = common::start_server_with_defaults().await.unwrap();
		let socket = &mut common::connect_ws(&addr).await?;

		//
		// Prepare the connection
		//
		let res = common::ws_signin(socket, USER, PASS, None, None, None).await;
		assert!(res.is_ok(), "result: {:?}", res);
		let res =
			common::ws_use(socket, Some(&Ulid::new().to_string()), Some(&Ulid::new().to_string()))
				.await;
		assert!(res.is_ok(), "result: {:?}", res);

		// Insert data
		let res = common::ws_send_msg_and_wait_response(
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
	async fn create() -> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = common::start_server_with_defaults().await.unwrap();
		let socket = &mut common::connect_ws(&addr).await?;

		//
		// Prepare the connection
		//
		let res = common::ws_signin(socket, USER, PASS, None, None, None).await;
		assert!(res.is_ok(), "result: {:?}", res);
		let res =
			common::ws_use(socket, Some(&Ulid::new().to_string()), Some(&Ulid::new().to_string()))
				.await;
		assert!(res.is_ok(), "result: {:?}", res);

		// Insert data
		let res = common::ws_send_msg_and_wait_response(
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
	async fn update() -> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = common::start_server_with_defaults().await.unwrap();
		let socket = &mut common::connect_ws(&addr).await?;

		//
		// Prepare the connection
		//
		let res = common::ws_signin(socket, USER, PASS, None, None, None).await;
		assert!(res.is_ok(), "result: {:?}", res);
		let res =
			common::ws_use(socket, Some(&Ulid::new().to_string()), Some(&Ulid::new().to_string()))
				.await;
		assert!(res.is_ok(), "result: {:?}", res);

		//
		// Setup the database
		//
		let res = common::ws_query(socket, r#"CREATE table SET name = "foo""#).await;
		assert!(res.is_ok(), "result: {:?}", res);

		// Insert data
		let res = common::ws_send_msg_and_wait_response(
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
	async fn merge() -> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = common::start_server_with_defaults().await.unwrap();
		let socket = &mut common::connect_ws(&addr).await?;

		//
		// Prepare the connection
		//
		let res = common::ws_signin(socket, USER, PASS, None, None, None).await;
		assert!(res.is_ok(), "result: {:?}", res);
		let res =
			common::ws_use(socket, Some(&Ulid::new().to_string()), Some(&Ulid::new().to_string()))
				.await;
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
		// Merge data
		//

		let res = common::ws_send_msg_and_wait_response(
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
		assert_eq!(res["name_for_merge"], "foo");
		assert_eq!(res["value_for_merge"], "bar");

		Ok(())
	}

	#[test(tokio::test)]
	async fn patch() -> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = common::start_server_with_defaults().await.unwrap();
		let socket = &mut common::connect_ws(&addr).await?;

		//
		// Prepare the connection
		//
		let res = common::ws_signin(socket, USER, PASS, None, None, None).await;
		assert!(res.is_ok(), "result: {:?}", res);
		let res =
			common::ws_use(socket, Some(&Ulid::new().to_string()), Some(&Ulid::new().to_string()))
				.await;
		assert!(res.is_ok(), "result: {:?}", res);

		//
		// Setup the database
		//
		let res =
			common::ws_query(socket, r#"CREATE table SET original_name = "original_value""#).await;
		assert!(res.is_ok(), "result: {:?}", res);
		let res = res.unwrap();
		assert!(res[0]["result"].is_array(), "result: {:?}", res);
		let res = &res[0]["result"].as_array().unwrap()[0];

		let what = res["id"].as_str().unwrap();

		//
		// Patch data
		//

		let ops = json!([
			{
				"op": "add",
				"path": "patch_name",
				"value": "patch_value"
			},
			{
				"op": "remove",
				"path": "original_name",
			}
		]);
		let res = common::ws_send_msg_and_wait_response(
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
		assert!(res["result"].is_object(), "result: {:?}", res);
		let res = res["result"].as_object().unwrap();
		assert_eq!(res.get("patch_name"), Some(json!("patch_value")).as_ref(), "result: {:?}", res);

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
		assert_eq!(res["patch_name"], "patch_value");

		Ok(())
	}

	#[test(tokio::test)]
	async fn delete() -> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = common::start_server_with_defaults().await.unwrap();
		let socket = &mut common::connect_ws(&addr).await?;

		//
		// Prepare the connection
		//
		let res = common::ws_signin(socket, USER, PASS, None, None, None).await;
		assert!(res.is_ok(), "result: {:?}", res);
		let res =
			common::ws_use(socket, Some(&Ulid::new().to_string()), Some(&Ulid::new().to_string()))
				.await;
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
		let res = common::ws_send_msg_and_wait_response(
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
	async fn format_json() -> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = common::start_server_with_defaults().await.unwrap();
		let socket = &mut common::connect_ws(&addr).await?;

		//
		// Prepare the connection
		//
		let res = common::ws_signin(socket, USER, PASS, None, None, None).await;
		assert!(res.is_ok(), "result: {:?}", res);
		let res =
			common::ws_use(socket, Some(&Ulid::new().to_string()), Some(&Ulid::new().to_string()))
				.await;
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
		let res =
			common::ws_send_msg_and_wait_response(socket, serde_json::to_string(&msg).unwrap())
				.await;
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
	async fn format_cbor() -> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = common::start_server_with_defaults().await.unwrap();
		let socket = &mut common::connect_ws(&addr).await?;

		//
		// Prepare the connection
		//
		let res = common::ws_signin(socket, USER, PASS, None, None, None).await;
		assert!(res.is_ok(), "result: {:?}", res);
		let res =
			common::ws_use(socket, Some(&Ulid::new().to_string()), Some(&Ulid::new().to_string()))
				.await;
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
		let res = common::ws_send_msg_and_wait_response(socket, msg).await;
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

		let res = common::ws_send_msg(socket, msg).await;
		assert!(res.is_ok(), "result: {:?}", res);

		let res = common::ws_recv_msg_with_fmt(socket, common::Format::Cbor).await;
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
	async fn format_pack() -> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = common::start_server_with_defaults().await.unwrap();
		let socket = &mut common::connect_ws(&addr).await?;

		//
		// Prepare the connection
		//
		let res = common::ws_signin(socket, USER, PASS, None, None, None).await;
		assert!(res.is_ok(), "result: {:?}", res);
		let res =
			common::ws_use(socket, Some(&Ulid::new().to_string()), Some(&Ulid::new().to_string()))
				.await;
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
		let res = common::ws_send_msg_and_wait_response(socket, msg).await;
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

		let res = common::ws_send_msg(socket, msg).await;
		assert!(res.is_ok(), "result: {:?}", res);
		let res = common::ws_recv_msg_with_fmt(socket, common::Format::Pack).await;
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
	async fn query() -> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = common::start_server_with_defaults().await.unwrap();
		let socket = &mut common::connect_ws(&addr).await?;

		//
		// Prepare the connection
		//
		let res = common::ws_signin(socket, USER, PASS, None, None, None).await;
		assert!(res.is_ok(), "result: {:?}", res);
		let res =
			common::ws_use(socket, Some(&Ulid::new().to_string()), Some(&Ulid::new().to_string()))
				.await;
		assert!(res.is_ok(), "result: {:?}", res);

		//
		// Run a CREATE query
		//
		let res = common::ws_send_msg_and_wait_response(
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
	async fn version() -> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = common::start_server_with_defaults().await.unwrap();
		let socket = &mut common::connect_ws(&addr).await?;

		// Send command
		let res = common::ws_send_msg_and_wait_response(
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

	// Validate that the WebSocket is able to process multiple queries concurrently
	#[test(tokio::test)]
	async fn concurrency() -> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = common::start_server_with_defaults().await.unwrap();
		let socket = &mut common::connect_ws(&addr).await?;

		//
		// Prepare the connection
		//
		let res = common::ws_signin(socket, USER, PASS, None, None, None).await;
		assert!(res.is_ok(), "result: {:?}", res);
		let res =
			common::ws_use(socket, Some(&Ulid::new().to_string()), Some(&Ulid::new().to_string()))
				.await;
		assert!(res.is_ok(), "result: {:?}", res);

		//
		// Run 5 queries that do a SLEEP and verify they all complete concurrently
		//

		// Send queries
		for i in 0..5 {
			let query = format!("SLEEP 1s; RETURN 'done-{}';", i);
			let query_msg = json!({
					"id": "1",
					"method": "query",
					"params": [query],
			});
			let res = common::ws_send_msg(socket, serde_json::to_string(&query_msg).unwrap()).await;
			assert!(res.is_ok(), "result: {:?}", res);
		}

		// Wait for queries to complete and verify they all completed within 2 seconds (assume they are executed concurrently)
		let msgs = common::ws_recv_all_msgs(socket, 5, Duration::from_secs(2)).await;
		assert!(msgs.is_ok(), "Error waiting for messages: {:?}", msgs.err());

		let msgs = msgs.unwrap();
		assert!(
			msgs.iter().all(|v| v["error"].is_null()),
			"Unexpected error received: {:#?}",
			msgs
		);

		Ok(())
	}
}
