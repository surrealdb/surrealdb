mod common;

#[cfg(surrealdb_unstable)]
mod graphql_integration {
	use std::{str::FromStr, time::Duration};

	macro_rules! assert_equal_arrs {
		($lhs: expr, $rhs: expr) => {
			let lhs = $lhs.as_array().unwrap().iter().collect::<std::collections::HashSet<_>>();
			let rhs = $rhs.as_array().unwrap().iter().collect::<std::collections::HashSet<_>>();
			assert_eq!(lhs, rhs)
		};
	}

	use http::header;
	use reqwest::Client;
	use serde_json::json;
	use test_log::test;
	use ulid::Ulid;

	use crate::common::{PASS, USER};

	use super::common;

	#[test(tokio::test)]
	async fn basic() -> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = common::start_server_gql_without_auth().await.unwrap();
		let gql_url = &format!("http://{addr}/graphql");
		let sql_url = &format!("http://{addr}/sql");

		let mut headers = reqwest::header::HeaderMap::new();
		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();
		headers.insert("surreal-ns", ns.parse()?);
		headers.insert("surreal-db", db.parse()?);
		headers.insert(header::ACCEPT, "application/json".parse()?);
		let client = Client::builder()
			.connect_timeout(Duration::from_millis(10))
			.default_headers(headers)
			.build()?;

		// check errors with no config
		{
			let res = client.post(gql_url).body("").send().await?;
			assert_eq!(res.status(), 400);
			let body = res.text().await?;
			assert!(body.contains("NotConfigured"), "body: {body}")
		}

		// add schema and data
		{
			let res = client
				.post(sql_url)
				.body(
					r#"
                    DEFINE CONFIG GRAPHQL AUTO;
                "#,
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
		}

		// check errors with no tables
		{
			let res = client.post(gql_url).body("").send().await?;
			assert_eq!(res.status(), 400);
			let body = res.text().await?;
			assert!(body.contains("no items found in database"), "body: {body}")
		}

		// add schema and data
		{
			let res = client
				.post(sql_url)
				.body(
					r#"
                    DEFINE TABLE foo SCHEMAFUL;
                    DEFINE FIELD val ON foo TYPE int;
                    CREATE foo:1 set val = 42;
                    CREATE foo:2 set val = 43;
                "#,
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
		}

		// fetch data via graphql
		{
			let res = client
				.post(gql_url)
				.body(json!({"query": r#"query{foo{id, val}}"#}).to_string())
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.text().await?;
			let expected = json!({
				"data": {
					"foo": [
						{
							"id": "foo:1",
							"val": 42
						},
						{
							"id": "foo:2",
							"val": 43
						}
					]
				}
			});
			assert_eq!(expected.to_string(), body)
		}

		// test limit
		{
			let res = client
				.post(gql_url)
				.body(json!({"query": r#"query{foo(limit: 1){id, val}}"#}).to_string())
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.text().await?;
			let expected = json!({
				"data": {
					"foo": [
						{
							"id": "foo:1",
							"val": 42
						}
					]
				}
			});
			assert_eq!(expected.to_string(), body)
		}

		// test start
		{
			let res = client
				.post(gql_url)
				.body(json!({"query": r#"query{foo(start: 1){id, val}}"#}).to_string())
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.text().await?;
			let expected = json!({
				"data": {
					"foo": [
						{
							"id": "foo:2",
							"val": 43
						}
					]
				}
			});
			assert_eq!(expected.to_string(), body)
		}

		// test order
		{
			let res = client
				.post(gql_url)
				.body(json!({"query": r#"query{foo(order: {desc: val}){id}}"#}).to_string())
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.text().await?;
			let expected = json!({
				"data": {
					"foo": [
						{
							"id": "foo:2",
						},
						{
							"id": "foo:1",
						}
					]
				}
			});
			assert_eq!(expected.to_string(), body)
		}

		// test filter
		{
			let res = client
				.post(gql_url)
				.body(json!({"query": r#"query{foo(filter: {val: {eq: 42}}){id}}"#}).to_string())
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.text().await?;
			let expected = json!({
				"data": {
					"foo": [
						{
							"id": "foo:1",
						}
					]
				}
			});
			assert_eq!(expected.to_string(), body)
		}

		Ok(())
	}

	#[test(tokio::test)]
	async fn basic_auth() -> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = common::start_server_gql().await.unwrap();
		let gql_url = &format!("http://{addr}/graphql");
		let sql_url = &format!("http://{addr}/sql");
		let signup_url = &format!("http://{addr}/signup");

		let mut headers = reqwest::header::HeaderMap::new();
		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();
		headers.insert("surreal-ns", ns.parse()?);
		headers.insert("surreal-db", db.parse()?);
		headers.insert(header::ACCEPT, "application/json".parse()?);
		let client = Client::builder()
			.connect_timeout(Duration::from_millis(10))
			.default_headers(headers)
			.build()?;

		// check errors on invalid auth
		{
			let res =
				client.post(gql_url).basic_auth("invalid", Some("invalid")).body("").send().await?;
			assert_eq!(res.status(), 401);
			let body = res.text().await?;
			assert!(body.contains("There was a problem with authentication"), "body: {body}")
		}

		// add schema and data
		{
			let res = client
				.post(sql_url)
				.basic_auth(USER, Some(PASS))
				.body(
					r#"
					DEFINE CONFIG GRAPHQL AUTO;
					DEFINE ACCESS user ON DATABASE TYPE RECORD
					SIGNUP ( CREATE user SET email = $email, pass = crypto::argon2::generate($pass) )
					SIGNIN ( SELECT * FROM user WHERE email = $email AND crypto::argon2::compare(pass, $pass) )
					DURATION FOR SESSION 60s, FOR TOKEN 1d;

                    DEFINE TABLE foo SCHEMAFUL PERMISSIONS FOR select WHERE $auth.email = email;
                    DEFINE FIELD email ON foo TYPE string;
                    DEFINE FIELD val ON foo TYPE int;
                    CREATE foo:1 set val = 42, email = "user@email.com";
                    CREATE foo:2 set val = 43, email = "other@email.com";
                "#,
				)
				.send()
				.await?;
			// assert_eq!(res.status(), 200);
			let body = res.text().await?;
			eprintln!("\n\n\n\n\n{body}\n\n\n\n\n\n");
		}

		// check works with root
		{
			let res = client
				.post(gql_url)
				.basic_auth(USER, Some(PASS))
				.body(json!({"query": r#"query{foo{id, val}}"#}).to_string())
				.send()
				.await?;
			// assert_eq!(res.status(), 200);
			let body = res.text().await?;
			let expected =
				json!({"data":{"foo":[{"id":"foo:1","val":42},{"id":"foo:2","val":43}]}});
			assert_eq!(expected.to_string(), body);
		}

		// check partial access
		{
			let req_body = serde_json::to_string(
				json!({
					"ns": ns,
					"db": db,
					"ac": "user",
					"email": "user@email.com",
					"pass": "pass",
				})
				.as_object()
				.unwrap(),
			)
			.unwrap();

			let res = client.post(signup_url).body(req_body).send().await?;
			assert_eq!(res.status(), 200, "body: {}", res.text().await?);
			let body: serde_json::Value = serde_json::from_str(&res.text().await?).unwrap();
			let token = body["token"].as_str().unwrap();

			let res = client
				.post(gql_url)
				.bearer_auth(token)
				.body(json!({"query": r#"query{foo{id, val}}"#}).to_string())
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.text().await?;
			let expected = json!({"data":{"foo":[{"id":"foo:1","val":42}]}});
			assert_eq!(expected.to_string(), body);
		}
		Ok(())
	}

	#[test(tokio::test)]
	async fn config() -> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = common::start_server_gql_without_auth().await.unwrap();
		let gql_url = &format!("http://{addr}/graphql");
		let sql_url = &format!("http://{addr}/sql");

		let mut headers = reqwest::header::HeaderMap::new();
		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();
		headers.insert("surreal-ns", ns.parse()?);
		headers.insert("surreal-db", db.parse()?);
		headers.insert(header::ACCEPT, "application/json".parse()?);
		let client = reqwest::Client::builder()
			.connect_timeout(Duration::from_millis(10))
			.default_headers(headers)
			.build()?;

		{
			let res = client.post(gql_url).body("").send().await?;
			assert_eq!(res.status(), 400);
			let body = res.text().await?;
			assert!(body.contains("NotConfigured"));
		}

		// add schema and data
		{
			let res = client
				.post(sql_url)
				.body(
					r#"
                    DEFINE CONFIG GRAPHQL AUTO;
					DEFINE TABLE foo;
					DEFINE FIELD val ON foo TYPE string;
					DEFINE TABLE bar;
					DEFINE FIELD val ON bar TYPE string;
                "#,
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
		}

		{
			let res = client
				.post(gql_url)
				.body(json!({ "query": r#"{__schema {queryType {fields {name}}}}"# }).to_string())
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.text().await?;
			let res_obj = serde_json::Value::from_str(&body).unwrap();
			let fields = &res_obj["data"]["__schema"]["queryType"]["fields"];
			let expected_fields = json!(
				[
					{
						"name": "foo"
					},
					{
						"name": "bar"
					},
					{
						"name": "_get_foo"
					},
					{
						"name": "_get_bar"
					},
					{
						"name": "_get"
					}
				]
			);
			assert_equal_arrs!(fields, &expected_fields);
		}

		{
			let res = client
				.post(sql_url)
				.body(
					r#"
                    DEFINE CONFIG OVERWRITE GRAPHQL TABLES INCLUDE foo;
                "#,
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
		}

		{
			let res = client
				.post(gql_url)
				.body(json!({ "query": r#"{__schema {queryType {fields {name}}}}"# }).to_string())
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.text().await?;
			let res_obj = serde_json::Value::from_str(&body).unwrap();
			let fields = &res_obj["data"]["__schema"]["queryType"]["fields"];
			let expected_fields = json!(
				[
					{
						"name": "foo"
					},
					{
						"name": "_get_foo"
					},
					{
						"name": "_get"
					}
				]
			);
			assert_equal_arrs!(fields, &expected_fields);
		}

		Ok(())
	}
}
