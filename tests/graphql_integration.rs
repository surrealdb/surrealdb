mod common;

mod graphql_integration {
	use std::time::Duration;

	macro_rules! assert_equal_arrs {
		($lhs: expr_2021, $rhs: expr_2021) => {
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

	use super::common;
	use crate::common::{PASS, USER};

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
			let body = res.json::<serde_json::Value>().await?;
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
			assert_eq!(expected, body)
		}

		// test limit
		{
			let res = client
				.post(gql_url)
				.body(json!({"query": r#"query{foo(limit: 1){id, val}}"#}).to_string())
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
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
			assert_eq!(expected, body)
		}

		// test start
		{
			let res = client
				.post(gql_url)
				.body(json!({"query": r#"query{foo(start: 1){id, val}}"#}).to_string())
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
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
			assert_eq!(expected, body)
		}

		// test order
		{
			let res = client
				.post(gql_url)
				.body(json!({"query": r#"query{foo(order: {desc: val}){id}}"#}).to_string())
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
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
			assert_eq!(expected, body)
		}

		// test filter
		{
			let res = client
				.post(gql_url)
				.body(json!({"query": r#"query{foo(filter: {val: {eq: 42}}){id}}"#}).to_string())
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			let expected = json!({
				"data": {
					"foo": [
						{
							"id": "foo:1",
						}
					]
				}
			});
			assert_eq!(expected, body)
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
			let body = res.json::<serde_json::Value>().await?;
			let expected =
				json!({"data":{"foo":[{"id":"foo:1","val":42},{"id":"foo:2","val":43}]}});
			assert_eq!(expected, body);
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
			let body = res.json::<serde_json::Value>().await?;
			let expected = json!({"data":{"foo":[{"id":"foo:1","val":42}]}});
			assert_eq!(expected, body);
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
					DEFINE FIELD id ON TABLE foo TYPE string;
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
			let res_obj: serde_json::Value = res.json().await?;
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
			let res_obj = res.json::<serde_json::Value>().await?;
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

	#[test(tokio::test)]
	async fn functions() -> Result<(), Box<dyn std::error::Error>> {
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

		// add schema and data
		{
			let res = client
				.post(sql_url)
				.body(
					r#"
					DEFINE CONFIG GRAPHQL auto;
                    DEFINE TABLE foo SCHEMAFUL;
                    DEFINE FIELD val ON foo TYPE int;
                    CREATE foo:1 set val = 86;
					DEFINE FUNCTION fn::num() -> int {return 42;};
					DEFINE FUNCTION fn::double($x: int) -> int {return $x * 2};
					DEFINE FUNCTION fn::foo() -> record<foo> {return foo:1};
					DEFINE FUNCTION fn::record() -> record {return foo:1};
                "#,
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
		}

		// functions returning records
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"query{fn_foo{id, val}, fn_record {id ...on foo {val}}}"#})
						.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			let expected = json!({
			  "data": {
				"fn_foo": {
				  "id": "foo:1",
				  "val": 86
				},
				"fn_record": {
					"id": "foo:1",
					"val": 86
				  }
			  }
			});
			assert_eq!(expected, body)
		}

		{
			let res = client
				.post(gql_url)
				.body(json!({"query": r#"query{fn_num, fn_double(x: 21)}"#}).to_string())
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			let expected = json!({
			  "data": {
				"fn_num": 42,
				"fn_double": 42
			  }
			});
			assert_eq!(expected, body)
		}

		Ok(())
	}
}
