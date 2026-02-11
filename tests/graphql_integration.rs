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
			.connect_timeout(Duration::from_secs(10))
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
				.body(json!({"query": r#"query{ foo { id, val } }"#}).to_string())
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
			.connect_timeout(Duration::from_secs(10))
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
			assert_eq!(body, expected);
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
			.connect_timeout(Duration::from_secs(10))
			.default_headers(headers)
			.build()?;

		{
			let res = client.post(gql_url).body("").send().await?;
			assert_eq!(res.status(), 400);
			let body = res.text().await?;
			assert!(body.contains("NotConfigured"), "{body}");
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
	async fn geometry() -> Result<(), Box<dyn std::error::Error>> {
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
			.connect_timeout(Duration::from_secs(10))
			.default_headers(headers)
			.build()?;

		// Set up schema with various geometry types
		{
			let res = client
				.post(sql_url)
				.body(
					r#"
					DEFINE CONFIG GRAPHQL AUTO;

					DEFINE TABLE place SCHEMAFUL;
					DEFINE FIELD name ON place TYPE string;
					DEFINE FIELD location ON place TYPE geometry<point>;

					DEFINE TABLE area SCHEMAFUL;
					DEFINE FIELD name ON area TYPE string;
					DEFINE FIELD boundary ON area TYPE geometry<polygon>;

					DEFINE TABLE feature SCHEMAFUL;
					DEFINE FIELD name ON feature TYPE string;
					DEFINE FIELD geom ON feature TYPE geometry;

					CREATE place:london SET name = "London", location = (-0.118092, 51.509865);
					CREATE place:paris SET name = "Paris", location = (2.349014, 48.864716);

					CREATE area:london SET name = "London Bounds", boundary = {
						type: "Polygon",
						coordinates: [[
							[-0.38314819, 51.37692386],
							[0.1785278, 51.37692386],
							[0.1785278, 51.61460570],
							[-0.38314819, 51.61460570],
							[-0.38314819, 51.37692386]
						]]
					};

					CREATE feature:point SET name = "A Point", geom = (1.0, 2.0);
					CREATE feature:line SET name = "A Line", geom = {
						type: "LineString",
						coordinates: [[0.0, 0.0], [1.0, 1.0], [2.0, 0.0]]
					};
				"#,
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
		}

		// Test 1: Query a specific geometry<point> field
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"query {
						place(order: {asc: name}) {
							id
							name
							location { type coordinates }
						}
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			let expected = json!({
				"data": {
					"place": [
						{
							"id": "place:london",
							"name": "London",
							"location": {
								"type": "Point",
								"coordinates": [-0.118092, 51.509865]
							}
						},
						{
							"id": "place:paris",
							"name": "Paris",
							"location": {
								"type": "Point",
								"coordinates": [2.349014, 48.864716]
							}
						}
					]
				}
			});
			assert_eq!(expected, body);
		}

		// Test 2: Query a specific geometry<polygon> field
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"query {
						area {
							id
							name
							boundary { type coordinates }
						}
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			let expected = json!({
				"data": {
					"area": [
						{
							"id": "area:london",
							"name": "London Bounds",
							"boundary": {
								"type": "Polygon",
								"coordinates": [[
									[-0.38314819, 51.37692386],
									[0.1785278, 51.37692386],
									[0.1785278, 51.6146057],
									[-0.38314819, 51.6146057],
									[-0.38314819, 51.37692386]
								]]
							}
						}
					]
				}
			});
			assert_eq!(expected, body);
		}

		// Test 3: Query a general geometry field (union type) with inline fragments
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"query {
						feature(order: {asc: name}) {
							id
							name
							geom {
								... on GeometryPoint { type coordinates }
								... on GeometryLineString { type coordinates }
							}
						}
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			let expected = json!({
				"data": {
					"feature": [
						{
							"id": "feature:line",
							"name": "A Line",
							"geom": {
								"type": "LineString",
								"coordinates": [[0.0, 0.0], [1.0, 1.0], [2.0, 0.0]]
							}
						},
						{
							"id": "feature:point",
							"name": "A Point",
							"geom": {
								"type": "Point",
								"coordinates": [1.0, 2.0]
							}
						}
					]
				}
			});
			assert_eq!(expected, body);
		}

		// Test 4: Fetch a single record by ID with geometry
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"query {
						_get_place(id: "london") {
							id
							name
							location { type coordinates }
						}
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			let expected = json!({
				"data": {
					"_get_place": {
						"id": "place:london",
						"name": "London",
						"location": {
							"type": "Point",
							"coordinates": [-0.118092, 51.509865]
						}
					}
				}
			});
			assert_eq!(expected, body);
		}

		// Test 5: Schema introspection shows geometry types
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"{
						__type(name: "GeometryType") {
							kind
							enumValues { name }
						}
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			let geo_type = &body["data"]["__type"];
			assert_eq!(geo_type["kind"], "ENUM");
			let enum_values = geo_type["enumValues"].as_array().unwrap();
			let names: Vec<&str> =
				enum_values.iter().map(|v| v["name"].as_str().unwrap()).collect();
			assert!(names.contains(&"Point"));
			assert!(names.contains(&"LineString"));
			assert!(names.contains(&"Polygon"));
			assert!(names.contains(&"MultiPoint"));
			assert!(names.contains(&"MultiLineString"));
			assert!(names.contains(&"MultiPolygon"));
			assert!(names.contains(&"GeometryCollection"));
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
			.connect_timeout(Duration::from_secs(10))
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

	#[test(tokio::test)]
	async fn relations() -> Result<(), Box<dyn std::error::Error>> {
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
			.connect_timeout(Duration::from_secs(10))
			.default_headers(headers)
			.build()?;

		// Set up schema: person -[likes]-> post, with rating on the relation
		{
			let res = client
				.post(sql_url)
				.body(
					r#"
					DEFINE CONFIG GRAPHQL AUTO;

					DEFINE TABLE person SCHEMAFUL;
					DEFINE FIELD name ON person TYPE string;

					DEFINE TABLE post SCHEMAFUL;
					DEFINE FIELD title ON post TYPE string;

					DEFINE TABLE likes TYPE RELATION FROM person TO post SCHEMAFUL;
					DEFINE FIELD rating ON likes TYPE int;
					DEFINE FIELD in ON likes TYPE record<person>;
					DEFINE FIELD out ON likes TYPE record<post>;

					CREATE person:alice SET name = "Alice";
					CREATE person:bob SET name = "Bob";
					CREATE post:p1 SET title = "First Post";
					CREATE post:p2 SET title = "Second Post";

					RELATE person:alice->likes->post:p1 SET rating = 5;
					RELATE person:alice->likes->post:p2 SET rating = 3;
					RELATE person:bob->likes->post:p1 SET rating = 4;
				"#,
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
		}

		// Test 1: Query outgoing relation field on person (person -> likes)
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"query {
						_get_person(id: "alice") {
							id
							name
							likes(order: {asc: rating}) {
								id
								rating
							}
						}
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			let person = &body["data"]["_get_person"];
			assert_eq!(person["id"], "person:alice");
			assert_eq!(person["name"], "Alice");
			let likes = person["likes"].as_array().unwrap();
			assert_eq!(likes.len(), 2);
			// Ordered by rating asc: 3 then 5
			assert_eq!(likes[0]["rating"], 3);
			assert_eq!(likes[1]["rating"], 5);
		}

		// Test 2: Query incoming relation field on post (likes -> post)
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"query {
						_get_post(id: "p1") {
							id
							title
							likes_in {
								id
								rating
							}
						}
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			let post = &body["data"]["_get_post"];
			assert_eq!(post["id"], "post:p1");
			assert_eq!(post["title"], "First Post");
			let likes_in = post["likes_in"].as_array().unwrap();
			assert_eq!(likes_in.len(), 2);
			// Both alice (rating 5) and bob (rating 4) liked p1
			let ratings: Vec<i64> =
				likes_in.iter().map(|l| l["rating"].as_i64().unwrap()).collect();
			assert!(ratings.contains(&5));
			assert!(ratings.contains(&4));
		}

		// Test 3: Relation field with limit
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"query {
						_get_person(id: "alice") {
							likes(limit: 1, order: {desc: rating}) {
								rating
							}
						}
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			let likes = body["data"]["_get_person"]["likes"].as_array().unwrap();
			assert_eq!(likes.len(), 1);
			assert_eq!(likes[0]["rating"], 5);
		}

		// Test 4: Empty relation result
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"query {
						_get_post(id: "p2") {
							title
							likes_in {
								rating
							}
						}
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			let post = &body["data"]["_get_post"];
			assert_eq!(post["title"], "Second Post");
			let likes_in = post["likes_in"].as_array().unwrap();
			// Only alice liked p2
			assert_eq!(likes_in.len(), 1);
			assert_eq!(likes_in[0]["rating"], 3);
		}

		// Test 5: Relation fields in list query context
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"query {
						person(order: {asc: name}) {
							name
							likes {
								rating
							}
						}
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			let people = body["data"]["person"].as_array().unwrap();
			assert_eq!(people.len(), 2);
			// Alice has 2 likes, Bob has 1
			assert_eq!(people[0]["name"], "Alice");
			assert_eq!(people[0]["likes"].as_array().unwrap().len(), 2);
			assert_eq!(people[1]["name"], "Bob");
			assert_eq!(people[1]["likes"].as_array().unwrap().len(), 1);
		}

		// Test 6: Schema introspection shows relation fields
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"{
						__type(name: "person") {
							fields { name }
						}
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			let fields = body["data"]["__type"]["fields"].as_array().unwrap();
			let field_names: Vec<&str> =
				fields.iter().map(|f| f["name"].as_str().unwrap()).collect();
			assert!(field_names.contains(&"id"), "missing 'id' field: {field_names:?}");
			assert!(field_names.contains(&"name"), "missing 'name' field: {field_names:?}");
			assert!(
				field_names.contains(&"likes"),
				"missing 'likes' relation field: {field_names:?}"
			);
		}

		// Test 7: Schema introspection shows incoming relation field on post
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"{
						__type(name: "post") {
							fields { name }
						}
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			let fields = body["data"]["__type"]["fields"].as_array().unwrap();
			let field_names: Vec<&str> =
				fields.iter().map(|f| f["name"].as_str().unwrap()).collect();
			assert!(field_names.contains(&"id"), "missing 'id' field: {field_names:?}");
			assert!(field_names.contains(&"title"), "missing 'title' field: {field_names:?}");
			assert!(
				field_names.contains(&"likes_in"),
				"missing 'likes_in' relation field: {field_names:?}"
			);
		}

		Ok(())
	}

	#[test(tokio::test)]
	async fn record_links() -> Result<(), Box<dyn std::error::Error>> {
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
			.connect_timeout(Duration::from_secs(10))
			.default_headers(headers)
			.build()?;

		// Set up schema: employee has a record<department> field
		{
			let res = client
				.post(sql_url)
				.body(
					r#"
					DEFINE CONFIG GRAPHQL AUTO;

					DEFINE TABLE department SCHEMAFUL;
					DEFINE FIELD name ON department TYPE string;
					DEFINE FIELD location ON department TYPE string;

					DEFINE TABLE employee SCHEMAFUL;
					DEFINE FIELD name ON employee TYPE string;
					DEFINE FIELD dept ON employee TYPE record<department>;

					CREATE department:eng SET name = "Engineering", location = "Building A";
					CREATE department:mkt SET name = "Marketing", location = "Building B";

					CREATE employee:e1 SET name = "Alice", dept = department:eng;
					CREATE employee:e2 SET name = "Bob", dept = department:mkt;
					CREATE employee:e3 SET name = "Charlie", dept = department:eng;
				"#,
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
		}

		// Test 1: Record-link dereferencing with nested sub-field selection
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"query {
						employee(order: {asc: name}) {
							name
							dept {
								id
								name
								location
							}
						}
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			let employees = body["data"]["employee"].as_array().unwrap();
			assert_eq!(employees.len(), 3);

			// Alice -> Engineering
			assert_eq!(employees[0]["name"], "Alice");
			assert_eq!(employees[0]["dept"]["name"], "Engineering");
			assert_eq!(employees[0]["dept"]["location"], "Building A");
			assert_eq!(employees[0]["dept"]["id"], "department:eng");

			// Bob -> Marketing
			assert_eq!(employees[1]["name"], "Bob");
			assert_eq!(employees[1]["dept"]["name"], "Marketing");

			// Charlie -> Engineering
			assert_eq!(employees[2]["name"], "Charlie");
			assert_eq!(employees[2]["dept"]["name"], "Engineering");
		}

		// Test 2: Single record fetch with nested record-link
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"query {
						_get_employee(id: "e2") {
							name
							dept {
								name
								location
							}
						}
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			let emp = &body["data"]["_get_employee"];
			assert_eq!(emp["name"], "Bob");
			assert_eq!(emp["dept"]["name"], "Marketing");
			assert_eq!(emp["dept"]["location"], "Building B");
		}

		// Test 3: Schema shows record-link field as the target table type
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"{
						__type(name: "employee") {
							fields {
								name
								type { name kind }
							}
						}
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			let fields = body["data"]["__type"]["fields"].as_array().unwrap();
			let dept_field = fields.iter().find(|f| f["name"] == "dept").unwrap();
			// The type should be the department table type (NON_NULL wrapper)
			let type_info = &dept_field["type"];
			// non-null wraps the named type
			assert_eq!(type_info["kind"], "NON_NULL");
		}

		Ok(())
	}

	#[test(tokio::test)]
	async fn self_referential_relations() -> Result<(), Box<dyn std::error::Error>> {
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
			.connect_timeout(Duration::from_secs(10))
			.default_headers(headers)
			.build()?;

		// Set up schema: user -[follows]-> user (self-referential)
		{
			let res = client
				.post(sql_url)
				.body(
					r#"
					DEFINE CONFIG GRAPHQL AUTO;

					DEFINE TABLE user SCHEMAFUL;
					DEFINE FIELD name ON user TYPE string;

					DEFINE TABLE follows TYPE RELATION FROM user TO user SCHEMAFUL;
					DEFINE FIELD in ON follows TYPE record<user>;
					DEFINE FIELD out ON follows TYPE record<user>;

					CREATE user:alice SET name = "Alice";
					CREATE user:bob SET name = "Bob";
					CREATE user:charlie SET name = "Charlie";

					RELATE user:alice->follows->user:bob;
					RELATE user:alice->follows->user:charlie;
					RELATE user:bob->follows->user:alice;
				"#,
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
		}

		// Test 1: user type has both outgoing (follows) and incoming (follows_in) fields
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"{
						__type(name: "user") {
							fields { name }
						}
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			let fields = body["data"]["__type"]["fields"].as_array().unwrap();
			let field_names: Vec<&str> =
				fields.iter().map(|f| f["name"].as_str().unwrap()).collect();
			assert!(
				field_names.contains(&"follows"),
				"missing 'follows' outgoing field: {field_names:?}"
			);
			assert!(
				field_names.contains(&"follows_in"),
				"missing 'follows_in' incoming field: {field_names:?}"
			);
		}

		// Test 2: Query outgoing follows (who does Alice follow?)
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"query {
						_get_user(id: "alice") {
							name
							follows {
								id
							}
						}
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			let user = &body["data"]["_get_user"];
			assert_eq!(user["name"], "Alice");
			let follows = user["follows"].as_array().unwrap();
			assert_eq!(follows.len(), 2, "Alice follows 2 users");
		}

		// Test 3: Query incoming follows (who follows Alice?)
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"query {
						_get_user(id: "alice") {
							name
							follows_in {
								id
							}
						}
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			let user = &body["data"]["_get_user"];
			assert_eq!(user["name"], "Alice");
			let followers = user["follows_in"].as_array().unwrap();
			assert_eq!(followers.len(), 1, "Only Bob follows Alice");
		}

		Ok(())
	}

	#[test(tokio::test)]
	async fn relation_with_record_link_traversal() -> Result<(), Box<dyn std::error::Error>> {
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
			.connect_timeout(Duration::from_secs(10))
			.default_headers(headers)
			.build()?;

		// Set up: author -[wrote]-> article, with traversal through in/out fields
		{
			let res = client
				.post(sql_url)
				.body(
					r#"
					DEFINE CONFIG GRAPHQL AUTO;

					DEFINE TABLE author SCHEMAFUL;
					DEFINE FIELD name ON author TYPE string;

					DEFINE TABLE article SCHEMAFUL;
					DEFINE FIELD title ON article TYPE string;

					DEFINE TABLE wrote TYPE RELATION FROM author TO article SCHEMAFUL;
					DEFINE FIELD in ON wrote TYPE record<author>;
					DEFINE FIELD out ON wrote TYPE record<article>;
					DEFINE FIELD year ON wrote TYPE int;

					CREATE author:a1 SET name = "Jane Doe";
					CREATE article:art1 SET title = "GraphQL in Practice";
					CREATE article:art2 SET title = "SurrealDB Deep Dive";

					RELATE author:a1->wrote->article:art1 SET year = 2024;
					RELATE author:a1->wrote->article:art2 SET year = 2025;
				"#,
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
		}

		// Test: Traverse from author through relation to article via record-link
		// author -> wrote (outgoing relation) -> out (record<article>) -> title
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"query {
						_get_author(id: "a1") {
							name
							wrote(order: {asc: year}) {
								year
								out {
									title
								}
							}
						}
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			let author = &body["data"]["_get_author"];
			assert_eq!(author["name"], "Jane Doe");
			let wrote = author["wrote"].as_array().unwrap();
			assert_eq!(wrote.len(), 2);

			// Ordered by year asc
			assert_eq!(wrote[0]["year"], 2024);
			assert_eq!(wrote[0]["out"]["title"], "GraphQL in Practice");
			assert_eq!(wrote[1]["year"], 2025);
			assert_eq!(wrote[1]["out"]["title"], "SurrealDB Deep Dive");
		}

		// Test: Traverse from article through incoming relation to author
		// article -> wrote_in (incoming relation) -> in (record<author>) -> name
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"query {
						_get_article(id: "art1") {
							title
							wrote_in {
								year
								in {
									name
								}
							}
						}
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			let article = &body["data"]["_get_article"];
			assert_eq!(article["title"], "GraphQL in Practice");
			let wrote_in = article["wrote_in"].as_array().unwrap();
			assert_eq!(wrote_in.len(), 1);
			assert_eq!(wrote_in[0]["year"], 2024);
			assert_eq!(wrote_in[0]["in"]["name"], "Jane Doe");
		}

		Ok(())
	}

	#[test(tokio::test)]
	#[cfg(feature = "storage-surrealkv")]
	async fn version() -> Result<(), Box<dyn std::error::Error>> {
		let (_dir, addr, _server) = common::start_server_gql_with_versioning().await.unwrap();
		let gql_url = &format!("http://{addr}/graphql");
		let sql_url = &format!("http://{addr}/sql");

		let mut headers = reqwest::header::HeaderMap::new();
		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();
		headers.insert("surreal-ns", ns.parse()?);
		headers.insert("surreal-db", db.parse()?);
		headers.insert(header::ACCEPT, "application/json".parse()?);
		let client = Client::builder()
			.connect_timeout(Duration::from_secs(10))
			.default_headers(headers)
			.build()?;

		// Set up schema and initial data
		{
			let res = client
				.post(sql_url)
				.body(
					r#"
					DEFINE CONFIG GRAPHQL AUTO;
					DEFINE TABLE item SCHEMAFUL;
					DEFINE FIELD name ON item TYPE string;
					DEFINE FIELD price ON item TYPE float;

					CREATE item:1 SET name = "Alpha", price = 10.0;
					CREATE item:2 SET name = "Beta", price = 20.0;
				"#,
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
		}

		// Sleep to create a time gap, then capture the timestamp
		{
			let res = client
				.post(sql_url)
				.body(
					r#"
					SLEEP 100ms;
					RETURN time::now();
				"#,
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			// Extract the timestamp from the second result
			let ts = body[1]["result"].as_str().unwrap().to_string();

			// Sleep again, then add more data and update existing records
			let res = client
				.post(sql_url)
				.body(
					r#"
					SLEEP 100ms;
					CREATE item:3 SET name = "Gamma", price = 30.0;
					UPDATE item:1 SET name = "Alpha Updated", price = 15.0;
				"#,
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);

			// Test 1: Query without version — should return current data (3 items)
			{
				let res = client
					.post(gql_url)
					.body(
						json!({"query": r#"query { item(order: {asc: id}) { id name price } }"#})
							.to_string(),
					)
					.send()
					.await?;
				assert_eq!(res.status(), 200);
				let body = res.json::<serde_json::Value>().await?;
				let items = body["data"]["item"].as_array().unwrap();
				assert_eq!(items.len(), 3, "Current data should have 3 items: {body}");
				// item:1 should be updated
				assert_eq!(items[0]["name"], "Alpha Updated");
				assert_eq!(items[0]["price"], 15.0);
			}

			// Test 2: Query with version — should return data as it was at
			// the captured timestamp (2 items, with original values)
			{
				let query = format!(
					r#"query {{ item(version: "{ts}", order: {{asc: id}}) {{ id name price }} }}"#
				);
				let res =
					client.post(gql_url).body(json!({"query": query}).to_string()).send().await?;
				assert_eq!(res.status(), 200);
				let body = res.json::<serde_json::Value>().await?;
				let items = body["data"]["item"].as_array().unwrap();
				assert_eq!(items.len(), 2, "Versioned query should have 2 items: {body}");
				// item:1 should still have original values
				assert_eq!(items[0]["name"], "Alpha");
				assert_eq!(items[0]["price"], 10.0);
				assert_eq!(items[1]["name"], "Beta");
			}

			// Test 3: _get_ with version — single record fetch at historical time
			{
				let query = format!(
					r#"query {{ _get_item(id: "1", version: "{ts}") {{ id name price }} }}"#
				);
				let res =
					client.post(gql_url).body(json!({"query": query}).to_string()).send().await?;
				assert_eq!(res.status(), 200);
				let body = res.json::<serde_json::Value>().await?;
				let item = &body["data"]["_get_item"];
				assert_eq!(
					item["name"], "Alpha",
					"Versioned _get_ should see original name: {body}"
				);
				assert_eq!(item["price"], 10.0);
			}

			// Test 4: _get_ without version — should see the updated value
			{
				let res = client
					.post(gql_url)
					.body(
						json!({"query": r#"query { _get_item(id: "1") { id name price } }"#})
							.to_string(),
					)
					.send()
					.await?;
				assert_eq!(res.status(), 200);
				let body = res.json::<serde_json::Value>().await?;
				let item = &body["data"]["_get_item"];
				assert_eq!(item["name"], "Alpha Updated");
				assert_eq!(item["price"], 15.0);
			}

			// Test 5: version argument with invalid datetime — should return error
			{
				let res = client
					.post(gql_url)
					.body(
						json!({"query": r#"query { item(version: "not-a-date") { id } }"#})
							.to_string(),
					)
					.send()
					.await?;
				assert_eq!(res.status(), 200);
				let body = res.json::<serde_json::Value>().await?;
				assert!(
					body["errors"].as_array().is_some_and(|e| !e.is_empty()),
					"Invalid version should produce an error: {body}"
				);
			}

			// Test 6: Schema introspection — verify version argument exists on list query
			{
				let res = client
					.post(gql_url)
					.body(
						json!({"query": r#"{
							__type(name: "Query") {
								fields {
									name
									args { name type { name } }
								}
							}
						}"#})
						.to_string(),
					)
					.send()
					.await?;
				assert_eq!(res.status(), 200);
				let body = res.json::<serde_json::Value>().await?;
				let fields = body["data"]["__type"]["fields"].as_array().unwrap();

				// Check the 'item' list query has a 'version' argument
				let item_field = fields.iter().find(|f| f["name"] == "item").unwrap();
				let version_arg =
					item_field["args"].as_array().unwrap().iter().find(|a| a["name"] == "version");
				assert!(
					version_arg.is_some(),
					"List query should have a 'version' argument: {body}"
				);
				assert_eq!(
					version_arg.unwrap()["type"]["name"],
					"String",
					"version argument should be of type String"
				);

				// Check the '_get_item' query has a 'version' argument
				let get_item_field = fields.iter().find(|f| f["name"] == "_get_item").unwrap();
				let version_arg = get_item_field["args"]
					.as_array()
					.unwrap()
					.iter()
					.find(|a| a["name"] == "version");
				assert!(
					version_arg.is_some(),
					"_get_ query should have a 'version' argument: {body}"
				);

				// Check the generic '_get' query has a 'version' argument
				let get_field = fields.iter().find(|f| f["name"] == "_get").unwrap();
				let version_arg =
					get_field["args"].as_array().unwrap().iter().find(|a| a["name"] == "version");
				assert!(
					version_arg.is_some(),
					"Generic _get query should have a 'version' argument: {body}"
				);
			}
		}

		Ok(())
	}

	#[test(tokio::test)]
	async fn filters() -> Result<(), Box<dyn std::error::Error>> {
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
			.connect_timeout(Duration::from_secs(10))
			.default_headers(headers)
			.build()?;

		// Set up schema and data
		{
			let res = client
				.post(sql_url)
				.body(
					r#"
					DEFINE CONFIG GRAPHQL AUTO;
					DEFINE TABLE product SCHEMAFUL;
					DEFINE FIELD name ON product TYPE string;
					DEFINE FIELD price ON product TYPE float;
					DEFINE FIELD quantity ON product TYPE int;
					DEFINE FIELD created ON product TYPE datetime;

					CREATE product:1 SET name = "Alpha Widget", price = 9.99, quantity = 100, created = d"2024-01-15T00:00:00Z";
					CREATE product:2 SET name = "Beta Widget", price = 19.99, quantity = 50, created = d"2024-03-20T00:00:00Z";
					CREATE product:3 SET name = "Gamma Tool", price = 29.99, quantity = 200, created = d"2024-06-01T00:00:00Z";
					CREATE product:4 SET name = "Delta Tool", price = 4.99, quantity = 10, created = d"2024-09-10T00:00:00Z";
					CREATE product:5 SET name = "Epsilon Widget", price = 49.99, quantity = 0, created = d"2025-01-05T00:00:00Z";
				"#,
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
		}

		// --- Test `where` is an alias for `filter` ---
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"query { product(where: { name: { eq: "Alpha Widget" } }) { id } }"#})
						.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			let products = body["data"]["product"].as_array().unwrap();
			assert_eq!(products.len(), 1);
			assert_eq!(products[0]["id"], "product:1");
		}

		// --- eq / ne ---
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"query { product(filter: { name: { ne: "Alpha Widget" } }) { id } }"#})
						.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			let products = body["data"]["product"].as_array().unwrap();
			assert_eq!(products.len(), 4);
		}

		// --- gt / lt on int ---
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"query { product(filter: { quantity: { gt: 50 } }) { id } }"#})
						.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			let products = body["data"]["product"].as_array().unwrap();
			// quantity > 50: product:1 (100), product:3 (200)
			assert_eq!(products.len(), 2);
		}

		// --- gte / lte on float ---
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"query { product(filter: { price: { gte: 19.99 } }) { id } }"#})
						.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			let products = body["data"]["product"].as_array().unwrap();
			// price >= 19.99: product:2 (19.99), product:3 (29.99), product:5 (49.99)
			assert_eq!(products.len(), 3);
		}

		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"query { product(filter: { price: { lte: 9.99 } }) { id } }"#})
						.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			let products = body["data"]["product"].as_array().unwrap();
			// price <= 9.99: product:1 (9.99), product:4 (4.99)
			assert_eq!(products.len(), 2);
		}

		// --- contains (string) ---
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"query { product(filter: { name: { contains: "Widget" } }) { id } }"#})
						.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			let products = body["data"]["product"].as_array().unwrap();
			// Widget: product:1, product:2, product:5
			assert_eq!(products.len(), 3);
		}

		// --- startsWith ---
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"query { product(filter: { name: { startsWith: "Delta" } }) { id } }"#})
						.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			let products = body["data"]["product"].as_array().unwrap();
			assert_eq!(products.len(), 1);
			assert_eq!(products[0]["id"], "product:4");
		}

		// --- endsWith ---
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"query { product(filter: { name: { endsWith: "Tool" } }) { id } }"#})
						.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			let products = body["data"]["product"].as_array().unwrap();
			// "Gamma Tool", "Delta Tool"
			assert_eq!(products.len(), 2);
		}

		// --- regex ---
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"query { product(filter: { name: { regex: "^(Alpha|Gamma)" } }) { id } }"#})
						.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			let products = body["data"]["product"].as_array().unwrap();
			// Alpha Widget, Gamma Tool
			assert_eq!(products.len(), 2);
		}

		// --- in (string list) ---
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"query { product(filter: { name: { in: ["Alpha Widget", "Delta Tool"] } }) { id } }"#})
						.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			let products = body["data"]["product"].as_array().unwrap();
			assert_eq!(products.len(), 2);
		}

		// --- in (int list) ---
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"query { product(filter: { quantity: { in: [100, 200] } }) { id } }"#})
						.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			let products = body["data"]["product"].as_array().unwrap();
			// product:1 (100), product:3 (200)
			assert_eq!(products.len(), 2);
		}

		// --- Implicit AND: multiple fields in one filter object ---
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"query { product(filter: { name: { contains: "Widget" }, price: { lt: 10 } }) { id } }"#})
						.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			let products = body["data"]["product"].as_array().unwrap();
			// Widget AND price < 10: product:1 (Alpha Widget, 9.99)
			assert_eq!(products.len(), 1);
			assert_eq!(products[0]["id"], "product:1");
		}

		// --- Multiple operators on the same field (implicit AND) ---
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"query { product(filter: { price: { gte: 10, lte: 30 } }) { id } }"#})
						.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			let products = body["data"]["product"].as_array().unwrap();
			// 10 <= price <= 30: product:2 (19.99), product:3 (29.99)
			assert_eq!(products.len(), 2);
		}

		// --- not operator ---
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"query { product(filter: { not: { name: { contains: "Widget" } } }) { id } }"#})
						.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			let products = body["data"]["product"].as_array().unwrap();
			// NOT Widget: product:3, product:4
			assert_eq!(products.len(), 2);
		}

		// --- and / or logical operators ---
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"query { product(filter: { or: [{ price: { lt: 5 } }, { price: { gt: 40 } }] }) { id } }"#})
						.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			let products = body["data"]["product"].as_array().unwrap();
			// price < 5 OR price > 40: product:4 (4.99), product:5 (49.99)
			assert_eq!(products.len(), 2);
		}

		// --- gt/lt on datetime ---
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"query { product(filter: { created: { gt: "2024-06-01T00:00:00Z" } }) { id } }"#})
						.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			let products = body["data"]["product"].as_array().unwrap();
			// after 2024-06-01: product:4, product:5
			assert_eq!(products.len(), 2);
		}

		Ok(())
	}

	#[test(tokio::test)]
	async fn nested_objects() -> Result<(), Box<dyn std::error::Error>> {
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
			.connect_timeout(Duration::from_secs(10))
			.default_headers(headers)
			.build()?;

		// Set up schema with nested objects and array-of-objects
		{
			let res = client
				.post(sql_url)
				.body(
					r#"
					DEFINE CONFIG GRAPHQL AUTO;

					DEFINE TABLE item SCHEMAFULL;
					DEFINE FIELD name ON item TYPE string;
					DEFINE FIELD time ON item TYPE object;
					DEFINE FIELD time.createdAt ON item TYPE datetime;
					DEFINE FIELD time.updatedAt ON item TYPE datetime;
					DEFINE FIELD tags ON item TYPE array<object>;
					DEFINE FIELD tags.* ON item TYPE object;
					DEFINE FIELD tags.*.label ON item TYPE string;
					DEFINE FIELD tags.*.priority ON item TYPE int;

					DEFINE TABLE article SCHEMAFULL;
					DEFINE FIELD title ON article TYPE string;
					DEFINE FIELD meta ON article TYPE option<object>;
					DEFINE FIELD meta.author ON article TYPE string;
					DEFINE FIELD meta.source ON article TYPE string;
				"#,
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
		}

		// Insert test data
		{
			let res = client
				.post(sql_url)
				.body(
					r#"
					CREATE item:alpha SET
						name = "Alpha",
						time = { createdAt: d"2024-01-15T10:00:00Z", updatedAt: d"2024-06-01T12:00:00Z" },
						tags = [
							{ label: "urgent", priority: 1 },
							{ label: "review", priority: 3 }
						];
					CREATE item:beta SET
						name = "Beta",
						time = { createdAt: d"2024-03-20T08:00:00Z", updatedAt: d"2024-07-10T16:00:00Z" },
						tags = [
							{ label: "feature", priority: 2 }
						];
				"#,
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
		}

		// --- Test 1: Query nested object sub-fields (time { createdAt, updatedAt }) ---
		{
			let res = client
				.post(gql_url)
				.body(
					json!({
						"query": r#"query {
							item(order: { asc: id }) {
								id
								name
								time {
									createdAt
									updatedAt
								}
							}
						}"#
					})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(body["errors"].is_null(), "Expected no errors, got: {:?}", body["errors"]);
			let items = body["data"]["item"].as_array().unwrap();
			assert_eq!(items.len(), 2);

			// First item
			assert_eq!(items[0]["id"], "item:alpha");
			assert_eq!(items[0]["name"], "Alpha");
			assert!(
				items[0]["time"]["createdAt"].as_str().unwrap().contains("2024-01-15"),
				"Expected createdAt to contain 2024-01-15, got: {}",
				items[0]["time"]["createdAt"]
			);
			assert!(
				items[0]["time"]["updatedAt"].as_str().unwrap().contains("2024-06-01"),
				"Expected updatedAt to contain 2024-06-01, got: {}",
				items[0]["time"]["updatedAt"]
			);

			// Second item
			assert_eq!(items[1]["id"], "item:beta");
			assert_eq!(items[1]["name"], "Beta");
			assert!(items[1]["time"]["createdAt"].as_str().unwrap().contains("2024-03-20"),);
		}

		// --- Test 2: Query array-of-object sub-fields (tags { label, priority }) ---
		{
			let res = client
				.post(gql_url)
				.body(
					json!({
						"query": r#"query {
							item(order: { asc: id }) {
								id
								tags {
									label
									priority
								}
							}
						}"#
					})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(body["errors"].is_null(), "Expected no errors, got: {:?}", body["errors"]);
			let items = body["data"]["item"].as_array().unwrap();
			assert_eq!(items.len(), 2);

			// First item has two tags
			let tags0 = items[0]["tags"].as_array().unwrap();
			assert_eq!(tags0.len(), 2);
			assert_eq!(tags0[0]["label"], "urgent");
			assert_eq!(tags0[0]["priority"], 1);
			assert_eq!(tags0[1]["label"], "review");
			assert_eq!(tags0[1]["priority"], 3);

			// Second item has one tag
			let tags1 = items[1]["tags"].as_array().unwrap();
			assert_eq!(tags1.len(), 1);
			assert_eq!(tags1[0]["label"], "feature");
			assert_eq!(tags1[0]["priority"], 2);
		}

		// --- Test 3: Select only specific sub-fields ---
		{
			let res = client
				.post(gql_url)
				.body(
					json!({
						"query": r#"query {
							item(order: { asc: id }) {
								name
								time {
									createdAt
								}
								tags {
									label
								}
							}
						}"#
					})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(body["errors"].is_null(), "Expected no errors, got: {:?}", body["errors"]);
			let items = body["data"]["item"].as_array().unwrap();
			assert_eq!(items.len(), 2);

			// time should only have createdAt (not updatedAt)
			assert!(items[0]["time"]["createdAt"].is_string());
			assert!(items[0]["time"].get("updatedAt").is_none());

			// tags should only have label (not priority)
			let tags = items[0]["tags"].as_array().unwrap();
			assert!(tags[0]["label"].is_string());
			assert!(tags[0].get("priority").is_none());
		}

		// --- Test 4: Single record fetch with nested objects ---
		{
			let res = client
				.post(gql_url)
				.body(
					json!({
						"query": r#"query {
							_get_item(id: "alpha") {
								id
								name
								time {
									createdAt
									updatedAt
								}
								tags {
									label
									priority
								}
							}
						}"#
					})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(body["errors"].is_null(), "Expected no errors, got: {:?}", body["errors"]);
			let item = &body["data"]["_get_item"];
			assert_eq!(item["id"], "item:alpha");
			assert_eq!(item["name"], "Alpha");
			assert!(item["time"]["createdAt"].as_str().unwrap().contains("2024-01-15"));
			let tags = item["tags"].as_array().unwrap();
			assert_eq!(tags.len(), 2);
			assert_eq!(tags[0]["label"], "urgent");
		}

		// --- Test 5: Schema introspection shows generated nested types ---
		{
			let res = client
				.post(gql_url)
				.body(
					json!({
						"query": r#"query {
							__type(name: "item_time") {
								name
								fields {
									name
									type {
										name
										kind
									}
								}
							}
						}"#
					})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(body["errors"].is_null(), "Expected no errors, got: {:?}", body["errors"]);
			let ty = &body["data"]["__type"];
			assert_eq!(ty["name"], "item_time");
			let fields = ty["fields"].as_array().unwrap();
			let field_names: Vec<&str> =
				fields.iter().map(|f| f["name"].as_str().unwrap()).collect();
			assert!(field_names.contains(&"createdAt"), "Expected createdAt field");
			assert!(field_names.contains(&"updatedAt"), "Expected updatedAt field");
		}

		// --- Test 6: Schema introspection for array element type ---
		{
			let res = client
				.post(gql_url)
				.body(
					json!({
						"query": r#"query {
							__type(name: "item_tags") {
								name
								fields {
									name
									type {
										name
										kind
									}
								}
							}
						}"#
					})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(body["errors"].is_null(), "Expected no errors, got: {:?}", body["errors"]);
			let ty = &body["data"]["__type"];
			assert_eq!(ty["name"], "item_tags");
			let fields = ty["fields"].as_array().unwrap();
			let field_names: Vec<&str> =
				fields.iter().map(|f| f["name"].as_str().unwrap()).collect();
			assert!(field_names.contains(&"label"), "Expected label field");
			assert!(field_names.contains(&"priority"), "Expected priority field");
		}

		// --- Test 7: Optional nested object fields handled gracefully ---
		{
			// Insert article data (table defined in setup)
			let res = client
				.post(sql_url)
				.body(
					r#"
					CREATE article:with_meta SET
						title = "Article One",
						meta = { author: "Alice", source: "Blog" };
					CREATE article:no_meta SET
						title = "Article Two";
				"#,
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);

			// Query the article with meta
			let res = client
				.post(gql_url)
				.body(
					json!({
						"query": r#"query {
							_get_article(id: "with_meta") {
								title
								meta {
									author
									source
								}
							}
						}"#
					})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(body["errors"].is_null(), "Expected no errors, got: {:?}", body["errors"]);
			let article = &body["data"]["_get_article"];
			assert_eq!(article["title"], "Article One");
			assert_eq!(article["meta"]["author"], "Alice");
			assert_eq!(article["meta"]["source"], "Blog");

			// Query the article without meta — should return null for meta
			let res = client
				.post(gql_url)
				.body(
					json!({
						"query": r#"query {
							_get_article(id: "no_meta") {
								title
								meta {
									author
									source
								}
							}
						}"#
					})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(body["errors"].is_null(), "Expected no errors, got: {:?}", body["errors"]);
			let article = &body["data"]["_get_article"];
			assert_eq!(article["title"], "Article Two");
			assert!(
				article["meta"].is_null(),
				"Expected meta to be null, got: {:?}",
				article["meta"]
			);
		}

		Ok(())
	}

	#[test(tokio::test)]
	async fn serialization() -> Result<(), Box<dyn std::error::Error>> {
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
			.connect_timeout(Duration::from_secs(10))
			.default_headers(headers)
			.build()?;

		// Set up schema with various field types to test serialization
		{
			let res = client
				.post(sql_url)
				.body(
					r#"
					DEFINE CONFIG GRAPHQL AUTO;

					DEFINE TABLE department SCHEMAFULL;
					DEFINE FIELD name ON department TYPE string;

					DEFINE TABLE widget SCHEMAFULL;
					DEFINE FIELD name ON widget TYPE string;
					DEFINE FIELD created ON widget TYPE datetime;
					DEFINE FIELD lifespan ON widget TYPE duration;
					DEFINE FIELD tracking ON widget TYPE uuid;
					DEFINE FIELD payload ON widget TYPE bytes;
					DEFINE FIELD tags ON widget TYPE array<string>;
					DEFINE FIELD dept ON widget TYPE option<record<department>>;

					CREATE department:eng SET name = "Engineering";
					CREATE department:mkt SET name = "Marketing";

					CREATE widget:alpha SET
						name = "Alpha",
						created = d"2024-06-15T10:30:00Z",
						lifespan = 1h30m,
						tracking = u"550e8400-e29b-41d4-a716-446655440000",
						payload = <bytes>"Hello",
						tags = ["urgent", "review"],
						dept = department:eng;

					CREATE widget:beta SET
						name = "Beta",
						created = d"2025-01-01T00:00:00Z",
						lifespan = 2d12h,
						tracking = u"6ba7b810-9dad-11d1-80b4-00c04fd430c8",
						payload = <bytes>"AB",
						tags = [],
						dept = NONE;
				"#,
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200, "SQL setup failed");
			let sql_body = res.text().await?;
			// Verify no errors in SQL setup
			assert!(!sql_body.contains("\"status\":\"ERR\""), "SQL setup had errors: {sql_body}");
		}

		// --- Test 1: Datetime is serialized as RFC 3339 string ---
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"query {
						_get_widget(id: "alpha") { created }
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			let status = res.status();
			let body = res.json::<serde_json::Value>().await?;
			assert_eq!(status, 200, "Expected 200, body: {body}");
			assert!(body["errors"].is_null(), "Unexpected errors: {:?}", body["errors"]);
			let created = body["data"]["_get_widget"]["created"]
				.as_str()
				.unwrap_or_else(|| panic!("created should be a string, body: {body}"));
			assert!(
				created.contains("2024-06-15"),
				"Expected RFC 3339 datetime containing '2024-06-15', got: {created}"
			);
			// Should not have SurrealQL d'...' wrapping
			assert!(
				!created.starts_with("d'"),
				"Datetime should not have SurrealQL d'' prefix, got: {created}"
			);
		}

		// --- Test 2: Duration is serialized as a clean string ---
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"query {
						_get_widget(id: "alpha") { lifespan }
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(body["errors"].is_null(), "Unexpected errors: {:?}", body["errors"]);
			let lifespan = body["data"]["_get_widget"]["lifespan"].as_str().unwrap();
			// Duration should be a clean string like "1h30m" without quotes/wrapping
			assert!(!lifespan.is_empty(), "Duration should not be empty");
			assert!(
				!lifespan.starts_with("d'") && !lifespan.starts_with('\''),
				"Duration should not have SurrealQL wrapping, got: {lifespan}"
			);
		}

		// --- Test 3: UUID is serialized as a standard UUID string ---
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"query {
						_get_widget(id: "alpha") { tracking }
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(body["errors"].is_null(), "Unexpected errors: {:?}", body["errors"]);
			let tracking = body["data"]["_get_widget"]["tracking"].as_str().unwrap();
			assert_eq!(
				tracking, "550e8400-e29b-41d4-a716-446655440000",
				"UUID should be in standard format"
			);
			// Should not have SurrealQL u'...' wrapping
			assert!(
				!tracking.starts_with("u'"),
				"UUID should not have SurrealQL u'' prefix, got: {tracking}"
			);
		}

		// --- Test 4: Bytes are serialized as base64 string ---
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"query {
						_get_widget(id: "alpha") { payload }
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(body["errors"].is_null(), "Unexpected errors: {:?}", body["errors"]);
			let payload = body["data"]["_get_widget"]["payload"].as_str().unwrap();
			// "Hello" → base64 = "SGVsbG8="
			assert_eq!(payload, "SGVsbG8=", "Bytes should be base64 encoded, got: {payload}");
		}

		// --- Test 5: RecordId in arrays/objects uses raw format ---
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"query {
						widget(order: {asc: id}) { id }
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(body["errors"].is_null(), "Unexpected errors: {:?}", body["errors"]);
			let widgets = body["data"]["widget"].as_array().unwrap();
			assert_eq!(widgets[0]["id"], "widget:alpha");
			assert_eq!(widgets[1]["id"], "widget:beta");
		}

		// --- Test 6: Arrays with nested values propagate correctly ---
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"query {
						_get_widget(id: "alpha") { tags }
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(body["errors"].is_null(), "Unexpected errors: {:?}", body["errors"]);
			let tags = body["data"]["_get_widget"]["tags"].as_array().unwrap();
			assert_eq!(tags.len(), 2);
			assert_eq!(tags[0], "urgent");
			assert_eq!(tags[1], "review");
		}

		// --- Test 7: Empty arrays don't cause panics ---
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"query {
						_get_widget(id: "beta") { tags }
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(body["errors"].is_null(), "Unexpected errors: {:?}", body["errors"]);
			let tags = body["data"]["_get_widget"]["tags"].as_array().unwrap();
			assert_eq!(tags.len(), 0);
		}

		// --- Test 8: option<record> field — set to a value ---
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"query {
						_get_widget(id: "alpha") {
							name
							dept {
								id
								name
							}
						}
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(body["errors"].is_null(), "Unexpected errors: {:?}", body["errors"]);
			let widget = &body["data"]["_get_widget"];
			assert_eq!(widget["name"], "Alpha");
			assert_eq!(widget["dept"]["id"], "department:eng");
			assert_eq!(widget["dept"]["name"], "Engineering");
		}

		// --- Test 9: option<record> field — set to NONE (null) ---
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"query {
						_get_widget(id: "beta") {
							name
							dept {
								id
								name
							}
						}
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(body["errors"].is_null(), "Unexpected errors: {:?}", body["errors"]);
			let widget = &body["data"]["_get_widget"];
			assert_eq!(widget["name"], "Beta");
			assert!(
				widget["dept"].is_null(),
				"Expected dept to be null for NONE value, got: {:?}",
				widget["dept"]
			);
		}

		// --- Test 10: Schema introspection shows option<record> as nullable type ---
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"{
						__type(name: "widget") {
							fields {
								name
								type {
									name
									kind
									ofType { name kind }
								}
							}
						}
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(body["errors"].is_null(), "Unexpected errors: {:?}", body["errors"]);
			let fields = body["data"]["__type"]["fields"].as_array().unwrap();
			let dept_field = fields.iter().find(|f| f["name"] == "dept").unwrap();
			let type_info = &dept_field["type"];
			// option<record<department>> should be nullable (not NON_NULL),
			// and the inner type should be "department" (not a union like "none_or_department")
			assert_ne!(
				type_info["kind"], "NON_NULL",
				"option<record> should be nullable, got: {type_info:?}"
			);
			// The type should resolve to the department table type (not a union)
			let type_name = type_info["name"].as_str().unwrap_or("");
			assert_eq!(
				type_name, "department",
				"option<record<department>> should resolve to 'department' type, got: {type_name}"
			);
		}

		Ok(())
	}

	#[test(tokio::test)]
	async fn mutations() -> Result<(), Box<dyn std::error::Error>> {
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
			.connect_timeout(Duration::from_secs(10))
			.default_headers(headers)
			.build()?;

		// Setup schema
		{
			let res = client
				.post(sql_url)
				.body(
					r#"
					DEFINE CONFIG GRAPHQL AUTO;
					DEFINE TABLE item SCHEMAFUL;
					DEFINE FIELD name ON item TYPE string;
					DEFINE FIELD price ON item TYPE int;
					DEFINE TABLE person SCHEMAFUL;
					DEFINE FIELD name ON person TYPE string;
					DEFINE TABLE post SCHEMAFUL;
					DEFINE FIELD title ON post TYPE string;
					DEFINE TABLE likes TYPE RELATION FROM person TO post SCHEMAFUL;
					DEFINE FIELD rating ON likes TYPE int;
				"#,
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
		}

		// --- Test 1: createItem (single create with explicit id) ---
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"mutation {
						createItem(data: { id: "1", name: "Widget", price: 100 }) {
							id
							name
							price
						}
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(body["errors"].is_null(), "Unexpected errors: {:?}", body["errors"]);
			let item = &body["data"]["createItem"];
			assert_eq!(item["id"], "item:1");
			assert_eq!(item["name"], "Widget");
			assert_eq!(item["price"], 100);
		}

		// --- Test 2: createItem (auto-generated id) ---
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"mutation {
						createItem(data: { name: "Gadget", price: 200 }) {
							id
							name
							price
						}
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(body["errors"].is_null(), "Unexpected errors: {:?}", body["errors"]);
			let item = &body["data"]["createItem"];
			// id should be auto-generated
			assert!(item["id"].as_str().unwrap().starts_with("item:"));
			assert_eq!(item["name"], "Gadget");
			assert_eq!(item["price"], 200);
		}

		// --- Test 3: updateItem ---
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"mutation {
						updateItem(id: "1", data: { name: "Super Widget" }) {
							id
							name
							price
						}
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(body["errors"].is_null(), "Unexpected errors: {:?}", body["errors"]);
			let item = &body["data"]["updateItem"];
			assert_eq!(item["id"], "item:1");
			assert_eq!(item["name"], "Super Widget");
			// price should be unchanged (MERGE, not CONTENT)
			assert_eq!(item["price"], 100);
		}

		// --- Test 4: upsertItem (existing record) ---
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"mutation {
						upsertItem(id: "1", data: { name: "Mega Widget", price: 150 }) {
							id
							name
							price
						}
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(body["errors"].is_null(), "Unexpected errors: {:?}", body["errors"]);
			let item = &body["data"]["upsertItem"];
			assert_eq!(item["id"], "item:1");
			assert_eq!(item["name"], "Mega Widget");
			assert_eq!(item["price"], 150);
		}

		// --- Test 5: upsertItem (new record) ---
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"mutation {
						upsertItem(id: "99", data: { name: "New Item", price: 50 }) {
							id
							name
							price
						}
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(body["errors"].is_null(), "Unexpected errors: {:?}", body["errors"]);
			let item = &body["data"]["upsertItem"];
			assert_eq!(item["id"], "item:99");
			assert_eq!(item["name"], "New Item");
			assert_eq!(item["price"], 50);
		}

		// --- Test 6: deleteItem ---
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"mutation {
						deleteItem(id: "99")
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(body["errors"].is_null(), "Unexpected errors: {:?}", body["errors"]);
			assert_eq!(body["data"]["deleteItem"], true);
		}

		// Verify deletion via query
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"query {
						_get_item(id: "99") { id }
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(body["errors"].is_null(), "Unexpected errors: {:?}", body["errors"]);
			assert!(body["data"]["_get_item"].is_null());
		}

		// --- Test 7: createManyItem (bulk create) ---
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"mutation {
						createManyItem(data: [
							{ id: "a", name: "Alpha", price: 10 },
							{ id: "b", name: "Beta", price: 20 },
							{ id: "c", name: "Gamma", price: 30 }
						]) {
							id
							name
							price
						}
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(body["errors"].is_null(), "Unexpected errors: {:?}", body["errors"]);
			let items = body["data"]["createManyItem"].as_array().unwrap();
			assert_eq!(items.len(), 3);
			assert_eq!(items[0]["id"], "item:a");
			assert_eq!(items[1]["id"], "item:b");
			assert_eq!(items[2]["id"], "item:c");
		}

		// --- Test 8: updateManyItem (bulk update with where) ---
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"mutation {
						updateManyItem(
							where: { price: { lt: 25 } },
							data: { price: 25 }
						) {
							id
							name
							price
						}
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(body["errors"].is_null(), "Unexpected errors: {:?}", body["errors"]);
			let items = body["data"]["updateManyItem"].as_array().unwrap();
			// items a (10) and b (20) should be updated, c (30) should not
			assert_eq!(items.len(), 2);
			for item in items {
				assert_eq!(item["price"], 25);
			}
		}

		// --- Test 9: deleteManyItem (bulk delete with where, returns count) ---
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"mutation {
						deleteManyItem(where: { price: { eq: 25 } })
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(body["errors"].is_null(), "Unexpected errors: {:?}", body["errors"]);
			assert_eq!(body["data"]["deleteManyItem"], 2);
		}

		// --- Test 10: Relation table mutation (createLikes via RELATE) ---
		{
			// First create the records to relate
			client
				.post(sql_url)
				.body(
					r#"
					CREATE person:alice SET name = "Alice";
					CREATE post:1 SET title = "Hello World";
				"#,
				)
				.send()
				.await?;

			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"mutation {
						createLikes(data: {
							in: "person:alice",
							out: "post:1",
							rating: 5
						}) {
							id
							rating
						}
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(body["errors"].is_null(), "Unexpected errors: {:?}", body["errors"]);
			let likes = &body["data"]["createLikes"];
			assert!(likes["id"].as_str().unwrap().starts_with("likes:"));
			assert_eq!(likes["rating"], 5);
		}

		// --- Test 11: Schema introspection shows mutation type ---
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"{
						__schema {
							mutationType {
								name
								fields { name }
							}
						}
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(body["errors"].is_null(), "Unexpected errors: {:?}", body["errors"]);
			let mutation_type = &body["data"]["__schema"]["mutationType"];
			assert_eq!(mutation_type["name"], "Mutation");

			let fields = mutation_type["fields"].as_array().unwrap();
			let field_names: Vec<&str> =
				fields.iter().map(|f| f["name"].as_str().unwrap()).collect();

			// Check that all expected mutation fields exist
			assert!(field_names.contains(&"createItem"), "Missing createItem");
			assert!(field_names.contains(&"updateItem"), "Missing updateItem");
			assert!(field_names.contains(&"upsertItem"), "Missing upsertItem");
			assert!(field_names.contains(&"deleteItem"), "Missing deleteItem");
			assert!(field_names.contains(&"createManyItem"), "Missing createManyItem");
			assert!(field_names.contains(&"updateManyItem"), "Missing updateManyItem");
			assert!(field_names.contains(&"upsertManyItem"), "Missing upsertManyItem");
			assert!(field_names.contains(&"deleteManyItem"), "Missing deleteManyItem");
		}

		// --- Test 12: Input type introspection ---
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"{
						createInput: __type(name: "CreateItemInput") {
							kind
							inputFields { name type { name kind ofType { name kind } } }
						}
						updateInput: __type(name: "UpdateItemInput") {
							kind
							inputFields { name type { name kind ofType { name kind } } }
						}
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(body["errors"].is_null(), "Unexpected errors: {:?}", body["errors"]);

			// CreateItemInput should exist as INPUT_OBJECT
			let create_input = &body["data"]["createInput"];
			assert_eq!(create_input["kind"], "INPUT_OBJECT");
			let create_fields = create_input["inputFields"].as_array().unwrap();
			let create_field_names: Vec<&str> =
				create_fields.iter().map(|f| f["name"].as_str().unwrap()).collect();
			assert!(create_field_names.contains(&"id"), "CreateInput missing 'id'");
			assert!(create_field_names.contains(&"name"), "CreateInput missing 'name'");
			assert!(create_field_names.contains(&"price"), "CreateInput missing 'price'");

			// UpdateItemInput should have all fields optional
			let update_input = &body["data"]["updateInput"];
			assert_eq!(update_input["kind"], "INPUT_OBJECT");
			let update_fields = update_input["inputFields"].as_array().unwrap();
			for field in update_fields {
				// No field should be NON_NULL in update input
				assert_ne!(
					field["type"]["kind"], "NON_NULL",
					"Update input field '{}' should be optional",
					field["name"]
				);
			}
		}

		Ok(())
	}

	#[test(tokio::test)]
	async fn depth_and_complexity_limits() -> Result<(), Box<dyn std::error::Error>> {
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
			.connect_timeout(Duration::from_secs(10))
			.default_headers(headers)
			.build()?;

		// Set up schema with depth and complexity limits
		{
			let res = client
				.post(sql_url)
				.body(
					r#"
					DEFINE CONFIG GRAPHQL AUTO DEPTH 3 COMPLEXITY 10;
					DEFINE TABLE person SCHEMAFUL;
					DEFINE FIELD name ON person TYPE string;
					DEFINE FIELD age ON person TYPE int;
					DEFINE TABLE post SCHEMAFUL;
					DEFINE FIELD title ON post TYPE string;
					DEFINE FIELD author ON post TYPE record<person>;
					DEFINE TABLE comment SCHEMAFUL;
					DEFINE FIELD text ON comment TYPE string;
					DEFINE FIELD post ON comment TYPE record<post>;
					CREATE person:1 SET name = 'Alice', age = 30;
					CREATE post:1 SET title = 'Hello', author = person:1;
					CREATE comment:1 SET text = 'Nice', post = post:1;
				"#,
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
		}

		// A simple shallow query should succeed (depth 2, within limit of 3)
		{
			let res = client
				.post(gql_url)
				.body(json!({"query": r#"{ person { id, name } }"#}).to_string())
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(body["errors"].is_null(), "Unexpected errors for shallow query: {:?}", body);
			assert!(body["data"]["person"].is_array(), "Expected person data");
		}

		// A deeply nested query should fail with depth limit error (depth > 3)
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"{ comment { text, post { title, author { name, age } } } }"#})
						.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			let errors = &body["errors"];
			assert!(errors.is_array(), "Expected errors for deep query, got: {:?}", body);
			let error_msg = errors[0]["message"].as_str().unwrap_or("");
			assert!(
				error_msg.contains("nested too deep") || error_msg.contains("too deep"),
				"Expected depth limit error, got: {error_msg}"
			);
		}

		// A query with too many fields should fail with complexity limit error (>10 fields)
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"{
						person { id, name, age }
						post { id, title }
						comment { id, text }
						p2: person { id, name, age }
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			let errors = &body["errors"];
			assert!(errors.is_array(), "Expected errors for complex query, got: {:?}", body);
			let error_msg = errors[0]["message"].as_str().unwrap_or("");
			assert!(
				error_msg.contains("too complex") || error_msg.contains("complexity"),
				"Expected complexity limit error, got: {error_msg}"
			);
		}

		// Reconfigure with higher limits and verify previously failing query works
		{
			let res = client
				.post(sql_url)
				.body(
					r#"
					DEFINE CONFIG OVERWRITE GRAPHQL AUTO DEPTH 10 COMPLEXITY 100;
				"#,
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
		}

		// The deeply nested query should now succeed
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"{ comment { text, post { title, author { name } } } }"#})
						.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(
				body["errors"].is_null(),
				"Expected no errors with raised limits, got: {:?}",
				body["errors"]
			);
		}

		// The high-field-count query should also succeed with higher complexity limit
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"{
						person { id, name, age }
						post { id, title }
						comment { id, text }
						p2: person { id, name, age }
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(
				body["errors"].is_null(),
				"Expected no errors with raised limits, got: {:?}",
				body["errors"]
			);
		}

		// Reconfigure without limits and verify everything works
		{
			let res = client
				.post(sql_url)
				.body(
					r#"
					DEFINE CONFIG OVERWRITE GRAPHQL AUTO;
				"#,
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
		}

		// All queries should succeed without any limits
		{
			let res = client
				.post(gql_url)
				.body(
					json!({"query": r#"{ comment { text, post { title, author { name, age } } } }"#})
						.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(
				body["errors"].is_null(),
				"Expected no errors without limits, got: {:?}",
				body["errors"]
			);
		}

		// Verify DEFINE CONFIG GRAPHQL round-trip preserves DEPTH and COMPLEXITY
		{
			let res = client
				.post(sql_url)
				.body(
					r#"
					DEFINE CONFIG OVERWRITE GRAPHQL AUTO DEPTH 5 COMPLEXITY 50;
					INFO FOR DB;
				"#,
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			let info_result = &body[1]["result"];
			let config_str = info_result["configs"]["GraphQL"].as_str().unwrap_or("");
			assert!(
				config_str.contains("DEPTH 5"),
				"Expected 'DEPTH 5' in config, got: {config_str}"
			);
			assert!(
				config_str.contains("COMPLEXITY 50"),
				"Expected 'COMPLEXITY 50' in config, got: {config_str}"
			);
		}

		Ok(())
	}

	#[test(tokio::test)]
	async fn auth_mutations() -> Result<(), Box<dyn std::error::Error>> {
		let (addr, _server) = common::start_server_gql().await.unwrap();
		let gql_url = &format!("http://{addr}/graphql");
		let sql_url = &format!("http://{addr}/sql");

		let mut headers = reqwest::header::HeaderMap::new();
		let ns = Ulid::new().to_string();
		let db = Ulid::new().to_string();
		headers.insert("surreal-ns", ns.parse()?);
		headers.insert("surreal-db", db.parse()?);
		headers.insert(header::ACCEPT, "application/json".parse()?);
		let client = Client::builder()
			.connect_timeout(Duration::from_secs(10))
			.default_headers(headers)
			.build()?;

		// Set up schema with an access method that has both SIGNIN and SIGNUP
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

					DEFINE TABLE user SCHEMAFUL
						PERMISSIONS FOR select, create, update, delete WHERE id = $auth;
					DEFINE FIELD email ON user TYPE string;
					DEFINE FIELD pass ON user TYPE string;

					DEFINE TABLE post SCHEMAFUL
						PERMISSIONS FOR select WHERE $auth != NONE
						FOR create, update, delete WHERE $auth != NONE;
					DEFINE FIELD title ON post TYPE string;
					DEFINE FIELD content ON post TYPE string;
				"#,
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200, "body: {}", res.text().await?);
		}

		// Test schema introspection: signIn and signUp should appear in Mutation type
		{
			let res = client
				.post(gql_url)
				.basic_auth(USER, Some(PASS))
				.body(
					json!({"query": r#"{
						__type(name: "Mutation") {
							fields { name }
						}
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(body["errors"].is_null(), "Introspection errors: {:?}", body["errors"]);
			let fields = &body["data"]["__type"]["fields"];
			let field_names: Vec<&str> =
				fields.as_array().unwrap().iter().map(|f| f["name"].as_str().unwrap()).collect();
			assert!(
				field_names.contains(&"signIn"),
				"Mutation should have signIn field, got: {field_names:?}"
			);
			assert!(
				field_names.contains(&"signUp"),
				"Mutation should have signUp field, got: {field_names:?}"
			);
		}

		// Test signUp: create a new user via GraphQL mutation
		let signup_token;
		{
			let res = client
				.post(gql_url)
				.basic_auth(USER, Some(PASS))
				.body(
					json!({"query": r#"mutation {
						signUp(access: "user", variables: { email: "alice@example.com", pass: "secret123" })
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(body["errors"].is_null(), "SignUp errors: {:?}", body["errors"]);
			let token = body["data"]["signUp"].as_str().unwrap();
			assert!(!token.is_empty(), "SignUp should return a non-empty JWT token");
			// JWT tokens have 3 parts separated by dots
			assert_eq!(token.split('.').count(), 3, "Token should be a valid JWT format");
			signup_token = token.to_string();
		}

		// Test that the signup token works for authentication
		{
			let res = client
				.post(gql_url)
				.bearer_auth(&signup_token)
				.body(json!({"query": r#"{ post { id } }"#}).to_string())
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(
				body["errors"].is_null(),
				"Authenticated query should succeed, got errors: {:?}",
				body["errors"]
			);
		}

		// Test signIn: authenticate with the newly created user
		let signin_token;
		{
			let res = client
				.post(gql_url)
				.basic_auth(USER, Some(PASS))
				.body(
					json!({"query": r#"mutation {
						signIn(access: "user", variables: { email: "alice@example.com", pass: "secret123" })
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(body["errors"].is_null(), "SignIn errors: {:?}", body["errors"]);
			let token = body["data"]["signIn"].as_str().unwrap();
			assert!(!token.is_empty(), "SignIn should return a non-empty JWT token");
			assert_eq!(token.split('.').count(), 3, "Token should be a valid JWT format");
			signin_token = token.to_string();
		}

		// Test that the signin token works for querying data
		{
			// First create a post using root to have some data
			let res = client
				.post(sql_url)
				.basic_auth(USER, Some(PASS))
				.body(r#"CREATE post:1 SET title = "Hello", content = "World";"#)
				.send()
				.await?;
			assert_eq!(res.status(), 200);

			// Then query using the signin token
			let res = client
				.post(gql_url)
				.bearer_auth(&signin_token)
				.body(json!({"query": r#"{ post { id title content } }"#}).to_string())
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(
				body["errors"].is_null(),
				"Query with signin token should succeed, got errors: {:?}",
				body["errors"]
			);
			let posts = &body["data"]["post"];
			assert!(posts.is_array(), "Expected array of posts");
			assert_eq!(posts.as_array().unwrap().len(), 1);
			assert_eq!(posts[0]["title"], "Hello");
		}

		// Test signIn with wrong credentials: should return an error
		{
			let res = client
				.post(gql_url)
				.basic_auth(USER, Some(PASS))
				.body(
					json!({"query": r#"mutation {
						signIn(access: "user", variables: { email: "alice@example.com", pass: "wrongpassword" })
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(body["errors"].is_array(), "SignIn with wrong password should return errors");
			let error_msg = body["errors"][0]["message"].as_str().unwrap_or("");
			assert!(
				error_msg.contains("Sign in failed"),
				"Error should mention sign in failure, got: {error_msg}"
			);
		}

		// Test signIn with non-existent access method: should return an error
		{
			let res = client
				.post(gql_url)
				.basic_auth(USER, Some(PASS))
				.body(
					json!({"query": r#"mutation {
						signIn(access: "nonexistent", variables: { email: "alice@example.com", pass: "secret123" })
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(
				body["errors"].is_array(),
				"SignIn with non-existent access should return errors"
			);
		}

		// Test signUp with duplicate email: should still work (creates another user record)
		{
			let res = client
				.post(gql_url)
				.basic_auth(USER, Some(PASS))
				.body(
					json!({"query": r#"mutation {
						signUp(access: "user", variables: { email: "bob@example.com", pass: "bobpass" })
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(body["errors"].is_null(), "Second signUp should succeed: {:?}", body["errors"]);
			let token = body["data"]["signUp"].as_str().unwrap();
			assert!(!token.is_empty());
		}

		// Test signIn field arguments via introspection
		{
			let res = client
				.post(gql_url)
				.basic_auth(USER, Some(PASS))
				.body(
					json!({"query": r#"{
						__type(name: "Mutation") {
							fields {
								name
								args { name type { name kind ofType { name } } }
								type { name kind ofType { name } }
							}
						}
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(body["errors"].is_null(), "Introspection errors: {:?}", body["errors"]);
			let fields = body["data"]["__type"]["fields"].as_array().unwrap();
			let sign_in = fields.iter().find(|f| f["name"] == "signIn").unwrap();
			let sign_up = fields.iter().find(|f| f["name"] == "signUp").unwrap();

			// signIn should return String! (NON_NULL String)
			assert_eq!(sign_in["type"]["kind"], "NON_NULL");
			assert_eq!(sign_in["type"]["ofType"]["name"], "String");

			// signUp should return String! (NON_NULL String)
			assert_eq!(sign_up["type"]["kind"], "NON_NULL");
			assert_eq!(sign_up["type"]["ofType"]["name"], "String");

			// signIn should have 'access' and 'variables' arguments
			let sign_in_args = sign_in["args"].as_array().unwrap();
			let arg_names: Vec<&str> =
				sign_in_args.iter().map(|a| a["name"].as_str().unwrap()).collect();
			assert!(arg_names.contains(&"access"), "signIn should have 'access' arg");
			assert!(arg_names.contains(&"variables"), "signIn should have 'variables' arg");

			// access should be String! (NON_NULL)
			let access_arg = sign_in_args.iter().find(|a| a["name"] == "access").unwrap();
			assert_eq!(access_arg["type"]["kind"], "NON_NULL");
			assert_eq!(access_arg["type"]["ofType"]["name"], "String");

			// variables should be JSON! (NON_NULL)
			let variables_arg = sign_in_args.iter().find(|a| a["name"] == "variables").unwrap();
			assert_eq!(variables_arg["type"]["kind"], "NON_NULL");
			assert_eq!(variables_arg["type"]["ofType"]["name"], "JSON");
		}

		// Test that when no signup clause exists, signUp is not available
		// (This test uses a separate ns/db with signin-only access)
		{
			let ns2 = Ulid::new().to_string();
			let db2 = Ulid::new().to_string();

			// Set up a signin-only access method in a new db
			let res = client
				.post(sql_url)
				.basic_auth(USER, Some(PASS))
				.header("surreal-ns", &ns2)
				.header("surreal-db", &db2)
				.body(
					r#"
					DEFINE CONFIG GRAPHQL AUTO;
					DEFINE ACCESS readonly_user ON DATABASE TYPE RECORD
						SIGNIN ( SELECT * FROM user WHERE email = $email AND crypto::argon2::compare(pass, $pass) )
						DURATION FOR SESSION 60s, FOR TOKEN 1d;
					DEFINE TABLE user SCHEMAFUL;
					DEFINE FIELD email ON user TYPE string;
					DEFINE FIELD pass ON user TYPE string;
				"#,
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200, "body: {}", res.text().await?);

			// Check that signIn exists but signUp does NOT
			let res = client
				.post(gql_url)
				.basic_auth(USER, Some(PASS))
				.header("surreal-ns", &ns2)
				.header("surreal-db", &db2)
				.body(
					json!({"query": r#"{
						__type(name: "Mutation") {
							fields { name }
						}
					}"#})
					.to_string(),
				)
				.send()
				.await?;
			assert_eq!(res.status(), 200);
			let body = res.json::<serde_json::Value>().await?;
			assert!(body["errors"].is_null(), "Introspection errors: {:?}", body["errors"]);
			let fields = &body["data"]["__type"]["fields"];
			let field_names: Vec<&str> =
				fields.as_array().unwrap().iter().map(|f| f["name"].as_str().unwrap()).collect();
			assert!(
				field_names.contains(&"signIn"),
				"Mutation should have signIn field when signin clause exists"
			);
			assert!(
				!field_names.contains(&"signUp"),
				"Mutation should NOT have signUp when no signup clause exists, got: {field_names:?}"
			);
		}

		Ok(())
	}
}
