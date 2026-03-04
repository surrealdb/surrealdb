// RUST_LOG=warn cargo test --no-default-features --features storage-mem,pgwire --test postgresql --
// postgresql
mod common;

#[cfg(feature = "pgwire")]
mod postgresql {
	use std::error::Error;

	use rand::{Rng, thread_rng};
	use rstest::rstest;
	use tokio_postgres::SimpleQueryMessage;

	use super::common::{self, PASS, StartServerArguments, USER};

	struct PgTestContext {
		client: tokio_postgres::Client,
		_server: common::Child,
	}

	async fn setup_pg() -> Result<PgTestContext, Box<dyn Error>> {
		let mut rng = thread_rng();
		let pg_port: u16 = rng.gen_range(24001..35000);
		let pg_addr = format!("127.0.0.1:{pg_port}");

		let (_http_addr, server) = common::start_server(StartServerArguments {
			auth: false,
			args: format!("--pgwire-listen {pg_addr}"),
			..Default::default()
		})
		.await?;

		let connstr =
			format!("host=127.0.0.1 port={pg_port} dbname=main.main user={USER} password={PASS}");

		let mut last_err = None;
		for _ in 0..10 {
			match tokio_postgres::connect(&connstr, tokio_postgres::NoTls).await {
				Ok((c, connection)) => {
					tokio::spawn(async move {
						if let Err(e) = connection.await {
							tracing::error!("pg connection error: {e}");
						}
					});
					return Ok(PgTestContext {
						client: c,
						_server: server,
					});
				}
				Err(e) => {
					last_err = Some(e);
					tokio::time::sleep(std::time::Duration::from_millis(500)).await;
				}
			}
		}
		Err(format!("Failed to connect to pgwire: {last_err:?}").into())
	}

	async fn seed_users(client: &tokio_postgres::Client) {
		client
			.simple_query(
				"INSERT INTO users (name, age, status) VALUES \
				 ('Alice', 30, 'active'), \
				 ('Bob', 25, 'inactive'), \
				 ('Charlie', 35, 'active')",
			)
			.await
			.expect("failed to seed users");
	}

	fn extract_rows(messages: &[SimpleQueryMessage]) -> Vec<Vec<Option<String>>> {
		messages
			.iter()
			.filter_map(|msg| match msg {
				SimpleQueryMessage::Row(row) => {
					let cols: Vec<Option<String>> = (0..row.columns().len())
						.map(|i| row.get(i).map(|s| s.to_string()))
						.collect();
					Some(cols)
				}
				_ => None,
			})
			.collect()
	}

	fn extract_column_names(messages: &[SimpleQueryMessage]) -> Vec<String> {
		for msg in messages {
			if let SimpleQueryMessage::Row(row) = msg {
				return row.columns().iter().map(|c| c.name().to_string()).collect();
			}
		}
		Vec::new()
	}

	fn assert_query(
		results: &[SimpleQueryMessage],
		expected_cols: &[&str],
		expected_rows: &[Vec<&str>],
		context: &str,
	) {
		let columns = extract_column_names(results);
		let rows = extract_rows(results);
		let exp_cols: Vec<String> = expected_cols.iter().map(|s| s.to_string()).collect();
		let exp_rows: Vec<Vec<Option<String>>> = expected_rows
			.iter()
			.map(|row| row.iter().map(|s| Some(s.to_string())).collect())
			.collect();
		assert_eq!(columns, exp_cols, "column mismatch – {context}");
		assert_eq!(rows, exp_rows, "row mismatch – {context}");
	}

	// ---------------------------------------------------------------
	// Computed column queries (seed a single row, project expressions)
	// ---------------------------------------------------------------

	#[rstest]
	#[case::addition("SELECT 1 + 1 AS result FROM vals", vec!["result"], vec![vec!["2"]])]
	#[case::subtraction("SELECT 10 - 3 AS result FROM vals", vec!["result"], vec![vec!["7"]])]
	#[case::multiplication("SELECT 3 * 4 AS result FROM vals", vec!["result"], vec![vec!["12"]])]
	#[case::division("SELECT 10 / 2 AS result FROM vals", vec!["result"], vec![vec!["5"]])]
	#[case::string_literal("SELECT 'hello' AS greeting FROM vals", vec!["greeting"], vec![vec!["hello"]])]
	#[case::negative("SELECT -5 AS result FROM vals", vec!["result"], vec![vec!["-5"]])]
	#[tokio::test]
	async fn test_expression(
		#[case] query: &str,
		#[case] expected_cols: Vec<&str>,
		#[case] expected_rows: Vec<Vec<&str>>,
	) {
		let ctx = setup_pg().await.unwrap();
		ctx.client
			.simple_query("INSERT INTO vals (x) VALUES (1)")
			.await
			.expect("failed to seed expression helper row");
		let results = ctx.client.simple_query(query).await.unwrap();
		assert_query(&results, &expected_cols, &expected_rows, query);
	}

	// ---------------------------------------------------------------
	// SELECT queries with seed data
	// ---------------------------------------------------------------

	#[rstest]
	#[case::select_all_fields(
		"SELECT name, age, status FROM users ORDER BY name",
		vec!["age", "name", "status"],
		vec![
			vec!["30", "Alice", "active"],
			vec!["25", "Bob", "inactive"],
			vec!["35", "Charlie", "active"],
		],
	)]
	#[case::where_gt(
		"SELECT name FROM users WHERE age > 25 ORDER BY name",
		vec!["name"],
		vec![vec!["Alice"], vec!["Charlie"]],
	)]
	#[case::where_eq(
		"SELECT name, age FROM users WHERE status = 'active' ORDER BY name",
		vec!["age", "name"],
		vec![vec!["30", "Alice"], vec!["35", "Charlie"]],
	)]
	#[case::order_by_desc(
		"SELECT name, age FROM users ORDER BY age DESC",
		vec!["age", "name"],
		vec![vec!["35", "Charlie"], vec!["30", "Alice"], vec!["25", "Bob"]],
	)]
	#[case::limit(
		"SELECT name FROM users ORDER BY name LIMIT 1",
		vec!["name"],
		vec![vec!["Alice"]],
	)]
	#[case::limit_offset(
		"SELECT name FROM users ORDER BY name LIMIT 1 OFFSET 1",
		vec!["name"],
		vec![vec!["Bob"]],
	)]
	#[case::where_between(
		"SELECT name FROM users WHERE age BETWEEN 25 AND 30 ORDER BY name",
		vec!["name"],
		vec![vec!["Alice"], vec!["Bob"]],
	)]
	#[case::where_in(
		"SELECT name FROM users WHERE name IN ('Alice', 'Charlie') ORDER BY name",
		vec!["name"],
		vec![vec!["Alice"], vec!["Charlie"]],
	)]
	#[tokio::test]
	async fn test_select(
		#[case] query: &str,
		#[case] expected_cols: Vec<&str>,
		#[case] expected_rows: Vec<Vec<&str>>,
	) {
		let ctx = setup_pg().await.unwrap();
		seed_users(&ctx.client).await;
		let results = ctx.client.simple_query(query).await.unwrap();
		assert_query(&results, &expected_cols, &expected_rows, query);
	}

	// ---------------------------------------------------------------
	// DML: INSERT
	// ---------------------------------------------------------------

	#[tokio::test]
	async fn test_insert() {
		let ctx = setup_pg().await.unwrap();
		ctx.client
			.simple_query("INSERT INTO products (name, price) VALUES ('Widget', 10)")
			.await
			.unwrap();

		let results = ctx.client.simple_query("SELECT name, price FROM products").await.unwrap();
		assert_query(&results, &["name", "price"], &[vec!["Widget", "10"]], "verify insert");
	}

	#[tokio::test]
	async fn test_insert_multiple_rows() {
		let ctx = setup_pg().await.unwrap();
		ctx.client
			.simple_query(
				"INSERT INTO products (name, price) VALUES ('Widget', 10), ('Gadget', 20)",
			)
			.await
			.unwrap();

		let results = ctx
			.client
			.simple_query("SELECT name, price FROM products ORDER BY name")
			.await
			.unwrap();
		assert_query(
			&results,
			&["name", "price"],
			&[vec!["Gadget", "20"], vec!["Widget", "10"]],
			"verify multi-row insert",
		);
	}

	// ---------------------------------------------------------------
	// DML: UPDATE
	// ---------------------------------------------------------------

	#[tokio::test]
	async fn test_update() {
		let ctx = setup_pg().await.unwrap();
		seed_users(&ctx.client).await;

		ctx.client.simple_query("UPDATE users SET age = 99 WHERE name = 'Alice'").await.unwrap();

		let results = ctx
			.client
			.simple_query("SELECT name, age FROM users WHERE name = 'Alice'")
			.await
			.unwrap();
		assert_query(&results, &["age", "name"], &[vec!["99", "Alice"]], "verify update");
	}

	// ---------------------------------------------------------------
	// DML: DELETE
	// ---------------------------------------------------------------

	#[tokio::test]
	async fn test_delete() {
		let ctx = setup_pg().await.unwrap();
		seed_users(&ctx.client).await;

		ctx.client.simple_query("DELETE FROM users WHERE name = 'Alice'").await.unwrap();

		let results =
			ctx.client.simple_query("SELECT name FROM users ORDER BY name").await.unwrap();
		assert_query(&results, &["name"], &[vec!["Bob"], vec!["Charlie"]], "verify delete");
	}

	// ---------------------------------------------------------------
	// DDL: DROP TABLE
	// ---------------------------------------------------------------

	#[tokio::test]
	async fn test_drop_table() {
		let ctx = setup_pg().await.unwrap();
		seed_users(&ctx.client).await;

		ctx.client.simple_query("DROP TABLE IF EXISTS users").await.unwrap();

		let result = ctx.client.simple_query("SELECT name FROM users").await;
		assert!(result.is_err(), "expected error after DROP TABLE, table should not exist");
	}

	// ---------------------------------------------------------------
	// Unsupported queries should return errors
	// ---------------------------------------------------------------

	#[rstest]
	#[case::create_table("CREATE TABLE foo (id INT PRIMARY KEY, name TEXT)")]
	#[case::create_index("CREATE INDEX idx ON foo (name)")]
	#[case::join("SELECT * FROM a JOIN b ON a.id = b.id")]
	#[tokio::test]
	async fn test_unsupported(#[case] query: &str) {
		let ctx = setup_pg().await.unwrap();
		let result = ctx.client.simple_query(query).await;
		assert!(result.is_err(), "expected error for unsupported query: {query}");
	}
}
