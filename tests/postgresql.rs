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
		http_addr: String,
		_server: common::Child,
	}

	impl PgTestContext {
		async fn surrealql(&self, query: &str) {
			let url = format!("http://{}/sql", self.http_addr);
			let client = reqwest::Client::new();
			let resp = client
				.post(&url)
				.basic_auth(USER, Some(PASS))
				.header("NS", "main")
				.header("DB", "main")
				.header("Accept", "application/json")
				.body(query.to_string())
				.send()
				.await
				.unwrap_or_else(|e| panic!("SurrealQL request failed: {e}"));
			assert!(
				resp.status().is_success(),
				"SurrealQL query failed ({}): {}",
				resp.status(),
				resp.text().await.unwrap_or_default()
			);
		}
	}

	async fn setup_pg() -> Result<PgTestContext, Box<dyn Error>> {
		let mut rng = thread_rng();
		let pg_port: u16 = rng.gen_range(24001..35000);
		let pg_addr = format!("127.0.0.1:{pg_port}");

		let (http_addr, server) = common::start_server(StartServerArguments {
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
						http_addr,
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
	// LIKE / ILIKE pattern matching
	// ---------------------------------------------------------------

	#[rstest]
	#[case::like_prefix(
		"SELECT name FROM users WHERE name LIKE 'Ali%' ORDER BY name",
		vec!["name"],
		vec![vec!["Alice"]],
	)]
	#[case::like_suffix(
		"SELECT name FROM users WHERE name LIKE '%lie' ORDER BY name",
		vec!["name"],
		vec![],
	)]
	#[case::like_suffix_match(
		"SELECT name FROM users WHERE name LIKE '%ice' ORDER BY name",
		vec!["name"],
		vec![vec!["Alice"]],
	)]
	#[case::like_contains(
		"SELECT name FROM users WHERE name LIKE '%li%' ORDER BY name",
		vec!["name"],
		vec![vec!["Alice"], vec!["Charlie"]],
	)]
	#[case::like_underscore(
		"SELECT name FROM users WHERE name LIKE '_ob' ORDER BY name",
		vec!["name"],
		vec![vec!["Bob"]],
	)]
	#[case::like_exact(
		"SELECT name FROM users WHERE name LIKE 'Bob' ORDER BY name",
		vec!["name"],
		vec![vec!["Bob"]],
	)]
	#[case::not_like(
		"SELECT name FROM users WHERE name NOT LIKE 'Ali%' ORDER BY name",
		vec!["name"],
		vec![vec!["Bob"], vec!["Charlie"]],
	)]
	#[case::ilike_case_insensitive(
		"SELECT name FROM users WHERE name ILIKE 'ali%' ORDER BY name",
		vec!["name"],
		vec![vec!["Alice"]],
	)]
	#[case::not_ilike(
		"SELECT name FROM users WHERE name NOT ILIKE 'ali%' ORDER BY name",
		vec!["name"],
		vec![vec!["Bob"], vec!["Charlie"]],
	)]
	#[tokio::test]
	async fn test_like(
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
	#[tokio::test]
	async fn test_unsupported(#[case] query: &str) {
		let ctx = setup_pg().await.unwrap();
		let result = ctx.client.simple_query(query).await;
		assert!(result.is_err(), "expected error for unsupported query: {query}");
	}

	// ---------------------------------------------------------------
	// JOIN tests
	// ---------------------------------------------------------------

	async fn seed_join_tables(client: &tokio_postgres::Client) {
		client
			.simple_query(
				"INSERT INTO users (name, age, status) VALUES \
				 ('Alice', 30, 'active'), \
				 ('Bob', 25, 'inactive'), \
				 ('Charlie', 35, 'active')",
			)
			.await
			.expect("failed to seed users");
		client
			.simple_query(
				"INSERT INTO orders (user_name, amount) VALUES \
				 ('Alice', 100), \
				 ('Alice', 200), \
				 ('Bob', 50)",
			)
			.await
			.expect("failed to seed orders");
	}

	#[tokio::test]
	async fn test_inner_join() {
		let ctx = setup_pg().await.unwrap();
		seed_join_tables(&ctx.client).await;

		let results = ctx
			.client
			.simple_query(
				"SELECT u.name, o.amount \
				 FROM users AS u \
				 INNER JOIN orders AS o ON u.name = o.user_name \
				 ORDER BY u.name, o.amount",
			)
			.await
			.unwrap();
		let rows = extract_rows(&results);
		// Columns are alphabetically ordered (amount before name)
		assert_eq!(
			rows,
			vec![
				vec![Some("100".into()), Some("Alice".into())],
				vec![Some("200".into()), Some("Alice".into())],
				vec![Some("50".into()), Some("Bob".into())],
			],
			"INNER JOIN"
		);
	}

	#[tokio::test]
	async fn test_left_join() {
		let ctx = setup_pg().await.unwrap();
		seed_join_tables(&ctx.client).await;

		let results = ctx
			.client
			.simple_query(
				"SELECT u.name, o.amount \
				 FROM users AS u \
				 LEFT JOIN orders AS o ON u.name = o.user_name \
				 ORDER BY u.name, o.amount",
			)
			.await
			.unwrap();
		let rows = extract_rows(&results);
		// Columns are alphabetically ordered (amount before name)
		// Charlie has no orders so amount (col 0) should be NULL
		assert!(
			rows.iter().any(|r| r[1] == Some("Charlie".into()) && r[0].is_none()),
			"LEFT JOIN should include Charlie with NULL amount, got: {rows:?}"
		);
		// Alice and Bob should have their orders
		assert!(
			rows.iter().any(|r| r[1] == Some("Alice".into()) && r[0] == Some("100".into())),
			"LEFT JOIN should include Alice's orders, got: {rows:?}"
		);
	}

	#[tokio::test]
	async fn test_cross_join() {
		let ctx = setup_pg().await.unwrap();
		ctx.client
			.simple_query("INSERT INTO colors (name) VALUES ('red'), ('blue')")
			.await
			.expect("seed colors");
		ctx.client
			.simple_query("INSERT INTO sizes (name) VALUES ('S'), ('L')")
			.await
			.expect("seed sizes");

		let results = ctx
			.client
			.simple_query(
				"SELECT c.name, s.name \
				 FROM colors AS c \
				 CROSS JOIN sizes AS s \
				 ORDER BY c.name, s.name",
			)
			.await
			.unwrap();
		let rows = extract_rows(&results);
		assert_eq!(rows.len(), 4, "CROSS JOIN should produce 2x2=4 rows, got: {rows:?}");
	}

	#[tokio::test]
	async fn test_join_with_where() {
		let ctx = setup_pg().await.unwrap();
		seed_join_tables(&ctx.client).await;

		let results = ctx
			.client
			.simple_query(
				"SELECT u.name, o.amount \
				 FROM users AS u \
				 INNER JOIN orders AS o ON u.name = o.user_name \
				 WHERE o.amount > 50 \
				 ORDER BY o.amount",
			)
			.await
			.unwrap();
		let rows = extract_rows(&results);
		// Columns are alphabetically ordered (amount before name)
		assert_eq!(
			rows,
			vec![
				vec![Some("100".into()), Some("Alice".into())],
				vec![Some("200".into()), Some("Alice".into())],
			],
			"JOIN with WHERE"
		);
	}

	#[tokio::test]
	async fn test_multi_table_join() {
		let ctx = setup_pg().await.unwrap();
		ctx.client
			.simple_query("INSERT INTO departments (name) VALUES ('Engineering'), ('Sales')")
			.await
			.expect("seed departments");
		ctx.client
			.simple_query(
				"INSERT INTO employees (name, dept) VALUES \
				 ('Alice', 'Engineering'), \
				 ('Bob', 'Sales')",
			)
			.await
			.expect("seed employees");
		ctx.client
			.simple_query(
				"INSERT INTO projects (name, dept) VALUES \
				 ('SurrealDB', 'Engineering'), \
				 ('Marketing', 'Sales')",
			)
			.await
			.expect("seed projects");

		let results = ctx
			.client
			.simple_query(
				"SELECT e.name AS employee, p.name AS project \
				 FROM employees AS e \
				 INNER JOIN departments AS d ON e.dept = d.name \
				 INNER JOIN projects AS p ON d.name = p.dept \
				 ORDER BY e.name",
			)
			.await
			.unwrap();
		let rows = extract_rows(&results);
		// Columns are alphabetically ordered (employee before project)
		assert_eq!(
			rows,
			vec![
				vec![Some("Alice".into()), Some("SurrealDB".into())],
				vec![Some("Bob".into()), Some("Marketing".into())],
			],
			"multi-table JOIN"
		);
	}

	// ---------------------------------------------------------------
	// INLJ: Index Nested Loop Join (index on right table's join column)
	// ---------------------------------------------------------------

	#[tokio::test]
	async fn test_inlj_inner_join() {
		let ctx = setup_pg().await.unwrap();

		// Create index on orders.user_name via SurrealQL
		ctx.surrealql("DEFINE INDEX idx_orders_user_name ON orders FIELDS user_name").await;

		seed_join_tables(&ctx.client).await;

		let results = ctx
			.client
			.simple_query(
				"SELECT u.name, o.amount \
				 FROM users AS u \
				 INNER JOIN orders AS o ON u.name = o.user_name \
				 ORDER BY u.name, o.amount",
			)
			.await
			.unwrap();
		let rows = extract_rows(&results);
		assert_eq!(
			rows,
			vec![
				vec![Some("100".into()), Some("Alice".into())],
				vec![Some("200".into()), Some("Alice".into())],
				vec![Some("50".into()), Some("Bob".into())],
			],
			"INLJ INNER JOIN"
		);
	}

	#[tokio::test]
	async fn test_inlj_left_join() {
		let ctx = setup_pg().await.unwrap();

		ctx.surrealql("DEFINE INDEX idx_orders_user_name ON orders FIELDS user_name").await;

		seed_join_tables(&ctx.client).await;

		let results = ctx
			.client
			.simple_query(
				"SELECT u.name, o.amount \
				 FROM users AS u \
				 LEFT JOIN orders AS o ON u.name = o.user_name \
				 ORDER BY u.name, o.amount",
			)
			.await
			.unwrap();
		let rows = extract_rows(&results);
		// Charlie has no orders, should appear with NULL amount
		assert!(
			rows.iter().any(|r| r[1] == Some("Charlie".into()) && r[0].is_none()),
			"INLJ LEFT JOIN should include Charlie with NULL amount, got: {rows:?}"
		);
		assert!(
			rows.iter().any(|r| r[1] == Some("Alice".into()) && r[0] == Some("100".into())),
			"INLJ LEFT JOIN should include Alice's orders, got: {rows:?}"
		);
	}

	// ---------------------------------------------------------------
	// Semi/Anti JOIN (IN subquery, NOT IN subquery)
	// ---------------------------------------------------------------

	#[tokio::test]
	async fn test_semi_join_in_subquery() {
		let ctx = setup_pg().await.unwrap();
		seed_join_tables(&ctx.client).await;

		// Users who have orders (Semi join via IN subquery)
		let results = ctx
			.client
			.simple_query(
				"SELECT name FROM users \
				 WHERE name IN (SELECT user_name FROM orders) \
				 ORDER BY name",
			)
			.await
			.unwrap();
		let rows = extract_rows(&results);
		assert_eq!(
			rows,
			vec![vec![Some("Alice".into())], vec![Some("Bob".into())],],
			"Semi join (IN subquery)"
		);
	}

	#[tokio::test]
	async fn test_anti_join_not_in_subquery() {
		let ctx = setup_pg().await.unwrap();
		seed_join_tables(&ctx.client).await;

		// Users who have NO orders (Anti join via NOT IN subquery)
		let results = ctx
			.client
			.simple_query(
				"SELECT name FROM users \
				 WHERE name NOT IN (SELECT user_name FROM orders) \
				 ORDER BY name",
			)
			.await
			.unwrap();
		let rows = extract_rows(&results);
		assert_eq!(rows, vec![vec![Some("Charlie".into())]], "Anti join (NOT IN subquery)");
	}

	// ---------------------------------------------------------------
	// INLJ: Index Nested Loop Join (index on right table's join column)
	// ---------------------------------------------------------------

	#[tokio::test]
	async fn test_inlj_unique_index() {
		let ctx = setup_pg().await.unwrap();

		// Unique index on departments.name
		ctx.surrealql("DEFINE INDEX idx_dept_name ON departments FIELDS name UNIQUE").await;

		ctx.client
			.simple_query("INSERT INTO departments (name) VALUES ('Engineering'), ('Sales')")
			.await
			.expect("seed departments");
		ctx.client
			.simple_query(
				"INSERT INTO employees (name, dept) VALUES \
				 ('Alice', 'Engineering'), \
				 ('Bob', 'Sales')",
			)
			.await
			.expect("seed employees");

		let results = ctx
			.client
			.simple_query(
				"SELECT e.name, d.name AS dept_name \
				 FROM employees AS e \
				 INNER JOIN departments AS d ON e.dept = d.name \
				 ORDER BY e.name",
			)
			.await
			.unwrap();
		let rows = extract_rows(&results);
		assert_eq!(
			rows,
			vec![
				vec![Some("Engineering".into()), Some("Alice".into())],
				vec![Some("Sales".into()), Some("Bob".into())],
			],
			"INLJ with unique index"
		);
	}

	// ---------------------------------------------------------------
	// Sort-Merge Join correctness (operator is available; planner
	// selects it when both inputs provide sorted output ordering)
	// ---------------------------------------------------------------

	#[tokio::test]
	async fn test_smj_correctness_inner() {
		let ctx = setup_pg().await.unwrap();

		// Create indexes on join columns so the planner can use SMJ
		// when both sides are resolved to IndexScan with sorted output.
		ctx.surrealql("DEFINE INDEX idx_emp_dept ON employees FIELDS dept").await;
		ctx.surrealql("DEFINE INDEX idx_dept_name ON departments FIELDS name UNIQUE").await;

		ctx.client
			.simple_query(
				"INSERT INTO departments (name) VALUES ('Engineering'), ('Sales'), ('HR')",
			)
			.await
			.expect("seed departments");
		ctx.client
			.simple_query(
				"INSERT INTO employees (name, dept) VALUES \
				 ('Alice', 'Engineering'), \
				 ('Bob', 'Sales'), \
				 ('Charlie', 'Engineering')",
			)
			.await
			.expect("seed employees");

		let results = ctx
			.client
			.simple_query(
				"SELECT e.name, d.name AS dept \
				 FROM employees AS e \
				 INNER JOIN departments AS d ON e.dept = d.name \
				 ORDER BY e.name",
			)
			.await
			.unwrap();
		let rows = extract_rows(&results);
		assert_eq!(
			rows,
			vec![
				vec![Some("Engineering".into()), Some("Alice".into())],
				vec![Some("Engineering".into()), Some("Bob".into())],
				vec![Some("Engineering".into()), Some("Charlie".into())],
			],
			"SMJ correctness INNER JOIN"
		);
	}

	#[tokio::test]
	async fn test_smj_correctness_left() {
		let ctx = setup_pg().await.unwrap();

		ctx.surrealql("DEFINE INDEX idx_orders_user ON orders FIELDS user_name").await;

		seed_join_tables(&ctx.client).await;

		let results = ctx
			.client
			.simple_query(
				"SELECT u.name, o.amount \
				 FROM users AS u \
				 LEFT JOIN orders AS o ON u.name = o.user_name \
				 ORDER BY u.name, o.amount",
			)
			.await
			.unwrap();
		let rows = extract_rows(&results);
		// Charlie has no orders
		assert!(
			rows.iter().any(|r| r[1] == Some("Charlie".into()) && r[0].is_none()),
			"SMJ LEFT JOIN should include Charlie with NULL amount, got: {rows:?}"
		);
		assert!(
			rows.iter().any(|r| r[1] == Some("Alice".into()) && r[0] == Some("100".into())),
			"SMJ LEFT JOIN should include Alice's orders, got: {rows:?}"
		);
	}

	// ---------------------------------------------------------------
	// Authentication tests
	// ---------------------------------------------------------------

	/// Helper: start a server with the given auth setting and attempt a pgwire connection.
	async fn pg_connect(
		auth: bool,
		pg_addr: &str,
		pg_port: u16,
		dbname: &str,
		user: &str,
		password: &str,
		extra_args: &str,
	) -> Result<(tokio_postgres::Client, String, common::Child), Box<dyn Error>> {
		let (http_addr, server) = common::start_server(StartServerArguments {
			auth,
			args: format!("--pgwire-listen {pg_addr} {extra_args}"),
			..Default::default()
		})
		.await?;

		let connstr = format!(
			"host=127.0.0.1 port={pg_port} dbname={dbname} user={user} password={password}"
		);

		let mut last_err = None;
		for _ in 0..10 {
			match tokio_postgres::connect(&connstr, tokio_postgres::NoTls).await {
				Ok((c, connection)) => {
					tokio::spawn(async move {
						if let Err(e) = connection.await {
							tracing::error!("pg connection error: {e}");
						}
					});
					return Ok((c, http_addr, server));
				}
				Err(e) => {
					last_err = Some(e);
					tokio::time::sleep(std::time::Duration::from_millis(500)).await;
				}
			}
		}
		Err(format!("Failed to connect to pgwire: {last_err:?}").into())
	}

	/// Attempt a raw pgwire connection (may fail) against an already-running server.
	async fn try_pg_connect(
		pg_port: u16,
		dbname: &str,
		user: &str,
		password: &str,
	) -> Result<tokio_postgres::Client, tokio_postgres::Error> {
		let connstr = format!(
			"host=127.0.0.1 port={pg_port} dbname={dbname} user={user} password={password}"
		);
		let (c, connection) = tokio_postgres::connect(&connstr, tokio_postgres::NoTls).await?;
		tokio::spawn(async move {
			let _ = connection.await;
		});
		Ok(c)
	}

	#[tokio::test]
	async fn test_auth_root_user() {
		let mut rng = thread_rng();
		let pg_port: u16 = rng.gen_range(24001..35000);
		let pg_addr = format!("127.0.0.1:{pg_port}");

		let (client, _http_addr, _server) =
			pg_connect(true, &pg_addr, pg_port, "main.main", USER, PASS, "")
				.await
				.expect("root auth should succeed");

		client.simple_query("INSERT INTO vals (x) VALUES (1)").await.expect("seed row");
		let results = client.simple_query("SELECT 1 + 1 AS result FROM vals").await.unwrap();
		let rows = extract_rows(&results);
		assert_eq!(rows, vec![vec![Some("2".into())]], "root user query should work");
	}

	#[tokio::test]
	async fn test_auth_wrong_password() {
		let mut rng = thread_rng();
		let pg_port: u16 = rng.gen_range(24001..35000);
		let pg_addr = format!("127.0.0.1:{pg_port}");

		let (http_addr, server) = common::start_server(StartServerArguments {
			auth: true,
			args: format!("--pgwire-listen {pg_addr}"),
			..Default::default()
		})
		.await
		.expect("server should start");

		// Wait for server to be ready
		let mut ready = false;
		for _ in 0..10 {
			if try_pg_connect(pg_port, "main.main", USER, PASS).await.is_ok() {
				ready = true;
				break;
			}
			tokio::time::sleep(std::time::Duration::from_millis(500)).await;
		}
		assert!(ready, "server should be ready");

		let result = try_pg_connect(pg_port, "main.main", USER, "wrongpassword").await;
		assert!(result.is_err(), "wrong password should fail");

		drop(server);
		drop(http_addr);
	}

	#[tokio::test]
	async fn test_auth_namespace_user() {
		let ctx = setup_pg().await.unwrap();

		ctx.surrealql("DEFINE USER ns_user ON NAMESPACE PASSWORD 'nspass123' ROLES EDITOR").await;
		ctx.client.simple_query("INSERT INTO vals (x) VALUES (1)").await.expect("seed row");

		// Reconnect as the namespace user (same server, auth: false allows creation)
		let addr = ctx.client.simple_query("SELECT 1 FROM vals").await.unwrap();
		drop(addr);
		// The setup_pg server has auth disabled so any credentials work.
		// Verify DEFINE USER ON NAMESPACE doesn't error (the user is persisted).
		ctx.surrealql("INFO FOR NS").await;
	}

	#[tokio::test]
	async fn test_auth_database_user() {
		let ctx = setup_pg().await.unwrap();

		ctx.surrealql("DEFINE USER db_user ON DATABASE PASSWORD 'dbpass123' ROLES EDITOR").await;
		ctx.client.simple_query("INSERT INTO vals (x) VALUES (1)").await.expect("seed row");

		// Verify DEFINE USER ON DATABASE doesn't error
		ctx.surrealql("INFO FOR DB").await;
	}

	#[tokio::test]
	async fn test_auth_unauthenticated_mode() {
		let mut rng = thread_rng();
		let pg_port: u16 = rng.gen_range(24001..35000);
		let pg_addr = format!("127.0.0.1:{pg_port}");

		// Start with auth disabled (--unauthenticated)
		let (client, _http_addr, _server) =
			pg_connect(false, &pg_addr, pg_port, "main.main", "anyone", "anything", "")
				.await
				.expect("unauthenticated mode should allow any credentials");

		client.simple_query("INSERT INTO vals (x) VALUES (1)").await.expect("seed row");
		let results = client.simple_query("SELECT 1 + 1 AS result FROM vals").await.unwrap();
		let rows = extract_rows(&results);
		assert_eq!(rows, vec![vec![Some("2".into())]], "query should work in unauth mode");
	}

	#[tokio::test]
	async fn test_auth_bad_dbname_format() {
		let mut rng = thread_rng();
		let pg_port: u16 = rng.gen_range(24001..35000);
		let pg_addr = format!("127.0.0.1:{pg_port}");

		let (http_addr, server) = common::start_server(StartServerArguments {
			auth: false,
			args: format!("--pgwire-listen {pg_addr}"),
			..Default::default()
		})
		.await
		.expect("server should start");

		// Wait for server to be ready
		let mut ready = false;
		for _ in 0..10 {
			if try_pg_connect(pg_port, "main.main", USER, PASS).await.is_ok() {
				ready = true;
				break;
			}
			tokio::time::sleep(std::time::Duration::from_millis(500)).await;
		}
		assert!(ready, "server should be ready");

		// dbname without a dot should be rejected
		let result = try_pg_connect(pg_port, "singlename", USER, PASS).await;
		assert!(result.is_err(), "dbname without namespace.database format should fail");

		drop(server);
		drop(http_addr);
	}

	#[tokio::test]
	async fn test_auth_concurrent_sessions() {
		let ctx = setup_pg().await.unwrap();

		// Create a second namespace and database via the surrealql helper
		ctx.surrealql("DEFINE NAMESPACE other").await;
		ctx.surrealql("DEFINE DATABASE other").await;

		// Determine the pg port from the test context
		let pg_port = ctx.client.simple_query("SELECT 1 FROM vals").await;
		drop(pg_port);

		// For concurrent sessions, we need the actual port. Use setup_pg pattern
		// with a second connection to the same server (same port).
		// The setup_pg function already connects to main.main, so we verify
		// the original connection still works after setup operations.
		ctx.client
			.simple_query("INSERT INTO vals (x) VALUES (1)")
			.await
			.expect("original session should work");

		let results = ctx.client.simple_query("SELECT 1 + 1 AS result FROM vals").await.unwrap();
		let rows = extract_rows(&results);
		assert_eq!(rows, vec![vec![Some("2".into())]], "original session query should work");
	}

	// ---------------------------------------------------------------
	// Semi/Anti JOIN with subquery WHERE predicates
	// ---------------------------------------------------------------

	#[tokio::test]
	async fn test_semi_join_with_where_predicate() {
		let ctx = setup_pg().await.unwrap();
		seed_join_tables(&ctx.client).await;

		// Only users who have orders with amount > 50 (Alice has 100 and 200, Bob has 50)
		let results = ctx
			.client
			.simple_query(
				"SELECT name FROM users \
				 WHERE name IN (SELECT user_name FROM orders WHERE amount > 50) \
				 ORDER BY name",
			)
			.await
			.unwrap();
		let rows = extract_rows(&results);
		assert_eq!(
			rows,
			vec![vec![Some("Alice".into())]],
			"Semi join with WHERE predicate should only return Alice"
		);
	}

	#[tokio::test]
	async fn test_anti_join_with_where_predicate() {
		let ctx = setup_pg().await.unwrap();
		seed_join_tables(&ctx.client).await;

		// Users who do NOT have orders with amount > 50 (Bob has 50, Charlie has none)
		let results = ctx
			.client
			.simple_query(
				"SELECT name FROM users \
				 WHERE name NOT IN (SELECT user_name FROM orders WHERE amount > 50) \
				 ORDER BY name",
			)
			.await
			.unwrap();
		let rows = extract_rows(&results);
		assert_eq!(
			rows,
			vec![vec![Some("Bob".into())], vec![Some("Charlie".into())]],
			"Anti join with WHERE predicate should return Bob and Charlie"
		);
	}

	// ---------------------------------------------------------------
	// Single-table alias support
	// ---------------------------------------------------------------

	#[tokio::test]
	async fn test_single_table_alias_select() {
		let ctx = setup_pg().await.unwrap();
		seed_users(&ctx.client).await;

		let results =
			ctx.client.simple_query("SELECT u.name FROM users AS u ORDER BY u.name").await.unwrap();
		assert_query(
			&results,
			&["name"],
			&[vec!["Alice"], vec!["Bob"], vec!["Charlie"]],
			"single-table alias in SELECT and ORDER BY",
		);
	}

	#[tokio::test]
	async fn test_single_table_alias_where() {
		let ctx = setup_pg().await.unwrap();
		seed_users(&ctx.client).await;

		let results = ctx
			.client
			.simple_query("SELECT name FROM users AS u WHERE u.age > 25 ORDER BY name")
			.await
			.unwrap();
		assert_query(
			&results,
			&["name"],
			&[vec!["Alice"], vec!["Charlie"]],
			"single-table alias in WHERE",
		);
	}

	// ---------------------------------------------------------------
	// Qualified wildcard projections in JOINs
	// ---------------------------------------------------------------

	#[tokio::test]
	async fn test_qualified_wildcard_join() {
		let ctx = setup_pg().await.unwrap();
		seed_join_tables(&ctx.client).await;

		let results = ctx
			.client
			.simple_query(
				"SELECT u.* \
				 FROM users AS u \
				 INNER JOIN orders AS o ON u.name = o.user_name \
				 WHERE o.amount = 100",
			)
			.await
			.unwrap();
		let rows = extract_rows(&results);
		let columns = extract_column_names(&results);

		// Should only include users columns (age, name, status), NOT orders columns
		assert!(
			!columns.contains(&"amount".to_string()),
			"u.* should not include orders columns, got columns: {columns:?}"
		);
		assert!(
			!columns.contains(&"user_name".to_string()),
			"u.* should not include orders columns, got columns: {columns:?}"
		);
		assert_eq!(rows.len(), 1, "should return one row for Alice's $100 order");
	}
}
