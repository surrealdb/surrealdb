#![allow(
	unexpected_cfgs,
	reason = "This test is only enabled when the `docker` feature is enabled which is an internal feature."
)]

mod common;

#[cfg(all(docker, feature = "storage-rocksdb"))]
mod database_upgrade {
	use std::net::Ipv4Addr;
	use std::time::Duration;

	use surrealdb::engine::any::{Any, connect};
	use surrealdb::opt::auth::Root;
	use surrealdb::{Connection, Surreal, Value};
	use test_log::test;
	use tokio::net::TcpListener;
	use tokio::time::{sleep, timeout};
	use tracing::{error, info};
	use ulid::Ulid;

	use super::common::docker::DockerContainer;

	const NS: &str = "test";
	const DB: &str = "test";
	const USER: &str = "root";
	const PASS: &str = "root";

	const DEAL_STORE_DATASET: &str = "tests/data/surreal-deal-store-mini.surql";

	const TIMEOUT_DURATION: Duration = Duration::from_secs(180);

	// This test includes a feature set supported since v2.0
	async fn upgrade_test_from_2_0(version: &str) {
		// Start the docker instance
		let (path, mut docker, client) = start_docker(version).await;

		// Create the data set
		import_data_on_docker(&client, "DEMO_DATA", DEAL_STORE_DATASET).await;
		create_data_on_docker(&client, "IDX", &DATA_IDX).await;
		create_data_on_docker(&client, "FTS", &DATA_FTS).await;
		create_data_on_docker(&client, "MTREE", &DATA_MTREE).await;

		// Check the data set
		check_data_on_docker(&client, "IDX", &CHECK_IDX).await;
		check_data_on_docker(&client, "DB", &CHECK_DB).await;
		check_data_on_docker(&client, "FTS", &CHECK_FTS).await;

		// Collect INFO FOR NS & INFO FOR DB
		let (info_ns, info_db) = get_info_ns_db(&client, version).await;
		// Extract the table names
		let table_names = extract_table_names(&info_db, version, 15);
		// Collect INFO FOR TABLE for each table
		let info_tables = get_info_tables(&client, &table_names, version).await;
		// Collect rows from every table
		let table_rows = get_table_rows(&client, &table_names, version).await;

		// Stop the docker instance
		docker.stop();

		// Extract the database directory
		docker.extract_data_dir(&path);

		// Connect to a local instance
		let db = new_local_instance(&path).await;

		// Check that the data has properly migrated
		check_migrated_data(&db, "IDX", &CHECK_IDX).await;
		check_migrated_data(&db, "DB", &CHECK_DB).await;
		check_migrated_data(&db, "FTS", &CHECK_FTS).await;
		check_migrated_data(&db, "MTREE", &CHECK_MTREE_DB).await;
		check_migrated_data(&db, "KNN_BRUTEFORCE", &CHECK_KNN_BRUTEFORCE).await;

		// Collect INFO FOR NS/DB on the migrated database
		let (migrated_info_ns, migrated_info_db) = get_info_ns_db(&db, "current").await;
		// Extract the table names
		let migrated_table_names = extract_table_names(&migrated_info_db, version, 15);
		// Collect INFO FOR TABLE for each table
		let migrated_info_tables = get_info_tables(&db, &migrated_table_names, version).await;
		// Collect rows from every table
		let migrated_table_rows = get_table_rows(&db, &migrated_table_names, version).await;

		// Check that the table names are matching
		assert_eq!(table_names, migrated_table_names);
		// Check that INFO FOR NS is matching
		check_value(&info_ns, &migrated_info_ns, "INFO FOR NS");
		// Check that INFO FOR DB is matching
		check_info_db(&info_db, &migrated_info_db);
		// Check that INFO FOR TABLE is matching
		check_values(&info_tables, &migrated_info_tables, "INFO FOR TABLE");
		// Check that the table rows are matching
		check_values(&table_rows, &migrated_table_rows, "SELECT * FROM {table}");
	}

	macro_rules! run {
		($future:expr) => {
			if timeout(TIMEOUT_DURATION, $future).await.is_err() {
				error!("test timed out");
				panic!();
			}
		};
	}

	#[test(tokio::test(flavor = "multi_thread"))]
	async fn upgrade_test_from_2_0_0() {
		run!(upgrade_test_from_2_0("v2.0.0"));
	}

	#[test(tokio::test(flavor = "multi_thread"))]
	async fn upgrade_test_from_2_0_1() {
		run!(upgrade_test_from_2_0("v2.0.1"));
	}

	#[test(tokio::test(flavor = "multi_thread"))]
	async fn upgrade_test_from_2_0_2() {
		run!(upgrade_test_from_2_0("v2.0.2"));
	}

	#[test(tokio::test(flavor = "multi_thread"))]
	async fn upgrade_test_from_2_0_3() {
		run!(upgrade_test_from_2_0("v2.0.3"));
	}

	#[test(tokio::test(flavor = "multi_thread"))]
	async fn upgrade_test_from_2_0_4() {
		run!(upgrade_test_from_2_0("v2.0.4"));
	}

	#[test(tokio::test(flavor = "multi_thread"))]
	async fn upgrade_test_from_2_1_0() {
		run!(upgrade_test_from_2_0("v2.1.0"));
	}

	#[test(tokio::test(flavor = "multi_thread"))]
	async fn upgrade_test_from_2_1_1() {
		run!(upgrade_test_from_2_0("v2.1.1"));
	}

	#[test(tokio::test(flavor = "multi_thread"))]
	async fn upgrade_test_from_2_1_2() {
		run!(upgrade_test_from_2_0("v2.1.2"));
	}

	#[test(tokio::test(flavor = "multi_thread"))]
	async fn upgrade_test_from_2_1_3() {
		run!(upgrade_test_from_2_0("v2.1.3"));
	}

	#[test(tokio::test(flavor = "multi_thread"))]
	async fn upgrade_test_from_2_1_4() {
		run!(upgrade_test_from_2_0("v2.1.4"));
	}

	#[test(tokio::test(flavor = "multi_thread"))]
	async fn upgrade_test_from_2_2_0() {
		run!(upgrade_test_from_2_0("v2.2.0"));
	}
	#[test(tokio::test(flavor = "multi_thread"))]
	async fn upgrade_test_from_2_2_1() {
		run!(upgrade_test_from_2_0("v2.2.1"));
	}

	// *******
	// DATASET
	// *******

	// Set of DATA for Standard and unique indexes
	const DATA_IDX: [&str; 4] = [
		"DEFINE INDEX idx_people_uniq_name ON TABLE people COLUMNS name UNIQUE",
		"DEFINE INDEX idx_org ON TABLE people COLUMNS org",
		"CREATE people:tobie SET name = 'Tobie', org='SurrealDB'",
		"CREATE people:jaime SET name = 'Jaime', org='SurrealDB'",
	];

	// Set of QUERY and RESULT to check for standard and unique indexes
	const CHECK_IDX: [Check; 2] = [
		(
			"SELECT name FROM people WITH INDEX idx_people_uniq_name WHERE name = 'Tobie'",
			Some("[{ name: 'Tobie' }]"),
		),
		(
			"SELECT name FROM people WITH INDEX idx_org WHERE org = 'SurrealDB'",
			Some("[{ name: 'Jaime' }, { name: 'Tobie' }]"),
		),
	];

	// Set of DATA for Full Text Search
	const DATA_FTS: [&str; 5] = [
		"DEFINE ANALYZER name TOKENIZERS class FILTERS lowercase,ngram(1,128)",
		"DEFINE ANALYZER userdefinedid TOKENIZERS blank FILTERS lowercase,ngram(1,32)",
		"DEFINE INDEX account_name_search_idx ON TABLE account COLUMNS name SEARCH ANALYZER name BM25(1.2,0.75) HIGHLIGHTS",
		"DEFINE INDEX account_user_defined_id_search_idx ON TABLE account COLUMNS user_defined_id SEARCH ANALYZER userdefinedid BM25 HIGHLIGHTS",
		"CREATE account SET name='Tobie', user_defined_id='Tobie'",
	];

	// Set of QUERY and RESULT to check for Full Text Search
	const CHECK_FTS: [Check; 1] = [(
		"SELECT search::highlight('<em>','</em>', 1) AS name FROM account WHERE name @1@ 'Tobie'",
		Some("[{ name: '<em>Tobie</em>' }]"),
	)];

	// Set of DATA for VectorSearch and  Knn Operator checking
	const DATA_MTREE: [&str; 4] = [
		"CREATE pts:1 SET point = [1,2,3,4]",
		"CREATE pts:2 SET point = [4,5,6,7]",
		"CREATE pts:3 SET point = [8,9,10,11]",
		"DEFINE INDEX mt_pts ON pts FIELDS point MTREE DIMENSION 4",
	];

	const CHECK_MTREE_DB: [Check; 1] = [(
		"SELECT id, vector::distance::euclidean(point, [2,3,4,5]) AS dist FROM pts WHERE point <|2|> [2,3,4,5]",
		Some("[{ dist: 2f, id: pts:1 }, { dist: 4f, id: pts:2 }]"),
	)];

	const CHECK_KNN_BRUTEFORCE: [Check; 1] = [(
		"SELECT id, vector::distance::euclidean(point, [2,3,4,5]) AS dist FROM pts WHERE point <|2,EUCLIDEAN|> [2,3,4,5]",
		Some("[{ dist: 2f, id: pts:1 }, { dist: 4f, id: pts:2 }]"),
	)];

	type Check = (&'static str, Option<&'static str>);

	const CHECK_DB: [Check; 1] = [("INFO FOR DB", None)];

	// *******
	// HELPERS
	// *******

	async fn request_port() -> u16 {
		let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
		listener.local_addr().unwrap().port()
	}

	async fn start_docker(docker_version: &str) -> (String, DockerContainer, Surreal<Any>) {
		use surrealdb::opt::WaitFor::Connection;
		// Location of the database files (RocksDB) in the Host
		let file_path = format!("/tmp/{}.db", Ulid::new());
		let port = request_port().await;
		let docker = DockerContainer::start(docker_version, &file_path, USER, PASS, port);
		let client = Surreal::<Any>::init();
		let db = client.clone();
		let localhost = Ipv4Addr::LOCALHOST;
		// We need HTTP because we are using the import method which is not available
		// with WS
		let endpoint = format!("http://{localhost}:{port}");
		info!("Wait for the database to be ready; endpoint => {endpoint}");
		tokio::spawn(async move {
			loop {
				if db.connect(&endpoint).await.is_ok() {
					break;
				}
				sleep(Duration::from_millis(500)).await;
			}
		});
		client.wait_for(Connection).await;
		info!("Sign into the database");
		client
			.signin(Root {
				username: USER,
				password: PASS,
			})
			.await
			.unwrap();
		info!("Select namespace and database");
		client.use_ns(NS).use_db(DB).await.unwrap();
		(file_path, docker, client)
	}

	async fn create_data_on_docker(client: &Surreal<Any>, info: &str, data: &[&str]) {
		info!("Create {info} data on Docker's instance");
		for l in data {
			info!("Run `{l}`");
			let data = surrealdb::opt::Raw::from(l.to_string());
			client.query(data).await.expect(l).check().expect(l);
		}
	}

	async fn import_data_on_docker(client: &Surreal<Any>, info: &str, path: &str) {
		info!("Import {info} data on Docker's instance: {path}");
		client.import(path).await.expect(info);
	}

	async fn check_data_on_docker(client: &Surreal<Any>, info: &str, queries: &[Check]) {
		info!("Check {info} data on Docker's instance");
		for (query, expected) in queries {
			info!("Run `{query}`");
			match expected {
				Some(expected) => {
					let data = surrealdb::opt::Raw::from(query.to_string());
					let response: Value =
						client.query(data).await.expect(query).take(0).expect(query);
					assert_eq!(response.to_string(), *expected, "{query}");
				}
				None => {
					client.query(*query).await.expect(query).check().expect(query);
				}
			}
		}
	}

	async fn get_info_ns_db(client: &Surreal<Any>, info: &str) -> (Value, Value) {
		info!("Collect INFO NS/DB for the database {info}");
		let mut results = client.query("INFO FOR NS; INFO FOR DB STRUCTURE").await.unwrap();
		let info_ns: Value = results.take(0).unwrap();
		let info_db: Value = results.take(1).unwrap();
		(info_ns, info_db)
	}

	fn extract_table_names(info_for_db: &Value, info: &str, expected_size: usize) -> Vec<String> {
		info!("Extract table names for the database {info}");
		let mut index = 0;
		let mut names = vec![];
		let tables = info_for_db.get("tables");
		loop {
			let t = tables.get(index);
			if t.is_none() {
				break;
			}
			let n = t.get("name").to_string().replace("'", "");
			names.push(n);
			index += 1;
		}
		assert_eq!(names.len(), expected_size);
		names
	}

	async fn get_info_tables(
		client: &Surreal<Any>,
		table_names: &[String],
		info: &str,
	) -> Vec<Value> {
		info!("Collect INFO TABLE(S) for the database {info}");
		let mut tables = vec![];
		for n in table_names {
			let data = surrealdb::opt::Raw::from(format!("INFO FOR TABLE `{n}`"));
			let table = client.query(data).await.unwrap().take(0).unwrap();
			tables.push(table);
		}
		tables
	}

	async fn get_table_rows(
		client: &Surreal<Any>,
		table_names: &[String],
		info: &str,
	) -> Vec<Value> {
		info!("Collect ROWS for the database {info}");
		let mut tables_rows = vec![];
		for n in table_names {
			let q = format!("SELECT * FROM `{n}`");
			info!("{q}");
			let rows: Value =
				client.query(surrealdb::opt::Raw::from(q)).await.unwrap().take(0).unwrap();
			tables_rows.push(rows);
		}
		tables_rows
	}

	fn check_info_key<'a>(prev: &'a Value, next: &'a Value, key: &str) -> (&'a Value, &'a Value) {
		let prev_value = prev.get(key);
		let next_value = next.get(key);
		check_value(prev_value, next_value, key);
		(prev_value, next_value)
	}

	fn check_values(prev: &[Value], next: &[Value], info: &str) {
		info!("Check {info}s {}/{}", prev.len(), next.len());
		for (i, (p, n)) in prev.iter().zip(next.iter()).enumerate() {
			check_value(p, n, format!("{info} {i}").as_str());
		}
	}

	fn check_value(prev: &Value, next: &Value, info: &str) {
		assert_eq!(prev, next, "{info}");
	}

	fn check_info_db(prev: &Value, next: &Value) {
		info!("Check INFO DB (analyzers, tables, indexes, users)");
		check_info_key(prev, next, "analyzers");
		check_info_key(prev, next, "users");
		check_info_key(prev, next, "indexes");
		check_info_key(prev, next, "tables");
	}

	async fn check_migrated_data(db: &Surreal<Any>, info: &str, queries: &[Check]) {
		info!("Check migrated {info} data");
		for (query, expected_results) in queries {
			info!("Run `{query}`");
			checked_query(db, query, *expected_results).await;
		}
	}

	// Executes the query and ensures to print out the query if it does not pass
	async fn checked_query<C>(db: &Surreal<C>, q: &str, expected: Option<&str>)
	where
		C: Connection,
	{
		info!("Run `{q}`");
		let data = surrealdb::opt::Raw::from(q.to_string());
		let mut res = db.query(data).await.expect(q).check().expect(q);
		if let Some(expected) = expected {
			let results: Value = res.take(0).unwrap();
			assert_eq!(results.to_string(), expected, "{q}");
		}
	}

	async fn new_local_instance(file_path: &String) -> Surreal<Any> {
		let endpoint = format!("rocksdb:{file_path}");
		info!("Create a new local instance; endpoint => {endpoint}");
		let db = connect(endpoint).await.unwrap();
		info!("Select namespace and database");
		db.use_ns(NS).use_db(DB).await.unwrap();
		db
	}
}
