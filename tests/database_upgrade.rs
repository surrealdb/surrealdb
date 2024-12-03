#![allow(unused_imports)]
#![allow(dead_code)]

mod common;

#[cfg(docker)]
mod database_upgrade {
	use super::common::docker::DockerContainer;
	use super::common::expected::Expected;
	use super::common::rest_client::RestClient;
	use serde_json::Value as JsonValue;
	use serial_test::serial;
	use std::time::Duration;
	use surrealdb::engine::any::{connect, Any};
	use surrealdb::{Connection, Surreal};
	use test_log::test;
	use tracing::info;
	use ulid::Ulid;

	const CNX_TIMEOUT: Duration = Duration::from_secs(180);
	const NS: &str = "test";
	const DB: &str = "test";
	const USER: &str = "root";
	const PASS: &str = "root";

	// This test include a feature set that is supported since v2.0
	async fn upgrade_test_from_2_0(version: &str) {
		// Start the docker instance
		let (path, mut docker, client) = start_docker(version).await;

		// Create the data set
		create_data_on_docker(&client, "IDX", &DATA_IDX).await;
		create_data_on_docker(&client, "FTS", &DATA_FTS).await;
		create_data_on_docker(&client, "MTREE", &DATA_MTREE).await;

		// Check the data set
		check_data_on_docker(&client, "IDX", &CHECK_IDX).await;
		check_data_on_docker(&client, "DB", &CHECK_DB).await;
		check_data_on_docker(&client, "FTS", &CHECK_FTS).await;
		check_data_on_docker(&client, "MTREE", &CHECK_MTREE_RPC).await;

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
	}

	#[test(tokio::test(flavor = "multi_thread"))]
	#[cfg(feature = "storage-rocksdb")]
	#[serial]
	async fn upgrade_test_from_2_0_0() {
		upgrade_test_from_2_0("2.0.0").await;
	}

	#[test(tokio::test(flavor = "multi_thread"))]
	#[cfg(feature = "storage-rocksdb")]
	#[serial]
	async fn upgrade_test_from_2_0_1() {
		upgrade_test_from_2_0("2.0.1").await;
	}

	#[test(tokio::test(flavor = "multi_thread"))]
	#[cfg(feature = "storage-rocksdb")]
	#[serial]
	async fn upgrade_test_from_2_0_2() {
		upgrade_test_from_2_0("2.0.2").await;
	}

	#[test(tokio::test(flavor = "multi_thread"))]
	#[cfg(feature = "storage-rocksdb")]
	#[serial]
	async fn upgrade_test_from_2_0_3() {
		upgrade_test_from_2_0("2.0.3").await;
	}

	#[test(tokio::test(flavor = "multi_thread"))]
	#[cfg(feature = "storage-rocksdb")]
	#[serial]
	async fn upgrade_test_from_2_0_4() {
		upgrade_test_from_2_0("2.0.4").await;
	}

	#[test(tokio::test(flavor = "multi_thread"))]
	#[cfg(feature = "storage-rocksdb")]
	#[serial]
	async fn upgrade_test_from_2_1_0() {
		upgrade_test_from_2_0("v2.1.0").await;
	}

	#[test(tokio::test(flavor = "multi_thread"))]
	#[cfg(feature = "storage-rocksdb")]
	#[serial]
	async fn upgrade_test_from_2_1_1() {
		upgrade_test_from_2_0("v2.1.1").await;
	}

	#[test(tokio::test(flavor = "multi_thread"))]
	#[cfg(feature = "storage-rocksdb")]
	#[serial]
	async fn upgrade_test_from_2_1_2() {
		upgrade_test_from_2_0("v2.1.2").await;
	}

	// *******
	// DATASET
	// *******

	// Set of DATA for Standard and unique indexes
	const DATA_IDX: [&str; 4] = [
		"DEFINE INDEX uniq_name ON TABLE person COLUMNS name UNIQUE",
		"DEFINE INDEX idx_company ON TABLE person COLUMNS company",
		"CREATE person:tobie SET name = 'Tobie', company='SurrealDB'",
		"CREATE person:jaime SET name = 'Jaime', company='SurrealDB'",
	];

	// Set of QUERY and RESULT to check for standard and unique indexes
	const CHECK_IDX: [Check; 2] = [
		(
			"SELECT name FROM person WITH INDEX uniq_name WHERE name = 'Tobie'",
			Expected::One("{\"name\":\"Tobie\"}"),
		),
		(
			"SELECT name FROM person WITH INDEX idx_company WHERE company = 'SurrealDB'",
			Expected::Two("{\"name\":\"Jaime\"}", "{\"name\":\"Tobie\"}"),
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
		Expected::One("{\"name\":\"<em>Tobie</em>\"}"),
	)];

	// Set of DATA for VectorSearch and  Knn Operator checking
	const DATA_MTREE: [&str; 4] = [
		"CREATE pts:1 SET point = [1,2,3,4]",
		"CREATE pts:2 SET point = [4,5,6,7]",
		"CREATE pts:3 SET point = [8,9,10,11]",
		"DEFINE INDEX mt_pts ON pts FIELDS point MTREE DIMENSION 4",
	];

	const CHECK_MTREE_RPC: [Check; 1] = [
		("SELECT id, vector::distance::euclidean(point, [2,3,4,5]) AS dist FROM pts WHERE point <2> [2,3,4,5]",
		 Expected::Two("{\"dist\": 2.0, \"id\": \"pts:1\"}", "{ \"dist\": 4.0, \"id\": \"pts:2\"}"))];

	const CHECK_MTREE_DB: [Check; 1] = [
		("SELECT id, vector::distance::euclidean(point, [2,3,4,5]) AS dist FROM pts WHERE point <|2|> [2,3,4,5]",
		 Expected::Two("{\"dist\": 2.0, \"id\": {\"tb\": \"pts\", \"id\": {\"Number\": 1}}}", "{ \"dist\": 4.0, \"id\": {\"tb\": \"pts\", \"id\": {\"Number\": 2}}}"))];
	const CHECK_KNN_BRUTEFORCE: [Check; 1] = [
		("SELECT id, vector::distance::euclidean(point, [2,3,4,5]) AS dist FROM pts WHERE point <|2,EUCLIDEAN|> [2,3,4,5]",
		 Expected::Two("{\"dist\": 2.0, \"id\": {\"tb\": \"pts\", \"id\": {\"Number\": 1}}}", "{ \"dist\": 4.0, \"id\": {\"tb\": \"pts\", \"id\": {\"Number\": 2}}}"))];

	type Check = (&'static str, Expected);

	const CHECK_DB: [Check; 1] = [("INFO FOR DB", Expected::Any)];

	// *******
	// HELPERS
	// *******

	async fn start_docker(docker_version: &str) -> (String, DockerContainer, RestClient) {
		// Location of the database files (RocksDB) in the Host
		let file_path = format!("/tmp/{}.db", Ulid::new());
		let docker = DockerContainer::start(docker_version, &file_path, USER, PASS);
		let client = RestClient::new(NS, DB, USER, PASS)
			.wait_for_connection(&CNX_TIMEOUT)
			.await
			.unwrap_or_else(|| {
				docker.logs();
				panic!("No connected client")
			});
		(file_path, docker, client)
	}

	async fn create_data_on_docker(client: &RestClient, info: &str, data: &[&str]) {
		info!("Create {info} data on Docker's instance");
		for l in data {
			client.checked_query(l, &Expected::Any).await;
		}
	}

	async fn check_data_on_docker(client: &RestClient, info: &str, queries: &[Check]) {
		info!("Check {info} data on Docker's instance");
		for (query, expected) in queries {
			client.checked_query(query, expected).await;
		}
	}

	async fn check_migrated_data(db: &Surreal<Any>, info: &str, queries: &[Check]) {
		info!("Check migrated {info} data");
		for (query, expected_results) in queries {
			checked_query(db, query, expected_results).await;
		}
	}

	// Executes the query and ensures to print out the query if it does not pass
	async fn checked_query<C>(db: &Surreal<C>, q: &str, expected: &Expected)
	where
		C: Connection,
	{
		let mut res = db.query(q).await.expect(q).check().expect(q);
		assert_eq!(res.num_statements(), 1, "Wrong number of result on query {q}");
		let results: Vec<JsonValue> = res.take(0).unwrap();
		expected.check_results(q, &results);
	}

	async fn new_local_instance(file_path: &String) -> Surreal<Any> {
		let db = connect(format!("file:{file_path}")).await.unwrap();
		db.use_ns(NS).await.unwrap();
		db.use_db(DB).await.unwrap();
		db
	}
}
