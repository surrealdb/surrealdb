mod common;

mod upgrade {
	use crate::common::docker::DockerContainer;
	use crate::common::expected::Expected;
	use crate::common::rest_client::RestClient;
	use serde_json::Value as JsonValue;
	use std::time::Duration;
	use surrealdb::engine::any::{connect, Any};
	use surrealdb::{Connection, Surreal};
	use test_log::test;
	use tracing::info;
	use ulid::Ulid;

	const PREVIOUS_DOCKER_VERSION: &str = "SURREALDB_TEST_DOCKER_PREVIOUS_VERSION";
	const DEFAULT_DOCKER_VERSION: &str = "v1.2.1";
	const CNX_TIMEOUT: Duration = Duration::from_secs(180);
	const NS: &str = "test";
	const DB: &str = "test";
	const USER: &str = "root";
	const PASS: &str = "root";

	// Optionally set the tag for the SurrealDB Docker image to upgrade from:
	// export SURREALDB_TEST_DOCKER_PREVIOUS_VERSION="v1.2.1"
	// We may also change the log level:
	// export RUST_LOG=info
	// To run this test:
	// cargo test --package surreal --test upgrade upgrade::upgrade_test
	#[test(tokio::test(flavor = "multi_thread"))]
	#[cfg(feature = "storage-rocksdb")]
	async fn upgrade_test() {
		// Get the version to migrate from (Docker TAG)
		let docker_version: String =
			std::env::var(PREVIOUS_DOCKER_VERSION).unwrap_or(DEFAULT_DOCKER_VERSION.to_string());

		// Location of the database files (RocksDB) in the Host
		let file_path = format!("/tmp/{}.db", Ulid::new());

		{
			// Start the docker instance
			let mut docker = DockerContainer::start(&docker_version, &file_path, USER, PASS);
			let client = RestClient::new(NS, DB, USER, PASS)
				.wait_for_connection(&CNX_TIMEOUT)
				.await
				.unwrap_or_else(|| {
					docker.logs();
					panic!("No connected client")
				});
			// Create data samples
			if docker_version.starts_with("1.0.") {
				create_data_for_1_0(&client).await;
			} else if docker_version.starts_with("v1.1.") {
				create_data_for_1_1(&client).await;
			} else if docker_version.starts_with("v1.2.") {
				create_data_for_1_2(&client).await;
			} else {
				panic!("Unsupported version {docker_version}");
			}
			// Stop the docker instance
			docker.stop();
			// Extract the database directory
			docker.extract_data_dir(&file_path);
		}
		{
			// Start a local RocksDB instance using the same location
			let db = new_local_instance(&file_path).await;
			// Check that the data has properly migrated
			if docker_version.starts_with("1.0.") {
				check_migrated_data_1_0(&db).await;
			} else if docker_version.starts_with("v1.1.") {
				check_migrated_data_1_1(&db).await;
			} else if docker_version.starts_with("v1.2.") {
				check_migrated_data_1_2(&db).await;
			} else {
				panic!("Unsupported version {docker_version}");
			}
		}
	}

	// Set of DATA for Full Text Search
	const DATA_FTS: [&str; 5] = [
		"DEFINE ANALYZER name TOKENIZERS class FILTERS lowercase,ngram(1,128)",
		"DEFINE ANALYZER userdefinedid TOKENIZERS blank FILTERS lowercase,ngram(1,32)",
		"DEFINE INDEX account_name_search_idx ON TABLE account COLUMNS name SEARCH ANALYZER name BM25(1.2,0.75) HIGHLIGHTS",
		"DEFINE INDEX account_user_defined_id_search_idx ON TABLE account COLUMNS user_defined_id SEARCH ANALYZER userdefinedid BM25 HIGHLIGHTS",
		"CREATE account SET name='Tobie', user_defined_id='Tobie'",
	];

	// Set of QUERY and RESULT to check for Full Text Search
	static CHECK_FTS: [Check; 1] =
		[("SELECT name FROM account WHERE name @@ 'Tobie'", Expected::One("{\"name\":\"Tobie\"}"))];

	// Set of DATA for VectorSearch and  Knn Operator checking
	const DATA_MTREE: [&str; 4] = [
		"CREATE pts:1 SET point = [1,2,3,4]",
		"CREATE pts:2 SET point = [4,5,6,7]",
		"CREATE pts:3 SET point = [8,9,10,11]",
		"DEFINE INDEX mt_pts ON pts FIELDS point MTREE DIMENSION 4",
	];

	static CHECK_MTREE_RPC: [Check; 1] = [
		("SELECT id, vector::distance::euclidean(point, [2,3,4,5]) AS dist FROM pts WHERE point <2> [2,3,4,5]",
		 Expected::Two("{\"dist\": 2.0, \"id\": \"pts:1\"}", "{ \"dist\": 4.0, \"id\": \"pts:2\"}"))];

	static CHECK_MTREE_DB: [Check; 1] = [
		("SELECT id, vector::distance::euclidean(point, [2,3,4,5]) AS dist FROM pts WHERE point <2> [2,3,4,5]",
		 Expected::Two("{\"dist\": 2.0, \"id\": {\"tb\": \"pts\", \"id\": {\"Number\": 1}}}", "{ \"dist\": 4.0, \"id\": {\"tb\": \"pts\", \"id\": {\"Number\": 2}}}"))];

	static CHECK_KNN_DB_BRUTEFORCE: [Check; 1] = [
		("SELECT id, vector::distance::euclidean(point, [2,3,4,5]) AS dist FROM pts WHERE point <2,EUCLIDEAN> [2,3,4,5]",
		 Expected::Two("{\"dist\": 2.0, \"id\": {\"tb\": \"pts\", \"id\": {\"Number\": 1}}}", "{ \"dist\": 4.0, \"id\": {\"tb\": \"pts\", \"id\": {\"Number\": 2}}}"))];

	type Check = (&'static str, Expected);

	const CHECK_DB: [Check; 1] = [("INFO FOR DB", Expected::Any)];

	async fn create_data_on_docker(client: &RestClient, data: &[&str]) {
		info!("Create data on Docker's instance");
		for l in data {
			client.checked_query(l, &Expected::Any).await;
		}
	}

	async fn create_data_for_1_0(client: &RestClient) {
		create_data_on_docker(client, &DATA_FTS).await;
		check_data_on_docker(client, &CHECK_FTS).await;
		check_data_on_docker(client, &CHECK_DB).await;
	}

	async fn check_migrated_data_1_0(db: &Surreal<Any>) {
		check_migrated_data(db, &CHECK_FTS).await;
		check_migrated_data(db, &CHECK_DB).await;
	}

	async fn create_data_for_1_1(client: &RestClient) {
		create_data_for_1_0(client).await;
		create_data_on_docker(client, &DATA_MTREE).await;
		check_data_on_docker(client, &CHECK_MTREE_RPC).await;
	}

	async fn check_migrated_data_1_1(db: &Surreal<Any>) {
		check_migrated_data_1_0(db).await;
		check_migrated_data(db, &CHECK_MTREE_DB).await;
	}

	async fn create_data_for_1_2(client: &RestClient) {
		create_data_for_1_1(client).await;
	}

	async fn check_migrated_data_1_2(db: &Surreal<Any>) {
		check_migrated_data_1_1(db).await;
		check_migrated_data(db, &CHECK_KNN_DB_BRUTEFORCE).await;
	}

	async fn check_data_on_docker(client: &RestClient, queries: &[Check]) {
		info!("Check data on Docker's instance");
		for (query, expected) in queries {
			client.checked_query(query, expected).await;
		}
	}

	async fn check_migrated_data(db: &Surreal<Any>, queries: &[Check]) {
		info!("Check migrated data");
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
		let db = connect(format!("file:{}", file_path)).await.unwrap();
		db.use_ns(NS).await.unwrap();
		db.use_db(DB).await.unwrap();
		db
	}
}
