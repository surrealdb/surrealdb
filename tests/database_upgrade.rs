mod common;

#[cfg(docker)]
mod database_upgrade {
	use super::common::docker::DockerContainer;
	use std::net::Ipv4Addr;
	use std::time::Duration;
	use surrealdb::engine::any::{connect, Any};
	use surrealdb::opt::auth::Root;
	use surrealdb::{Connection, Surreal, Value};
	use test_log::test;
	use tokio::net::TcpListener;
	use tokio::sync::Semaphore;
	use tokio::time::sleep;
	use tokio::time::timeout;
	use tracing::error;
	use tracing::info;
	use ulid::Ulid;

	const NS: &str = "test";
	const DB: &str = "test";
	const USER: &str = "root";
	const PASS: &str = "root";

	// Limit number of running containers at the time
	static PERMITS: Semaphore = Semaphore::const_new(3);

	const TIMEOUT_DURATION: Duration = Duration::from_secs(180);

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

	macro_rules! run {
		($future:expr) => {
			let permit = PERMITS.acquire().await.unwrap();
			if timeout(TIMEOUT_DURATION, $future).await.is_err() {
				drop(permit);
				error!("test timed out");
				panic!();
			}
			drop(permit);
		};
	}

	#[test(tokio::test(flavor = "multi_thread"))]
	#[cfg(feature = "storage-rocksdb")]
	async fn upgrade_test_from_2_0_0() {
		run!(upgrade_test_from_2_0("v2.0.0"));
	}

	#[test(tokio::test(flavor = "multi_thread"))]
	#[cfg(feature = "storage-rocksdb")]
	async fn upgrade_test_from_2_0_1() {
		run!(upgrade_test_from_2_0("v2.0.1"));
	}

	#[test(tokio::test(flavor = "multi_thread"))]
	#[cfg(feature = "storage-rocksdb")]
	async fn upgrade_test_from_2_0_2() {
		run!(upgrade_test_from_2_0("v2.0.2"));
	}

	#[test(tokio::test(flavor = "multi_thread"))]
	#[cfg(feature = "storage-rocksdb")]
	async fn upgrade_test_from_2_0_3() {
		run!(upgrade_test_from_2_0("v2.0.3"));
	}

	#[test(tokio::test(flavor = "multi_thread"))]
	#[cfg(feature = "storage-rocksdb")]
	async fn upgrade_test_from_2_0_4() {
		run!(upgrade_test_from_2_0("v2.0.4"));
	}

	#[test(tokio::test(flavor = "multi_thread"))]
	#[cfg(feature = "storage-rocksdb")]
	async fn upgrade_test_from_2_1_0() {
		run!(upgrade_test_from_2_0("v2.1.0"));
	}

	#[test(tokio::test(flavor = "multi_thread"))]
	#[cfg(feature = "storage-rocksdb")]
	async fn upgrade_test_from_2_1_1() {
		run!(upgrade_test_from_2_0("v2.1.1"));
	}

	#[test(tokio::test(flavor = "multi_thread"))]
	#[cfg(feature = "storage-rocksdb")]
	async fn upgrade_test_from_2_1_2() {
		run!(upgrade_test_from_2_0("v2.1.2"));
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
			Some("[{ name: 'Tobie' }]"),
		),
		(
			"SELECT name FROM person WITH INDEX idx_company WHERE company = 'SurrealDB'",
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

	const CHECK_MTREE_DB: [Check; 1] = [
		("SELECT id, vector::distance::euclidean(point, [2,3,4,5]) AS dist FROM pts WHERE point <|2|> [2,3,4,5]",
		Some("[{ dist: 2f, id: pts:1 }, { dist: 4f, id: pts:2 }]"))];

	const CHECK_KNN_BRUTEFORCE: [Check; 1] = [
		("SELECT id, vector::distance::euclidean(point, [2,3,4,5]) AS dist FROM pts WHERE point <|2,EUCLIDEAN|> [2,3,4,5]",
		 Some("[{ dist: 2f, id: pts:1 }, { dist: 4f, id: pts:2 }]"))];

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
		let endpoint = format!("ws://{localhost}:{port}");
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
			client.query(*l).await.expect(l).check().expect(l);
		}
	}

	async fn check_data_on_docker(client: &Surreal<Any>, info: &str, queries: &[Check]) {
		info!("Check {info} data on Docker's instance");
		for (query, expected) in queries {
			info!("Run `{query}`");
			match expected {
				Some(expected) => {
					let response: Value =
						client.query(*query).await.expect(query).take(0).expect(query);
					assert_eq!(response.to_string(), *expected, "{query}");
				}
				None => {
					client.query(*query).await.expect(query).check().expect(query);
				}
			}
		}
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
		let mut res = db.query(q).await.expect(q).check().expect(q);
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
