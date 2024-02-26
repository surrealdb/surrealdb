mod upgrade {
	use http::{header, HeaderMap, StatusCode};
	use reqwest::Client;
	use serde_json::Value as JsonValue;
	use std::process::Command;
	use std::time::{Duration, SystemTime};
	use surrealdb::engine::any::{connect, Any};
	use surrealdb::{Connection, Surreal};
	use test_log::test;
	use tokio::time::sleep;
	use tracing::{debug, error, info, warn};
	use ulid::Ulid;

	const PREVIOUS_DOCKER_VERSION: &str = "SURREALDB_TEST_DOCKER_PREVIOUS_VERSION";
	const DEFAULT_DOCKER_VERSION: &str = "v1.2.1";
	const DOCKER_EXPOSED_PORT: usize = 8000;
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
			let mut docker = DockerContainer::start(&docker_version, &file_path);
			let client = RestClient::new().wait_for_connection().await.unwrap_or_else(|| {
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

	static CHECK_MTREE: [Check; 1] = [
		("SELECT id, vector::distance::euclidean(point, [2,3,4,5]) AS dist FROM pts WHERE point <2> [2,3,4,5]",
		 Expected::Two("{\"dist\": 2.0, \"id\": \"pts:1\" }", "{  \"dist\": 4.0, \"id\": \"pts:2\" }"))];

	type Check = (&'static str, Expected);
	enum Expected {
		Any,
		One(&'static str),
		Two(&'static str, &'static str),
	}

	impl Expected {
		fn check_results(&self, q: &str, results: &[JsonValue]) {
			match self {
				Expected::Any => {}
				Expected::One(expected) => {
					assert_eq!(results.len(), 1, "Wrong number of result for {}", q);
					Self::check_json(q, &results[0], expected);
				}
				Expected::Two(expected1, expected2) => {
					assert_eq!(results.len(), 2, "Wrong number of result for {}", q);
					Self::check_json(q, &results[0], expected1);
					Self::check_json(q, &results[1], expected2);
				}
			}
		}

		fn check_json(q: &str, result: &JsonValue, expected: &str) {
			let expected: JsonValue = serde_json::from_str(expected).expect(expected);
			assert_eq!(result, &expected, "Unexpected result on query {}", q);
		}
	}

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
		check_data_on_docker(client, &CHECK_MTREE).await;
	}

	async fn check_migrated_data_1_1(db: &Surreal<Any>) {
		check_migrated_data_1_0(db).await;
		check_migrated_data(db, &CHECK_MTREE).await;
	}

	async fn create_data_for_1_2(client: &RestClient) {
		create_data_for_1_1(client).await;
	}

	async fn check_migrated_data_1_2(db: &Surreal<Any>) {
		check_migrated_data_1_1(db).await;
	}

	async fn check_data_on_docker(client: &RestClient, queries: &[Check]) {
		info!("Check data on Docker's instance");
		for (query, expected) in queries.to_owned().into_iter() {
			client.checked_query(query, expected).await;
		}
	}

	async fn check_migrated_data(db: &Surreal<Any>, queries: &[Check]) {
		info!("Check migrated data");
		for (query, expected_results) in queries.into_iter() {
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

	struct DockerContainer {
		id: String,
		running: bool,
	}

	impl DockerContainer {
		fn start(version: &str, file_path: &str) -> Self {
			let docker_image = format!("surrealdb/surrealdb:{version}");
			info!("Start Docker image {docker_image} with file {file_path}");
			let mut args = Arguments::new([
				"run",
				"-p",
				&format!("127.0.0.1:8000:{DOCKER_EXPOSED_PORT}"),
				"-d",
			]);
			args.add([docker_image]);
			args.add(["start", "--auth", "--user", USER, "--pass", PASS]);
			args.add([format!("file:{file_path}")]);
			let id = Self::docker(args);
			Self {
				id,
				running: true,
			}
		}

		fn logs(&self) {
			info!("Logging Docker container {}", self.id);
			Self::docker(Arguments::new(["logs", &self.id]));
		}
		fn stop(&mut self) {
			if self.running {
				info!("Stopping Docker container {}", self.id);
				Self::docker(Arguments::new(["stop", &self.id]));
				self.running = false;
			}
		}

		fn extract_data_dir(&self, file_path: &str) {
			let container_src_path = format!("{}:{file_path}", self.id);
			info!("Extract directory from Docker container {}", container_src_path);
			Self::docker(Arguments::new(["cp", &container_src_path, file_path]));
		}

		fn docker(args: Arguments) -> String {
			let mut command = Command::new("docker");

			let output = command.args(args.0).output().unwrap();
			let std_out = String::from_utf8(output.stdout).unwrap().trim().to_string();
			if !output.stderr.is_empty() {
				error!("{}", String::from_utf8(output.stderr).unwrap());
			}
			assert_eq!(output.status.code(), Some(0), "Docker command failure: {:?}", command);
			std_out
		}
	}

	impl Drop for DockerContainer {
		fn drop(&mut self) {
			// Be sure the container is stopped
			self.stop();
			// Delete the container
			info!("Delete Docker container {}", self.id);
			Self::docker(Arguments::new(["rm", &self.id]));
		}
	}

	struct Arguments(Vec<String>);

	impl Arguments {
		fn new<I, S>(args: I) -> Self
		where
			I: IntoIterator<Item = S>,
			S: Into<String>,
		{
			let mut a = Self(vec![]);
			a.add(args);
			a
		}

		fn add<I, S>(&mut self, args: I)
		where
			I: IntoIterator<Item = S>,
			S: Into<String>,
		{
			for arg in args {
				self.0.push(arg.into());
			}
		}
	}

	struct RestClient {
		c: Client,
		u: String,
	}

	impl RestClient {
		fn new() -> Self {
			let mut headers = HeaderMap::new();
			headers.insert("NS", NS.parse().unwrap());
			headers.insert("DB", DB.parse().unwrap());
			headers.insert(header::ACCEPT, "application/json".parse().unwrap());
			let c = Client::builder()
				.connect_timeout(Duration::from_millis(10))
				.default_headers(headers)
				.build()
				.expect("Client::builder()...build()");
			Self {
				c,
				u: format!("http://127.0.0.1:{DOCKER_EXPOSED_PORT}/sql"),
			}
		}

		async fn wait_for_connection(self) -> Option<Self> {
			sleep(Duration::from_secs(2)).await;
			let start = SystemTime::now();
			while start.elapsed().unwrap() < CNX_TIMEOUT {
				sleep(Duration::from_secs(2)).await;
				if let Some(r) = self.query("INFO FOR ROOT").await {
					if r.status() == StatusCode::OK {
						return Some(self);
					}
				}
				warn!("DB not yet responding");
				sleep(Duration::from_secs(2)).await;
			}
			None
		}

		async fn query(&self, q: &str) -> Option<reqwest::Response> {
			match self.c.post(&self.u).basic_auth(USER, Some(PASS)).body(q.to_string()).send().await
			{
				Ok(r) => Some(r),
				Err(e) => {
					error!("{e}");
					None
				}
			}
		}

		async fn checked_query(&self, q: &str, expected: &Expected) {
			let r = self.query(q).await.unwrap_or_else(|| panic!("No response for {q}"));
			assert_eq!(
				r.status(),
				StatusCode::OK,
				"Wrong response for {q} -> {}",
				r.text().await.expect(q)
			);
			// Convert the result to JSON
			let j: JsonValue = r.json().await.expect(q);
			debug!("{q} => {j:#}");
			// The result should be an array
			let results_with_status = j.as_array().expect(q);
			assert_eq!(results_with_status.len(), 1, "Wrong number of results on query {q}");
			let result_with_status = &results_with_status[0];
			// Check the status
			let status = result_with_status
				.get("status")
				.unwrap_or_else(|| panic!("No status on query: {q}"));
			assert_eq!(status.as_str(), Some("OK"), "Wrong status for {q} => {status:#}");
			// Extract the results
			let results = result_with_status
				.get("result")
				.unwrap_or_else(|| panic!("No result for query: {q}"));
			if !matches!(expected, Expected::Any) {
				// Check the results
				let results = results.as_array().expect(q);
				expected.check_results(q, &results);
			}
		}
	}
}
