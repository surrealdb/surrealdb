mod upgrade {
	use http::{header, HeaderMap, StatusCode};
	use reqwest::Client;
	use serde_json::Value as JsonValue;
	use std::process::Command;
	use std::time::{Duration, SystemTime};
	use surrealdb::engine::any::{connect, Any};
	use surrealdb::{Connection, Response, Surreal};
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
	async fn upgrade_test() {
		// Get the version to migrate from (Docker TAG)
		let docker_version: String =
			std::env::var(PREVIOUS_DOCKER_VERSION).unwrap_or(DEFAULT_DOCKER_VERSION.to_string());

		// Location of the database files (RocksDB) in the Host
		let file_path = format!("/tmp/{}.db", Ulid::new());
		{
			// Start the docker instance
			let mut docker = DockerContainer::start(&docker_version, &file_path);
			let client = RestClient::new().wait_for_connection().await;
			// Create data samples
			create_data_on_docker(&client).await;
			// Check that the data are okay on the original instance
			check_data_on_docker(&client).await;
			// Stop the docker instance
			docker.stop();
		}
		{
			// Start a local RocksDB instance using the same location
			let db = new_local_instance(&file_path).await;
			// Check that the data has properly migrated
			check_migrated_data(&db).await;
		}
	}

	const DATA: [&str; 5] = [
        "DEFINE ANALYZER name TOKENIZERS class FILTERS lowercase,ngram(1,128)",
        "DEFINE ANALYZER userdefinedid TOKENIZERS blank FILTERS lowercase,ngram(1,32)",
        "DEFINE INDEX account_name_search_idx ON TABLE account COLUMNS name SEARCH ANALYZER name BM25(1.2,0.75) HIGHLIGHTS",
        "DEFINE INDEX account_user_defined_id_search_idx ON TABLE account COLUMNS user_defined_id SEARCH ANALYZER userdefinedid BM25 HIGHLIGHTS",
        "CREATE account SET name='Tobie', user_defined_id='Tobie'",
    ];

	async fn create_data_on_docker(client: &RestClient) {
		info!("Create data on Docker's instance");
		for l in DATA {
			client.checked_query(l, None).await;
		}
	}

	async fn check_data_on_docker(client: &RestClient) {
		info!("Check data on Docker's instance");

		// Check that the full-text search is working
		client
			.checked_query(
				"SELECT name FROM account WHERE name @@ 'Tobie'",
				Some("[{\"name\":\"Tobie\"}]"),
			)
			.await;

		// Check that we can deserialize the table definitions
		client.checked_query("INFO FOR ROOT", None).await;
	}

	async fn check_migrated_data(db: &Surreal<Any>) {
		info!("Check migrated data");

		// Check that the full-text search is working
		let mut res = checked_query(db, "SELECT name FROM account WHERE name @@ 'Tobie'").await;
		assert_eq!(res.num_statements(), 1);
		let n: Vec<String> = res.take("name").expect("Take name");
		assert_eq!(n, vec!["Tobie"]);

		// Check that we can deserialize the table definitions
		let res = checked_query(db, "INFO FOR DB").await;
		assert_eq!(res.num_statements(), 1);
	}

	// Executes the query and ensures to print out the query if it does not pass
	async fn checked_query<C>(db: &Surreal<C>, q: &str) -> Response
	where
		C: Connection,
	{
		db.query(q).await.expect(q).check().expect(q)
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
			let mut args =
				Arguments::new(["run", "-p", &format!("8000:{DOCKER_EXPOSED_PORT}"), "-d"]);
			args.add(["-v"]);
			args.add([format!("{file_path}:{file_path}")]);
			args.add([docker_image]);
			args.add(["start", "--log", "trace"]);
			args.add(["--auth", "--user", USER, "--pass", PASS]);
			args.add([format!("file:{file_path}")]);
			let id = Self::docker(args);
			Self {
				id,
				running: true,
			}
		}

		fn stop(&mut self) {
			if self.running {
				info!("Stopping Docker container {}", self.id);
				Self::docker(Arguments::new(["stop", &self.id]));
				self.running = false;
			}
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

		async fn wait_for_connection(self) -> Self {
			let start = SystemTime::now();
			while start.elapsed().unwrap() < CNX_TIMEOUT {
				sleep(Duration::from_secs(2)).await;
				if self.query("INFO FOR ROOT").await.status() == 200 {
					return self;
				}
				warn!("DB not yet responding");
				sleep(Duration::from_secs(2)).await;
			}
			panic!("Cannot connect to DB");
		}

		async fn query(&self, q: &str) -> reqwest::Response {
			self.c
				.post(&self.u)
				.basic_auth(USER, Some(PASS))
				.body(q.to_string())
				.send()
				.await
				.expect(q)
		}

		async fn checked_query(&self, q: &str, expected_json_result: Option<&str>) {
			let r = self.query(q).await;
			assert_eq!(r.status(), StatusCode::OK);
			if let Some(expected) = expected_json_result {
				// Convert the result to JSON
				let j: JsonValue = r.json().await.expect(q);
				debug!("{q} => {j:#}");
				// The result is should be an array
				let a = j.as_array().expect(q);
				// Extract the first item of the array
				let r0 = a.first().unwrap_or_else(|| panic!("Empty array on query: {q}"));
				// Check the status
				let status = r0.get("status").unwrap_or_else(|| panic!("No status on query: {q}"));
				assert_eq!(status.as_str(), Some("OK"), "Wrong status for {q} => {status:#}");
				// Check we have a result
				let result = r0.get("result").unwrap_or_else(|| panic!("No result for query: {q}"));
				// Compare the result with what is expected
				let expected: JsonValue = serde_json::from_str(expected).expect(expected);
				assert_eq!(format!("{:#}", result), format!("{:#}", expected));
			}
		}
	}
}
