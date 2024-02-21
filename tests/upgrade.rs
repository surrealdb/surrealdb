mod upgrade {
    use std::process::Command;
    use std::time::{Duration, SystemTime};
    use surrealdb::engine::any::{connect, Any};
    use surrealdb::engine::remote::http::{Client, Http};
    use surrealdb::opt::auth::Root;
    use surrealdb::Surreal;
    use test_log::test;
    use tokio::time::sleep;
    use tracing::{error, info, warn};
    use ulid::Ulid;

    const DOCKER_VERSION: &str = "SURREALDB_TEST_DOCKER_PREVIOUS_VERSION";
    const NS: &str = "test";
    const DB: &str = "test";
    const USER: &str = "root";
    const PASS: &str = "root";

    // Optionally set the tag for the SurrealDB Docker image to upgrade from:
    // export SURREALDB_TEST_DOCKER_PREVIOUS_VERSION="v1.2.1"
    // To run this test:
    // cargo test --package surreal --test upgrade upgrade::upgrade_test
    #[test(tokio::test(flavor = "multi_thread"))]
    async fn upgrade_test() {
        // Get the version to migrate from (Docker TAG)
        let docker_version: String = std::env::var(DOCKER_VERSION).unwrap_or("v1.2.1".to_string());

        // Location of the database files (RocksDB) in the Host
        let file_path = format!("/tmp/{}.db", Ulid::new());
        {
            // Start the docker instance
            let docker = DockerContainer::start(&docker_version, &file_path);
            let db = wait_for_connection().await;
            // Create data samples
            create_data(&db).await;
            // Stop the docker instance
            docker.stop();
        }
        {
            // Start a local RocksDB instance using the same location
            let db = new_local_instance(&file_path).await;
            // Perform checks
            check_data(&db).await;
        }
    }

    async fn create_data(db: &Surreal<Client>) {
        info!("Create data");
        let data = [
            "DEFINE ANALYZER name TOKENIZERS class FILTERS lowercase,ngram(1,128)",
            "DEFINE ANALYZER userdefinedid TOKENIZERS blank FILTERS lowercase,ngram(1,32)",
            "DEFINE INDEX account_name_search_idx ON TABLE account COLUMNS name SEARCH ANALYZER name BM25(1.2,0.75) HIGHLIGHTS",
            "DEFINE INDEX account_user_defined_id_search_idx ON TABLE account COLUMNS user_defined_id SEARCH ANALYZER userdefinedid BM25 HIGHLIGHTS",
            "CREATE account SET name='Tobie', user_defined_id='Tobie'"
        ];
        for l in data {
            db.query(l).await.expect(l).check().expect(l);
        }
    }

    async fn check_data(db: &Surreal<Any>) {
        info!("Check data");

        let mut res = db.query("SELECT name FROM account").await.unwrap().check().unwrap();
        assert_eq!(res.num_statements(), 1);
        let n: Vec<String> = res.take("name").unwrap();
        assert_eq!(n, vec!["Tobie"]);

        let res = db.query("INFO FOR DB").await.unwrap().check().unwrap();
        assert_eq!(res.num_statements(), 1);
        println!("{:?}", res);
    }

    async fn wait_for_connection() -> Surreal<Client> {
        let start = SystemTime::now();
        while start.elapsed().unwrap() < Duration::from_secs(180) {
            sleep(Duration::from_secs(2)).await;
            if let Ok(db) = Surreal::new::<Http>("127.0.0.1:8000").await {
                info!("DB connected!");
                db.signin(Root {
                    username: USER,
                    password: PASS,
                })
                    .await
                    .unwrap();
                db.use_ns(NS).use_db(DB).await.unwrap();
                return db;
            }
            warn!("DB not yet responding");
            sleep(Duration::from_secs(2)).await;
        }
        panic!("Cannot connect to DB");
    }

    async fn new_local_instance(file_path: &String) -> Surreal<Any> {
        let db = connect(format!("file:{}", file_path)).await.unwrap();
        db.use_ns(NS).await.unwrap();
        db.use_db(DB).await.unwrap();
        db
    }

    struct DockerContainer {
        id: String,
    }

    impl DockerContainer {
        fn start(version: &str, file_path: &str) -> Self {
            let mut args = Arguments::new(["run", "-p", "8000:8000", "-d"]);
            args.add(["-v"]);
            args.add([format!("{file_path}:{file_path}")]);
            args.add([format!("surrealdb/surrealdb:{version}")]);
            args.add(["start", "--log", "trace"]);
            args.add(["--auth", "--user", USER, "--pass", PASS]);
            args.add([format!("file:{file_path}")]);
            let id = Self::docker(args);
            Self {
                id,
            }
        }

        fn stop(&self) {
            info!("Stopping docker");
            Self::docker(Arguments::new(["stop", &self.id]));
        }

        fn docker(args: Arguments) -> String {
            let mut command = Command::new("docker");

            let output = command.args(args.0).output().unwrap();
            let std_out = String::from_utf8(output.stdout).unwrap().trim().to_string();
            if !std_out.is_empty() {
                info!("{}", std_out);
            }
            if !output.stderr.is_empty() {
                error!("{}", String::from_utf8(output.stderr).unwrap());
            }
            assert_eq!(output.status.code(), Some(0), "Docker command failure: {:?}", command);
            std_out
        }
    }

    impl Drop for DockerContainer {
        fn drop(&mut self) {
            self.stop();
            Self::docker(Arguments::new(["rm", &self.id]));
        }
    }

    struct Arguments(Vec<String>);

    impl Arguments {
        fn new<I, S>(args: I) -> Self
            where
                I: IntoIterator<Item=S>,
                S: Into<String>,
        {
            let mut a = Self(vec![]);
            a.add(args);
            a
        }

        fn add<I, S>(&mut self, args: I)
            where
                I: IntoIterator<Item=S>,
                S: Into<String>,
        {
            for arg in args {
                self.0.push(arg.into());
            }
        }
    }
}
