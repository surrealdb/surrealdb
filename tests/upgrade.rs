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

    #[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
    async fn upgrade_test() {
        let file_path = format!("/tmp/{}.db", Ulid::new());
        {
            let docker = DockerContainer::start("v1.2.1", &file_path);
            let db = wait_for_connection().await;
            create_data(&db).await;
            docker.stop();
        }
        {
            let db = new_local_instance(&file_path).await;
            check_data(&db).await;
        }
    }

    async fn wait_for_connection() -> Surreal<Client> {
        let start = SystemTime::now();
        while start.elapsed().unwrap() < Duration::from_secs(180) {
            sleep(Duration::from_secs(2)).await;
            if let Ok(db) = Surreal::new::<Http>("127.0.0.1:8000").await {
                info!("DB connected!");
                db.signin(Root {
                    username: "root",
                    password: "root",
                })
                    .await
                    .unwrap();
                db.use_ns("test").use_db("test").await.unwrap();
                return db;
            }
            warn!("DB not yet responding");
            sleep(Duration::from_secs(2)).await;
        }
        panic!("Cannot connect to DB");
    }

    async fn new_local_instance(file_path: &String) -> Surreal<Any> {
        let db = connect(format!("file:/{}", file_path)).await.unwrap();
        db.use_ns("test").await.unwrap();
        db.use_db("test").await.unwrap();
        db
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
            let res = db.query(l).await.unwrap();
            println!("{res:?}");
        }
    }

    async fn check_data(db: &Surreal<Any>) {
        info!("Check data");
        let res = db.query("INFO FOR ROOT").await.unwrap().check().unwrap();
        assert_eq!(res.num_statements(), 1);
        println!("{:?}", res);

        let mut res = db.query("SELECT name FROM account").await.unwrap().check().unwrap();
        assert_eq!(res.num_statements(), 1);
        let n: Vec<String> = res.take("name").unwrap();
        assert_eq!(n, vec!["Tobie"]);
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
            args.add(["--auth", "--user", "root", "--pass", "root"]);
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
