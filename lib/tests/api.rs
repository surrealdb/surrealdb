#[allow(unused_imports, dead_code)]
mod api_integration {
	use chrono::DateTime;
	use once_cell::sync::Lazy;
	use serde::Deserialize;
	use serde::Serialize;
	use serde_json::json;
	use std::borrow::Cow;
	use std::ops::Bound;
	use std::sync::Arc;
	use std::sync::Mutex;
	use std::time::Duration;
	use surrealdb::dbs::capabilities::Capabilities;
	use surrealdb::error::Api as ApiError;
	use surrealdb::error::Db as DbError;
	use surrealdb::opt::auth::Database;
	use surrealdb::opt::auth::Jwt;
	use surrealdb::opt::auth::Namespace;
	use surrealdb::opt::auth::Root;
	use surrealdb::opt::auth::Scope;
	use surrealdb::opt::Config;
	use surrealdb::opt::PatchOp;
	use surrealdb::opt::Resource;
	use surrealdb::sql::statements::BeginStatement;
	use surrealdb::sql::statements::CommitStatement;
	use surrealdb::sql::thing;
	use surrealdb::sql::Thing;
	use surrealdb::sql::Value;
	use surrealdb::Error;
	use surrealdb::Surreal;
	use tracing_subscriber::filter::EnvFilter;
	use tracing_subscriber::fmt;
	use tracing_subscriber::layer::SubscriberExt;
	use tracing_subscriber::util::SubscriberInitExt;
	use ulid::Ulid;

	const NS: &str = "test-ns";
	const ROOT_USER: &str = "root";
	const ROOT_PASS: &str = "root";
	const TICK_INTERVAL: Duration = Duration::from_secs(1);
	// Used to ensure that only one test at a time is setting up the underlaying datastore.
	// When auth is enabled, multiple tests may try to create the same root user at the same time.
	static SETUP_MUTEX: Lazy<Arc<Mutex<()>>> = Lazy::new(|| Arc::new(Mutex::new(())));

	#[derive(Debug, Serialize)]
	struct Record<'a> {
		name: &'a str,
	}

	#[derive(Debug, Deserialize)]
	struct RecordId {
		id: Thing,
	}

	#[derive(Debug, Deserialize)]
	struct RecordName {
		name: String,
	}

	#[derive(Debug, Deserialize, Eq, PartialEq, Ord, PartialOrd)]
	struct RecordBuf {
		id: Thing,
		name: String,
	}

	#[derive(Debug, Serialize, Deserialize)]
	struct AuthParams<'a> {
		email: &'a str,
		pass: &'a str,
	}

	fn init_logger() {
		let test_writer = fmt::layer().with_test_writer();
		let builder = fmt::Subscriber::builder().with_env_filter(EnvFilter::from_default_env());
		let subscriber = builder.finish();
		let _ = subscriber.with(test_writer).try_init();
	}

	#[cfg(feature = "protocol-ws")]
	mod ws {
		use super::*;
		use surrealdb::engine::remote::ws::Client;
		use surrealdb::engine::remote::ws::Ws;

		async fn new_db() -> Surreal<Client> {
			let _guard = SETUP_MUTEX.lock().unwrap();
			init_logger();
			let db = Surreal::new::<Ws>("127.0.0.1:8000").await.unwrap();
			db.signin(Root {
				username: ROOT_USER,
				password: ROOT_PASS,
			})
			.await
			.unwrap();
			db
		}

		include!("api/mod.rs");
	}

	#[cfg(feature = "protocol-http")]
	mod http {
		use super::*;
		use surrealdb::engine::remote::http::Client;
		use surrealdb::engine::remote::http::Http;

		async fn new_db() -> Surreal<Client> {
			let _guard = SETUP_MUTEX.lock().unwrap();
			init_logger();
			let db = Surreal::new::<Http>("127.0.0.1:8000").await.unwrap();
			db.signin(Root {
				username: ROOT_USER,
				password: ROOT_PASS,
			})
			.await
			.unwrap();
			db
		}

		include!("api/mod.rs");
		include!("api/backup.rs");
	}

	#[cfg(feature = "kv-mem")]
	mod mem {
		use super::*;
		use surrealdb::engine::any;
		use surrealdb::engine::local::Db;
		use surrealdb::engine::local::Mem;
		use surrealdb::iam;

		async fn new_db() -> Surreal<Db> {
			init_logger();
			let root = Root {
				username: ROOT_USER,
				password: ROOT_PASS,
			};
			let config = Config::new()
				.user(root)
				.tick_interval(TICK_INTERVAL)
				.capabilities(Capabilities::all());
			let db = Surreal::new::<Mem>(config).await.unwrap();
			db.signin(root).await.unwrap();
			db
		}

		#[tokio::test]
		async fn memory_allowed_as_address() {
			init_logger();
			any::connect("memory").await.unwrap();
		}

		#[tokio::test]
		async fn signin_first_not_necessary() {
			init_logger();
			let db = Surreal::new::<Mem>(()).await.unwrap();
			db.use_ns("namespace").use_db("database").await.unwrap();
			let Some(record): Option<RecordId> = db.create(("item", "foo")).await.unwrap() else {
				panic!("record not found");
			};
			assert_eq!(record.id.to_string(), "item:foo");
		}

		#[tokio::test]
		async fn cant_sign_into_default_root_account() {
			init_logger();
			let db = Surreal::new::<Mem>(()).await.unwrap();
			let Error::Db(DbError::InvalidAuth) = db
				.signin(Root {
					username: ROOT_USER,
					password: ROOT_PASS,
				})
				.await
				.unwrap_err()
			else {
				panic!("unexpected successful login");
			};
		}

		#[tokio::test]
		async fn credentials_activate_authentication() {
			init_logger();
			let config = Config::new().user(Root {
				username: ROOT_USER,
				password: ROOT_PASS,
			});
			let db = Surreal::new::<Mem>(config).await.unwrap();
			db.use_ns("namespace").use_db("database").await.unwrap();
			let res = db.create(Resource::from("item:foo")).await;
			let Error::Db(DbError::IamError(iam::Error::NotAllowed {
				actor: _,
				action: _,
				resource: _,
			})) = res.unwrap_err()
			else {
				panic!("expected permissions error");
			};
		}

		#[tokio::test]
		async fn surreal_clone() {
			use surrealdb::engine::any::Any;

			let db: Surreal<Db> = Surreal::init();
			db.clone().connect::<Mem>(()).await.unwrap();
			db.use_ns("test").use_db("test").await.unwrap();

			let db: Surreal<Any> = Surreal::init();
			db.clone().connect("memory").await.unwrap();
			db.use_ns("test").use_db("test").await.unwrap();
		}

		include!("api/mod.rs");
		include!("api/backup.rs");
	}

	#[cfg(feature = "kv-rocksdb")]
	mod file {
		use super::*;
		use surrealdb::engine::local::Db;
		use surrealdb::engine::local::File;

		async fn new_db() -> Surreal<Db> {
			let _guard = SETUP_MUTEX.lock().unwrap();
			init_logger();
			let path = format!("/tmp/{}.db", Ulid::new());
			let root = Root {
				username: ROOT_USER,
				password: ROOT_PASS,
			};
			let config = Config::new()
				.user(root)
				.tick_interval(TICK_INTERVAL)
				.capabilities(Capabilities::all());
			let db = Surreal::new::<File>((path, config)).await.unwrap();
			db.signin(root).await.unwrap();
			db
		}

		include!("api/mod.rs");
		include!("api/backup.rs");
	}

	#[cfg(feature = "kv-rocksdb")]
	mod rocksdb {
		use super::*;
		use surrealdb::engine::local::Db;
		use surrealdb::engine::local::RocksDb;

		async fn new_db() -> Surreal<Db> {
			let _guard = SETUP_MUTEX.lock().unwrap();
			init_logger();
			let path = format!("/tmp/{}.db", Ulid::new());
			let root = Root {
				username: ROOT_USER,
				password: ROOT_PASS,
			};
			let config = Config::new()
				.user(root)
				.tick_interval(TICK_INTERVAL)
				.capabilities(Capabilities::all());
			let db = Surreal::new::<RocksDb>((path, config)).await.unwrap();
			db.signin(root).await.unwrap();
			db
		}

		include!("api/mod.rs");
		include!("api/backup.rs");
	}

	#[cfg(feature = "kv-speedb")]
	mod speedb {
		use super::*;
		use surrealdb::engine::local::Db;
		use surrealdb::engine::local::SpeeDb;

		async fn new_db() -> Surreal<Db> {
			let _guard = SETUP_MUTEX.lock().unwrap();
			init_logger();
			let path = format!("/tmp/{}.db", Ulid::new());
			let root = Root {
				username: ROOT_USER,
				password: ROOT_PASS,
			};
			let config = Config::new()
				.user(root)
				.tick_interval(TICK_INTERVAL)
				.capabilities(Capabilities::all());
			let db = Surreal::new::<SpeeDb>((path, config)).await.unwrap();
			db.signin(root).await.unwrap();
			db
		}

		include!("api/mod.rs");
		include!("api/backup.rs");
	}

	#[cfg(feature = "kv-tikv")]
	mod tikv {
		use super::*;
		use surrealdb::engine::local::Db;
		use surrealdb::engine::local::TiKv;

		async fn new_db() -> Surreal<Db> {
			let _guard = SETUP_MUTEX.lock().unwrap();
			init_logger();
			let root = Root {
				username: ROOT_USER,
				password: ROOT_PASS,
			};
			let config = Config::new()
				.user(root)
				.tick_interval(TICK_INTERVAL)
				.capabilities(Capabilities::all());
			let db = Surreal::new::<TiKv>(("127.0.0.1:2379", config)).await.unwrap();
			db.signin(root).await.unwrap();
			db
		}

		include!("api/mod.rs");
		include!("api/backup.rs");
	}

	#[cfg(feature = "kv-fdb")]
	mod fdb {
		use super::*;
		use surrealdb::engine::local::Db;
		use surrealdb::engine::local::FDb;

		async fn new_db() -> Surreal<Db> {
			let _guard = SETUP_MUTEX.lock().unwrap();
			init_logger();
			let root = Root {
				username: ROOT_USER,
				password: ROOT_PASS,
			};
			let config = Config::new()
				.user(root)
				.tick_interval(TICK_INTERVAL)
				.capabilities(Capabilities::all());
			let db = Surreal::new::<FDb>(("/etc/foundationdb/fdb.cluster", config)).await.unwrap();
			db.signin(root).await.unwrap();
			db
		}

		include!("api/mod.rs");
		include!("api/backup.rs");
	}

	#[cfg(feature = "protocol-http")]
	mod any {
		use super::*;
		use surrealdb::engine::any::Any;

		async fn new_db() -> Surreal<Any> {
			let _guard = SETUP_MUTEX.lock().unwrap();
			init_logger();
			let db = surrealdb::engine::any::connect("http://127.0.0.1:8000").await.unwrap();
			db.signin(Root {
				username: ROOT_USER,
				password: ROOT_PASS,
			})
			.await
			.unwrap();
			db
		}

		include!("api/mod.rs");
		include!("api/backup.rs");
	}
}
