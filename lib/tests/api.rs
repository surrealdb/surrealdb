#[allow(unused_imports, dead_code)]
mod api_integration {
	use chrono::DateTime;
	use once_cell::sync::Lazy;
	use semver::Version;
	use serde::Deserialize;
	use serde::Serialize;
	use serde_json::json;
	use serial_test::serial;
	use std::borrow::Cow;
	use std::ops::Bound;
	use std::sync::Arc;
	use std::sync::Mutex;
	use std::time::Duration;
	use surrealdb::error::Api as ApiError;
	use surrealdb::error::Db as DbError;
	use surrealdb::opt::auth::Database;
	use surrealdb::opt::auth::Jwt;
	use surrealdb::opt::auth::Namespace;
	use surrealdb::opt::auth::Record as RecordAccess;
	use surrealdb::opt::auth::Root;
	use surrealdb::opt::capabilities::Capabilities;
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
	use tokio::sync::Semaphore;
	use tokio::sync::SemaphorePermit;
	use tracing_subscriber::filter::EnvFilter;
	use tracing_subscriber::fmt;
	use tracing_subscriber::layer::SubscriberExt;
	use tracing_subscriber::util::SubscriberInitExt;
	use ulid::Ulid;

	const NS: &str = "test-ns";
	const ROOT_USER: &str = "root";
	const ROOT_PASS: &str = "root";
	const TICK_INTERVAL: Duration = Duration::from_secs(1);

	#[derive(Debug, Serialize)]
	struct Record {
		name: String,
	}

	#[derive(Debug, Clone, Deserialize, Eq, PartialEq, Ord, PartialOrd)]
	struct RecordId {
		id: Thing,
	}

	#[derive(Debug, Deserialize)]
	struct RecordName {
		name: String,
	}

	#[derive(Debug, Clone, Deserialize, Eq, PartialEq, Ord, PartialOrd)]
	struct RecordBuf {
		id: Thing,
		name: String,
	}

	#[derive(Debug, Serialize, Deserialize)]
	struct AuthParams<'a> {
		email: &'a str,
		pass: &'a str,
	}

	#[cfg(feature = "protocol-ws")]
	mod ws {
		use super::*;
		use futures::poll;
		use std::pin::pin;
		use std::task::Poll;
		use surrealdb::engine::remote::ws::Client;
		use surrealdb::engine::remote::ws::Ws;

		async fn new_db() -> (SemaphorePermit<'static>, Surreal<Client>) {
			let permit = PERMITS.acquire().await.unwrap();
			let db = Surreal::new::<Ws>("127.0.0.1:8000").await.unwrap();
			db.signin(Root {
				username: ROOT_USER,
				password: ROOT_PASS,
			})
			.await
			.unwrap();
			(permit, db)
		}

		#[test_log::test(tokio::test)]
		async fn any_engine_can_connect() {
			let permit = PERMITS.acquire().await.unwrap();
			surrealdb::engine::any::connect("ws://127.0.0.1:8000").await.unwrap();
			drop(permit);
		}

		#[test_log::test(tokio::test)]
		async fn wait_for() {
			use surrealdb::opt::WaitFor::{Connection, Database};

			let permit = PERMITS.acquire().await.unwrap();

			// Create an unconnected client
			// At this point wait_for should continue to wait for both the connection and database selection.
			let db: Surreal<ws::Client> = Surreal::init();
			assert_eq!(poll!(pin!(db.wait_for(Connection))), Poll::Pending);
			assert_eq!(poll!(pin!(db.wait_for(Database))), Poll::Pending);

			// Connect to the server
			// The connection event should fire and allow wait_for to return immediately when waiting for a connection.
			// When waiting for a database to be selected, it should continue waiting.
			db.connect::<Ws>("127.0.0.1:8000").await.unwrap();
			assert_eq!(poll!(pin!(db.wait_for(Connection))), Poll::Ready(()));
			assert_eq!(poll!(pin!(db.wait_for(Database))), Poll::Pending);

			// Sign into the server
			// At this point the connection has already been established but the database hasn't been selected yet.
			db.signin(Root {
				username: ROOT_USER,
				password: ROOT_PASS,
			})
			.await
			.unwrap();
			assert_eq!(poll!(pin!(db.wait_for(Connection))), Poll::Ready(()));
			assert_eq!(poll!(pin!(db.wait_for(Database))), Poll::Pending);

			// Selecting a namespace shouldn't fire the database selection event.
			db.use_ns("namespace").await.unwrap();
			assert_eq!(poll!(pin!(db.wait_for(Connection))), Poll::Ready(()));
			assert_eq!(poll!(pin!(db.wait_for(Database))), Poll::Pending);

			// Select the database to use
			// Both the connection and database events have fired, wait_for should return immediately for both.
			db.use_db("database").await.unwrap();
			assert_eq!(poll!(pin!(db.wait_for(Connection))), Poll::Ready(()));
			assert_eq!(poll!(pin!(db.wait_for(Database))), Poll::Ready(()));

			drop(permit);
		}

		include!("api/mod.rs");
		include!("api/live.rs");
	}

	#[cfg(feature = "protocol-http")]
	mod http {
		use super::*;
		use surrealdb::engine::remote::http::Client;
		use surrealdb::engine::remote::http::Http;

		async fn new_db() -> (SemaphorePermit<'static>, Surreal<Client>) {
			let permit = PERMITS.acquire().await.unwrap();
			let db = Surreal::new::<Http>("127.0.0.1:8000").await.unwrap();
			db.signin(Root {
				username: ROOT_USER,
				password: ROOT_PASS,
			})
			.await
			.unwrap();
			(permit, db)
		}

		#[test_log::test(tokio::test)]
		async fn any_engine_can_connect() {
			let permit = PERMITS.acquire().await.unwrap();
			surrealdb::engine::any::connect("http://127.0.0.1:8000").await.unwrap();
			drop(permit);
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

		async fn new_db() -> (SemaphorePermit<'static>, Surreal<Db>) {
			let permit = PERMITS.acquire().await.unwrap();
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
			(permit, db)
		}

		#[test_log::test(tokio::test)]
		async fn memory_allowed_as_address() {
			surrealdb::engine::any::connect("memory").await.unwrap();
		}

		#[test_log::test(tokio::test)]
		async fn any_engine_can_connect() {
			surrealdb::engine::any::connect("mem://").await.unwrap();
			surrealdb::engine::any::connect("memory").await.unwrap();
		}

		#[test_log::test(tokio::test)]
		async fn signin_first_not_necessary() {
			let db = Surreal::new::<Mem>(()).await.unwrap();
			db.use_ns("namespace").use_db("database").await.unwrap();
			let Some(record): Option<RecordId> = db.create(("item", "foo")).await.unwrap() else {
				panic!("record not found");
			};
			assert_eq!(record.id.to_string(), "item:foo");
		}

		#[test_log::test(tokio::test)]
		async fn cant_sign_into_default_root_account() {
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

		#[test_log::test(tokio::test)]
		async fn credentials_activate_authentication() {
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

		#[test_log::test(tokio::test)]
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
		include!("api/live.rs");
		include!("api/backup.rs");
	}

	#[cfg(feature = "kv-rocksdb")]
	mod file {
		use super::*;
		use surrealdb::engine::local::Db;
		use surrealdb::engine::local::File;

		async fn new_db() -> (SemaphorePermit<'static>, Surreal<Db>) {
			let permit = PERMITS.acquire().await.unwrap();
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
			(permit, db)
		}

		#[test_log::test(tokio::test)]
		async fn any_engine_can_connect() {
			let path = format!("{}.db", Ulid::new());
			surrealdb::engine::any::connect(format!("file://{path}")).await.unwrap();
			surrealdb::engine::any::connect(format!("file:///tmp/{path}")).await.unwrap();
			tokio::fs::remove_dir_all(path).await.unwrap();
		}

		include!("api/mod.rs");
		include!("api/live.rs");
		include!("api/backup.rs");
	}

	#[cfg(feature = "kv-rocksdb")]
	mod rocksdb {
		use super::*;
		use surrealdb::engine::local::Db;
		use surrealdb::engine::local::RocksDb;

		async fn new_db() -> (SemaphorePermit<'static>, Surreal<Db>) {
			let permit = PERMITS.acquire().await.unwrap();
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
			(permit, db)
		}

		#[test_log::test(tokio::test)]
		async fn any_engine_can_connect() {
			let path = format!("{}.db", Ulid::new());
			surrealdb::engine::any::connect(format!("rocksdb://{path}")).await.unwrap();
			surrealdb::engine::any::connect(format!("rocksdb:///tmp/{path}")).await.unwrap();
			tokio::fs::remove_dir_all(path).await.unwrap();
		}

		include!("api/mod.rs");
		include!("api/live.rs");
		include!("api/backup.rs");
	}

	#[cfg(feature = "kv-tikv")]
	mod tikv {
		use super::*;
		use surrealdb::engine::local::Db;
		use surrealdb::engine::local::TiKv;

		async fn new_db() -> (SemaphorePermit<'static>, Surreal<Db>) {
			let permit = PERMITS.acquire().await.unwrap();
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
			(permit, db)
		}

		#[test_log::test(tokio::test)]
		async fn any_engine_can_connect() {
			let permit = PERMITS.acquire().await.unwrap();
			surrealdb::engine::any::connect("tikv://127.0.0.1:2379").await.unwrap();
			drop(permit);
		}

		include!("api/mod.rs");
		include!("api/live.rs");
		include!("api/backup.rs");
	}

	#[cfg(feature = "kv-fdb")]
	mod fdb {
		use super::*;
		use surrealdb::engine::local::Db;
		use surrealdb::engine::local::FDb;

		async fn new_db() -> (SemaphorePermit<'static>, Surreal<Db>) {
			let permit = PERMITS.acquire().await.unwrap();
			let root = Root {
				username: ROOT_USER,
				password: ROOT_PASS,
			};
			let config = Config::new()
				.user(root)
				.tick_interval(TICK_INTERVAL)
				.capabilities(Capabilities::all());
			let path = "/etc/foundationdb/fdb.cluster";
			surrealdb::engine::any::connect((format!("fdb://{path}"), config.clone()))
				.await
				.unwrap();
			let db = Surreal::new::<FDb>((path, config)).await.unwrap();
			db.signin(root).await.unwrap();
			(permit, db)
		}

		include!("api/mod.rs");
		include!("api/live.rs");
		include!("api/backup.rs");
	}

	#[cfg(feature = "kv-surrealkv")]
	mod surrealkv {
		use super::*;
		use surrealdb::engine::local::Db;
		use surrealdb::engine::local::SurrealKV;

		async fn new_db() -> (SemaphorePermit<'static>, Surreal<Db>) {
			let permit = PERMITS.acquire().await.unwrap();
			let path = format!("/tmp/{}.db", Ulid::new());
			let root = Root {
				username: ROOT_USER,
				password: ROOT_PASS,
			};
			let config = Config::new()
				.user(root)
				.tick_interval(TICK_INTERVAL)
				.capabilities(Capabilities::all());
			let db = Surreal::new::<SurrealKV>((path, config)).await.unwrap();
			db.signin(root).await.unwrap();
			(permit, db)
		}

		#[test_log::test(tokio::test)]
		async fn any_engine_can_connect() {
			let path = format!("{}.db", Ulid::new());
			surrealdb::engine::any::connect(format!("surrealkv://{path}")).await.unwrap();
			surrealdb::engine::any::connect(format!("surrealkv:///tmp/{path}")).await.unwrap();
			tokio::fs::remove_dir_all(path).await.unwrap();
		}

		#[test_log::test(tokio::test)]
		async fn select_with_version() {
			let (permit, db) = new_db().await;
			db.use_ns(NS).use_db(Ulid::new().to_string()).await.unwrap();
			drop(permit);

			// Create the initial version and record its timestamp.
			let _ =
				db.query("CREATE user:john SET name = 'John v1'").await.unwrap().check().unwrap();
			let create_ts = chrono::Utc::now();

			// Create a new version by updating the record.
			let _ =
				db.query("UPDATE user:john SET name = 'John v2'").await.unwrap().check().unwrap();

			// Without VERSION, SELECT should return the latest update.
			let mut response = db.query("SELECT * FROM user").await.unwrap().check().unwrap();
			let Some(name): Option<String> = response.take("name").unwrap() else {
				panic!("query returned no record");
			};
			assert_eq!(name, "John v2");

			// SELECT with VERSION of `create_ts` should return the initial record.
			let version = create_ts.to_rfc3339();
			let mut response = db
				.query(format!("SELECT * FROM user VERSION d'{}'", version))
				.await
				.unwrap()
				.check()
				.unwrap();
			let Some(name): Option<String> = response.take("name").unwrap() else {
				panic!("query returned no record");
			};
			assert_eq!(name, "John v1");

			let mut response = db
				.query(format!("SELECT name FROM user VERSION d'{}'", version))
				.await
				.unwrap()
				.check()
				.unwrap();
			let Some(name): Option<String> = response.take("name").unwrap() else {
				panic!("query returned no record");
			};
			assert_eq!(name, "John v1");

			let mut response = db
				.query(format!("SELECT name FROM user:john VERSION d'{}'", version))
				.await
				.unwrap()
				.check()
				.unwrap();
			let Some(name): Option<String> = response.take("name").unwrap() else {
				panic!("query returned no record");
			};
			assert_eq!(name, "John v1");
		}

		include!("api/mod.rs");
		include!("api/live.rs");
		include!("api/backup.rs");
	}

	#[cfg(feature = "protocol-http")]
	mod any {
		use super::*;
		use surrealdb::engine::any::Any;

		async fn new_db() -> (SemaphorePermit<'static>, Surreal<Any>) {
			let permit = PERMITS.acquire().await.unwrap();
			let db = surrealdb::engine::any::connect("http://127.0.0.1:8000").await.unwrap();
			db.signin(Root {
				username: ROOT_USER,
				password: ROOT_PASS,
			})
			.await
			.unwrap();
			(permit, db)
		}

		include!("api/mod.rs");
		include!("api/backup.rs");
	}
}
