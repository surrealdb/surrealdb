#[allow(unused_imports, dead_code)]
mod api_integration {
	use chrono::DateTime;
	use semver::Version;
	use serde::Deserialize;
	use serde::Serialize;
	use serde_json::json;
	use serial_test::serial;
	use std::borrow::Cow;
	use std::ops::Bound;
	use std::sync::Arc;
	use std::sync::LazyLock;
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
	use surrealdb::{Error, RecordId, Surreal, Value};
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

	#[derive(Debug, Serialize)]
	struct Record {
		name: String,
	}

	#[derive(Debug, Clone, Deserialize, PartialEq, PartialOrd)]
	struct ApiRecordId {
		id: RecordId,
	}

	#[derive(Debug, Deserialize)]
	struct RecordName {
		name: String,
	}

	#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, PartialOrd)]
	struct RecordBuf {
		id: RecordId,
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
		use surrealdb::RecordIdKey;

		async fn new_db() -> (SemaphorePermit<'static>, Surreal<Db>) {
			let permit = PERMITS.acquire().await.unwrap();
			let root = Root {
				username: ROOT_USER,
				password: ROOT_PASS,
			};
			let config = Config::new().user(root).capabilities(Capabilities::all());
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
			let Some(record): Option<ApiRecordId> = db.create(("item", "foo")).await.unwrap()
			else {
				panic!("record not found");
			};
			assert_eq!(*record.id.key(), RecordIdKey::from("foo".to_owned()));
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
		#[allow(deprecated)]
		use surrealdb::engine::local::File;

		async fn new_db() -> (SemaphorePermit<'static>, Surreal<Db>) {
			let permit = PERMITS.acquire().await.unwrap();
			let path = format!("/tmp/{}.db", Ulid::new());
			let root = Root {
				username: ROOT_USER,
				password: ROOT_PASS,
			};
			let config = Config::new().user(root).capabilities(Capabilities::all());
			#[allow(deprecated)]
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
			let config = Config::new().user(root).capabilities(Capabilities::all());
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
			let config = Config::new().user(root).capabilities(Capabilities::all());
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
			let config = Config::new().user(root).capabilities(Capabilities::all());
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
			let config = Config::new().user(root).capabilities(Capabilities::all());
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

		#[test_log::test(tokio::test)]
		async fn create_with_version() {
			let (permit, db) = new_db().await;
			db.use_ns(NS).use_db(Ulid::new().to_string()).await.unwrap();
			drop(permit);

			// Create a record in the past.
			let _ = db
				.query("CREATE user:john SET name = 'John' VERSION d'2024-08-19T08:00:00Z'")
				.await
				.unwrap()
				.check()
				.unwrap();

			// Without VERSION, SELECT should return the record.
			let mut response = db.query("SELECT * FROM user:john").await.unwrap().check().unwrap();
			let Some(name): Option<String> = response.take("name").unwrap() else {
				panic!("query returned no record");
			};
			assert_eq!(name, "John");

			// SELECT with the VERSION set to the creation timestamp or later should return the record.
			let mut response = db
				.query("SELECT * FROM user:john VERSION d'2024-08-19T08:00:00Z'")
				.await
				.unwrap()
				.check()
				.unwrap();
			let Some(name): Option<String> = response.take("name").unwrap() else {
				panic!("query returned no record");
			};
			assert_eq!(name, "John");

			// SELECT with the VERSION set before the creation timestamp should return nothing.
			let mut response = db
				.query("SELECT * FROM user:john VERSION d'2024-08-19T07:00:00Z'")
				.await
				.unwrap()
				.check()
				.unwrap();
			let response: Option<String> = response.take("name").unwrap();
			assert!(response.is_none());
		}

		#[test_log::test(tokio::test)]
		async fn insert_with_version() {
			let (permit, db) = new_db().await;
			db.use_ns(NS).use_db(Ulid::new().to_string()).await.unwrap();
			drop(permit);

			// Create a record in the past.
			let _ = db
				.query("INSERT INTO user { id: user:john, name: 'John' } VERSION d'2024-08-19T08:00:00Z'")
				.await
				.unwrap()
				.check()
				.unwrap();

			// Without VERSION, SELECT should return the record.
			let mut response = db.query("SELECT * FROM user:john").await.unwrap().check().unwrap();
			let Some(name): Option<String> = response.take("name").unwrap() else {
				panic!("query returned no record");
			};
			assert_eq!(name, "John");

			// SELECT with the VERSION set to the creation timestamp or later should return the record.
			let mut response = db
				.query("SELECT * FROM user:john VERSION d'2024-08-19T08:00:00Z'")
				.await
				.unwrap()
				.check()
				.unwrap();
			let Some(name): Option<String> = response.take("name").unwrap() else {
				panic!("query returned no record");
			};
			assert_eq!(name, "John");

			// SELECT with the VERSION set before the creation timestamp should return nothing.
			let mut response = db
				.query("SELECT * FROM user:john VERSION d'2024-08-19T07:00:00Z'")
				.await
				.unwrap()
				.check()
				.unwrap();
			let response: Option<String> = response.take("name").unwrap();
			assert!(response.is_none());
		}

		#[test_log::test(tokio::test)]
		async fn info_for_db_with_versioned_tables() {
			let (permit, db) = new_db().await;
			db.use_ns(NS).use_db(Ulid::new().to_string()).await.unwrap();
			drop(permit);

			// Record the timestamp before creating a testing table.
			let ts_before_create = chrono::Utc::now().to_rfc3339();

			// Create the testing table.
			let _ = db.query("DEFINE TABLE person").await.unwrap().check().unwrap();

			// Record the timestamp after creating the testing table.
			let ts_after_create = chrono::Utc::now().to_rfc3339();

			// Check that historical query shows no table before it was created.
			let q = format!("INFO FOR DB VERSION d'{}'", ts_before_create);
			let mut response = db.query(q).await.unwrap().check().unwrap();
			let info = response.take::<Value>(0).unwrap().to_string();
			assert!(info.contains("tables: {  }"));

			// Now check that the table shows up later.
			let q = format!("INFO FOR DB VERSION d'{}'", ts_after_create);
			let mut response = db.query(q).await.unwrap().check().unwrap();
			let info = response.take::<Value>(0).unwrap().to_string();
			assert!(info.contains(
				"tables: { person: 'DEFINE TABLE person TYPE ANY SCHEMALESS PERMISSIONS NONE' }"
			));
		}

		#[test_log::test(tokio::test)]
		async fn info_for_table_with_versioned_fields() {
			let (permit, db) = new_db().await;
			db.use_ns(NS).use_db(Ulid::new().to_string()).await.unwrap();
			drop(permit);

			// Create the testing table.
			let _ = db.query("DEFINE TABLE person").await.unwrap().check().unwrap();

			// Record the timestamp before creating a field.
			let ts_before_field = chrono::Utc::now().to_rfc3339();
			let _ = db
				.query("DEFINE FIELD firstName ON TABLE person TYPE string")
				.await
				.unwrap()
				.check()
				.unwrap();

			// Record the timestamp after creating the field.
			let ts_after_field = chrono::Utc::now().to_rfc3339();

			// Check that historical query shows no field before it was created.
			let q = format!("INFO FOR TABLE person VERSION d'{}'", ts_before_field);
			let mut response = db.query(q).await.unwrap().check().unwrap();
			let info = response.take::<Value>(0).unwrap().to_string();
			assert!(info.contains("fields: {  }"));

			// Now check that the field shows up later.
			let q = format!("INFO FOR TABLE person VERSION d'{}'", ts_after_field);
			let mut response = db.query(q).await.unwrap().check().unwrap();
			let info = response.take::<Value>(0).unwrap().to_string();
			assert!(info.contains(
				"fields: { firstName: 'DEFINE FIELD firstName ON person TYPE string PERMISSIONS FULL' }"
			));
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
