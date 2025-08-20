use std::future::Future;

use serde::{Deserialize, Serialize};
use surrealdb::{Connection, RecordId, Surreal};
use tokio::sync::SemaphorePermit;

/// Tests for this module are defined using this macro.
///
/// Every module implementing tests uses this macro at the end of the file.
/// This macro creates an `include_test` macro defined in that file which will
/// generate a set of short functions to call all the test functions defined. in
/// the file.
///
/// This macro is then called by the include test macro in this file for all the
/// different versions of the tests.
macro_rules! define_include_tests {
	($crate_name:ident => { $( $( #[$m:meta] )* $test_name:ident),* $(,)? }) => {
		macro_rules! include_tests {
			($name:ident) => {
				$(
					$(#[$m])*
					async fn $test_name(){
						super::$crate_name::$test_name($name).await
					}
				)*

			};
		}
		pub(crate) use include_tests;
	};
}

macro_rules! include_tests {
	($create_db:ident => $($name:ident),*) => {
		$(
			super::$name::include_tests!($create_db);
		)*
	};
}

mod backup;
mod backup_version;
mod basic;
mod live;
mod serialisation;
mod version;

const NS: &str = "test-ns";
const ROOT_USER: &str = "root";
const ROOT_PASS: &str = "root";

#[cfg(any(feature = "kv-rocksdb", feature = "kv-surrealkv",))]
static TEMP_DIR: std::sync::LazyLock<std::path::PathBuf> =
	std::sync::LazyLock::new(|| temp_dir::TempDir::new().unwrap().child("sdb-test"));

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

/// Trait for creating a database.
///
/// Implemented for functions which return a future of a database.
///
/// Used to be able to define tests on multiple types of databases only once.
trait CreateDb {
	type Con: Connection;

	async fn create_db(&self) -> (SemaphorePermit<'static>, Surreal<Self::Con>);
}

impl<F, Fut, C> CreateDb for F
where
	F: Fn() -> Fut,
	Fut: Future<Output = (SemaphorePermit<'static>, Surreal<C>)>,
	C: Connection,
{
	type Con = C;

	async fn create_db(&self) -> (SemaphorePermit<'static>, Surreal<Self::Con>) {
		(self)().await
	}
}

#[cfg(feature = "protocol-ws")]
mod ws {
	use std::pin::pin;
	use std::task::Poll;

	use futures::poll;
	use surrealdb::Surreal;
	use surrealdb::engine::remote::ws::{Client, Ws};
	use surrealdb::opt::auth::Root;
	use tokio::sync::{Semaphore, SemaphorePermit};

	use super::{ROOT_PASS, ROOT_USER};
	use crate::api_integration::ws;

	static PERMITS: Semaphore = Semaphore::const_new(1);

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
		// At this point wait_for should continue to wait for both the connection and
		// database selection.
		let db: Surreal<ws::Client> = Surreal::init();
		assert_eq!(poll!(pin!(db.wait_for(Connection))), Poll::Pending);
		assert_eq!(poll!(pin!(db.wait_for(Database))), Poll::Pending);

		// Connect to the server
		// The connection event should fire and allow wait_for to return immediately
		// when waiting for a connection. When waiting for a database to be selected,
		// it should continue waiting.
		db.connect::<Ws>("127.0.0.1:8000").await.unwrap();
		assert_eq!(poll!(pin!(db.wait_for(Connection))), Poll::Ready(()));
		assert_eq!(poll!(pin!(db.wait_for(Database))), Poll::Pending);

		// Sign into the server
		// At this point the connection has already been established but the database
		// hasn't been selected yet.
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
		// Both the connection and database events have fired, wait_for should return
		// immediately for both.
		db.use_db("database").await.unwrap();
		assert_eq!(poll!(pin!(db.wait_for(Connection))), Poll::Ready(()));
		assert_eq!(poll!(pin!(db.wait_for(Database))), Poll::Ready(()));

		drop(permit);
	}

	include_tests!(new_db => basic, serialisation, live);
}

#[cfg(feature = "protocol-http")]
mod http {

	use surrealdb::Surreal;
	use surrealdb::engine::remote::http::{Client, Http};
	use surrealdb::opt::auth::Root;
	use tokio::sync::{Semaphore, SemaphorePermit};

	use super::{ROOT_PASS, ROOT_USER};

	static PERMITS: Semaphore = Semaphore::const_new(1);

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

	include_tests!(new_db => basic, serialisation, backup);
}

#[cfg(feature = "kv-mem")]
mod mem {
	use surrealdb::engine::local::{Db, Mem};
	use surrealdb::error::Db as DbError;
	use surrealdb::opt::auth::Root;
	use surrealdb::opt::capabilities::{Capabilities, ExperimentalFeature};
	use surrealdb::opt::{Config, Resource};
	use surrealdb::{RecordIdKey, Surreal};
	use surrealdb_core::iam;
	use tokio::sync::{Semaphore, SemaphorePermit};

	use super::{ROOT_PASS, ROOT_USER};
	use crate::api_integration::ApiRecordId;

	static PERMITS: Semaphore = Semaphore::const_new(1);

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
		let Some(record): Option<ApiRecordId> = db.create(("item", "foo")).await.unwrap() else {
			panic!("record not found");
		};
		assert_eq!(*record.id.key(), RecordIdKey::from("foo".to_owned()));
	}

	#[test_log::test(tokio::test)]
	async fn cant_sign_into_default_root_account() {
		let db = Surreal::new::<Mem>(()).await.unwrap();
		let Some(DbError::InvalidAuth) = db
			.signin(Root {
				username: ROOT_USER,
				password: ROOT_PASS,
			})
			.await
			.unwrap_err()
			.downcast_ref()
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
		let Some(DbError::IamError(iam::Error::NotAllowed {
			actor: _,
			action: _,
			resource: _,
		})) = res.unwrap_err().downcast_ref()
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

	#[test_log::test(tokio::test)]
	async fn experimental_features() {
		let surql = "
		    USE NAMESPACE namespace DATABASE database;
			DEFINE FIELD using ON house TYPE record<utility> REFERENCE ON DELETE CASCADE;
		";
		// Experimental features are rejected by default
		let db = Surreal::new::<Mem>(()).await.unwrap();
		db.query(surql).await.unwrap_err();
		// Experimental features can be allowed
		let capabilities = Capabilities::new()
			.with_experimental_feature_allowed(ExperimentalFeature::RecordReferences);
		let config = Config::new().capabilities(capabilities);
		let db = Surreal::new::<Mem>(config).await.unwrap();
		db.query(surql).await.unwrap().check().unwrap();
	}

	include_tests!(new_db => basic, serialisation, live, backup);
}

#[cfg(feature = "kv-rocksdb")]
mod rocksdb {
	use surrealdb::Surreal;
	use surrealdb::engine::local::{Db, RocksDb};
	use surrealdb::opt::Config;
	use surrealdb::opt::auth::Root;
	use surrealdb::opt::capabilities::Capabilities;
	use tokio::sync::{Semaphore, SemaphorePermit};
	use ulid::Ulid;

	use super::{ROOT_PASS, ROOT_USER, TEMP_DIR};

	static PERMITS: Semaphore = Semaphore::const_new(1);

	async fn new_db() -> (SemaphorePermit<'static>, Surreal<Db>) {
		let permit = PERMITS.acquire().await.unwrap();
		let path = TEMP_DIR.join(Ulid::new().to_string());
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
		let db_dir = Ulid::new().to_string();
		// Create a database directory using an absolute path
		surrealdb::engine::any::connect(format!(
			"rocksdb://{}",
			TEMP_DIR.join("absolute").join(&db_dir).display()
		))
		.await
		.unwrap();
		// Switch to the temporary directory, if possible, to test relative paths
		if std::env::set_current_dir(&*TEMP_DIR).is_ok() {
			// Create a database directory using a relative path
			surrealdb::engine::any::connect(format!("rocksdb://relative/{db_dir}")).await.unwrap();
		}
	}

	include_tests!(new_db => basic, serialisation, live, backup);
}

#[cfg(feature = "kv-tikv")]
mod tikv {
	use surrealdb::Surreal;
	use surrealdb::engine::local::{Db, TiKv};
	use surrealdb::opt::Config;
	use surrealdb::opt::auth::Root;
	use surrealdb::opt::capabilities::Capabilities;
	use tokio::sync::{Semaphore, SemaphorePermit};

	use super::{ROOT_PASS, ROOT_USER};

	static PERMITS: Semaphore = Semaphore::const_new(1);

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

	include_tests!(new_db => basic, serialisation, live, backup);
}

#[cfg(any(feature = "kv-fdb-7_1", feature = "kv-fdb-7_3"))]
mod fdb {
	use surrealdb::Surreal;
	use surrealdb::engine::local::{Db, FDb};
	use surrealdb::opt::Config;
	use surrealdb::opt::auth::Root;
	use surrealdb::opt::capabilities::Capabilities;
	use tokio::sync::{Semaphore, SemaphorePermit};

	use super::{ROOT_PASS, ROOT_USER};

	static PERMITS: Semaphore = Semaphore::const_new(1);

	async fn new_db() -> (SemaphorePermit<'static>, Surreal<Db>) {
		let permit = PERMITS.acquire().await.unwrap();
		let root = Root {
			username: ROOT_USER,
			password: ROOT_PASS,
		};
		let config = Config::new().user(root).capabilities(Capabilities::all());
		let path = "/etc/foundationdb/fdb.cluster";
		surrealdb::engine::any::connect((format!("fdb://{path}"), config.clone())).await.unwrap();
		let db = Surreal::new::<FDb>((path, config)).await.unwrap();
		db.signin(root).await.unwrap();
		(permit, db)
	}

	include_tests!(new_db => basic, serialisation, live, backup);
}

#[cfg(feature = "kv-surrealkv")]
mod surrealkv {
	use surrealdb::Surreal;
	use surrealdb::engine::local::{Db, SurrealKv};
	use surrealdb::opt::Config;
	use surrealdb::opt::auth::Root;
	use surrealdb::opt::capabilities::Capabilities;
	use tokio::sync::{Semaphore, SemaphorePermit};
	use ulid::Ulid;

	use super::{ROOT_PASS, ROOT_USER, TEMP_DIR};

	static PERMITS: Semaphore = Semaphore::const_new(1);

	async fn new_db() -> (SemaphorePermit<'static>, Surreal<Db>) {
		let permit = PERMITS.acquire().await.unwrap();
		let path = TEMP_DIR.join(Ulid::new().to_string());
		let root = Root {
			username: ROOT_USER,
			password: ROOT_PASS,
		};
		let config = Config::new().user(root).capabilities(Capabilities::all());
		let db = Surreal::new::<SurrealKv>((path, config)).await.unwrap();
		db.signin(root).await.unwrap();
		(permit, db)
	}

	#[test_log::test(tokio::test)]
	async fn any_engine_can_connect() {
		let db_dir = Ulid::new().to_string();
		// Create a database directory using an absolute path
		surrealdb::engine::any::connect(format!(
			"surrealkv://{}",
			TEMP_DIR.join("absolute").join(&db_dir).display()
		))
		.await
		.unwrap();
		// Switch to the temporary directory, if possible, to test relative paths
		if std::env::set_current_dir(&*TEMP_DIR).is_ok() {
			// Create a database directory using a relative path
			surrealdb::engine::any::connect(format!("surrealkv://relative/{db_dir}"))
				.await
				.unwrap();
		}
	}

	include_tests!(new_db => basic, serialisation, live, backup);
}

#[cfg(feature = "kv-surrealkv")]
mod surrealkv_versioned {
	use surrealdb::Surreal;
	use surrealdb::engine::local::{Db, SurrealKv};
	use surrealdb::opt::Config;
	use surrealdb::opt::auth::Root;
	use surrealdb::opt::capabilities::Capabilities;
	use tokio::sync::{Semaphore, SemaphorePermit};
	use ulid::Ulid;

	use super::{ROOT_PASS, ROOT_USER, TEMP_DIR};

	static PERMITS: Semaphore = Semaphore::const_new(1);

	async fn new_db() -> (SemaphorePermit<'static>, Surreal<Db>) {
		let permit = PERMITS.acquire().await.unwrap();
		let path = TEMP_DIR.join(Ulid::new().to_string());
		let root = Root {
			username: ROOT_USER,
			password: ROOT_PASS,
		};
		let config = Config::new().user(root).capabilities(Capabilities::all());
		let db = Surreal::new::<SurrealKv>((path, config)).versioned().await.unwrap();
		db.signin(root).await.unwrap();
		(permit, db)
	}

	#[test_log::test(tokio::test)]
	async fn any_engine_can_connect() {
		let db_dir = Ulid::new().to_string();
		// Create a database directory using an absolute path
		surrealdb::engine::any::connect(format!(
			"surrealkv+versioned://{}",
			TEMP_DIR.join("absolute").join(&db_dir).display()
		))
		.await
		.unwrap();
		// Switch to the temporary directory, if possible, to test relative paths
		if std::env::set_current_dir(&*TEMP_DIR).is_ok() {
			// Create a database directory using a relative path
			surrealdb::engine::any::connect(format!("surrealkv+versioned://relative/{db_dir}"))
				.await
				.unwrap();
		}
	}

	include_tests!(new_db => basic, serialisation, version, live, backup, backup_version);
}

#[cfg(feature = "protocol-http")]
mod any {
	use surrealdb::Surreal;
	use surrealdb::engine::any::Any;
	use surrealdb::opt::auth::Root;
	use tokio::sync::{Semaphore, SemaphorePermit};

	use super::{ROOT_PASS, ROOT_USER};

	static PERMITS: Semaphore = Semaphore::const_new(1);

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

	include_tests!(new_db => basic, serialisation, backup);
}
