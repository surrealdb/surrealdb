#![allow(clippy::unwrap_used)]

use std::future::Future;

use surrealdb::opt::Config;
use surrealdb::types::{RecordId, SurrealValue};
use surrealdb::{Connection, Surreal};
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
mod run;
mod serialisation;
mod session_isolation;
mod version;

const ROOT_USER: &str = "root";
const ROOT_PASS: &str = "root";

#[cfg(any(feature = "kv-rocksdb", feature = "kv-surrealkv",))]
static TEMP_DIR: std::sync::LazyLock<std::path::PathBuf> =
	std::sync::LazyLock::new(|| temp_dir::TempDir::new().unwrap().child("sdb-test"));

#[derive(Debug, SurrealValue)]
struct Record {
	name: String,
}

#[derive(Debug, Clone, PartialEq, PartialOrd, SurrealValue)]
struct ApiRecordId {
	id: RecordId,
}

#[derive(Debug, SurrealValue)]
struct RecordName {
	name: String,
}

#[derive(Debug, Clone, PartialEq, PartialOrd, SurrealValue)]
struct RecordBuf {
	id: RecordId,
	name: String,
}

#[derive(Debug, SurrealValue)]
struct AuthParams {
	email: String,
	pass: String,
}

/// Trait for creating a database.
///
/// Implemented for functions which return a future of a database.
///
/// Used to be able to define tests on multiple types of databases only once.
trait CreateDb {
	type Con: Connection;

	async fn create_db(&self, config: Config) -> (SemaphorePermit<'static>, Surreal<Self::Con>);
}

impl<F, Fut, C> CreateDb for F
where
	F: Fn(Config) -> Fut,
	Fut: Future<Output = (SemaphorePermit<'static>, Surreal<C>)>,
	C: Connection,
{
	type Con = C;

	async fn create_db(&self, config: Config) -> (SemaphorePermit<'static>, Surreal<Self::Con>) {
		(self)(config).await
	}
}

// --------------------------------------------------
// Any engine tests
// --------------------------------------------------

#[cfg(feature = "protocol-http")]
mod any {
	use surrealdb::Surreal;
	use surrealdb::engine::any::Any;
	use surrealdb::opt::Config;
	use surrealdb::opt::auth::Root;
	use tokio::sync::{Semaphore, SemaphorePermit};

	use super::{ROOT_PASS, ROOT_USER};

	static PERMITS: Semaphore = Semaphore::const_new(1);

	async fn new_db(config: Config) -> (SemaphorePermit<'static>, Surreal<Any>) {
		let permit = PERMITS.acquire().await.unwrap();
		let db = surrealdb::engine::any::connect(("http://127.0.0.1:8000", config)).await.unwrap();
		db.signin(Root {
			username: ROOT_USER.to_string(),
			password: ROOT_PASS.to_string(),
		})
		.await
		.unwrap();
		(permit, db)
	}

	include_tests!(new_db => basic, serialisation, backup, session_isolation, run);
}

// --------------------------------------------------
// HTTP engine tests
// --------------------------------------------------

#[cfg(feature = "protocol-http")]
mod http {

	use surrealdb::Surreal;
	use surrealdb::engine::remote::http::{Client, Http};
	use surrealdb::opt::Config;
	use surrealdb::opt::auth::Root;
	use tokio::sync::{Semaphore, SemaphorePermit};

	use super::{ROOT_PASS, ROOT_USER};

	static PERMITS: Semaphore = Semaphore::const_new(1);

	async fn new_db(config: Config) -> (SemaphorePermit<'static>, Surreal<Client>) {
		let permit = PERMITS.acquire().await.unwrap();
		let db = Surreal::new::<Http>(("127.0.0.1:8000", config)).await.unwrap();
		db.signin(Root {
			username: ROOT_USER.to_string(),
			password: ROOT_PASS.to_string(),
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

	include_tests!(new_db => basic, serialisation, backup, session_isolation, run);
}

// --------------------------------------------------
// WebSocket engine tests
// --------------------------------------------------

#[cfg(feature = "protocol-ws")]
mod ws {
	use std::pin::pin;
	use std::task::Poll;

	use futures::poll;
	use surrealdb::Surreal;
	use surrealdb::engine::remote::ws::{Client, Ws};
	use surrealdb::opt::Config;
	use surrealdb::opt::auth::Root;
	use surrealdb_types::SurrealValue;
	use tokio::sync::{Semaphore, SemaphorePermit};

	use super::{ROOT_PASS, ROOT_USER};
	use crate::api_integration::ws;

	static PERMITS: Semaphore = Semaphore::const_new(1);

	async fn new_db(config: Config) -> (SemaphorePermit<'static>, Surreal<Client>) {
		let permit = PERMITS.acquire().await.unwrap();
		let db = Surreal::new::<Ws>(("127.0.0.1:8000", config)).await.unwrap();
		db.signin(Root {
			username: ROOT_USER.to_string(),
			password: ROOT_PASS.to_string(),
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
			username: ROOT_USER.to_string(),
			password: ROOT_PASS.to_string(),
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

	/// Test WebSocket message size limits to ensure proper handling of large messages.
	///
	/// This test verifies that:
	/// 1. Messages within the configured size limits are processed successfully
	/// 2. Messages exceeding the size limits are properly rejected with appropriate error messages
	/// 3. The WebSocket configuration correctly applies both message and frame size limits
	///
	/// The test uses a custom WebSocket configuration with a 256 MiB message size limit
	/// and tests various message sizes including edge cases.
	#[test_log::test(tokio::test)]
	async fn check_max_size() {
		use std::fmt;

		use surrealdb::opt::{Config, WebsocketConfig};
		use ulid::Ulid;

		/// Test content structure for validating large message handling
		#[derive(Clone, SurrealValue, PartialEq)]
		struct Content {
			content: String,
		}

		impl fmt::Debug for Content {
			fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
				write!(f, "Content {{ content: \"...\" }}")
			}
		}

		impl Content {
			/// Creates test content with a string of the specified length
			fn new(len: usize) -> Self {
				Self {
					content: "a".repeat(len),
				}
			}
		}

		// Set a 256 MiB limit for testing large message handling
		let max_size = 128 << 20;

		let permit = PERMITS.acquire().await.unwrap();
		// Configure WebSocket with custom size limits for testing
		let ws_config = WebsocketConfig::default().max_message_size(max_size);
		let config = Config::new().websocket(ws_config).unwrap();
		let db = Surreal::new::<Ws>(("127.0.0.1:8000", config)).await.unwrap();
		db.signin(Root {
			username: ROOT_USER.to_string(),
			password: ROOT_PASS.to_string(),
		})
		.await
		.unwrap();
		db.use_ns(Ulid::new().to_string()).use_db(Ulid::new().to_string()).await.unwrap();
		drop(permit);

		// Test various message sizes that should be accepted
		{
			let sizes = [0, 1, 1024, max_size - (1 << 20)];

			for size in sizes {
				let content = Content::new(size);

				let response: Option<Content> =
					db.upsert(("table", "test")).content(content.clone()).await.unwrap();

				assert_eq!(content, response.unwrap(), "size: {size}");
			}
		}

		// Test message size that should be rejected
		{
			let content = Content::new(max_size + (1 << 20));

			let error = db
				.upsert::<Option<Content>>(("table", "test"))
				.content(content.clone())
				.await
				.unwrap_err();

			let error_str = error.to_string();

			assert!(error_str.starts_with("Message too long: "), "{error_str}");
		}
	}

	/// Test that repeated WebSocket queries don't leak pending request entries.
	///
	/// This is a regression test for #6822 where each WS query left an entry
	/// in the pending_requests map, causing unbounded memory growth.
	#[test_log::test(tokio::test)]
	async fn repeated_queries_no_leak() {
		use ulid::Ulid;

		let permit = PERMITS.acquire().await.unwrap();
		let config = Config::new();
		let db = Surreal::new::<Ws>(("127.0.0.1:8000", config)).await.unwrap();
		db.signin(Root {
			username: ROOT_USER.to_string(),
			password: ROOT_PASS.to_string(),
		})
		.await
		.unwrap();
		let ns = Ulid::new().to_string();
		let dbn = Ulid::new().to_string();
		db.use_ns(&ns).use_db(&dbn).await.unwrap();

		// Define the table so that selecting a non-existent record returns
		// None rather than erroring with "table does not exist".
		db.query("DEFINE TABLE user SCHEMAFULL").await.unwrap();

		// Run many queries in a tight loop. Before the fix, each query would
		// leak a PendingRequest entry (~768 bytes). With 1000 iterations this
		// would accumulate without bound.
		for i in 0..1000u32 {
			let result: Option<super::RecordName> = db.select(("user", "test")).await.unwrap();
			// The record doesn't exist, so we expect None
			assert!(result.is_none(), "iteration {i}: expected None for non-existent record");
		}

		drop(permit);
	}

	include_tests!(new_db => basic, serialisation, live, session_isolation, run);
}

// --------------------------------------------------
// Storage engine tests
// --------------------------------------------------

#[cfg(feature = "kv-mem")]
mod mem {
	use surrealdb::Surreal;
	use surrealdb::engine::local::{Db, Mem};
	use surrealdb::opt::auth::Root;
	use surrealdb::opt::capabilities::{Capabilities, ExperimentalFeature};
	use surrealdb::opt::{Config, Resource};
	use surrealdb::types::RecordIdKey;
	use surrealdb_types::RecordId;
	use tokio::sync::{Semaphore, SemaphorePermit};

	use super::{ROOT_PASS, ROOT_USER};
	use crate::api_integration::ApiRecordId;

	static PERMITS: Semaphore = Semaphore::const_new(1);

	async fn new_db(config: Config) -> (SemaphorePermit<'static>, Surreal<Db>) {
		let permit = PERMITS.acquire().await.unwrap();
		let root = Root {
			username: ROOT_USER.to_string(),
			password: ROOT_PASS.to_string(),
		};
		let config = config.user(root.clone());
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
		let Some(record): Option<ApiRecordId> =
			db.create(RecordId::new("item", "foo")).await.unwrap()
		else {
			panic!("record not found");
		};
		assert_eq!(record.id.key, RecordIdKey::from("foo"));
	}

	#[test_log::test(tokio::test)]
	async fn cant_sign_into_default_root_account() {
		let db = Surreal::new::<Mem>(()).await.unwrap();

		let err = db
			.signin(Root {
				username: ROOT_USER.to_string(),
				password: ROOT_PASS.to_string(),
			})
			.await
			.unwrap_err();
		assert!(err.is_not_allowed(), "expected auth (NotAllowed) error: {}", err);
	}

	#[test_log::test(tokio::test)]
	async fn credentials_activate_authentication() {
		let config = Config::new().user(Root {
			username: ROOT_USER.to_string(),
			password: ROOT_PASS.to_string(),
		});
		let db = Surreal::new::<Mem>(config).await.unwrap();
		db.use_ns("namespace").use_db("database").await.unwrap();
		let res = db.create(Resource::from("item:foo")).await;
		let err = res.unwrap_err();
		assert!(err.is_not_allowed(), "expected permissions (NotAllowed) error, got: {}", err);
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
			DEFINE BUCKET test BACKEND \"memory\";
		";
		// Experimental features are rejected by default
		let db = Surreal::new::<Mem>(()).await.unwrap();
		db.query(surql).await.unwrap_err();
		// Experimental features can be allowed
		let capabilities =
			Capabilities::new().with_experimental_feature_allowed(ExperimentalFeature::Files);
		let config = Config::new().capabilities(capabilities);
		let db = Surreal::new::<Mem>(config).await.unwrap();
		db.query(surql).await.unwrap().check().unwrap();
	}

	include_tests!(new_db => basic, serialisation, live, backup, session_isolation, run);
}

#[cfg(feature = "kv-rocksdb")]
mod rocksdb {
	use surrealdb::Surreal;
	use surrealdb::engine::local::{Db, RocksDb};
	use surrealdb::opt::Config;
	use surrealdb::opt::auth::Root;
	use tokio::sync::{Semaphore, SemaphorePermit};
	use ulid::Ulid;

	use super::{ROOT_PASS, ROOT_USER, TEMP_DIR};

	static PERMITS: Semaphore = Semaphore::const_new(1);

	async fn new_db(config: Config) -> (SemaphorePermit<'static>, Surreal<Db>) {
		let permit = PERMITS.acquire().await.unwrap();
		let path = TEMP_DIR.join(Ulid::new().to_string());
		let root = Root {
			username: ROOT_USER.to_string(),
			password: ROOT_PASS.to_string(),
		};
		let config = config.user(root.clone());
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

	include_tests!(new_db => basic, serialisation, live, backup, session_isolation, run);
}

#[cfg(feature = "kv-surrealkv")]
mod surrealkv {
	use surrealdb::Surreal;
	use surrealdb::engine::local::{Db, SurrealKv};
	use surrealdb::opt::Config;
	use surrealdb::opt::auth::Root;
	use tokio::sync::{Semaphore, SemaphorePermit};
	use ulid::Ulid;

	use super::{ROOT_PASS, ROOT_USER, TEMP_DIR};

	static PERMITS: Semaphore = Semaphore::const_new(1);

	async fn new_db(config: Config) -> (SemaphorePermit<'static>, Surreal<Db>) {
		let permit = PERMITS.acquire().await.unwrap();
		let path = TEMP_DIR.join(Ulid::new().to_string());
		let root = Root {
			username: ROOT_USER.to_string(),
			password: ROOT_PASS.to_string(),
		};
		let config = config.user(root.clone());
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

	include_tests!(new_db => basic, serialisation, live, backup, session_isolation, run);
}

#[cfg(feature = "kv-tikv")]
mod tikv {
	use surrealdb::Surreal;
	use surrealdb::engine::local::{Db, TiKv};
	use surrealdb::opt::Config;
	use surrealdb::opt::auth::Root;
	use tokio::sync::{Semaphore, SemaphorePermit};

	use super::{ROOT_PASS, ROOT_USER};

	static PERMITS: Semaphore = Semaphore::const_new(1);

	async fn new_db(config: Config) -> (SemaphorePermit<'static>, Surreal<Db>) {
		let permit = PERMITS.acquire().await.unwrap();
		let root = Root {
			username: ROOT_USER.to_string(),
			password: ROOT_PASS.to_string(),
		};
		let config = config.user(root.clone());
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

	include_tests!(new_db => basic, serialisation, live, backup, session_isolation, run);
}

// --------------------------------------------------
// Versioned storage engine tests
// --------------------------------------------------

#[cfg(feature = "kv-mem")]
mod mem_versioned {
	use surrealdb::Surreal;
	use surrealdb::engine::local::{Db, Mem};
	use surrealdb::opt::Config;
	use surrealdb::opt::auth::Root;
	use tokio::sync::{Semaphore, SemaphorePermit};

	use super::{ROOT_PASS, ROOT_USER};

	static PERMITS: Semaphore = Semaphore::const_new(1);

	async fn new_db(config: Config) -> (SemaphorePermit<'static>, Surreal<Db>) {
		let permit = PERMITS.acquire().await.unwrap();
		let root = Root {
			username: ROOT_USER.to_string(),
			password: ROOT_PASS.to_string(),
		};
		let config = config.user(root.clone());
		let db = Surreal::new::<Mem>(config).versioned().await.unwrap();
		db.signin(root).await.unwrap();
		(permit, db)
	}

	#[test_log::test(tokio::test)]
	async fn any_engine_can_connect() {
		surrealdb::engine::any::connect("memory?versioned=true").await.unwrap();
	}

	include_tests!(new_db => basic, serialisation, live, backup, session_isolation, run, version, backup_version);
}

#[cfg(feature = "kv-surrealkv")]
mod surrealkv_versioned {
	use surrealdb::Surreal;
	use surrealdb::engine::local::{Db, SurrealKv};
	use surrealdb::opt::Config;
	use surrealdb::opt::auth::Root;
	use tokio::sync::{Semaphore, SemaphorePermit};
	use ulid::Ulid;

	use super::{ROOT_PASS, ROOT_USER, TEMP_DIR};

	static PERMITS: Semaphore = Semaphore::const_new(1);

	async fn new_db(config: Config) -> (SemaphorePermit<'static>, Surreal<Db>) {
		let permit = PERMITS.acquire().await.unwrap();
		let path = TEMP_DIR.join(Ulid::new().to_string());
		let root = Root {
			username: ROOT_USER.to_string(),
			password: ROOT_PASS.to_string(),
		};
		let config = config.user(root.clone());
		let db = Surreal::new::<SurrealKv>((path, config)).versioned().await.unwrap();
		db.signin(root).await.unwrap();
		(permit, db)
	}

	#[test_log::test(tokio::test)]
	async fn any_engine_can_connect() {
		let db_dir = Ulid::new().to_string();
		// Create a database directory using an absolute path
		surrealdb::engine::any::connect(format!(
			"surrealkv://{}?versioned=true",
			TEMP_DIR.join("absolute").join(&db_dir).display()
		))
		.await
		.unwrap();
		// Switch to the temporary directory, if possible, to test relative paths
		if std::env::set_current_dir(&*TEMP_DIR).is_ok() {
			// Create a database directory using a relative path
			surrealdb::engine::any::connect(format!(
				"surrealkv://relative/{db_dir}?versioned=true"
			))
			.await
			.unwrap();
		}
	}

	include_tests!(new_db => basic, serialisation, live, backup, session_isolation, run, version, backup_version);
}
