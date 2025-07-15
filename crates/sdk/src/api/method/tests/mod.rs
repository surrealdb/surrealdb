#![cfg(any(feature = "protocol-http", feature = "protocol-ws"))]
#![cfg(not(target_family = "wasm"))]

mod protocol;
mod server;
mod types;

use crate::QueryResults;
use crate::api::Surreal;
use crate::api::opt::PatchOp;
use crate::api::opt::auth::Database;
use crate::api::opt::auth::Jwt;
use crate::api::opt::auth::Namespace;
use crate::api::opt::auth::RecordCredentials;
use crate::api::opt::auth::Root;
use semver::Version;
use std::collections::BTreeMap;
use std::ops::Bound;
use surrealdb_core::sql::statements::{BeginStatement, CommitStatement};
use types::USER;
use types::User;

// static DB: LazyLock<Surreal> = LazyLock::new(Surreal::init);

#[tokio::test]
async fn api() {
	let (channel, server_handle) = protocol::TestServer::serve().await;
	let db = Surreal::new(channel, "test".try_into().unwrap());

	// let DB = Surreal::connect("memory").await.unwrap();

	// // connect to the mock server
	// DB.connect::<Test>(()).with_capacity(512).await.unwrap();

	// health
	let _ = db.health().await.unwrap();

	// invalidate
	let _: () = db.invalidate().await.unwrap();

	// use
	let _ = db.use_ns("test-ns").use_db("test-db").await.unwrap();

	// signup
	let _: Jwt = db
		.signup(RecordCredentials {
			namespace: "test-ns",
			database: "test-db",
			access: "access",
			params: BTreeMap::new(),
		})
		.await
		.unwrap();

	// signin
	let _: Jwt = db
		.signin(Root {
			username: "root",
			password: "root",
		})
		.await
		.unwrap();
	let _: Jwt = db
		.signin(Namespace {
			namespace: "test-ns",
			username: "user",
			password: "pass",
		})
		.await
		.unwrap();
	let _: Jwt = db
		.signin(Database {
			namespace: "test-ns",
			database: "test-db",
			username: "user",
			password: "pass",
		})
		.await
		.unwrap();
	let _: Jwt = db
		.signin(RecordCredentials {
			namespace: "test-ns",
			database: "test-db",
			access: "access",
			params: BTreeMap::new(),
		})
		.await
		.unwrap();

	// authenticate
	let _: () = db.authenticate(Jwt(String::new())).await.unwrap();

	// query
	let _: QueryResults = db.query("SELECT * FROM user").await.unwrap();
	let _: QueryResults =
		db.query("CREATE user:john SET name = $name").bind(("name", "John Doe")).await.unwrap();
	let _: QueryResults = db
		.query("CREATE user:john SET name = $name")
		.bind(User {
			id: "john".to_owned(),
			name: "John Doe".to_owned(),
		})
		.await
		.unwrap();
	let _: QueryResults = db
		.query(BeginStatement::default().to_string())
		.query("CREATE account:one SET balance = 135605.16")
		.query("CREATE account:two SET balance = 91031.31")
		.query("UPDATE account:one SET balance += 300.00")
		.query("UPDATE account:two SET balance -= 300.00")
		.query(CommitStatement::default().to_string())
		.await
		.unwrap();

	// create
	let _: Option<User> = db.create(USER).await.unwrap();
	let _: Option<User> = db.create((USER, "john")).await.unwrap();
	let _: Option<User> = db.create(USER).content(User::default()).await.unwrap();
	let _: Option<User> = db.create((USER, "john")).content(User::default()).await.unwrap();

	// select
	let _: Vec<User> = db.select(USER).await.unwrap();
	let _: Option<User> = db.select((USER, "john")).await.unwrap();
	let _: Vec<User> = db.select(USER).range(..).await.unwrap();
	let _: Vec<User> = db.select(USER).range(.."john").await.unwrap();
	let _: Vec<User> = db.select(USER).range(..="john").await.unwrap();
	let _: Vec<User> = db.select(USER).range("jane"..).await.unwrap();
	let _: Vec<User> = db.select(USER).range("jane".."john").await.unwrap();
	let _: Vec<User> = db.select(USER).range("jane"..="john").await.unwrap();
	let _: Vec<User> = db.select(USER).range("jane"..="john").await.unwrap();
	let _: Vec<User> =
		db.select(USER).range((Bound::Excluded("jane"), Bound::Included("john"))).await.unwrap();

	// update
	let _: Vec<User> = db.update(USER).await.unwrap();
	let _: Option<User> = db.update((USER, "john")).await.unwrap();
	let _: Vec<User> = db.update(USER).content(User::default()).await.unwrap();
	let _: Vec<User> =
		db.update(USER).range("jane".."john").content(User::default()).await.unwrap();
	let _: Option<User> = db.update((USER, "john")).content(User::default()).await.unwrap();

	// insert
	let _: Vec<User> = db.insert(USER).await.unwrap();
	let _: Option<User> = db.insert((USER, "john")).await.unwrap();
	let _: Vec<User> = db.insert(USER).content(User::default()).await.unwrap();
	let _: Option<User> = db.insert((USER, "john")).content(User::default()).await.unwrap();

	// merge
	let _: Vec<User> = db.update(USER).merge(User::default()).await.unwrap();
	let _: Vec<User> = db.update(USER).range("jane".."john").merge(User::default()).await.unwrap();
	let _: Option<User> = db.update((USER, "john")).merge(User::default()).await.unwrap();

	// patch
	let _: Vec<User> = db.update(USER).patch(PatchOp::remove("/name")).await.unwrap();
	let _: Vec<User> =
		db.update(USER).range("jane".."john").patch(PatchOp::remove("/name")).await.unwrap();
	let _: Option<User> = db.update((USER, "john")).patch(PatchOp::remove("/name")).await.unwrap();

	// delete
	let _: Vec<User> = db.delete(USER).await.unwrap();
	let _: Option<User> = db.delete((USER, "john")).await.unwrap();
	let _: Vec<User> = db.delete(USER).range("jane".."john").await.unwrap();

	// export
	let _: () = db.export("backup.sql").await.unwrap();

	// import
	let _: () = db.import("backup.sql").await.unwrap();

	// version
	let _: Version = db.version().await.unwrap();

	// run
	let _: Option<User> = db.run("foo").await.unwrap();
}

fn assert_send(_: impl Send) {}

#[test]
fn futures_are_send_sync() {
	assert_send(async {
		let (channel, server_handle) = protocol::TestServer::serve().await;

		let db = Surreal::new(channel, "test".try_into().unwrap());

		db.signin(Root {
			username: "root",
			password: "root",
		})
		.await
		.unwrap();
		db.use_ns("test-ns").use_db("test-db").await.unwrap();
		let _: Vec<User> = db.select(USER).await.unwrap();
	});
}
