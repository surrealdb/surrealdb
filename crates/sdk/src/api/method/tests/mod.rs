#![cfg(any(feature = "protocol-http", feature = "protocol-ws"))]
#![cfg(not(target_family = "wasm"))]

mod protocol;
mod server;
mod types;

use std::ops::Bound;
use std::sync::LazyLock;

use protocol::{Client, Test};
use semver::Version;
use types::{USER, User};

use crate::api::method::tests::types::AuthParams;
use crate::api::opt::PatchOp;
use crate::api::opt::auth::{Database, Jwt, Namespace, Record, Root};
use crate::api::{Response as QueryResponse, Surreal};
use crate::core::expr::TopLevelExpr;

static DB: LazyLock<Surreal<Client>> = LazyLock::new(Surreal::init);

#[tokio::test]
async fn api() {
	// connect to the mock server
	DB.connect::<Test>(()).with_capacity(512).await.unwrap();

	// health
	let _: () = DB.health().await.unwrap();

	// invalidate
	let _: () = DB.invalidate().await.unwrap();

	// use
	let _: () = DB.use_ns("test-ns").use_db("test-db").await.unwrap();

	// signup
	let _: Jwt = DB
		.signup(Record {
			namespace: "test-ns",
			database: "test-db",
			access: "access",
			params: AuthParams {},
		})
		.await
		.unwrap();

	// signin
	let _: Jwt = DB
		.signin(Root {
			username: "root",
			password: "root",
		})
		.await
		.unwrap();
	let _: Jwt = DB
		.signin(Namespace {
			namespace: "test-ns",
			username: "user",
			password: "pass",
		})
		.await
		.unwrap();
	let _: Jwt = DB
		.signin(Database {
			namespace: "test-ns",
			database: "test-db",
			username: "user",
			password: "pass",
		})
		.await
		.unwrap();
	let _: Jwt = DB
		.signin(Record {
			namespace: "test-ns",
			database: "test-db",
			access: "access",
			params: AuthParams {},
		})
		.await
		.unwrap();

	// authenticate
	let _: () = DB.authenticate(Jwt(String::new())).await.unwrap();

	// query
	let _: QueryResponse = DB.query("SELECT * FROM user").await.unwrap();
	let _: QueryResponse =
		DB.query("CREATE user:john SET name = $name").bind(("name", "John Doe")).await.unwrap();
	let _: QueryResponse = DB
		.query("CREATE user:john SET name = $name")
		.bind(User {
			id: "john".to_owned(),
			name: "John Doe".to_owned(),
		})
		.await
		.unwrap();
	let _: QueryResponse = DB
		.query(TopLevelExpr::Begin)
		.query("CREATE account:one SET balance = 135605.16")
		.query("CREATE account:two SET balance = 91031.31")
		.query("UPDATE account:one SET balance += 300.00")
		.query("UPDATE account:two SET balance -= 300.00")
		.query(TopLevelExpr::Commit)
		.await
		.unwrap();

	// create
	let _: Option<User> = DB.create(USER).await.unwrap();
	let _: Option<User> = DB.create((USER, "john")).await.unwrap();
	let _: Option<User> = DB.create(USER).content(User::default()).await.unwrap();
	let _: Option<User> = DB.create((USER, "john")).content(User::default()).await.unwrap();

	// select
	let _: Vec<User> = DB.select(USER).await.unwrap();
	let _: Option<User> = DB.select((USER, "john")).await.unwrap();
	let _: Vec<User> = DB.select(USER).range(..).await.unwrap();
	let _: Vec<User> = DB.select(USER).range(.."john").await.unwrap();
	let _: Vec<User> = DB.select(USER).range(..="john").await.unwrap();
	let _: Vec<User> = DB.select(USER).range("jane"..).await.unwrap();
	let _: Vec<User> = DB.select(USER).range("jane".."john").await.unwrap();
	let _: Vec<User> = DB.select(USER).range("jane"..="john").await.unwrap();
	let _: Vec<User> = DB.select(USER).range("jane"..="john").await.unwrap();
	let _: Vec<User> =
		DB.select(USER).range((Bound::Excluded("jane"), Bound::Included("john"))).await.unwrap();

	// update
	let _: Vec<User> = DB.update(USER).await.unwrap();
	let _: Option<User> = DB.update((USER, "john")).await.unwrap();
	let _: Vec<User> = DB.update(USER).content(User::default()).await.unwrap();
	let _: Vec<User> =
		DB.update(USER).range("jane".."john").content(User::default()).await.unwrap();
	let _: Option<User> = DB.update((USER, "john")).content(User::default()).await.unwrap();

	// insert
	let _: Vec<User> = DB.insert(USER).await.unwrap();
	let _: Option<User> = DB.insert((USER, "john")).await.unwrap();
	let _: Vec<User> = DB.insert(USER).content(User::default()).await.unwrap();
	let _: Option<User> = DB.insert((USER, "john")).content(User::default()).await.unwrap();

	// merge
	let _: Vec<User> = DB.update(USER).merge(User::default()).await.unwrap();
	let _: Vec<User> = DB.update(USER).range("jane".."john").merge(User::default()).await.unwrap();
	let _: Option<User> = DB.update((USER, "john")).merge(User::default()).await.unwrap();

	// patch
	let _: Vec<User> = DB.update(USER).patch(PatchOp::remove("/name")).await.unwrap();
	let _: Vec<User> =
		DB.update(USER).range("jane".."john").patch(PatchOp::remove("/name")).await.unwrap();
	let _: Option<User> = DB.update((USER, "john")).patch(PatchOp::remove("/name")).await.unwrap();

	// delete
	let _: Vec<User> = DB.delete(USER).await.unwrap();
	let _: Option<User> = DB.delete((USER, "john")).await.unwrap();
	let _: Vec<User> = DB.delete(USER).range("jane".."john").await.unwrap();

	// export
	let _: () = DB.export("backup.sql").await.unwrap();

	// import
	let _: () = DB.import("backup.sql").await.unwrap();

	// version
	let _: Version = DB.version().await.unwrap();

	// run
	let _: Option<User> = DB.run("foo").await.unwrap();
}

fn assert_send_sync(_: impl Send + Sync) {}

#[test]
fn futures_are_send_sync() {
	assert_send_sync(async {
		let db = Surreal::new::<Test>(()).await.unwrap();
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
