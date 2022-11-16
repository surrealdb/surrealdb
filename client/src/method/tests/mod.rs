#![cfg(any(feature = "http", feature = "ws"))]
#![cfg(not(target_arch = "wasm32"))]

mod protocol;
mod server;
mod types;

use crate::param::Database;
use crate::param::Jwt;
use crate::param::NameSpace;
use crate::param::PatchOp;
use crate::param::Root;
use crate::param::Scope;
use crate::Result;
use crate::StaticClient;
use crate::Surreal;
use protocol::Client;
use protocol::Test;
use semver::Version;
use std::ops::Bound;
use surrealdb::sql::statements::BeginStatement;
use surrealdb::sql::statements::CommitStatement;
use surrealdb::sql::Value;
use types::AuthParams;
use types::User;
use types::USER;

static CLIENT: Surreal<Client> = Surreal::new();

#[tokio::test]
async fn api() {
	// connect to the mock server
	CLIENT.connect::<Test>(()).with_capacity(512).await.unwrap();

	// health
	let _: () = CLIENT.health().await.unwrap();

	// invalidate
	let _: () = CLIENT.invalidate().await.unwrap();

	// use
	let _: () = CLIENT.use_ns("test-ns").use_db("test-db").await.unwrap();

	// signup
	let _: Jwt = CLIENT
		.signup(Scope {
			namespace: "test-ns",
			database: "test-db",
			scope: "scope",
			params: AuthParams {},
		})
		.await
		.unwrap();

	// signin
	let _: () = CLIENT
		.signin(Root {
			username: "root",
			password: "root",
		})
		.await
		.unwrap();
	let _: Jwt = CLIENT
		.signin(NameSpace {
			namespace: "test-ns",
			username: "user",
			password: "pass",
		})
		.await
		.unwrap();
	let _: Jwt = CLIENT
		.signin(Database {
			namespace: "test-ns",
			database: "test-db",
			username: "user",
			password: "pass",
		})
		.await
		.unwrap();
	let _: Jwt = CLIENT
		.signin(Scope {
			namespace: "test-ns",
			database: "test-db",
			scope: "scope",
			params: AuthParams {},
		})
		.await
		.unwrap();

	// authenticate
	let _: () = CLIENT.authenticate(Jwt(String::new())).await.unwrap();

	// query
	let _: Vec<Result<Vec<Value>>> = CLIENT.query("SELECT * FROM user").await.unwrap();
	let _: Vec<Result<Vec<Value>>> =
		CLIENT.query("CREATE user:john SET name = $name").bind("name", "John Doe").await.unwrap();
	let _: Vec<Result<Vec<Value>>> = CLIENT
		.query(BeginStatement)
		.query("CREATE account:one SET balance = 135605.16")
		.query("CREATE account:two SET balance = 91031.31")
		.query("UPDATE account:one SET balance += 300.00")
		.query("UPDATE account:two SET balance -= 300.00")
		.query(CommitStatement)
		.await
		.unwrap();

	// create
	let _: User = CLIENT.create(USER).await.unwrap();
	let _: User = CLIENT.create((USER, "john")).await.unwrap();
	let _: User = CLIENT.create(USER).content(User::default()).await.unwrap();
	let _: User = CLIENT.create((USER, "john")).content(User::default()).await.unwrap();

	// select
	let _: Vec<User> = CLIENT.select(USER).await.unwrap();
	let _: Option<User> = CLIENT.select((USER, "john")).await.unwrap();
	let _: Vec<User> = CLIENT.select(USER).range(..).await.unwrap();
	let _: Vec<User> = CLIENT.select(USER).range(.."john").await.unwrap();
	let _: Vec<User> = CLIENT.select(USER).range(..="john").await.unwrap();
	let _: Vec<User> = CLIENT.select(USER).range("jane"..).await.unwrap();
	let _: Vec<User> = CLIENT.select(USER).range("jane".."john").await.unwrap();
	let _: Vec<User> = CLIENT.select(USER).range("jane"..="john").await.unwrap();
	let _: Vec<User> = CLIENT.select(USER).range("jane"..="john").await.unwrap();
	let _: Vec<User> = CLIENT
		.select(USER)
		.range((Bound::Excluded("jane"), Bound::Included("john")))
		.await
		.unwrap();

	// update
	let _: Vec<User> = CLIENT.update(USER).await.unwrap();
	let _: Option<User> = CLIENT.update((USER, "john")).await.unwrap();
	let _: Vec<User> = CLIENT.update(USER).content(User::default()).await.unwrap();
	let _: Vec<User> =
		CLIENT.update(USER).range("jane".."john").content(User::default()).await.unwrap();
	let _: Option<User> = CLIENT.update((USER, "john")).content(User::default()).await.unwrap();

	// merge
	let _: Vec<User> = CLIENT.update(USER).merge(User::default()).await.unwrap();
	let _: Vec<User> =
		CLIENT.update(USER).range("jane".."john").merge(User::default()).await.unwrap();
	let _: Option<User> = CLIENT.update((USER, "john")).merge(User::default()).await.unwrap();

	// patch
	let _: Vec<User> = CLIENT.update(USER).patch(PatchOp::remove("/name")).await.unwrap();
	let _: Vec<User> =
		CLIENT.update(USER).range("jane".."john").patch(PatchOp::remove("/name")).await.unwrap();
	let _: Option<User> =
		CLIENT.update((USER, "john")).patch(PatchOp::remove("/name")).await.unwrap();

	// delete
	let _: () = CLIENT.delete(USER).await.unwrap();
	let _: () = CLIENT.delete((USER, "john")).await.unwrap();
	let _: () = CLIENT.delete(USER).range("jane".."john").await.unwrap();

	// version
	let _: Version = CLIENT.version().await.unwrap();
}
