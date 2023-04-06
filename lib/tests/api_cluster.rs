#![allow(unused_imports, dead_code)]

use serde::Deserialize;
use serde::Serialize;
use serde_json::json;
use std::borrow::Cow;
use std::ops::Bound;
use surrealdb::opt::auth::Database;
use surrealdb::opt::auth::Jwt;
use surrealdb::opt::auth::Namespace;
use surrealdb::opt::auth::Root;
use surrealdb::opt::auth::Scope;
use surrealdb::opt::PatchOp;
use surrealdb::sql::serde::serialize_internal;
use surrealdb::sql::statements::BeginStatement;
use surrealdb::sql::statements::CommitStatement;
use surrealdb::sql::thing;
use surrealdb::sql::Thing;
use surrealdb::Surreal;
use ulid::Ulid;

const NS: &str = "test-ns";
const ROOT_USER: &str = "root";
const ROOT_PASS: &str = "root";

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

#[cfg(feature = "protocol-ws")]
mod cluster {
	// use super::*;
	// use surrealdb::engine::remote::ws::Client;
	// use surrealdb::engine::remote::ws::Ws;
	//
	// async fn new_db() -> Surreal<Client> {
	// 	let db = Surreal::new::<Ws>("127.0.0.1:8000").await.unwrap();
	// 	db.signin(Root {
	// 		username: ROOT_USER,
	// 		password: ROOT_PASS,
	// 	})
	// 	.await
	// 	.unwrap();
	// 	db
	// }
	//
	// async fn new_db_replica() -> Surreal<Client> {
	// 	let db = Surreal::new::<Ws>("127.0.0.1:8001").await.unwrap();
	// 	db.signin(Root {
	// 		username: ROOT_USER,
	// 		password: ROOT_PASS,
	// 	})
	// 	.await
	// 	.unwrap();
	// 	db
	// }
	//
	// include!("api/mod.rs");
	// // include!("api/auth.rs");
	// include!("api/cluster.rs");
}
