#![allow(unused_imports, dead_code)]

use serde::Deserialize;
use serde::Serialize;
use serde_json::json;
use std::borrow::Cow;
use std::ops::Bound;
use surrealdb::param::Database;
use surrealdb::param::Jwt;
use surrealdb::param::NameSpace;
use surrealdb::param::PatchOp;
use surrealdb::param::Root;
use surrealdb::param::Scope;
use surrealdb::sql::statements::BeginStatement;
use surrealdb::sql::statements::CommitStatement;
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
	id: String,
}

#[derive(Debug, Deserialize)]
struct RecordName {
	name: String,
}

#[derive(Debug, Deserialize, Eq, PartialEq, Ord, PartialOrd)]
struct RecordBuf {
	id: String,
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
	use surrealdb::net::WsClient;
	use surrealdb::protocol::Ws;

	async fn new_db() -> Surreal<WsClient> {
		let db = Surreal::connect::<Ws>("localhost:8000").await.unwrap();
		db.signin(Root {
			username: ROOT_USER,
			password: ROOT_PASS,
		})
		.await
		.unwrap();
		db
	}

	include!("api/mod.rs");
	include!("api/auth.rs");
}

#[cfg(feature = "protocol-http")]
mod http {
	use super::*;
	use surrealdb::net::HttpClient;
	use surrealdb::protocol::Http;

	async fn new_db() -> Surreal<HttpClient> {
		let db = Surreal::connect::<Http>("localhost:8000").await.unwrap();
		db.signin(Root {
			username: ROOT_USER,
			password: ROOT_PASS,
		})
		.await
		.unwrap();
		db
	}

	include!("api/mod.rs");
	include!("api/auth.rs");
	include!("api/backup.rs");
}

#[cfg(feature = "kv-mem")]
mod mem {
	use super::*;
	use surrealdb::embedded::Db;
	use surrealdb::storage::Mem;

	async fn new_db() -> Surreal<Db> {
		Surreal::connect::<Mem>(()).await.unwrap()
	}

	include!("api/mod.rs");
	include!("api/backup.rs");
}

#[cfg(feature = "kv-rocksdb")]
mod file {
	use super::*;
	use surrealdb::embedded::Db;
	use surrealdb::storage::File;

	async fn new_db() -> Surreal<Db> {
		let path = format!("/tmp/{}.db", Ulid::new());
		Surreal::connect::<File>(path.as_str()).await.unwrap()
	}

	include!("api/mod.rs");
	include!("api/backup.rs");
}

#[cfg(feature = "kv-tikv")]
mod tikv {
	use super::*;
	use surrealdb::embedded::Db;
	use surrealdb::storage::TiKv;

	async fn new_db() -> Surreal<Db> {
		Surreal::connect::<TiKv>("127.0.0.1:2379").await.unwrap()
	}

	include!("api/mod.rs");
	include!("api/backup.rs");
}

#[cfg(feature = "kv-fdb")]
mod fdb {
	use super::*;
	use surrealdb::embedded::Db;
	use surrealdb::storage::FDb;

	async fn new_db() -> Surreal<Db> {
		Surreal::connect::<FDb>("/tmp/fdb.cluster").await.unwrap()
	}

	include!("api/mod.rs");
	include!("api/backup.rs");
}

#[cfg(feature = "protocol-http")]
mod any {
	use super::*;
	use surrealdb::any::Any;

	async fn new_db() -> Surreal<Any> {
		let db = surrealdb::connect("http://localhost:8000").await.unwrap();
		db.signin(Root {
			username: ROOT_USER,
			password: ROOT_PASS,
		})
		.await
		.unwrap();
		db
	}

	include!("api/mod.rs");
	include!("api/auth.rs");
	include!("api/backup.rs");
}
