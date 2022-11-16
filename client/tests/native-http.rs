#![cfg(feature = "http")]
#![cfg(not(target_arch = "wasm32"))]

mod types;

use crate::types::AuthParams;
use serde_json::json;
use std::ops::Bound;
use surrealdb::sql::statements::BeginStatement;
use surrealdb::sql::statements::CommitStatement;
use surrealdb_rs::param::Database;
use surrealdb_rs::param::Jwt;
use surrealdb_rs::param::NameSpace;
use surrealdb_rs::param::PatchOp;
use surrealdb_rs::param::Root;
use surrealdb_rs::param::Scope;
use surrealdb_rs::protocol::Http;
use surrealdb_rs::Surreal;
use tokio::fs::remove_file;
use types::*;
use ulid::Ulid;

#[tokio::test]
async fn connect() {
	let client = Surreal::connect::<Http>(DB_ENDPOINT).await.unwrap();
	client.health().await.unwrap();
}

#[tokio::test]
async fn connect_with_capacity() {
	let client = Surreal::connect::<Http>(DB_ENDPOINT).with_capacity(512).await.unwrap();
	client.health().await.unwrap();
}

#[tokio::test]
async fn invalidate() {
	let table = Ulid::new().to_string();
	let client = Surreal::connect::<Http>(DB_ENDPOINT).await.unwrap();
	client.use_ns(NS).use_db(DB).await.unwrap();
	client
		.signin(Root {
			username: ROOT_USER,
			password: ROOT_PASS,
		})
		.await
		.unwrap();
	client.invalidate().await.unwrap();
	match client.create(table).await {
		Ok(result) => {
			let option: Option<RecordId> = result;
			assert!(option.is_none());
		}
		Err(error) => {
			assert!(error
				.to_string()
				.contains("You don't have permission to perform this query type"));
		}
	}
}

#[tokio::test]
async fn yuse() {
	let client = Surreal::connect::<Http>(DB_ENDPOINT).await.unwrap();
	client.use_ns(NS).use_db(DB).await.unwrap();
}

#[tokio::test]
async fn signup_scope() {
	let scope = Ulid::new().to_string();
	let client = Surreal::connect::<Http>(DB_ENDPOINT).await.unwrap();
	client
		.signin(Root {
			username: ROOT_USER,
			password: ROOT_PASS,
		})
		.await
		.unwrap();
	client.use_ns(NS).use_db(DB).await.unwrap();
	client
		.query(format!(
			"
DEFINE SCOPE {scope} SESSION 1s
  SIGNUP ( CREATE user SET email = $email, pass = crypto::argon2::generate($pass) )
  SIGNIN ( SELECT * FROM user WHERE email = $email AND crypto::argon2::compare(pass, $pass) )
;"
		))
		.await
		.unwrap();
	client
		.signup(Scope {
			namespace: NS,
			database: DB,
			scope: &scope,
			params: AuthParams {
				email: "john.doe@example.com",
				pass: "password123",
			},
		})
		.await
		.unwrap();
}

#[tokio::test]
async fn signin_root() {
	let client = Surreal::connect::<Http>(DB_ENDPOINT).await.unwrap();
	client
		.signin(Root {
			username: ROOT_USER,
			password: ROOT_PASS,
		})
		.await
		.unwrap();
}

#[tokio::test]
async fn signin_ns() {
	let user = Ulid::new().to_string();
	let pass = "password123";
	let client = Surreal::connect::<Http>(DB_ENDPOINT).await.unwrap();
	client
		.signin(Root {
			username: ROOT_USER,
			password: ROOT_PASS,
		})
		.await
		.unwrap();
	client.use_ns(NS).use_db(DB).await.unwrap();
	client.query(format!("DEFINE LOGIN {user} ON NAMESPACE PASSWORD '{pass}'")).await.unwrap();
	let _: Jwt = client
		.signin(NameSpace {
			namespace: NS,
			username: &user,
			password: pass,
		})
		.await
		.unwrap();
}

#[tokio::test]
async fn signin_db() {
	let user = Ulid::new().to_string();
	let pass = "password123";
	let client = Surreal::connect::<Http>(DB_ENDPOINT).await.unwrap();
	client
		.signin(Root {
			username: ROOT_USER,
			password: ROOT_PASS,
		})
		.await
		.unwrap();
	client.use_ns(NS).use_db(DB).await.unwrap();
	client.query(format!("DEFINE LOGIN {user} ON DATABASE PASSWORD '{pass}'")).await.unwrap();
	let _: Jwt = client
		.signin(Database {
			namespace: NS,
			database: DB,
			username: &user,
			password: pass,
		})
		.await
		.unwrap();
}

#[tokio::test]
async fn signin_scope() {
	let scope = Ulid::new().to_string();
	let email = format!("{scope}@example.com");
	let pass = "password123";
	let client = Surreal::connect::<Http>(DB_ENDPOINT).await.unwrap();
	client
		.signin(Root {
			username: ROOT_USER,
			password: ROOT_PASS,
		})
		.await
		.unwrap();
	client.use_ns(NS).use_db(DB).await.unwrap();
	client
		.query(format!(
			"
DEFINE SCOPE {scope} SESSION 1s
  SIGNUP ( CREATE user SET email = $email, pass = crypto::argon2::generate($pass) )
  SIGNIN ( SELECT * FROM user WHERE email = $email AND crypto::argon2::compare(pass, $pass) )
;"
		))
		.await
		.unwrap();
	client
		.signup(Scope {
			namespace: NS,
			database: DB,
			scope: &scope,
			params: AuthParams {
				pass,
				email: &email,
			},
		})
		.await
		.unwrap();

	client
		.signin(Scope {
			namespace: NS,
			database: DB,
			scope: &scope,
			params: AuthParams {
				pass,
				email: &email,
			},
		})
		.await
		.unwrap();
}

#[tokio::test]
async fn authenticate() {
	let user = Ulid::new().to_string();
	let pass = "password123";
	let client = Surreal::connect::<Http>(DB_ENDPOINT).await.unwrap();
	client
		.signin(Root {
			username: ROOT_USER,
			password: ROOT_PASS,
		})
		.await
		.unwrap();
	client.use_ns(NS).use_db(DB).await.unwrap();
	client.query(format!("DEFINE LOGIN {user} ON NAMESPACE PASSWORD '{pass}'")).await.unwrap();
	let token = client
		.signin(NameSpace {
			namespace: NS,
			username: &user,
			password: pass,
		})
		.await
		.unwrap();
	client.authenticate(token).await.unwrap();
}

#[tokio::test]
async fn query() {
	let client = Surreal::connect::<Http>(DB_ENDPOINT).await.unwrap();
	client.query("SELECT * FROM record").await.unwrap();
}

#[tokio::test]
async fn query_binds() {
	let user = Ulid::new().to_string();
	let client = Surreal::connect::<Http>(DB_ENDPOINT).await.unwrap();
	client.use_ns(NS).use_db(DB).await.unwrap();
	client
		.query("CREATE type::thing($table, john) SET name = $name")
		.bind("table", user)
		.bind("name", "John Doe")
		.await
		.unwrap();
}

#[tokio::test]
async fn query_chaining() {
	let account = Ulid::new().to_string();
	let client = Surreal::connect::<Http>(DB_ENDPOINT).await.unwrap();
	client.use_ns(NS).use_db(DB).await.unwrap();
	client
		.query(BeginStatement)
		.query("CREATE type::thing($table, one) SET balance = 135605.16")
		.query("CREATE type::thing($table, two) SET balance = 91031.31")
		.query("UPDATE type::thing($table, one) SET balance += 300.00")
		.query("UPDATE type::thing($table, two) SET balance -= 300.00")
		.query(CommitStatement)
		.bind("table", account)
		.await
		.unwrap();
}

#[tokio::test]
async fn create_record_no_id() {
	let table = Ulid::new().to_string();
	let client = Surreal::connect::<Http>(DB_ENDPOINT).await.unwrap();
	client
		.signin(Root {
			username: ROOT_USER,
			password: ROOT_PASS,
		})
		.await
		.unwrap();
	client.use_ns(NS).use_db(DB).await.unwrap();
	let _: RecordId = client.create(table).await.unwrap();
}

#[tokio::test]
async fn create_record_with_id() {
	let table = Ulid::new().to_string();
	let client = Surreal::connect::<Http>(DB_ENDPOINT).await.unwrap();
	client
		.signin(Root {
			username: ROOT_USER,
			password: ROOT_PASS,
		})
		.await
		.unwrap();
	client.use_ns(NS).use_db(DB).await.unwrap();
	let _: RecordId = client.create((table, "john")).await.unwrap();
}

#[tokio::test]
async fn create_record_no_id_with_content() {
	let table = Ulid::new().to_string();
	let client = Surreal::connect::<Http>(DB_ENDPOINT).await.unwrap();
	client
		.signin(Root {
			username: ROOT_USER,
			password: ROOT_PASS,
		})
		.await
		.unwrap();
	client.use_ns(NS).use_db(DB).await.unwrap();
	let _: RecordId = client
		.create(table)
		.content(Record {
			name: "John Doe",
		})
		.await
		.unwrap();
}

#[tokio::test]
async fn create_record_with_id_with_content() {
	let table = Ulid::new().to_string();
	let client = Surreal::connect::<Http>(DB_ENDPOINT).await.unwrap();
	client
		.signin(Root {
			username: ROOT_USER,
			password: ROOT_PASS,
		})
		.await
		.unwrap();
	client.use_ns(NS).use_db(DB).await.unwrap();
	let record: RecordId = client
		.create((table.as_str(), "john"))
		.content(Record {
			name: "John Doe",
		})
		.await
		.unwrap();
	assert_eq!(record.id, format!("{table}:john"));
}

#[tokio::test]
async fn select_table() {
	let table = Ulid::new().to_string();
	let client = Surreal::connect::<Http>(DB_ENDPOINT).await.unwrap();
	client.use_ns(NS).use_db(DB).await.unwrap();
	let _: Vec<RecordId> = client.select(table.as_str()).await.unwrap();
}

#[tokio::test]
async fn select_record_id() {
	let table = Ulid::new().to_string();
	let client = Surreal::connect::<Http>(DB_ENDPOINT).await.unwrap();
	client.use_ns(NS).use_db(DB).await.unwrap();
	let _: Option<RecordId> = client.select((table.as_str(), "john")).await.unwrap();
}

#[tokio::test]
async fn select_record_ranges() {
	let table = Ulid::new().to_string();
	let client = Surreal::connect::<Http>(DB_ENDPOINT).await.unwrap();
	client.use_ns(NS).use_db(DB).await.unwrap();
	let _: Vec<RecordId> = client.select(table.as_str()).range(..).await.unwrap();
	let _: Vec<RecordId> = client.select(table.as_str()).range(.."john").await.unwrap();
	let _: Vec<RecordId> = client.select(table.as_str()).range(..="john").await.unwrap();
	let _: Vec<RecordId> = client.select(table.as_str()).range("jane"..).await.unwrap();
	let _: Vec<RecordId> = client.select(table.as_str()).range("jane".."john").await.unwrap();
	let _: Vec<RecordId> = client.select(table.as_str()).range("jane"..="john").await.unwrap();
	let _: Vec<RecordId> = client.select(table.as_str()).range("jane"..="john").await.unwrap();
	let _: Vec<RecordId> = client
		.select(table.as_str())
		.range((Bound::Excluded("jane"), Bound::Included("john")))
		.await
		.unwrap();
}

#[tokio::test]
async fn update_table() {
	let table = Ulid::new().to_string();
	let client = Surreal::connect::<Http>(DB_ENDPOINT).await.unwrap();
	client.use_ns(NS).use_db(DB).await.unwrap();
	let _: Vec<RecordId> = client.update(table).await.unwrap();
}

#[tokio::test]
async fn update_record_id() {
	let table = Ulid::new().to_string();
	let client = Surreal::connect::<Http>(DB_ENDPOINT).await.unwrap();
	client.use_ns(NS).use_db(DB).await.unwrap();
	client
		.signin(Root {
			username: ROOT_USER,
			password: ROOT_PASS,
		})
		.await
		.unwrap();
	let _: Option<RecordId> = client.update((table, "john")).await.unwrap();
}

#[tokio::test]
async fn update_table_with_content() {
	let table = Ulid::new().to_string();
	let client = Surreal::connect::<Http>(DB_ENDPOINT).await.unwrap();
	client.use_ns(NS).use_db(DB).await.unwrap();
	let _: Vec<RecordId> = client
		.update(table)
		.content(Record {
			name: "John Doe",
		})
		.await
		.unwrap();
}

#[tokio::test]
async fn update_record_range_with_content() {
	let table = Ulid::new().to_string();
	let client = Surreal::connect::<Http>(DB_ENDPOINT).await.unwrap();
	client.use_ns(NS).use_db(DB).await.unwrap();
	let _: Vec<RecordId> = client
		.update(table)
		.range("jane".."john")
		.content(Record {
			name: "John Doe",
		})
		.await
		.unwrap();
}

#[tokio::test]
async fn update_record_id_with_content() {
	let table = Ulid::new().to_string();
	let client = Surreal::connect::<Http>(DB_ENDPOINT).await.unwrap();
	client
		.signin(Root {
			username: ROOT_USER,
			password: ROOT_PASS,
		})
		.await
		.unwrap();
	client.use_ns(NS).use_db(DB).await.unwrap();
	let _: Option<RecordId> = client
		.update((table, "john"))
		.content(Record {
			name: "John Doe",
		})
		.await
		.unwrap();
}

#[tokio::test]
async fn merge_table() {
	let table = Ulid::new().to_string();
	let client = Surreal::connect::<Http>(DB_ENDPOINT).await.unwrap();
	client.use_ns(NS).use_db(DB).await.unwrap();
	let _: Vec<RecordId> = client
		.update(table)
		.merge(Record {
			name: "John Doe",
		})
		.await
		.unwrap();
}

#[tokio::test]
async fn merge_record_range() {
	let table = Ulid::new().to_string();
	let client = Surreal::connect::<Http>(DB_ENDPOINT).await.unwrap();
	client.use_ns(NS).use_db(DB).await.unwrap();
	let _: Vec<RecordId> = client
		.update(table)
		.range("jane".."john")
		.merge(Record {
			name: "John Doe",
		})
		.await
		.unwrap();
}

#[tokio::test]
async fn merge_record_id() {
	let table = Ulid::new().to_string();
	let client = Surreal::connect::<Http>(DB_ENDPOINT).await.unwrap();
	client
		.signin(Root {
			username: ROOT_USER,
			password: ROOT_PASS,
		})
		.await
		.unwrap();
	client.use_ns(NS).use_db(DB).await.unwrap();
	let _: Option<RecordId> = client
		.update((table, "john"))
		.merge(Record {
			name: "John Doe",
		})
		.await
		.unwrap();
}

#[tokio::test]
async fn patch_table() {
	let table = Ulid::new().to_string();
	let client = Surreal::connect::<Http>(DB_ENDPOINT).await.unwrap();
	client.use_ns(NS).use_db(DB).await.unwrap();
	let _: Vec<RecordId> = client
		.update(table)
		.patch(PatchOp::replace("/baz", "boo"))
		.patch(PatchOp::add("/hello", ["world"]))
		.patch(PatchOp::remove("/foo"))
		.await
		.unwrap();
}

#[tokio::test]
async fn patch_record_range() {
	let table = Ulid::new().to_string();
	let client = Surreal::connect::<Http>(DB_ENDPOINT).await.unwrap();
	client.use_ns(NS).use_db(DB).await.unwrap();
	let _: Vec<RecordId> = client
		.update(table)
		.range("jane".."john")
		.patch(PatchOp::replace("/baz", "boo"))
		.patch(PatchOp::add("/hello", ["world"]))
		.patch(PatchOp::remove("/foo"))
		.await
		.unwrap();
}

#[tokio::test]
async fn patch_record_id() {
	let table = Ulid::new().to_string();
	let id = "record";
	let client = Surreal::connect::<Http>(DB_ENDPOINT).await.unwrap();
	client
		.signin(Root {
			username: ROOT_USER,
			password: ROOT_PASS,
		})
		.await
		.unwrap();
	client.use_ns(NS).use_db(DB).await.unwrap();
	let _: Option<RecordId> = client
		.create((table.as_str(), id))
		.content(json!({
			"baz": "qux",
			"foo": "bar"
		}))
		.await
		.unwrap();
	let _: Option<serde_json::Value> = client
		.update((table.as_str(), id))
		.patch(PatchOp::replace("/baz", "boo"))
		.patch(PatchOp::add("/hello", ["world"]))
		.patch(PatchOp::remove("/foo"))
		.await
		.unwrap();
	let value: Option<serde_json::Value> = client.select((table.as_str(), id)).await.unwrap();
	assert_eq!(
		value,
		Some(json!({
			"id": format!("{table}:{id}"),
			"baz": "boo",
			"hello": ["world"]
		}))
	);
}

#[tokio::test]
async fn delete_table() {
	let table = Ulid::new().to_string();
	let client = Surreal::connect::<Http>(DB_ENDPOINT).await.unwrap();
	client.use_ns(NS).use_db(DB).await.unwrap();
	client.delete(table).await.unwrap();
}

#[tokio::test]
async fn delete_record_id() {
	let table = Ulid::new().to_string();
	let client = Surreal::connect::<Http>(DB_ENDPOINT).await.unwrap();
	client
		.signin(Root {
			username: ROOT_USER,
			password: ROOT_PASS,
		})
		.await
		.unwrap();
	client.use_ns(NS).use_db(DB).await.unwrap();
	client.delete((table, "john")).await.unwrap();
}

#[tokio::test]
async fn delete_record_range() {
	let table = Ulid::new().to_string();
	let client = Surreal::connect::<Http>(DB_ENDPOINT).await.unwrap();
	client.use_ns(NS).use_db(DB).await.unwrap();
	client.delete(table).range("jane".."john").await.unwrap();
}

#[tokio::test]
async fn version() {
	let client = Surreal::connect::<Http>(DB_ENDPOINT).await.unwrap();
	client.version().await.unwrap();
}

#[tokio::test]
async fn set_unset() {
	let client = Surreal::connect::<Http>(DB_ENDPOINT).await.unwrap();
	client
		.set(
			"user",
			Record {
				name: "John Doe",
			},
		)
		.await
		.unwrap();
	client.unset("user").await.unwrap();
}

#[tokio::test]
async fn export_import() {
	let db = Ulid::new().to_string();
	let client = Surreal::connect::<Http>(DB_ENDPOINT).await.unwrap();
	client.use_ns(NS).use_db(&db).await.unwrap();
	client
		.signin(Root {
			username: ROOT_USER,
			password: ROOT_PASS,
		})
		.await
		.unwrap();
	for i in 0..10 {
		let _: RecordId = client
			.create("user")
			.content(Record {
				name: &format!("User {i}"),
			})
			.await
			.unwrap();
	}
	let file = format!("{db}.sql");
	client.export(&file).await.unwrap();
	client.import(&file).await.unwrap();
	remove_file(file).await.unwrap();
}

#[tokio::test]
async fn return_bool() {
	let client = Surreal::connect::<Http>(DB_ENDPOINT).await.unwrap();
	client.query("RETURN true").await.unwrap();
}
