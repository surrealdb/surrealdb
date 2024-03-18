mod error;
pub mod person;

use rocket::serde::{Deserialize, Serialize};
use std::env;
use std::sync::Arc;
use surrealdb::engine::any;
use surrealdb::opt::auth::Root;
use surrealdb::Surreal;


#[derive(Serialize, Deserialize)]
#[serde(crate = "rocket::serde")]
pub struct Person {
	name: String,
}

pub type Db = Arc<Surreal<any::Any>>;

pub async fn create_db_connection() -> Result<Db, Box<dyn std::error::Error>> {
	let endpoint = env::var("SURREALDB_ENDPOINT").unwrap_or_else(|_| "memory".to_owned());
	let db = any::connect(&endpoint).await?;

	db.signin(Root {
		username: "root",
		password: "root",
	})
	.await?;

	db.use_ns("namespace").use_db("database").await?;

	Ok(Arc::new(db))
}
