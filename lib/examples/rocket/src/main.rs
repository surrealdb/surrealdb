#[macro_use]
extern crate rocket;

use rocket::serde::{Deserialize, Serialize};
use std::env;
use std::sync::Arc;
use surrealdb::engine::any;
use surrealdb::opt::auth::Root;
use surrealdb::Surreal;
mod error;
mod person;

#[derive(Serialize, Deserialize)]
#[serde(crate = "rocket::serde")]
struct Person {
	name: String,
}

type Db = Arc<Surreal<any::Any>>;

async fn create_db_connection() -> Result<Db, Box<dyn std::error::Error>> {
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

#[launch]
async fn rocket()  -> _ {
	let db_conn = create_db_connection().await.unwrap();
	 rocket::build()
		.mount("/", routes![person::create,person::read, person::update, person::delete, person::list]).manage(db_conn)
}
