mod error;
pub mod person;

use rocket::{routes, Build};
use std::env;
use surrealdb::engine::any;
use surrealdb::engine::any::Any;
use surrealdb::opt::auth::Root;
use surrealdb::opt::Config;
use surrealdb::Surreal;

pub type Db = Surreal<Any>;

pub async fn create_db_connection() -> Result<Db, Box<dyn std::error::Error>> {
	let endpoint = env::var("SURREALDB_ENDPOINT").unwrap_or_else(|_| "memory".to_owned());
	let root = Root {
		username: "root",
		password: "root",
	};
	let config = Config::new().user(root);
	let db = any::connect((endpoint, config)).await?;
	db.signin(root).await?;
	db.use_ns("namespace").use_db("database").await?;

	Ok(db)
}

pub fn router(db_conn: Surreal<Any>) -> rocket::Rocket<Build> {
	rocket::build()
		.mount(
			"/",
			routes![
				person::create,
				person::read,
				person::update,
				person::delete,
				person::list,
				person::delete_all
			],
		)
		.manage(db_conn)
}
