#[macro_use] extern crate rocket;

use rocket::{Rocket, Build, State};
use rocket::serde::{Deserialize, Serialize};
use surrealdb::Surreal;
use std::env;
use std::sync::Arc;
use surrealdb::engine::any;
use surrealdb::opt::auth::Root;
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

async fn setup_rocket() -> Result<Rocket<Build>, Box<dyn std::error::Error>> {
    let db_conn = create_db_connection().await?;
    let rocket = rocket::build()
        .manage(db_conn)
        .mount(
            "/",
            routes![
                person::create,
                person::read,
                person::update,
                person::list
            ],
        );
    Ok(rocket)
}

#[rocket::main]
async fn main() {
    if let Err(e) = setup_rocket().await {
        println!("Failed to launch Rocket: {}", e);
    }
}
