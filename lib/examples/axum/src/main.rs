mod error;
mod person;

use axum::routing::{delete, get, post, put};
use axum::Router;
use surrealdb::engine::remote::ws::Ws;
use surrealdb::opt::auth::Root;
use surrealdb::Surreal;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
	let db = Surreal::new::<Ws>("localhost:8000").await?;

	db.signin(Root {
		username: "root",
		password: "root",
	})
	.await?;

	db.use_ns("namespace").use_db("database").await?;

	let router = Router::new()
		.route("/person/:id", post(person::create))
		.route("/person/:id", get(person::read))
		.route("/person/:id", put(person::update))
		.route("/person/:id", delete(person::delete))
		.route("/people", get(person::list))
		.with_state(db);

	let listener = TcpListener::bind("localhost:8080").await?;

	axum::serve(listener, router).await?;

	Ok(())
}
