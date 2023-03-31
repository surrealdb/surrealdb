mod error;
mod person;

use axum::routing::{delete, get, post, put};
use axum::{Router, Server};
use std::net::SocketAddr;
use surrealdb::engine::remote::ws::Ws;
use surrealdb::opt::auth::Root;
use surrealdb::Surreal;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
	let db = Surreal::new::<Ws>("localhost:8000").await?;

	db.signin(Root {
		username: "root",
		password: "root",
	})
	.await?;

	db.use_ns("namespace").use_db("database").await?;

	let app = Router::new()
		.route("/person/:id", post(person::create))
		.route("/person/:id", get(person::read))
		.route("/person/:id", put(person::update))
		.route("/person/:id", delete(person::delete))
		.route("/people", get(person::list))
		.with_state(db);

	let addr = SocketAddr::from(([127, 0, 0, 1], 8080));

	Server::bind(&addr).serve(app.into_make_service()).await?;

	Ok(())
}
