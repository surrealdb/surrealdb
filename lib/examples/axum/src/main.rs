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

	let listener = TcpListener::bind("localhost:8080").await?;
	let router = create_router(db);

	axum::serve(listener, router).await?;

	Ok(())
}

fn create_router(db: Surreal<surrealdb::engine::remote::ws::Client>) -> Router {
	Router::new()
		//curl -X POST -H "Content-Type: application/json" -d '{"name":"John Doe"}' http://localhost:8080/person/1
		.route("/person/:id", post(person::create))
		//curl -X GET http://localhost:8080/person/1
		.route("/person/:id", get(person::read))
		//curl -X PUT -H "Content-Type: application/json" -d '{"name":"Jane Doe"}' http://localhost:8080/person/1
		.route("/person/:id", put(person::update))
		//curl -X DELETE http://localhost:8080/person/1
		.route("/person/:id", delete(person::delete))
		//curl -X GET http://localhost:8080/people
		.route("/people", get(person::list))
		.with_state(db)
}
