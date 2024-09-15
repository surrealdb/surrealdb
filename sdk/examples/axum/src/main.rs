use axum_example::create_router;
use std::env;
use surrealdb::engine::any;
use surrealdb::opt::auth::Root;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
	let endpoint = env::var("SURREALDB_ENDPOINT").unwrap_or_else(|_| "memory".to_owned());
	let db = any::connect(endpoint).await?;

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
