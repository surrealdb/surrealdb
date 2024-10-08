use axum_example::create_router;
use std::env;
use surrealdb::engine::any;
use surrealdb::opt::auth::Root;
use surrealdb::opt::Config;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
	// Allow the endpoint to be configured via a `SURREALDB_ENDPOINT` environment variable
	// or fallback to memory. This makes it possible to configure the endpoint at runtime.
	let endpoint = env::var("SURREALDB_ENDPOINT").unwrap_or_else(|_| "memory".to_owned());

	// Define the root user.
	let root = Root {
		username: "root",
		password: "root",
	};

	// Activate authentication on local engines by supplying the root user to be used.
	let config = Config::new().user(root);

	// Create the database connection.
	let db = any::connect((endpoint, config)).await?;

	// Sign in as root.
	db.signin(root).await?;

	// Configure the namespace amd database to use.
	db.use_ns("namespace").use_db("database").await?;

	// Configure and start the Axum server.
	let listener = TcpListener::bind("localhost:8080").await?;
	let router = create_router(db);
	axum::serve(listener, router).await?;

	Ok(())
}
