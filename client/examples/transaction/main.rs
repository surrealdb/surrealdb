use surrealdb::sql::statements::BeginStatement;
use surrealdb::sql::statements::CommitStatement;
use surrealdb_rs::param::Root;
use surrealdb_rs::protocol::Ws;
use surrealdb_rs::Surreal;

#[tokio::main]
async fn main() -> surrealdb_rs::Result<()> {
	tracing_subscriber::fmt::init();

	let client = Surreal::connect::<Ws>("localhost:8000").await?;

	client
		.signin(Root {
			username: "root",
			password: "root",
		})
		.await?;

	client.use_ns("namespace").use_db("database").await?;

	let results = client
		// Start transaction
		.query(BeginStatement)
		// Setup accounts
		.query("CREATE account:one SET balance = 135605.16")
		.query("CREATE account:two SET balance = 91031.31")
		// Move money
		.query("UPDATE account:one SET balance += 300.00")
		.query("UPDATE account:two SET balance -= 300.00")
		// Finalise
		.query(CommitStatement)
		.await?;

	for result in results {
		let response = result?;
		tracing::info!("{response:?}");
	}

	Ok(())
}
