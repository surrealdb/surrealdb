use serde::Deserialize;
use surrealdb_rs::param::Root;
use surrealdb_rs::protocol::Ws;
use surrealdb_rs::Surreal;

const ACCOUNT: &str = "account";

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct Account {
	id: String,
	balance: String,
}

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

	let accounts: Vec<Account> = client.select(ACCOUNT).range("one".."two").await?;

	tracing::info!("{accounts:?}");

	Ok(())
}
