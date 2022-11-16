use serde::Deserialize;
use surrealdb_rs::param::from_value;
use surrealdb_rs::param::Root;
use surrealdb_rs::protocol::Ws;
use surrealdb_rs::Surreal;

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct User {
	id: String,
	name: String,
	company: String,
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

	let mut results = client
		.query("CREATE user SET name = $name, company = $company")
		.bind("name", "John Doe")
		.bind("company", "ACME Corporation")
		.await?;

	let value = results.remove(0)?.remove(0);
	let user: User = from_value(&value)?;
	tracing::info!("{user:?}");

	Ok(())
}
