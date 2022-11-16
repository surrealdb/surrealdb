use surrealdb_rs::protocol::Ws;
use surrealdb_rs::Surreal;

#[tokio::main]
async fn main() -> surrealdb_rs::Result<()> {
	tracing_subscriber::fmt::init();

	let client = Surreal::connect::<Ws>("localhost:8000").await?;

	let version = client.version().await?;

	tracing::info!("{version:?}");

	Ok(())
}
