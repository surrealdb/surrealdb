use surrealdb::protocol::Ws;
use surrealdb::Surreal;

#[tokio::main]
async fn main() -> surrealdb::Result<()> {
	let db = Surreal::connect::<Ws>("localhost:8000").await?;

	let version = db.version().await?;

	println!("{version:?}");

	Ok(())
}
