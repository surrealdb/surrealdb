#[tokio::main]
async fn main() -> surrealdb::Result<()> {
	let db = surrealdb::any::connect("ws://localhost:8000").await?;

	let version = db.version().await?;

	println!("{version:?}");

	Ok(())
}
