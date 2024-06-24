use surrealdb::engine::remote::ws::Ws;
use surrealdb::Surreal;

#[tokio::main]
async fn main() -> surrealdb::Result<()> {
	let db = Surreal::new::<Ws>("localhost:8000").await?;
	let _ = db.query("INFO FOR ROOT").await.unwrap().check().is_ok();
	Ok(())
}
