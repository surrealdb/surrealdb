use surrealdb::engine::local::Mem;
use surrealdb::Surreal;

#[tokio::main]
async fn main() -> surrealdb::Result<()> {
	let db = Surreal::new::<Mem>(()).await?;
	let _ = db.query("INFO FOR ROOT").await.unwrap().check().is_ok();
	Ok(())
}
