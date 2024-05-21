use surrealdb::engine::local::TiKv;
use surrealdb::Surreal;

#[tokio::main]
async fn main() -> surrealdb::Result<()> {
	let db = Surreal::new::<TiKv>("localhost:2379").await?;
	let _ = db.query("INFO FOR ROOT").await.unwrap().check().is_ok();
	Ok(())
}
