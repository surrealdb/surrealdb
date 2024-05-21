use surrealdb::engine::local::SurrealKV;
use surrealdb::Surreal;

#[tokio::main]
async fn main() -> surrealdb::Result<()> {
	let db = Surreal::new::<SurrealKV>("/tmp/sdk_test_sql1").await?;
	let _ = db.query("INFO FOR ROOT").await.unwrap().check().is_ok();
	Ok(())
}
