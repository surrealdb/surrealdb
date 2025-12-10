mod api;
mod controller;
pub mod events;
mod method;
pub mod utils;
#[macro_use]
pub(crate) mod mac;

pub use api::Surreal;
use surrealdb_types::RecordId;

async fn main() {
	let surreal = Surreal::new();
	surreal.connect("ws://localhost:8000").await.unwrap();
	surreal.query("SELECT * FROM user").var("name", "John Doe").await.unwrap();
	surreal.r#use().default().await.unwrap();
	surreal
		.select(RecordId::new("user", 123))
		.fields(vec!["name", "email"])
		.limit(10)
		.start(0)
		.await
		.unwrap();

	let tx = surreal.begin_transaction().await.unwrap();
	tx.select(RecordId::new("user", 123))
		.fields(vec!["name", "email"])
		.limit(10)
		.start(0)
		.await
		.unwrap();
}
