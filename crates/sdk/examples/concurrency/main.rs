use std::sync::LazyLock;
use surrealdb::engine::remote::ws::Client;
use surrealdb::engine::remote::ws::Ws;
use surrealdb::Surreal;
use tokio::sync::mpsc;

static DB: LazyLock<Surreal<Client>> = LazyLock::new(Surreal::init);

const NUM: usize = 100_000;

#[tokio::main]
async fn main() -> surrealdb::Result<()> {
	DB.connect::<Ws>("localhost:8000").with_capacity(NUM).await?;

	DB.use_ns("namespace").use_db("database").await?;

	let (tx, mut rx) = mpsc::channel::<()>(1);

	for idx in 0..NUM {
		let sender = tx.clone();
		tokio::spawn(async move {
			let mut result = DB.query("SELECT * FROM $idx").bind(("idx", idx)).await.unwrap();

			let db_idx: Option<usize> = result.take(0).unwrap();
			if let Some(db_idx) = db_idx {
				println!("{idx}: {db_idx}");
			}

			drop(sender);
		});
	}

	drop(tx);

	rx.recv().await;

	Ok(())
}
