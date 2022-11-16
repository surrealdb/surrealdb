use surrealdb_rs::net::WsClient;
use surrealdb_rs::protocol::Ws;
use surrealdb_rs::StaticClient;
use surrealdb_rs::Surreal;
use tokio::sync::mpsc;

static CLIENT: Surreal<WsClient> = Surreal::new();

const NUM: usize = 100_000;

#[tokio::main]
async fn main() -> surrealdb_rs::Result<()> {
	tracing_subscriber::fmt::init();

	CLIENT.connect::<Ws>("localhost:8000").with_capacity(NUM).await?;

	CLIENT.use_ns("namespace").use_db("database").await?;

	let (tx, mut rx) = mpsc::channel::<()>(1);

	for idx in 0..NUM {
		let sender = tx.clone();
		tokio::spawn(async move {
			let mut result = CLIENT.query("SELECT * FROM $idx").bind("idx", idx).await.unwrap();
			let db_idx = result.remove(0).unwrap().remove(0);
			tracing::info!("{idx}: {db_idx}");
			drop(sender);
		});
	}

	drop(tx);

	rx.recv().await;

	Ok(())
}
