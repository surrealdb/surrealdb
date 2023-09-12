use futures::StreamExt;
use serde::Deserialize;
use std::collections::HashMap;
use surrealdb::engine::remote::ws::Ws;
use surrealdb::method::Notification;
use surrealdb::opt::auth::Root;
use surrealdb::sql::Thing;
use surrealdb::Result;
use surrealdb::Surreal;

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct Account {
	id: Thing,
	#[serde(flatten)]
	extra: HashMap<String, serde_json::Value>,
}

#[tokio::main]
async fn main() -> surrealdb::Result<()> {
	tracing_subscriber::fmt::init();

	let db = Surreal::new::<Ws>("localhost:8000").await?;

	db.signin(Root {
		username: "root",
		password: "root",
	})
	.await?;

	db.use_ns("test").use_db("test").await?;

	let mut accounts = db.select("accounts").live().await?;

	while let Some(notification) = accounts.next().await {
		print(notification);
	}

	Ok(())
}

fn print(result: Result<Notification<Account>>) {
	match result {
		Ok(notification) => {
			let action = notification.action;
			let id = notification.data.id;
			let extra = notification.data.extra;
			println!("{action}: {id}; {extra:?}");
		}
		Err(error) => eprintln!("{error}"),
	}
}
