use futures::StreamExt;
use serde::Deserialize;
use surrealdb::engine::remote::ws::Ws;
use surrealdb::opt::auth::Root;
use surrealdb::Notification;
use surrealdb::RecordId;
use surrealdb::Result;
use surrealdb::Surreal;

const ACCOUNT: &str = "account";

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct Account {
	id: RecordId,
	balance: String,
}

#[tokio::main]
async fn main() -> surrealdb::Result<()> {
	let db = Surreal::new::<Ws>("localhost:8000").await?;

	db.signin(Root {
		username: "root",
		password: "root",
	})
	.await?;

	db.use_ns("namespace").use_db("database").await?;

	let mut accounts = db.select(ACCOUNT).range("one".."two").live().await?;

	while let Some(notification) = accounts.next().await {
		print(notification);
	}

	Ok(())
}

fn print(result: Result<Notification<Account>>) {
	match result {
		Ok(notification) => {
			let action = notification.action;
			let account = notification.data;
			println!("{action:?}: {account:?}");
		}
		Err(error) => eprintln!("{error}"),
	}
}
