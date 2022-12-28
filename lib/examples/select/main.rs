use serde::Deserialize;
use surrealdb::param::Root;

const ACCOUNT: &str = "account";

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct Account {
	id: String,
	balance: String,
}

#[tokio::main]
async fn main() -> surrealdb::Result<()> {
	let db = surrealdb::any::connect("ws://localhost:8000").await?;

	db.signin(Root {
		username: "root",
		password: "root",
	})
	.await?;

	db.use_ns("namespace").use_db("database").await?;

	let accounts: Vec<Account> = db.select(ACCOUNT).range("one".."two").await?;

	println!("{accounts:?}");

	Ok(())
}
