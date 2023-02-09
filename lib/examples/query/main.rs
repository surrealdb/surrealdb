use serde::Deserialize;
use serde::Serialize;
use surrealdb::engine::remote::ws::Ws;
use surrealdb::opt::auth::Root;
use surrealdb::Surreal;

#[derive(Debug, Serialize, Deserialize)]
#[allow(dead_code)]
struct User {
	id: String,
	name: String,
	company: String,
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

	let sql = "CREATE user SET name = $name, company = $company";

	let mut results = db
		.query(sql)
		.bind(User {
			id: "john".to_owned(),
			name: "John Doe".to_owned(),
			company: "ACME Corporation".to_owned(),
		})
		.await?;

	// print the created user:
	let user: Option<User> = results.take(0)?;
	println!("{user:?}");

	let mut response = db.query("SELECT * FROM user WHERE name.first = 'John'").await?;

	// print all users:
	let users: Vec<User> = response.take(0)?;
	println!("{users:?}");

	Ok(())
}
