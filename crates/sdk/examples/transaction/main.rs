use surrealdb::engine::remote::ws::Ws;
use surrealdb::opt::auth::Root;
use surrealdb::Surreal;

#[tokio::main]
async fn main() -> surrealdb::Result<()> {
	let db = Surreal::new::<Ws>("localhost:8000").await?;

	db.signin(Root {
		username: "root",
		password: "root",
	})
	.await?;

	db.use_ns("namespace").use_db("database").await?;

	#[rustfmt::skip]
    let response = db

        // Start transaction
        .query("BEGIN")

        // Setup accounts
        .query("
            CREATE account:one SET balance = 135605.16;
            CREATE account:two SET balance = 91031.31;
        ")

        // Move money
        .query("
            UPDATE account:one SET balance += 300.00;
            UPDATE account:two SET balance -= 300.00;
        ")

        // Finalise
        .query("COMMIT")
        .await?;

	// See if any errors were returned
	response.check()?;

	Ok(())
}
