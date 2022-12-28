use surrealdb::param::Root;
use surrealdb::sql::statements::BeginStatement;
use surrealdb::sql::statements::CommitStatement;

#[tokio::main]
async fn main() -> surrealdb::Result<()> {
	let db = surrealdb::any::connect("ws://localhost:8000").await?;

	db.signin(Root {
		username: "root",
		password: "root",
	})
	.await?;

	db.use_ns("namespace").use_db("database").await?;

	#[rustfmt::skip]
    let response = db

        // Start transaction
        .query(BeginStatement)

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
        .query(CommitStatement)
        .await?;

	// See if any errors were returned
	response.check()?;

	Ok(())
}
