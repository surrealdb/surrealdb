use serde::{Deserialize, Serialize};
use surrealdb::engine::remote::ws::Ws;
use surrealdb::opt::auth::Root;
use surrealdb::opt::Resource;
use surrealdb::sql::{Datetime, Id, Thing};
use surrealdb::Surreal;

// Dance classes table name
const DANCE: &str = "dance";
// Students table name
const STUDENT: &str = "student";

// Dance class table schema
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct DanceClass {
	id: Thing,
	name: String,
	created_at: Datetime,
}

// Student table schema
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct Student {
	id: Thing,
	name: String,
	classes: Vec<Thing>,
	created_at: Datetime,
}

// Student model with full class details
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
struct StudentClasses {
	id: Thing,
	name: String,
	classes: Vec<DanceClass>,
	created_at: Datetime,
}

#[tokio::main]
async fn main() -> surrealdb::Result<()> {
	// Connect to the database server
	let db = Surreal::new::<Ws>("localhost:8000").await?;

	// Sign in into the server
	db.signin(Root {
		username: "root",
		password: "root",
	})
	.await?;

	// Select the namespace and database to use
	db.use_ns("namespace").use_db("database").await?;

	// Create a dance class and store the result
	let classes: Vec<DanceClass> = db
		.create(DANCE)
		.content(DanceClass {
			id: Thing::from((DANCE, Id::rand())),
			name: "Introduction to Dancing".to_owned(),
			created_at: Datetime::default(),
		})
		.await?;

	// Create a student and assign them to the previous dance class
	// We don't care about the result here so we don't need to
	// type-hint and store it. We use `Resource::from` to return
	// a `sql::Value` instead and ignore it.
	db.create(Resource::from(STUDENT))
		.content(Student {
			id: Thing::from((STUDENT, Id::rand())),
			name: "Jane Doe".to_owned(),
			classes: classes.into_iter().map(|class| class.id).collect(),
			created_at: Datetime::default(),
		})
		.await?;

	// Prepare the SQL query to retrieve students and full class info
	let sql = format!("SELECT * FROM {STUDENT} FETCH classes");

	// Run the query
	let mut results = db.query(sql).await?;

	// Extract the first query statement result and deserialise it as a vector of students
	let students: Vec<StudentClasses> = results.take(0)?;

	// Use the result as you see fit. In this case we are simply pretty printing it.
	dbg!(students);

	Ok(())
}
