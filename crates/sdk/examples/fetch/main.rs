use serde::{Deserialize, Serialize};
use surrealdb::engine::remote::ws::Ws;
use surrealdb::opt::auth::Root;
use surrealdb::opt::Resource;
use surrealdb::Surreal;
use surrealdb::{Datetime, RecordId};

// Dance classes table name
const DANCE: &str = "dance";
// Students table name
const STUDENT: &str = "student";

// Dance class table schema
#[derive(Debug, Serialize, Deserialize)]
struct DanceClass {
	id: RecordId,
	name: String,
	created_at: Datetime,
}

// Student table schema
#[derive(Debug, Serialize)]
struct Student {
	id: RecordId,
	name: String,
	classes: Vec<RecordId>,
	created_at: Datetime,
}

// Student model with full class details
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct StudentClasses {
	id: RecordId,
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
	let classes: Option<DanceClass> = db
		.create(DANCE)
		.content(DanceClass {
			id: RecordId::from((DANCE, "dc101")),
			name: "Introduction to Dancing".to_owned(),
			created_at: Datetime::default(),
		})
		.await?;

	// Create a student and assign her to the previous dance class
	// We don't care about the result here so we don't need to
	// type-hint and store it. We use `Resource::from` to return
	// a `sql::Value` instead and ignore it.
	db.create(Resource::from(STUDENT))
		.content(Student {
			id: RecordId::from((STUDENT, "jane")),
			name: "Jane Doe".to_owned(),
			classes: classes.into_iter().map(|class| class.id).collect(),
			created_at: Datetime::default(),
		})
		.await?;

	// Prepare the query to retrieve students and full class info
	let q = format!("SELECT * FROM {STUDENT} FETCH classes");

	// Run the query
	let mut results = db.query(q).await?;

	// Extract the first query statement result and deserialise it as a vector of students
	let students: Vec<StudentClasses> = results.take(0)?;

	// Use the result as you see fit. In this case we are simply pretty printing it.
	println!("Students = {:?}", students);

	Ok(())
}
