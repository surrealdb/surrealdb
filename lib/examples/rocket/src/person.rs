use std::sync::Arc;

use rocket::http::Status;
use rocket::response::status::Custom;
use rocket::serde::{json::Json, Deserialize, Serialize};
use rocket::State;
use surrealdb::engine::any::Any;
use surrealdb::Surreal;

#[derive(Deserialize, Serialize)]
#[serde(crate = "rocket::serde")]
pub struct Person {
	name: String,
}

type Db = Arc<Surreal<Any>>;

const PERSON: &str = "person";

// curl -X POST -H "Content-Type: application/json" -d '{"name":"John Doe"}' http://localhost:8080/person/1
#[post("/person/<id>", data = "<person_data>")]
pub async fn create(
	db: &State<Db>,
	id: String,
	person_data: Json<Person>,
) -> Result<Json<Option<Person>>, Custom<String>> {
	db.create((PERSON, &*id))
		.content(person_data.into_inner())
		.await
		.map_err(|e| Custom(Status::InternalServerError, e.to_string()))
		.map(Json)
}
// curl -X DELETE http://localhost:8080/person/1
#[delete("/person/<id>")]
pub async fn delete(db: &State<Db>, id: String) -> Result<Json<Option<Person>>, Custom<String>> {
	db.delete((PERSON, &*id))
		.await
		.map_err(|e| Custom(Status::InternalServerError, e.to_string()))
		.map(Json)
}

// curl -X GET http://localhost:8080/person/1
#[get("/person/<id>")]
pub async fn read(db: &State<Db>, id: String) -> Result<Json<Option<Person>>, Custom<String>> {
	db.select((PERSON, &*id))
		.await
		.map_err(|e| Custom(Status::InternalServerError, e.to_string()))
		.map(Json)
}

// curl -X PUT -H "Content-Type: application/json" -d '{"name":"Jane Doe"}' http://localhost:8080/person/1
#[put("/person/<id>", data = "<person_data>")]
pub async fn update(
	db:  &State<Db>,
	id: String,
	person_data: Json<Person>,
) -> Result<Json<Option<Person>>, Custom<String>> {
	db.update((PERSON, &*id))
		.content(person_data.into_inner())
		.await
		.map_err(|e| Custom(Status::InternalServerError, e.to_string()))
		.map(Json)
}

// curl http://localhost:8080/people
#[get("/people")]
pub async fn list(db: &State<Db>) -> Result<Json<Vec<Person>>, Custom<String>> {
	db.select(PERSON)
		.await
		.map_err(|e| Custom(Status::InternalServerError, e.to_string()))
		.map(Json)
}
