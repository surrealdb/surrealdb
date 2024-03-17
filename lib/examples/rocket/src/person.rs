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

type Db = State<Surreal<Any>>;

const PERSON: &str = "person";

#[post("/person/<id>", data = "<person_data>")]
pub async fn create(
	db: &Db,
	id: String,
	person_data: Json<Person>,
) -> Result<Json<Option<Person>>, Custom<String>> {
	db.create((PERSON, &*id))
		.content(person_data.into_inner())
		.await
		.map_err(|e| Custom(Status::InternalServerError, e.to_string()))
		.map(Json)
}

#[get("/person/<id>")]
pub async fn read(db: &Db, id: String) -> Result<Json<Option<Person>>, Custom<String>> {
	db.select((PERSON, &*id))
		.await
		.map_err(|e| Custom(Status::InternalServerError, e.to_string()))
		.map(Json)
}

#[put("/person/<id>", data = "<person_data>")]
pub async fn update(
	db: &Db,
	id: String,
	person_data: Json<Person>,
) -> Result<Json<Option<Person>>, Custom<String>> {
	db.update((PERSON, &*id))
		.content(person_data.into_inner())
		.await
		.map_err(|e| Custom(Status::InternalServerError, e.to_string()))
		.map(Json)
}

#[get("/people")]
pub async fn list(db: &Db) -> Result<Json<Vec<Person>>, Custom<String>> {
	db.select(PERSON)
		.await
		.map_err(|e| Custom(Status::InternalServerError, e.to_string()))
		.map(Json)
}
