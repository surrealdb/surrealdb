use rocket::serde::{json::Json, Deserialize, Serialize};
use rocket::State;
use rocket::response::status::Custom;
use rocket::http::Status;
use std::sync::Arc;
use surrealdb::engine::any::Any;
use surrealdb::Surreal;
use crate::error::Error; // Ensure this is correctly imported
use rocket::response::status;

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
    db.create((PERSON, &*id)).content(person_data.into_inner()).await
        .map_err(|e| Custom(Status::InternalServerError, e.to_string()))
        .map(Json)
}

#[get("/person/<id>")]
pub async fn read(
    db: &Db,
    id: String,
) -> Result<Json<Option<Person>>, Custom<String>> {
    db.select((PERSON, &*id)).await
        .map_err(|e| Custom(Status::InternalServerError, e.to_string()))
        .map(Json)
}

#[put("/person/<id>", data = "<person_data>")]
pub async fn update(
    db: &Db,
    id: String,
    person_data: Json<Person>,
) -> Result<Json<Option<Person>>, Custom<String>> {
    db.update((PERSON, &*id)).content(person_data.into_inner()).await
        .map_err(|e| Custom(Status::InternalServerError, e.to_string()))
        .map(Json)
}

#[delete("/person/<id>")]
pub async fn delete(
    db: &Db,
    id: String,
) -> Result<status::Custom<Json<&str>>, Custom<String>> {
    db.delete((PERSON, &*id)).await
        .map_err(|e| Custom(Status::InternalServerError, e.to_string()))
        // Return a simple confirmation message upon successful deletion
        .map(|_| Custom(Status::Ok, Json("Deleted")))
}



#[get("/people")]
pub async fn list(
    db: &Db,
) -> Result<Json<Vec<Person>>, Custom<String>> {
    db.select(PERSON).await
        .map_err(|e| Custom(Status::InternalServerError, e.to_string()))
        .map(Json)
}
