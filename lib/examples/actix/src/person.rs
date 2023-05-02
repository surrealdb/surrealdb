use crate::error::Error;
use crate::DB;
use actix_web::web::Json;
use actix_web::web::Path;
use actix_web::{delete, get, post, put};
use serde::Deserialize;
use serde::Serialize;

const PERSON: &str = "person";

#[derive(Serialize, Deserialize)]
pub struct Person {
	name: String,
}

#[post("/person/{id}")]
pub async fn create(id: Path<String>, person: Json<Person>) -> Result<Json<Option<Person>>, Error> {
	let person = DB.create((PERSON, &*id)).content(person).await?;
	Ok(Json(person))
}

#[get("/person/{id}")]
pub async fn read(id: Path<String>) -> Result<Json<Option<Person>>, Error> {
	let person = DB.select((PERSON, &*id)).await?;
	Ok(Json(person))
}

#[put("/person/{id}")]
pub async fn update(id: Path<String>, person: Json<Person>) -> Result<Json<Option<Person>>, Error> {
	let person = DB.update((PERSON, &*id)).content(person).await?;
	Ok(Json(person))
}

#[delete("/person/{id}")]
pub async fn delete(id: Path<String>) -> Result<Json<Option<Person>>, Error> {
	let person = DB.delete((PERSON, &*id)).await?;
	Ok(Json(person))
}

#[get("/people")]
pub async fn list() -> Result<Json<Vec<Person>>, Error> {
	let people = DB.select(PERSON).await?;
	Ok(Json(people))
}
