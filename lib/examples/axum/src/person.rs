use crate::error::Error;
use axum::extract::Path;
use axum::extract::State;
use axum::Json;
use serde::Deserialize;
use serde::Serialize;
use surrealdb::engine::any::Any;
use surrealdb::Surreal;

const PERSON: &str = "person";

type Db = State<Surreal<Any>>;

#[derive(Serialize, Deserialize)]
pub struct Person {
	name: String,
}

pub async fn create(
	db: Db,
	id: Path<String>,
	Json(person): Json<Person>,
) -> Result<Json<Option<Person>>, Error> {
	let person = db.create((PERSON, &*id)).content(person).await?;
	Ok(Json(person))
}

pub async fn read(db: Db, id: Path<String>) -> Result<Json<Option<Person>>, Error> {
	let person = db.select((PERSON, &*id)).await?;
	Ok(Json(person))
}

pub async fn update(
	db: Db,
	id: Path<String>,
	Json(person): Json<Person>,
) -> Result<Json<Option<Person>>, Error> {
	let person = db.update((PERSON, &*id)).content(person).await?;
	Ok(Json(person))
}

pub async fn delete(db: Db, id: Path<String>) -> Result<Json<Option<Person>>, Error> {
	let person = db.delete((PERSON, &*id)).await?;
	Ok(Json(person))
}

pub async fn list(db: Db) -> Result<Json<Vec<Person>>, Error> {
	let people = db.select(PERSON).await?;
	Ok(Json(people))
}
