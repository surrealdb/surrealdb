mod error {
	use actix_web::{HttpResponse, ResponseError};
	use thiserror::Error;

	#[derive(Error, Debug)]
	pub enum Error {
		#[error("database error")]
		Db,
	}

	impl ResponseError for Error {
		fn error_response(&self) -> HttpResponse {
			match self {
				Error::Db => HttpResponse::InternalServerError().body(self.to_string()),
			}
		}
	}

	impl From<surrealdb::Error> for Error {
		fn from(error: surrealdb::Error) -> Self {
			eprintln!("{error}");
			Self::Db
		}
	}
}

mod person {
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
	pub async fn create(id: Path<String>, person: Json<Person>) -> Result<Json<Person>, Error> {
		let person = DB.create((PERSON, &*id)).content(person).await?;
		Ok(Json(person))
	}

	#[get("/person/{id}")]
	pub async fn read(id: Path<String>) -> Result<Json<Option<Person>>, Error> {
		let person = DB.select((PERSON, &*id)).await?;
		Ok(Json(person))
	}

	#[put("/person/{id}")]
	pub async fn update(id: Path<String>, person: Json<Person>) -> Result<Json<Person>, Error> {
		let person = DB.update((PERSON, &*id)).content(person).await?;
		Ok(Json(person))
	}

	#[delete("/person/{id}")]
	pub async fn delete(id: Path<String>) -> Result<Json<()>, Error> {
		DB.delete((PERSON, &*id)).await?;
		Ok(Json(()))
	}

	#[get("/people")]
	pub async fn list() -> Result<Json<Vec<Person>>, Error> {
		let people = DB.select(PERSON).await?;
		Ok(Json(people))
	}
}

use actix_web::{App, HttpServer};
use surrealdb::engine::remote::ws::Client;
use surrealdb::engine::remote::ws::Ws;
use surrealdb::opt::auth::Root;
use surrealdb::Surreal;

static DB: Surreal<Client> = Surreal::init();

#[actix_web::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
	DB.connect::<Ws>("localhost:8000").await?;

	DB.signin(Root {
		username: "root",
		password: "root",
	})
	.await?;

	DB.use_ns("namespace").use_db("database").await?;

	HttpServer::new(|| {
		App::new()
			.service(person::create)
			.service(person::read)
			.service(person::update)
			.service(person::delete)
			.service(person::list)
	})
	.bind(("localhost", 8080))?
	.run()
	.await?;

	Ok(())
}
