mod error;
mod person;

use actix_web::{App, HttpServer};
use surrealdb::any::Any;
use surrealdb::any::StaticConnect;
use surrealdb::param::Root;
use surrealdb::Surreal;

static DB: Surreal<Any> = Surreal::new();

#[actix_web::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
	DB.connect("ws://localhost:8000").await?;

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
	.bind(("127.0.0.1", 8080))?
	.run()
	.await?;

	Ok(())
}
