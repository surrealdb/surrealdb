mod error;
mod person;

use actix_web::{App, HttpServer};
use surrealdb::engines::remote::ws::Client;
use surrealdb::engines::remote::ws::Ws;
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
