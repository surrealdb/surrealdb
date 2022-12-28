mod error;
mod person;

use actix_web::{App, HttpServer};
use surrealdb::net::WsClient;
use surrealdb::opt::auth::Root;
use surrealdb::protocol::Ws;
use surrealdb::StaticConnect;
use surrealdb::Surreal;

static DB: Surreal<WsClient> = Surreal::new();

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
