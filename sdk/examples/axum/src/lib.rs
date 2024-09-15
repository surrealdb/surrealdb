mod error;
mod person;

use axum::{
	routing::{delete, get, post, put},
	Router,
};
use surrealdb::engine::any::Any;
use surrealdb::Surreal;

pub fn create_router(db: Surreal<Any>) -> Router {
	Router::new()
		//curl -X POST -H "Content-Type: application/json" -d '{"name":"John Doe"}' http://localhost:8080/person/1
		.route("/person/:id", post(person::create))
		//curl -X GET http://localhost:8080/person/1
		.route("/person/:id", get(person::read))
		//curl -X PUT -H "Content-Type: application/json" -d '{"name":"Jane Doe"}' http://localhost:8080/person/1
		.route("/person/:id", put(person::update))
		//curl -X DELETE http://localhost:8080/person/1
		.route("/person/:id", delete(person::delete))
		//curl -X GET http://localhost:8080/people
		.route("/people", get(person::list))
		.with_state(db)
}
