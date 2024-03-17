use rocket::http::Status;
use rocket::response::{self, Responder, Response};
use rocket::Request;
use serde_json::json;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
	#[error("database error")]
	Db,
}

impl<'r> Responder<'r, 'static> for Error {
	fn respond_to(self, _: &'r Request<'_>) -> response::Result<'static> {
		let error_message = json!({ "error": self.to_string() });
		Response::build()
			.status(Status::InternalServerError)
			.header(rocket::http::ContentType::JSON)
			.sized_body(
				error_message.to_string().len(),
				std::io::Cursor::new(error_message.to_string()),
			)
			.ok()
	}
}

impl From<surrealdb::Error> for Error {
	fn from(error: surrealdb::Error) -> Self {
		eprintln!("{error}");
		Self::Db
	}
}
