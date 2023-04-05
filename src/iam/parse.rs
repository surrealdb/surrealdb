use crate::err::Error;
use crate::iam::base::{Engine, BASE64};
use std::str;
use surrealdb::sql::json;
use surrealdb::sql::Value;

pub fn parse(value: &str) -> Result<Value, Error> {
	trace!("parse value: {}", value);
	// Extract the middle part of the token
	let value = value.splitn(3, '.').skip(1).take(1).next().ok_or(Error::InvalidAuth);
	if value.is_err() {
		error!("invalid auth token middle part");
	}
	let value = value.unwrap();
	// Decode the base64 token data content
	let value = BASE64.decode(value).map_err(|_| Error::InvalidAuth);
	if value.is_err() {
		error!("invalid auth token b64 content");
	}
	let value = value.unwrap();
	// Convert the decoded data to a string
	let value = str::from_utf8(&value).map_err(|_| Error::InvalidAuth);
	if value.is_err() {
		error!("invalid auth token data string content");
	}
	let value = value.unwrap();
	// Parse the token data into SurrealQL
	json(value).map_err(|_| Error::InvalidAuth)
}
