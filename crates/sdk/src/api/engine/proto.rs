use surrealdb_types::{SurrealValue, Value};

#[derive(Debug, SurrealValue)]
#[surreal(untagged, uppercase)]
#[doc(hidden)]
#[non_exhaustive]
pub enum Status {
	Ok,
	Err,
}

#[derive(Debug, SurrealValue)]
#[doc(hidden)]
#[non_exhaustive]
pub struct QueryMethodResponse {
	pub time: String,
	pub status: Status,
	pub result: Value,
}
