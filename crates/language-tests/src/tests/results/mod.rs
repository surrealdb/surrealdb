use std::any::Any;
use surrealdb_core::dbs::Response;
use surrealdb_core::err::Error as CoreError;
use surrealdb_core::syn::error::RenderedError;

/// The result of a test
#[derive(Debug)]
pub enum TestResult {
	ParserError(RenderedError),
	RunningError(CoreError),
	Import(String, String),
	Timeout,
	Results(Vec<Response>),
	Paniced(Box<dyn Any + Send + 'static>),
}
