use crate::dbs::executor::Executor;
use crate::dbs::response::Responses;
use crate::dbs::session::Session;
use crate::err::Error;
use crate::sql;
use crate::sql::query::Query;
use crate::sql::value::Value;
use std::collections::HashMap;

pub type Variables = Option<HashMap<String, Value>>;

pub async fn execute(txt: &str, session: Session, vars: Variables) -> Result<Responses, Error> {
	// Create a new query executor
	let mut exe = Executor::new();
	// Create a new execution context
	let ctx = session.context();
	// Parse the SQL query text
	let ast = sql::parse(txt)?;
	// Process all statements
	exe.ns = session.ns;
	exe.db = session.db;
	exe.execute(ctx, ast).await
}

pub async fn process(ast: Query, session: Session, vars: Variables) -> Result<Responses, Error> {
	// Create a new query executor
	let mut exe = Executor::new();
	// Store session info on context
	let ctx = session.context();
	// Process all statements
	exe.ns = session.ns;
	exe.db = session.db;
	exe.execute(ctx, ast).await
}

pub fn export(session: Session) -> Result<String, Error> {
	// Create a new query executor
	let mut exe = Executor::new();
	// Create a new execution context
	let ctx = session.context();
	// Process database export
	exe.ns = session.ns;
	exe.db = session.db;
	exe.export(ctx)
}
