use crate::ctx::Context;
use crate::dbs::executor::Executor;
use crate::dbs::response::Responses;
use crate::err::Error;
use crate::sql;
use crate::sql::query::Query;
use std::collections::HashMap;

pub type Vars<'a> = Option<HashMap<&'a str, String>>;

pub fn execute(txt: &str, vars: Vars) -> Result<Responses, Error> {
	// Parse the SQL query into an AST
	let ast = sql::parse(txt)?;
	// Create a new execution context
	let ctx = Context::background().freeze();
	// Create a new query executor
	let exe = Executor::new();
	// Process all of the queries
	exe.execute(&ctx, ast)
}

pub fn process(ast: Query, vars: Vars) -> Result<Responses, Error> {
	// Create a new execution context
	let ctx = Context::background().freeze();
	// Create a new query executor
	let exe = Executor::new();
	// Process all of the queries
	exe.execute(&ctx, ast)
}
