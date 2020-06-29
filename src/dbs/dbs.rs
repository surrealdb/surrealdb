use crate::dbs::exe::Executor;
use crate::dbs::res::Responses;
use crate::err::Error;
use crate::sql;
use crate::sql::query::Query;
use ctx::Context;
use std::collections::HashMap;

type Vars<'a> = Option<HashMap<&'a str, String>>;

pub fn execute(txt: &str, vars: Vars) -> Result<Responses, Error> {
	// Parse the SQL query into an AST
	let ast = sql::parse(txt)?;
	// Create a new query executor
	let exe = Executor::new();
	// Process all of the queries
	exe.execute(ast)
}

pub fn process(ast: Query, vars: Vars) -> Result<Responses, Error> {
	// Create a new execution context
	// let ctx = None;
	// ctx.set("server.ip");
	// ctx.set("client.ip");
	// Create a new query executor
	let exe = Executor::new();
	// Process all of the queries
	exe.execute(ast)
}
