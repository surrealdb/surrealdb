use crate::dbs::executor::Executor;
use crate::dbs::response::Responses;
use crate::dbs::session::Session;
use crate::dbs::variables::Attach;
use crate::dbs::variables::Variables;
use crate::err::Error;
use crate::sql;
use crate::sql::query::Query;
use hyper::body::Sender;

pub async fn execute(txt: &str, session: Session, vars: Variables) -> Result<Responses, Error> {
	// Create a new query executor
	let mut exe = Executor::new();
	// Create a new execution context
	let ctx = session.context();
	// Attach the defined variables
	let ctx = vars.attach(ctx);
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
	// Attach the defined variables
	let ctx = vars.attach(ctx);
	// Process all statements
	exe.ns = session.ns;
	exe.db = session.db;
	exe.execute(ctx, ast).await
}

pub async fn export(session: Session, sender: Sender) -> Result<(), Error> {
	// Create a new query executor
	let mut exe = Executor::new();
	// Create a new execution context
	let ctx = session.context();
	// Process database export
	exe.ns = session.ns;
	exe.db = session.db;
	exe.export(ctx, sender).await
}
