use crate::dbs::Attach;
use crate::dbs::Executor;
use crate::dbs::Options;
use crate::dbs::Response;
use crate::dbs::Session;
use crate::dbs::Variables;
use crate::err::Error;
use crate::kvs::Store;
use crate::sql;
use crate::sql::query::Query;
use std::sync::Arc;

pub async fn execute(
	kvs: Store,
	txt: &str,
	session: Session,
	vars: Variables,
) -> Result<Vec<Response>, Error> {
	// Create a new query options
	let mut opt = Options::default();
	// Create a new query executor
	let mut exe = Executor::new(kvs);
	// Create a new execution context
	let ctx = session.context();
	// Attach the defined variables
	let ctx = vars.attach(ctx);
	// Parse the SQL query text
	let ast = sql::parse(txt)?;
	// Process all statements
	opt.auth = Arc::new(session.au);
	opt.ns = session.ns.map(Arc::new);
	opt.db = session.db.map(Arc::new);
	exe.execute(ctx, opt, ast).await
}

pub async fn process(
	kvs: Store,
	ast: Query,
	session: Session,
	vars: Variables,
) -> Result<Vec<Response>, Error> {
	// Create a new query options
	let mut opt = Options::default();
	// Create a new query executor
	let mut exe = Executor::new(kvs);
	// Store session info on context
	let ctx = session.context();
	// Attach the defined variables
	let ctx = vars.attach(ctx);
	// Process all statements
	opt.auth = Arc::new(session.au);
	opt.ns = session.ns.map(Arc::new);
	opt.db = session.db.map(Arc::new);
	exe.execute(ctx, opt, ast).await
}
