use crate::dbs::Attach;
use crate::dbs::Executor;
use crate::dbs::Options;
use crate::dbs::Responses;
use crate::dbs::Session;
use crate::dbs::Variables;
use crate::err::Error;
use crate::kvs::Store;
use crate::sql;
use crate::sql::query::Query;
use hyper::body::Sender;
use std::sync::Arc;

pub async fn execute(
	db: Store,
	txt: &str,
	session: Session,
	vars: Variables,
) -> Result<Responses, Error> {
	// Create a new query options
	let mut opt = Options::default();
	// Create a new query executor
	let mut exe = Executor::new(db);
	// Create a new execution context
	let ctx = session.context();
	// Attach the defined variables
	let ctx = vars.attach(ctx);
	// Parse the SQL query text
	let ast = sql::parse(txt)?;
	// Process all statements
	opt.ns = session.ns.map(Arc::new);
	opt.db = session.db.map(Arc::new);
	exe.execute(ctx, opt, ast).await
}

pub async fn process(
	db: Store,
	ast: Query,
	session: Session,
	vars: Variables,
) -> Result<Responses, Error> {
	// Create a new query options
	let mut opt = Options::default();
	// Create a new query executor
	let mut exe = Executor::new(db);
	// Store session info on context
	let ctx = session.context();
	// Attach the defined variables
	let ctx = vars.attach(ctx);
	// Process all statements
	opt.ns = session.ns.map(Arc::new);
	opt.db = session.db.map(Arc::new);
	exe.execute(ctx, opt, ast).await
}

pub async fn export(db: Store, session: Session, sender: Sender) -> Result<(), Error> {
	// Create a new query options
	let mut opt = Options::default();
	// Create a new query executor
	let mut exe = Executor::new(db);
	// Create a new execution context
	let ctx = session.context();
	// Process database export
	opt.ns = session.ns.map(Arc::new);
	opt.db = session.db.map(Arc::new);
	exe.export(ctx, opt, sender).await
}
