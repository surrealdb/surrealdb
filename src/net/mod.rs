mod conf;
mod export;
mod fail;
mod head;
mod import;
mod key;
mod log;
mod output;
mod root;
mod signin;
mod signup;
mod sql;
mod status;
mod sync;
mod version;

use crate::err::Error;
use crate::kvs::Datastore;
use once_cell::sync::OnceCell;
use std::net::SocketAddr;
use std::sync::Arc;
use uuid::Uuid;
use warp::Filter;

const ID: &'static str = "Request-Id";

static DB: OnceCell<Arc<Datastore>> = OnceCell::new();

#[tokio::main]
pub async fn init(bind: &str, path: &str) -> Result<(), Error> {
	// Parse the desired binding socket address
	let adr: SocketAddr = bind.parse().expect("Unable to parse socket address");
	// Parse and setup desired datastore
	let dbs = Datastore::new(path).await.expect("Unable to parse datastore path");
	// Store database instance
	let _ = DB.set(Arc::new(dbs));
	// Setup web routes
	let net = root::config()
		// Version endpoint
		.or(version::config())
		// Status endpoint
		.or(status::config())
		// Signup endpoint
		.or(signup::config())
		// Signin endpoint
		.or(signin::config())
		// Export endpoint
		.or(export::config())
		// Import endpoint
		.or(import::config())
		// Backup endpoint
		.or(sync::config())
		// SQL query endpoint
		.or(sql::config())
		// API query endpoint
		.or(key::config())
		// Catch all errors
		.recover(fail::recover)
		// End routes setup
	;
	// Enable response compression
	let net = net.with(warp::compression::gzip());
	// Specify a generic version header
	let net = net.with(head::version());
	// Specify a generic server header
	let net = net.with(head::server());
	// Specify an ID for each request
	let net = net.map(|reply| {
		let val = Uuid::new_v4().to_string();
		warp::reply::with_header(reply, ID, val)
	});
	// Log all requests to the console
	let net = net.with(log::write());

	info!("Starting web server on {}", adr);

	warp::serve(net).run(adr).await;

	Ok(())
}
