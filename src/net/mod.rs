mod export;
mod fail;
mod head;
mod import;
mod index;
mod key;
mod log;
mod output;
mod rpc;
mod session;
mod signin;
mod signup;
mod sql;
mod status;
mod sync;
mod version;

use crate::cli::CF;
use crate::err::Error;
use warp::Filter;

const LOG: &str = "surrealdb::net";

pub async fn init() -> Result<(), Error> {
	// Setup web routes
	let net = index::config()
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
		// RPC query endpoint
		.or(rpc::config())
		// SQL query endpoint
		.or(sql::config())
		// API query endpoint
		.or(key::config())
		// Catch all errors
		.recover(fail::recover)
		// End routes setup
	;
	// Specify a generic version header
	let net = net.with(head::version());
	// Specify a generic server header
	let net = net.with(head::server());
	// Set cors headers on all requests
	let net = net.with(head::cors());
	// Log all requests to the console
	let net = net.with(log::write());

	// Get local copy of options
	let opt = CF.get().unwrap();

	info!(target: LOG, "Starting web server on {}", &opt.bind);

	info!(target: LOG, "Started web server on {}", &opt.bind);

	if let (Some(crt), Some(key)) = (&opt.crt, &opt.key) {
		warp::serve(net).tls().cert_path(crt).key_path(key).run(opt.bind).await
	} else {
		warp::serve(net).run(opt.bind).await
	};

	Ok(())
}
