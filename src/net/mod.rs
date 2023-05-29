mod export;
mod fail;
mod head;
mod health;
mod import;
mod index;
mod input;
mod key;
mod limiter;
mod log;
mod output;
mod params;
mod rpc;
mod session;
mod signals;
mod signin;
mod signup;
mod sql;
mod status;
mod sync;
mod version;

use self::limiter::LimiterOptions;
use crate::cli::CF;
use crate::err::Error;
use clap::Args;
use warp::Filter;

const LOG: &str = "surrealdb::net";

#[derive(Args, Debug)]
pub struct NetOptions {
	#[command(flatten)]
	limiter: LimiterOptions,
}

pub async fn init(
	NetOptions {
		limiter,
	}: NetOptions,
) -> Result<(), Error> {
	// Configure rate limiting
	limiter::init(limiter)?;

	// Setup web routes
	let net = index::config()
		// Version endpoint
		.or(version::config())
		// Status endpoint
		.or(status::config())
		// Health endpoint
		.or(health::config())
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
	// Trace requests
	let net = net.with(warp::trace::request());

	// Get local copy of options
	let opt = CF.get().unwrap();

	info!(target: LOG, "Starting web server on {}", &opt.bind);

	if let (Some(c), Some(k)) = (&opt.crt, &opt.key) {
		// Bind the server to the desired port
		let (adr, srv) = warp::serve(net)
			.tls()
			.cert_path(c)
			.key_path(k)
			.bind_with_graceful_shutdown(opt.bind, async move {
				// Capture the shutdown signals and log that the graceful shutdown has started
				let result = signals::listen().await.expect("Failed to listen to shutdown signal");
				info!(target: LOG, "{} received. Start graceful shutdown...", result);
			});
		// Log the server startup status
		info!(target: LOG, "Started web server on {}", &adr);
		// Run the server forever
		srv.await;
		// Log the server shutdown event
		info!(target: LOG, "Shutdown complete. Bye!")
	} else {
		// Bind the server to the desired port
		let (adr, srv) = warp::serve(net).bind_with_graceful_shutdown(opt.bind, async move {
			// Capture the shutdown signals and log that the graceful shutdown has started
			let result = signals::listen().await.expect("Failed to listen to shutdown signal");
			info!(target: LOG, "{} received. Start graceful shutdown...", result);
		});
		// Log the server startup status
		info!(target: LOG, "Started web server on {}", &adr);
		// Run the server forever
		srv.await;
		// Log the server shutdown event
		info!(target: LOG, "Shutdown complete. Bye!")
	};

	Ok(())
}
