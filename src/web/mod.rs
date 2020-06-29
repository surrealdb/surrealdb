mod export;
mod head;
mod import;
mod key;
mod log;
mod root;
mod signin;
mod signup;
mod sql;
mod status;
mod sync;
mod version;

use failure::Error;
use std::net::SocketAddr;
use warp::Filter;

#[tokio::main]
pub async fn init(opts: &clap::ArgMatches) -> Result<(), Error> {
	//
	let adr = opts.value_of("bind").unwrap();
	//
	let adr: SocketAddr = adr.parse().expect("Unable to parse socket address");

	let web = root::config()
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
		// Endpoints for sql queries
		.or(sql::config())
		// Key for key queries
		.or(key::config())
		// End routes setup
	;

	let web = web.with(warp::compression::gzip());

	let web = web.with(head::version());

	let web = web.with(head::unique());

	let web = web.with(log::write());

	info!("Starting web server on {}", adr);

	warp::serve(web).run(adr).await;

	Ok(())
}
