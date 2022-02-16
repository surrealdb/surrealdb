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
use std::net::SocketAddr;
use uuid::Uuid;
use warp::Filter;

const ID: &'static str = "Request-Id";

#[tokio::main]
pub async fn init(bind: &str) -> Result<(), Error> {
	//
	let adr: SocketAddr = bind.parse().expect("Unable to parse socket address");

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

	let net = net.with(warp::compression::gzip());

	let net = net.with(head::server());

	let net = net.with(head::version());

	let net = net.map(|reply| {
		let val = Uuid::new_v4().to_string();
		warp::reply::with_header(reply, ID, val)
	});

	let net = net.with(log::write());

	info!("Starting web server on {}", adr);

	warp::serve(net).run(adr).await;

	Ok(())
}
