mod conf;
mod export;
mod fail;
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

use anyhow::Error;
use std::net::SocketAddr;
use uuid::Uuid;
use warp::Filter;

const ID: &'static str = "Request-Id";

#[tokio::main]
pub async fn init(conf: &clap::ArgMatches) -> Result<(), Error> {
	//
	let adr = conf.value_of("bind").unwrap();
	//
	let adr: SocketAddr = adr.parse().expect("Unable to parse socket address");
	//
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
		// SQL query endpoint
		.or(sql::config())
		// API query endpoint
		.or(key::config())
		// Catch all errors
		.recover(fail::recover)
		// End routes setup
	;

	let web = web.with(warp::compression::gzip());

	let web = web.with(head::server());

	let web = web.with(head::version());

	let web = web.map(|reply| {
		let val = Uuid::new_v4().to_string();
		warp::reply::with_header(reply, ID, val)
	});

	let web = web.with(log::write());

	info!("Starting web server on {}", adr);

	warp::serve(web).run(adr).await;

	Ok(())
}
