mod conf;
mod config;
mod export;
mod fail;
mod head;
mod import;
mod index;
mod key;
mod log;
mod output;
mod signin;
mod signup;
mod sql;
mod status;
mod sync;
mod version;

use crate::err::Error;
use config::Config;
use once_cell::sync::OnceCell;
use surrealdb::Datastore;
use warp::Filter;

static DB: OnceCell<Datastore> = OnceCell::new();

static CF: OnceCell<Config> = OnceCell::new();

#[tokio::main]
pub async fn init(matches: &clap::ArgMatches) -> Result<(), Error> {
	// Parse the server config options
	let cfg = config::parse(matches);
	// Parse and setup the desired kv datastore
	let dbs = Datastore::new(&cfg.path).await?;
	// Store database instance
	let _ = DB.set(dbs);
	// Store config options
	let _ = CF.set(cfg);
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
	// Log all requests to the console
	let net = net.with(log::write());

	// Get local copy of options
	let opt = CF.get().unwrap();

	info!("Starting web server on {}", &opt.bind);

	warp::serve(net).run(opt.bind).await;

	Ok(())
}
