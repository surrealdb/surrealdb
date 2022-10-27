use super::config;
use super::log;
use crate::cnf::LOGO;
use crate::dbs;
use crate::env;
use crate::err::Error;
use crate::iam;
use crate::net;

#[tokio::main]
pub async fn init(matches: &clap::ArgMatches) -> Result<(), Error> {
	// Set the default log level
	match matches.get_one::<String>("log").map(String::as_str) {
		Some("warn") => log::init(0),
		Some("info") => log::init(1),
		Some("debug") => log::init(2),
		Some("trace") => log::init(3),
		Some("full") => log::init(4),
		_ => unreachable!(),
	};
	// Output SurrealDB logo
	println!("{}", LOGO);
	// Setup the cli options
	config::init(matches);
	// Initiate environment
	env::init().await?;
	// Initiate master auth
	iam::init().await?;
	// Start the kvs server
	dbs::init().await?;
	// Start the web server
	net::init().await?;
	// All ok
	Ok(())
}
