use super::config;
use crate::cnf::LOGO;
use crate::dbs;
use crate::env;
use crate::err::Error;
use crate::iam;
use crate::net;

#[tokio::main]
pub async fn init(matches: &clap::ArgMatches) -> Result<(), Error> {
	// Initialize opentelemetry and logging
	crate::o11y::builder().with_log_level(matches.get_one::<String>("log").unwrap()).init();
	// Check if a banner should be outputted
	if !matches.is_present("no-banner") {
		// Output SurrealDB logo
		println!("{LOGO}");
	}
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
