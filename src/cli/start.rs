use super::config;
use crate::cnf::LOGO;
use crate::dbs;
use crate::err::Error;
use crate::net;

#[tokio::main]
pub async fn init(matches: &clap::ArgMatches) -> Result<(), Error> {
	// output SurrealDB logo
	println!("{}", LOGO);
	// Setup the cli options
	config::init(matches);
	// Start the kvs server
	dbs::init().await?;
	// Start the web server
	net::init().await?;
	// All ok
	Ok(())
}
