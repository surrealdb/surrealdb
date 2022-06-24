use crate::cnf::LOGO;
use crate::err::Error;
use crate::net;

pub fn init(matches: &clap::ArgMatches) -> Result<(), Error> {
	// output SurrealDB logo
	println!("{}", LOGO);
	// Start up the web server
	net::init(matches)?;
	// Don't error when done
	Ok(())
}
