use crate::kvs;
use crate::net;
use anyhow::Error;
use clap;

const LOGO: &'static str = "
 .d8888b.                                             888 8888888b.  888888b.
d88P  Y88b                                            888 888  'Y88b 888  '88b
Y88b.                                                 888 888    888 888  .88P
 'Y888b.   888  888 888d888 888d888  .d88b.   8888b.  888 888    888 8888888K.
    'Y88b. 888  888 888P'   888P'   d8P  Y8b     '88b 888 888    888 888  'Y88b
      '888 888  888 888     888     88888888 .d888888 888 888    888 888    888
Y88b  d88P Y88b 888 888     888     Y8b.     888  888 888 888  .d88P 888   d88P
 'Y8888P'   'Y88888 888     888      'Y8888  'Y888888 888 8888888P'  8888888P'

";

pub fn init(matches: &clap::ArgMatches) -> Result<(), Error> {
	// output SurrealDB logo
	println!("{}", LOGO);
	// Parse the database endpoint path
	let path = matches.value_of("path").unwrap();
	// Parse the server binding address
	let bind = matches.value_of("bind").unwrap();
	// Start up the kvs storage
	kvs::init(path)?;
	// Start up the web server
	net::init(bind)?;
	// Don't error when done
	Ok(())
}
