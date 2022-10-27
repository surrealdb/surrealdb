use crate::env::release;
use crate::err::Error;

pub fn init(_: &clap::ArgMatches) -> Result<(), Error> {
	println!("{}", release());
	Ok(())
}
