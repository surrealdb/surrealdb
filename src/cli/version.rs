use crate::env::release;
use crate::err::Error;

pub fn init() -> Result<(), Error> {
	println!("{}", release());
	Ok(())
}
