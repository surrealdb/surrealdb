pub mod verify;

use crate::cli::CF;
use crate::err::Error;

pub const BASIC: &str = "Basic ";

pub async fn init() -> Result<(), Error> {
	// Get local copy of options
	let opt = CF.get().unwrap();
	// Log authentication options
	match opt.pass {
		Some(_) => {
			info!("Root authentication is enabled");
			info!("Root username is '{}'", opt.user);
		}
		None => info!("Root authentication is disabled"),
	};
	// All ok
	Ok(())
}
