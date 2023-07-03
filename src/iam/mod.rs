pub mod verify;

use crate::cli::CF;
use crate::err::Error;
use surrealdb::iam::LOG;

pub const BASIC: &str = "Basic ";

pub async fn init() -> Result<(), Error> {
	// Get local copy of options
	let opt = CF.get().unwrap();
	// Log authentication options
	match opt.pass {
		Some(_) => {
			info!(target: LOG, "Root authentication is enabled");
			info!(target: LOG, "Root username is '{}'", opt.user);
		}
		None => info!(target: LOG, "Root authentication is disabled"),
	};
	// All ok
	Ok(())
}
