pub mod verify;

use crate::cli::CF;
use crate::err::Error;
use surrealdb::dbs::AUTH_ENABLED;
use surrealdb::iam::LOG;

pub const BASIC: &str = "Basic ";

pub async fn init() -> Result<(), Error> {
	// Get local copy of options
	let opt = CF.get().unwrap();
	// Check if authentication is enabled
	if opt.no_auth {
		warn!(target: LOG, "************************************************************");
		warn!(
			target: LOG,
			"Authentication is disabled! This is not recommended for production use."
		);
		warn!(target: LOG, "************************************************************");
	} else {
		info!(target: LOG, "Authentication is enabled");
	}

	let _ = AUTH_ENABLED.set(!opt.no_auth).unwrap();
	Ok(())
}
