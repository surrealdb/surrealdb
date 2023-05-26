pub mod base;
pub mod clear;
pub mod parse;
pub mod signin;
pub mod signup;
pub mod token;
pub mod verify;

use surrealdb::dbs::AUTH_ENABLED;
use crate::cli::CF;
use crate::err::Error;

pub const BASIC: &str = "Basic ";
pub const TOKEN: &str = "Bearer ";
pub const ROOT_USER: &str = "root";
pub const ROOT_PASS: &str = "surrealdb";

const LOG: &str = "surrealdb::iam";

pub async fn init() -> Result<(), Error> {
	// Get local copy of options
	let opt = CF.get().unwrap();
	// Check if authentication is enabled
	if opt.auth {
		info!(target: LOG, "Authentication is enabled");
	} else {
		warn!(target: LOG, "************************************************************");
		warn!(target: LOG, "Authentication is disabled! This is not recommended for production use.");
		warn!(target: LOG, "************************************************************");
	}

	let _ = AUTH_ENABLED.set(opt.auth).unwrap();
	Ok(())
}
