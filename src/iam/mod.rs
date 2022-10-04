pub mod clear;
pub mod parse;
pub mod signin;
pub mod signup;
pub mod token;
pub mod verify;

use crate::cli::CF;
use crate::err::Error;

pub const BASIC: &str = "Basic ";
pub const TOKEN: &str = "Bearer ";

const LOG: &str = "surrealdb::iam";

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
