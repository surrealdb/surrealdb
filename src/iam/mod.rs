pub mod clear;
pub mod signin;
pub mod signup;
pub mod token;
pub mod verify;

use crate::cli::CF;
use crate::err::Error;

const LOG: &str = "surrealdb::iam";

pub async fn init() -> Result<(), Error> {
	// Get local copy of options
	let opt = CF.get().unwrap();
	// Log authentication options
	match opt.pass {
		Some(_) => info!(target: LOG, "Root authentication is enabled"),
		None => info!(target: LOG, "Root authentication is disabled"),
	};
	// All ok
	Ok(())
}
