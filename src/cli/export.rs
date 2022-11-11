use crate::cli::LOG;
use crate::err::Error;
use reqwest::blocking::Client;
use reqwest::header::ACCEPT;
use std::fs::OpenOptions;

pub fn init(matches: &clap::ArgMatches) -> Result<(), Error> {
	// Set the default logging level
	crate::cli::log::init(3);
	// Try to parse the file argument
	let file = matches.value_of("file").unwrap();
	// Try to open the specified file
	let mut file = OpenOptions::new().write(true).create(true).truncate(true).open(file)?;
	// Parse all other cli arguments
	let user = matches.value_of("user").unwrap();
	let pass = matches.value_of("pass").unwrap();
	let conn = matches.value_of("conn").unwrap();
	let ns = matches.value_of("ns").unwrap();
	let db = matches.value_of("db").unwrap();
	// Set the correct export URL
	let conn = format!("{}/export", conn);
	// Export the data from the database
	let mut res = Client::new()
		.get(&conn)
		.header(ACCEPT, "application/octet-stream")
		.basic_auth(user, Some(pass))
		.header("NS", ns)
		.header("DB", db)
		.send()?;
	// Check import result and report error
	if res.status().is_success() {
		res.copy_to(&mut file)?;
		info!(target: LOG, "The SQL file was exported successfully");
	} else if res.status().is_client_error() || res.status().is_server_error() {
		error!(target: LOG, "Request failed with status {}. Body: {}", res.status(), res.text()?);
	} else {
		error!(target: LOG, "Unexpected response status {}", res.status());
	}
	// Everything OK
	Ok(())
}
