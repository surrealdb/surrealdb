use crate::cli::LOG;
use crate::err::Error;
use reqwest::blocking::Client;
use reqwest::header::ACCEPT;
use std::fs::OpenOptions;
use std::io::prelude::Read;

pub fn init(matches: &clap::ArgMatches) -> Result<(), Error> {
	// Set the default logging level
	crate::cli::log::init(3);
	// Try to parse the file argument
	let file = matches.value_of("file").unwrap();
	// Try to open the specified file
	let mut file = OpenOptions::new().read(true).open(file)?;
	// Read the full contents of the file
	let mut body = String::new();
	file.read_to_string(&mut body)?;
	// Parse all other cli arguments
	let user = matches.value_of("user").unwrap();
	let pass = matches.value_of("pass").unwrap();
	let conn = matches.value_of("conn").unwrap();
	let ns = matches.value_of("ns").unwrap();
	let db = matches.value_of("db").unwrap();
	// Set the correct import URL
	let conn = format!("{}/import", conn);
	// Import the data into the database
	Client::new()
		.post(&conn)
		.header(ACCEPT, "application/octet-stream")
		.basic_auth(user, Some(pass))
		.header("NS", ns)
		.header("DB", db)
		.body(body)
		.send()?
		.error_for_status()?;
	// Output a success message
	info!(target: LOG, "The SQL file was imported successfully");
	// Everything OK
	Ok(())
}
