use anyhow::Error;
use reqwest::blocking::Client;
use reqwest::header::CONTENT_TYPE;
use std::fs::OpenOptions;
use std::io::copy;

pub fn init(matches: &clap::ArgMatches) -> Result<(), Error> {
	// Ensure that the command has a file
	// argument. If no file argument has
	// been provided, then return an error.

	let file = matches.value_of("file").unwrap();

	// Attempt to open the specified file,
	// and if there is a problem opening
	// the file, then return an error.

	let mut file = OpenOptions::new().write(true).create(true).truncate(true).open(file)?;

	// Parse all other cli arguments

	let user = matches.value_of("user").unwrap();

	let pass = matches.value_of("pass").unwrap();

	let conn = matches.value_of("conn").unwrap();

	let ns = matches.value_of("ns").unwrap();

	let db = matches.value_of("db").unwrap();

	let conn = format!("{}/export", conn);

	// Create and send the HTTP request
	// specifying the basic auth header
	// and the specified content-type.

	let mut res = Client::new()
		.get(&conn)
		.header(CONTENT_TYPE, "application/octet-stream")
		.basic_auth(user, Some(pass))
		.header("NS", ns)
		.header("DB", db)
		.send()?
		.error_for_status()?;

	// Copy the contents of the http get
	// response to the specified ouput
	// file and pass along any errors.

	copy(&mut res, &mut file)?;

	// Output an informational message
	// and return an Ok to signify that
	// this command has been successful.

	info!("The SQL file was exported successfully");

	Ok(())
}
