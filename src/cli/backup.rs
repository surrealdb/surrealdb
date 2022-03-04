use crate::err::Error;
use reqwest::blocking::Body;
use reqwest::blocking::Client;
use reqwest::header::CONTENT_TYPE;
use std::fs::OpenOptions;
use std::io::copy;

const TYPE: &str = "application/octet-stream";

pub fn init(matches: &clap::ArgMatches) -> Result<(), Error> {
	// Attempt to open the specified file,
	// and if there is a problem opening
	// the file, then return an error.

	let from = matches.value_of("from").unwrap();

	// Attempt to open the specified file,
	// and if there is a problem opening
	// the file, then return an error.

	let into = matches.value_of("into").unwrap();

	// Process the response, checking
	// for any errors, and outputting
	// the responses back to the user.

	if from.ends_with(".db") && into.ends_with(".db") {
		backup_file_to_file(matches, from, into)
	} else if from.ends_with(".db") {
		backup_file_to_http(matches, from, into)
	} else if into.ends_with(".db") {
		backup_http_to_file(matches, from, into)
	} else {
		backup_http_to_http(matches, from, into)
	}
}

fn backup_file_to_file(_: &clap::ArgMatches, from: &str, into: &str) -> Result<(), Error> {
	let mut from = OpenOptions::new().read(true).open(from)?;

	let mut into = OpenOptions::new().write(true).create(true).truncate(true).open(into)?;

	copy(&mut from, &mut into)?;

	Ok(())
}

fn backup_http_to_file(matches: &clap::ArgMatches, from: &str, into: &str) -> Result<(), Error> {
	let user = matches.value_of("user").unwrap();

	let pass = matches.value_of("pass").unwrap();

	let from = format!("{}/sync", from);

	let mut from = Client::new()
		.get(&from)
		.basic_auth(user, Some(pass))
		.header(CONTENT_TYPE, TYPE)
		.send()?
		.error_for_status()?;

	let mut into = OpenOptions::new().write(true).create(true).truncate(true).open(into)?;

	copy(&mut from, &mut into)?;

	Ok(())
}

fn backup_file_to_http(matches: &clap::ArgMatches, from: &str, into: &str) -> Result<(), Error> {
	let user = matches.value_of("user").unwrap();

	let pass = matches.value_of("pass").unwrap();

	let into = format!("{}/sync", into);

	let from = OpenOptions::new().read(true).open(from)?;

	Client::new()
		.post(&into)
		.basic_auth(user, Some(pass))
		.header(CONTENT_TYPE, TYPE)
		.body(from)
		.send()?
		.error_for_status()?;

	Ok(())
}

fn backup_http_to_http(matches: &clap::ArgMatches, from: &str, into: &str) -> Result<(), Error> {
	let user = matches.value_of("user").unwrap();

	let pass = matches.value_of("pass").unwrap();

	let from = format!("{}/sync", from);

	let into = format!("{}/sync", into);

	let from = Client::new()
		.get(&from)
		.basic_auth(user, Some(pass))
		.header(CONTENT_TYPE, TYPE)
		.send()?
		.error_for_status()?;

	Client::new()
		.post(&into)
		.basic_auth(user, Some(pass))
		.header(CONTENT_TYPE, TYPE)
		.body(Body::new(from))
		.send()?
		.error_for_status()?;

	Ok(())
}
