use crate::cnf::SERVER_AGENT;
use crate::err::Error;
use reqwest::blocking::Body;
use reqwest::blocking::Client;
use reqwest::header::CONTENT_TYPE;
use reqwest::header::USER_AGENT;
use std::fs::OpenOptions;
use std::io::copy;

const TYPE: &str = "application/octet-stream";

pub fn init(matches: &clap::ArgMatches) -> Result<(), Error> {
	// Try to parse the specified source file
	let from = matches.value_of("from").unwrap();
	// Try to parse the specified output file
	let into = matches.value_of("into").unwrap();
	// Process the source->destination response
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
	// Try to open the source file
	let mut from = OpenOptions::new().read(true).open(from)?;
	// Try to open the output file
	let mut into = OpenOptions::new().write(true).create(true).truncate(true).open(into)?;
	// Copy the data to the destination
	copy(&mut from, &mut into)?;
	// Everything OK
	Ok(())
}

fn backup_http_to_file(matches: &clap::ArgMatches, from: &str, into: &str) -> Result<(), Error> {
	// Parse the specified username
	let user = matches.value_of("user").unwrap();
	// Parse the specified password
	let pass = matches.value_of("pass").unwrap();
	// Set the correct source URL
	let from = format!("{from}/sync");
	// Try to open the source http
	let mut from = Client::new()
		.get(from)
		.basic_auth(user, Some(pass))
		.header(USER_AGENT, SERVER_AGENT)
		.header(CONTENT_TYPE, TYPE)
		.send()?
		.error_for_status()?;
	// Try to open the output file
	let mut into = OpenOptions::new().write(true).create(true).truncate(true).open(into)?;
	// Copy the data to the destination
	copy(&mut from, &mut into)?;
	// Everything OK
	Ok(())
}

fn backup_file_to_http(matches: &clap::ArgMatches, from: &str, into: &str) -> Result<(), Error> {
	// Parse the specified username
	let user = matches.value_of("user").unwrap();
	// Parse the specified password
	let pass = matches.value_of("pass").unwrap();
	// Try to open the source file
	let from = OpenOptions::new().read(true).open(from)?;
	// Set the correct output URL
	let into = format!("{into}/sync");
	// Copy the data to the destination
	Client::new()
		.post(into)
		.basic_auth(user, Some(pass))
		.header(USER_AGENT, SERVER_AGENT)
		.header(CONTENT_TYPE, TYPE)
		.body(from)
		.send()?
		.error_for_status()?;
	// Everything OK
	Ok(())
}

fn backup_http_to_http(matches: &clap::ArgMatches, from: &str, into: &str) -> Result<(), Error> {
	// Parse the specified username
	let user = matches.value_of("user").unwrap();
	// Parse the specified password
	let pass = matches.value_of("pass").unwrap();
	// Set the correct source URL
	let from = format!("{from}/sync");
	// Set the correct output URL
	let into = format!("{into}/sync");
	// Try to open the source file
	let from = Client::new()
		.get(from)
		.basic_auth(user, Some(pass))
		.header(USER_AGENT, SERVER_AGENT)
		.header(CONTENT_TYPE, TYPE)
		.send()?
		.error_for_status()?;
	// Copy the data to the destination
	Client::new()
		.post(into)
		.basic_auth(user, Some(pass))
		.header(USER_AGENT, SERVER_AGENT)
		.header(CONTENT_TYPE, TYPE)
		.body(Body::new(from))
		.send()?
		.error_for_status()?;
	// Everything OK
	Ok(())
}
