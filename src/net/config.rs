use std::net::SocketAddr;

#[derive(Clone, Debug)]
pub struct Config {
	pub bind: SocketAddr,
	pub path: String,
	pub user: String,
	pub pass: String,
	pub crt: Option<String>,
	pub key: Option<String>,
}

pub fn parse(matches: &clap::ArgMatches) -> Config {
	// Parse the server binding address
	let bind = matches
		.value_of("bind")
		.unwrap()
		.parse::<SocketAddr>()
		.expect("Unable to parse socket address");
	// Parse the database endpoint path
	let path = matches.value_of("path").unwrap().to_owned();
	// Parse the root username for authentication
	let user = matches.value_of("user").unwrap().to_owned();
	// Parse the root password for authentication
	let pass = matches.value_of("pass").unwrap().to_owned();
	// Parse any TLS server security options
	let crt = matches.value_of("web-crt").map(|v| v.to_owned());
	let key = matches.value_of("web-key").map(|v| v.to_owned());
	// Return a new config object
	Config {
		bind,
		path,
		user,
		pass,
		crt,
		key,
	}
}
