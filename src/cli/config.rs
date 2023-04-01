use once_cell::sync::OnceCell;
use std::net::SocketAddr;

pub static CF: OnceCell<Config> = OnceCell::new();

#[derive(Clone, Debug)]
pub struct Config {
	pub strict: bool,
	pub bind: SocketAddr,
	pub path: String,
	pub user: String,
	pub pass: Option<String>,
	pub crt: Option<String>,
	pub key: Option<String>,
}

pub fn init(matches: &clap::ArgMatches) {
	// Parse the server binding address
	let bind = matches.get_one::<&str>("bind").unwrap().parse::<SocketAddr>().unwrap();
	// Parse the database endpoint path
	let path = matches.get_one::<String>("path").unwrap().to_owned();
	// Parse the root username for authentication
	let user = matches.get_one::<String>("user").unwrap().to_owned();
	// Parse the root password for authentication
	let pass = matches.get_one::<String>("pass").map(|v| v.to_owned());
	// Parse any TLS server security options
	let crt = matches.get_one::<String>("web-crt").map(|v| v.to_owned());
	let key = matches.get_one::<String>("web-key").map(|v| v.to_owned());
	// Check if database strict mode is enabled
	let strict = matches.contains_id("strict");
	// Store the new config object
	let _ = CF.set(Config {
		strict,
		bind,
		path,
		user,
		pass,
		crt,
		key,
	});
}
