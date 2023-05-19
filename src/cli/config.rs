use once_cell::sync::OnceCell;
use std::{
	collections::hash_map::DefaultHasher,
	hash::{Hash, Hasher},
	net::SocketAddr,
};

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

impl Config {
	/// Returns true if the username and password are that of root.
	///
	/// Returns false if no root password is configured or the username
	/// or password doesn't match.
	pub(crate) fn verify_root(&self, user: &str, pass: &str) -> bool {
		if let Some(p) = self.pass.as_ref() {
			#[inline(never)]
			fn hash(u: &str, p: &str) -> u64 {
				let mut hasher = DefaultHasher::new();
				u.hash(&mut hasher);
				p.hash(&mut hasher);
				hasher.finish()
			}

			// Intended to block incorrect credentials in constant time
			// to avoid a timing side-channel.
			if hash(&self.user, p) == hash(user, pass) {
				p == pass && user == self.user
			} else {
				// Hash(es) didn't match
				false
			}
		} else {
			// No root password = cannot possibly be correct
			false
		}
	}
}

pub fn init(matches: &clap::ArgMatches) {
	// Parse the server binding address
	let bind = matches.value_of("bind").unwrap().parse::<SocketAddr>().unwrap();
	// Parse the database endpoint path
	let path = matches.value_of("path").unwrap().to_owned();
	// Parse the root username for authentication
	let user = matches.value_of("user").unwrap().to_owned();
	// Parse the root password for authentication
	let pass = matches.value_of("pass").map(|v| v.to_owned());
	// Parse any TLS server security options
	let crt = matches.value_of("web-crt").map(|v| v.to_owned());
	let key = matches.value_of("web-key").map(|v| v.to_owned());
	// Check if database strict mode is enabled
	let strict = matches.is_present("strict");
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

#[cfg(test)]
mod tests {
	use super::Config;
	use std::net::{IpAddr, Ipv4Addr, SocketAddr};

	#[test]
	fn verify_root() {
		let mut cfg = Config {
			user: "root".to_owned(),
			pass: None,
			strict: Default::default(),
			bind: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
			path: Default::default(),
			crt: Default::default(),
			key: Default::default(),
		};

		assert!(!cfg.verify_root("root", "any"));

		cfg.pass = Some("secret".to_string());

		assert!(!cfg.verify_root("admin", "secret"));
		assert!(!cfg.verify_root("root", "12345"));
		assert!(cfg.verify_root("root", "secret"));
	}
}
