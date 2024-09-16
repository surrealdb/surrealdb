use crate::cli::CF;
use crate::err::Error;
use clap::Args;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use surrealdb::dbs::capabilities::{Capabilities, FuncTarget, NetTarget, Targets};
use surrealdb::kvs::Datastore;

#[derive(Args, Debug)]
pub struct StartCommandDbsOptions {
	#[arg(help = "Whether strict mode is enabled on this database instance")]
	#[arg(env = "SURREAL_STRICT", short = 's', long = "strict")]
	#[arg(default_value_t = false)]
	strict_mode: bool,
	#[arg(help = "The maximum duration that a set of statements can run for")]
	#[arg(env = "SURREAL_QUERY_TIMEOUT", long)]
	#[arg(value_parser = super::cli::validator::duration)]
	query_timeout: Option<Duration>,
	#[arg(help = "The maximum duration that any single transaction can run for")]
	#[arg(env = "SURREAL_TRANSACTION_TIMEOUT", long)]
	#[arg(value_parser = super::cli::validator::duration)]
	transaction_timeout: Option<Duration>,
	#[arg(help = "Whether to allow unauthenticated access", help_heading = "Authentication")]
	#[arg(env = "SURREAL_UNAUTHENTICATED", long = "unauthenticated")]
	#[arg(default_value_t = false)]
	unauthenticated: bool,
	#[command(flatten)]
	#[command(next_help_heading = "Capabilities")]
	capabilities: DbsCapabilities,
	#[arg(help = "Sets the directory for storing temporary database files")]
	#[arg(env = "SURREAL_TEMPORARY_DIRECTORY", long = "temporary-directory")]
	#[arg(value_parser = super::cli::validator::dir_exists)]
	temporary_directory: Option<PathBuf>,
}

#[derive(Args, Debug)]
struct DbsCapabilities {
	//
	// Allow
	//
	#[arg(help = "Allow all capabilities except for those more specifically denied")]
	#[arg(env = "SURREAL_CAPS_ALLOW_ALL", short = 'A', long, conflicts_with = "deny_all")]
	allow_all: bool,

	#[cfg(feature = "scripting")]
	#[arg(help = "Allow execution of embedded scripting functions")]
	#[arg(env = "SURREAL_CAPS_ALLOW_SCRIPT", long, conflicts_with_all = ["allow_all", "deny_scripting"])]
	allow_scripting: bool,

	#[arg(help = "Allow guest users to execute queries")]
	#[arg(env = "SURREAL_CAPS_ALLOW_GUESTS", long, conflicts_with_all = ["allow_all", "deny_guests"])]
	allow_guests: bool,

	#[arg(
		help = "Allow execution of all functions except for functions that are specifically denied. Alternatively, you can provide a comma-separated list of function names to allow",
		long_help = r#"Allow execution of all functions except for functions that are specifically denied. Alternatively, you can provide a comma-separated list of function names to allow
Specifically denied functions and function families prevail over any other allowed function execution.
Function names must be in the form <family>[::<name>]. For example:
 - 'http' or 'http::*' -> Include all functions in the 'http' family
 - 'http::get' -> Include only the 'get' function in the 'http' family
"#
	)]
	#[arg(env = "SURREAL_CAPS_ALLOW_FUNC", long)]
	// If the arg is provided without value, then assume it's "", which gets parsed into Targets::All
	#[arg(default_missing_value_os = "", num_args = 0..)]
	#[arg(value_parser = super::cli::validator::func_targets)]
	allow_funcs: Option<Targets<FuncTarget>>,

	#[arg(
		help = "Allow all outbound network connections except for network targets that are specifically denied. Alternatively, you can provide a comma-separated list of network targets to allow",
		long_help = r#"Allow all outbound network connections except for network targets that are specifically denied. Alternatively, you can provide a comma-separated list of network targets to allow
Specifically denied network targets prevail over any other allowed outbound network connections.
Targets must be in the form of <host>[:<port>], <ipv4|ipv6>[/<mask>]. For example:
 - 'surrealdb.com', '127.0.0.1' or 'fd00::1' -> Match outbound connections to these hosts on any port
 - 'surrealdb.com:80', '127.0.0.1:80' or 'fd00::1:80' -> Match outbound connections to these hosts on port 80
 - '10.0.0.0/8' or 'fd00::/8' -> Match outbound connections to any host in these networks
"#
	)]
	#[arg(env = "SURREAL_CAPS_ALLOW_NET", long)]
	// If the arg is provided without value, then assume it's "", which gets parsed into Targets::All
	#[arg(default_missing_value_os = "", num_args = 0..)]
	#[arg(value_parser = super::cli::validator::net_targets)]
	allow_net: Option<Targets<NetTarget>>,

	//
	// Deny
	//
	#[arg(help = "Deny all capabilities except for those more specifically allowed")]
	#[arg(env = "SURREAL_CAPS_DENY_ALL", short = 'D', long, conflicts_with = "allow_all")]
	deny_all: bool,

	#[cfg(feature = "scripting")]
	#[arg(help = "Deny execution of embedded scripting functions")]
	#[arg(env = "SURREAL_CAPS_DENY_SCRIPT", long, conflicts_with_all = ["deny_all", "allow_scripting"])]
	deny_scripting: bool,

	#[arg(help = "Deny guest users to execute queries")]
	#[arg(env = "SURREAL_CAPS_DENY_GUESTS", long, conflicts_with_all = ["deny_all", "allow_guests"])]
	deny_guests: bool,

	#[arg(
		help = "Deny execution of all functions except for functions that are specifically allowed. Alternatively, you can provide a comma-separated list of function names to deny",
		long_help = r#"Deny execution of all functions except for functions that are specifically allowed. Alternatively, you can provide a comma-separated list of function names to deny.
Specifically allowed functions and function families prevail over a general denial of function execution.
Function names must be in the form <family>[::<name>]. For example:
 - 'http' or 'http::*' -> Include all functions in the 'http' family
 - 'http::get' -> Include only the 'get' function in the 'http' family
"#
	)]
	#[arg(env = "SURREAL_CAPS_DENY_FUNC", long)]
	// If the arg is provided without value, then assume it's "", which gets parsed into Targets::All
	#[arg(default_missing_value_os = "", num_args = 0..)]
	#[arg(value_parser = super::cli::validator::func_targets)]
	deny_funcs: Option<Targets<FuncTarget>>,

	#[arg(
		help = "Deny all outbound network connections except for network targets that are specifically allowed. Alternatively, you can provide a comma-separated list of network targets to deny",
		long_help = r#"Deny all outbound network connections except for network targets that are specifically allowed. Alternatively, you can provide a comma-separated list of network targets to deny.
Specifically allowed network targets prevail over a general denial of outbound network connections.
Targets must be in the form of <host>[:<port>], <ipv4|ipv6>[/<mask>]. For example:
 - 'surrealdb.com', '127.0.0.1' or 'fd00::1' -> Match outbound connections to these hosts on any port
 - 'surrealdb.com:80', '127.0.0.1:80' or 'fd00::1:80' -> Match outbound connections to these hosts on port 80
 - '10.0.0.0/8' or 'fd00::/8' -> Match outbound connections to any host in these networks
"#
	)]
	#[arg(env = "SURREAL_CAPS_DENY_NET", long)]
	// If the arg is provided without value, then assume it's "", which gets parsed into Targets::All
	#[arg(default_missing_value_os = "", num_args = 0..)]
	#[arg(value_parser = super::cli::validator::net_targets)]
	deny_net: Option<Targets<NetTarget>>,
}

impl DbsCapabilities {
	#[cfg(feature = "scripting")]
	fn get_scripting(&self) -> bool {
		// Even if there was a global deny, we allow if there is a specific allow for scripting
		// Even if there is a global allow, we deny if there is a specific deny for scripting
		self.allow_scripting || (self.allow_all && !self.deny_scripting)
	}

	#[cfg(not(feature = "scripting"))]
	fn get_scripting(&self) -> bool {
		false
	}

	fn get_allow_guests(&self) -> bool {
		// Even if there was a global deny, we allow if there is a specific allow for guests
		// Even if there is a global allow, we deny if there is a specific deny for guests
		self.allow_guests || (self.allow_all && !self.deny_guests)
	}

	fn get_allow_funcs(&self) -> Targets<FuncTarget> {
		// If there was a global deny, we allow if there is a general allow or some specific allows for functions
		if self.deny_all {
			match &self.allow_funcs {
				Some(Targets::Some(_)) => return self.allow_funcs.clone().unwrap(), // We already checked for Some
				Some(Targets::All) => return Targets::All,
				Some(_) => return Targets::None,
				None => return Targets::None,
			}
		}

		// If there was a general deny for functions, we allow if there are specific allows for functions
		if let Some(Targets::All) = self.deny_funcs {
			match &self.allow_funcs {
				Some(Targets::Some(_)) => return self.allow_funcs.clone().unwrap(), // We already checked for Some
				Some(_) => return Targets::None,
				None => return Targets::None,
			}
		}

		// If there are no high level denies but there is a global allow, we allow functions
		if self.allow_all {
			return Targets::All;
		}

		// If there are no high level, we allow the provided functions
		// If nothing was provided, we allow functions by default (Targets::All)
		self.allow_funcs.clone().unwrap_or(Targets::All) // Functions are enabled by default for the server
	}

	fn get_allow_net(&self) -> Targets<NetTarget> {
		// If there was a global deny, we allow if there is a general allow or some specific allows for networks
		if self.deny_all {
			match &self.allow_net {
				Some(Targets::Some(_)) => return self.allow_net.clone().unwrap(), // We already checked for Some
				Some(Targets::All) => return Targets::All,
				Some(_) => return Targets::None,
				None => return Targets::None,
			}
		}

		// If there was a general deny for networks, we allow if there are specific allows for networks
		if let Some(Targets::All) = self.deny_net {
			match &self.allow_net {
				Some(Targets::Some(_)) => return self.allow_net.clone().unwrap(), // We already checked for Some
				Some(_) => return Targets::None,
				None => return Targets::None,
			}
		}

		// If there are no high level denies but there is a global allow, we allow networks
		if self.allow_all {
			return Targets::All;
		}

		// If there are no high level denies, we allow the provided networks
		// If nothing was provided, we do not allow network by default (Targets::None)
		self.allow_net.clone().unwrap_or(Targets::None)
	}

	fn get_deny_funcs(&self) -> Targets<FuncTarget> {
		// Allowed functions already consider a global deny and a general deny for functions
		// On top of what is explicitly allowed, we deny what is specifically denied
		match &self.deny_funcs {
			Some(Targets::Some(_)) => self.deny_funcs.clone().unwrap(), // We already checked for Some
			Some(_) => Targets::None,
			None => Targets::None,
		}
	}

	fn get_deny_net(&self) -> Targets<NetTarget> {
		// Allowed networks already consider a global deny and a general deny for networks
		// On top of what is explicitly allowed, we deny what is specifically denied
		match &self.deny_net {
			Some(Targets::Some(_)) => self.deny_net.clone().unwrap(), // We already checked for Some
			Some(_) => Targets::None,
			None => Targets::None,
		}
	}

	fn get_deny_all(&self) -> bool {
		self.deny_all
	}
}

impl From<DbsCapabilities> for Capabilities {
	fn from(caps: DbsCapabilities) -> Self {
		Capabilities::default()
			.with_scripting(caps.get_scripting())
			.with_guest_access(caps.get_allow_guests())
			.with_functions(caps.get_allow_funcs())
			.without_functions(caps.get_deny_funcs())
			.with_network_targets(caps.get_allow_net())
			.without_network_targets(caps.get_deny_net())
	}
}

pub async fn init(
	StartCommandDbsOptions {
		strict_mode,
		query_timeout,
		transaction_timeout,
		unauthenticated,
		capabilities,
		temporary_directory,
	}: StartCommandDbsOptions,
) -> Result<Datastore, Error> {
	// Get local copy of options
	let opt = CF.get().unwrap();
	// Log specified strict mode
	debug!("Database strict mode is {strict_mode}");
	// Log specified query timeout
	if let Some(v) = query_timeout {
		debug!("Maximum query processing timeout is {v:?}");
	}
	// Log specified parse timeout
	if let Some(v) = transaction_timeout {
		debug!("Maximum transaction processing timeout is {v:?}");
	}
	// Log whether authentication is disabled
	if unauthenticated {
		warn!("❌🔒 IMPORTANT: Authentication is disabled. This is not recommended for production use. 🔒❌");
	}
	// Warn about the impact of denying all capabilities
	if capabilities.get_deny_all() {
		warn!("You are denying all capabilities by default. Although this is recommended, beware that any new capabilities will also be denied.");
	}
	// Convert the capabilities
	let capabilities = capabilities.into();
	// Log the specified server capabilities
	debug!("Server capabilities: {capabilities}");
	// Parse and setup the desired kv datastore
	let dbs = Datastore::new(&opt.path)
		.await?
		.with_notifications()
		.with_strict_mode(strict_mode)
		.with_query_timeout(query_timeout)
		.with_transaction_timeout(transaction_timeout)
		.with_auth_enabled(!unauthenticated)
		.with_temporary_directory(temporary_directory)
		.with_capabilities(capabilities);
	// Ensure the storage version is up-to-date to prevent corruption
	dbs.check_version().await?;
	// Setup initial server auth credentials
	if let (Some(user), Some(pass)) = (opt.user.as_ref(), opt.pass.as_ref()) {
		dbs.initialise_credentials(user, pass).await?;
	}
	// Bootstrap the datastore
	dbs.bootstrap().await?;
	// All ok
	Ok(dbs)
}

pub async fn fix(path: String) -> Result<(), Error> {
	// Parse and setup the desired kv datastore
	let dbs = Arc::new(Datastore::new(&path).await?);
	// Ensure the storage version is up-to-date to prevent corruption
	let version = dbs.get_version().await?;
	// Apply fixes
	version.fix(dbs).await?;
	// Log success
	println!("Database storage version was updated successfully. Please carefully read back logs to see if any manual changes need to be applied");
	// All ok
	Ok(())
}

#[cfg(test)]
mod tests {
	use std::str::FromStr;

	use surrealdb::dbs::Session;
	use surrealdb::iam::verify::verify_root_creds;
	use surrealdb::kvs::{LockType::*, TransactionType::*};
	use test_log::test;
	use wiremock::{matchers::method, Mock, MockServer, ResponseTemplate};

	use super::*;
	use surrealdb::opt::auth::Root;

	#[test(tokio::test)]
	async fn test_setup_superuser() {
		let ds = Datastore::new("memory").await.unwrap();
		let creds = Root {
			username: "root",
			password: "root",
		};

		// Setup the initial user if there are no root users
		assert_eq!(
			ds.transaction(Read, Optimistic).await.unwrap().all_root_users().await.unwrap().len(),
			0
		);
		ds.initialise_credentials(creds.username, creds.password).await.unwrap();
		assert_eq!(
			ds.transaction(Read, Optimistic).await.unwrap().all_root_users().await.unwrap().len(),
			1
		);
		verify_root_creds(&ds, creds.username, creds.password).await.unwrap();

		// Do not setup the initial root user if there are root users:
		// Test the scenario by making sure the custom password doesn't change.
		let sql = "DEFINE USER root ON ROOT PASSWORD 'test' ROLES OWNER";
		let sess = Session::owner();
		ds.execute(sql, &sess, None).await.unwrap();
		let pass_hash = ds
			.transaction(Read, Optimistic)
			.await
			.unwrap()
			.get_root_user(creds.username)
			.await
			.unwrap()
			.hash
			.clone();

		ds.initialise_credentials(creds.username, creds.password).await.unwrap();
		assert_eq!(
			pass_hash,
			ds.transaction(Read, Optimistic)
				.await
				.unwrap()
				.get_root_user(creds.username)
				.await
				.unwrap()
				.hash
				.clone()
		)
	}

	#[test(tokio::test)]
	async fn test_capabilities() {
		let server1 = {
			let s = MockServer::start().await;
			let get = Mock::given(method("GET"))
				.respond_with(ResponseTemplate::new(200).set_body_string("SUCCESS"))
				.expect(1);

			s.register(get).await;
			s
		};

		let server2 = {
			let s = MockServer::start().await;
			let get = Mock::given(method("GET"))
				.respond_with(ResponseTemplate::new(200).set_body_string("SUCCESS"))
				.expect(1);
			let head =
				Mock::given(method("HEAD")).respond_with(ResponseTemplate::new(200)).expect(0);

			s.register(get).await;
			s.register(head).await;

			s
		};

		// (Datastore, Session, Query, Succeeds, Response Contains)
		let cases = vec![
			//
			// Functions and Networking are allowed
			//
			(
				Datastore::new("memory").await.unwrap().with_capabilities(
					Capabilities::default()
						.with_functions(Targets::<FuncTarget>::All)
						.with_network_targets(Targets::<NetTarget>::All),
				),
				Session::owner(),
				format!("RETURN http::get('{}')", server1.uri()),
				true,
				"SUCCESS".to_string(),
			),
			//
			// Scripting is allowed
			//
			(
				Datastore::new("memory")
					.await
					.unwrap()
					.with_capabilities(Capabilities::default().with_scripting(true)),
				Session::owner(),
				"RETURN function() { return '1' }".to_string(),
				true,
				"1".to_string(),
			),
			//
			// Scripting is not allowed
			//
			(
				Datastore::new("memory")
					.await
					.unwrap()
					.with_capabilities(Capabilities::default().with_scripting(false)),
				Session::owner(),
				"RETURN function() { return '1' }".to_string(),
				false,
				"Scripting functions are not allowed".to_string(),
			),
			//
			// Anonymous actor when guest access is allowed and auth is enabled, succeeds
			//
			(
				Datastore::new("memory")
					.await
					.unwrap()
					.with_auth_enabled(true)
					.with_capabilities(Capabilities::default().with_guest_access(true)),
				Session::default(),
				"RETURN 1".to_string(),
				true,
				"1".to_string(),
			),
			//
			// Anonymous actor when guest access is not allowed and auth is enabled, throws error
			//
			(
				Datastore::new("memory")
					.await
					.unwrap()
					.with_auth_enabled(true)
					.with_capabilities(Capabilities::default().with_guest_access(false)),
				Session::default(),
				"RETURN 1".to_string(),
				false,
				"Not enough permissions to perform this action".to_string(),
			),
			//
			// Anonymous actor when guest access is not allowed and auth is disabled, succeeds
			//
			(
				Datastore::new("memory")
					.await
					.unwrap()
					.with_auth_enabled(false)
					.with_capabilities(Capabilities::default().with_guest_access(false)),
				Session::default(),
				"RETURN 1".to_string(),
				true,
				"1".to_string(),
			),
			//
			// Authenticated user when guest access is not allowed and auth is enabled, succeeds
			//
			(
				Datastore::new("memory")
					.await
					.unwrap()
					.with_auth_enabled(true)
					.with_capabilities(Capabilities::default().with_guest_access(false)),
				Session::viewer(),
				"RETURN 1".to_string(),
				true,
				"1".to_string(),
			),
			//
			// Some functions are not allowed
			//
			(
				Datastore::new("memory").await.unwrap().with_capabilities(
					Capabilities::default()
						.with_functions(Targets::<FuncTarget>::Some(
							[FuncTarget::from_str("string::*").unwrap()].into(),
						))
						.without_functions(Targets::<FuncTarget>::Some(
							[FuncTarget::from_str("string::len").unwrap()].into(),
						)),
				),
				Session::owner(),
				"RETURN string::len('a')".to_string(),
				false,
				"Function 'string::len' is not allowed".to_string(),
			),
			(
				Datastore::new("memory").await.unwrap().with_capabilities(
					Capabilities::default()
						.with_functions(Targets::<FuncTarget>::Some(
							[FuncTarget::from_str("string::*").unwrap()].into(),
						))
						.without_functions(Targets::<FuncTarget>::Some(
							[FuncTarget::from_str("string::len").unwrap()].into(),
						)),
				),
				Session::owner(),
				"RETURN string::lowercase('A')".to_string(),
				true,
				"a".to_string(),
			),
			(
				Datastore::new("memory").await.unwrap().with_capabilities(
					Capabilities::default()
						.with_functions(Targets::<FuncTarget>::Some(
							[FuncTarget::from_str("string::*").unwrap()].into(),
						))
						.without_functions(Targets::<FuncTarget>::Some(
							[FuncTarget::from_str("string::len").unwrap()].into(),
						)),
				),
				Session::owner(),
				"RETURN time::now()".to_string(),
				false,
				"Function 'time::now' is not allowed".to_string(),
			),
			//
			// Some net targets are not allowed
			//
			(
				Datastore::new("memory").await.unwrap().with_capabilities(
					Capabilities::default()
						.with_functions(Targets::<FuncTarget>::All)
						.with_network_targets(Targets::<NetTarget>::Some(
							[
								NetTarget::from_str(&server1.address().to_string()).unwrap(),
								NetTarget::from_str(&server2.address().to_string()).unwrap(),
							]
							.into(),
						))
						.without_network_targets(Targets::<NetTarget>::Some(
							[NetTarget::from_str(&server1.address().to_string()).unwrap()].into(),
						)),
				),
				Session::owner(),
				format!("RETURN http::get('{}')", server1.uri()),
				false,
				format!("Access to network target '{}' is not allowed", server1.address()),
			),
			(
				Datastore::new("memory").await.unwrap().with_capabilities(
					Capabilities::default()
						.with_functions(Targets::<FuncTarget>::All)
						.with_network_targets(Targets::<NetTarget>::Some(
							[
								NetTarget::from_str(&server1.address().to_string()).unwrap(),
								NetTarget::from_str(&server2.address().to_string()).unwrap(),
							]
							.into(),
						))
						.without_network_targets(Targets::<NetTarget>::Some(
							[NetTarget::from_str(&server1.address().to_string()).unwrap()].into(),
						)),
				),
				Session::owner(),
				"RETURN http::get('http://1.1.1.1')".to_string(),
				false,
				"Access to network target '1.1.1.1:80' is not allowed".to_string(),
			),
			(
				Datastore::new("memory").await.unwrap().with_capabilities(
					Capabilities::default()
						.with_functions(Targets::<FuncTarget>::All)
						.with_network_targets(Targets::<NetTarget>::Some(
							[
								NetTarget::from_str(&server1.address().to_string()).unwrap(),
								NetTarget::from_str(&server2.address().to_string()).unwrap(),
							]
							.into(),
						))
						.without_network_targets(Targets::<NetTarget>::Some(
							[NetTarget::from_str(&server1.address().to_string()).unwrap()].into(),
						)),
				),
				Session::owner(),
				format!("RETURN http::get('{}')", server2.uri()),
				true,
				"SUCCESS".to_string(),
			),
		];

		for (idx, (ds, sess, query, succeeds, contains)) in cases.into_iter().enumerate() {
			info!("Test case {idx}: query={query}, succeeds={succeeds}");
			let res = ds.execute(&query, &sess, None).await;

			if !succeeds && res.is_err() {
				let res = res.unwrap_err();
				assert!(
					res.to_string().contains(&contains),
					"Unexpected error for test case {}: {:?}",
					idx,
					res.to_string()
				);
				continue;
			}

			let res = res.unwrap().remove(0).output();
			let res = if succeeds {
				assert!(res.is_ok(), "Unexpected error for test case {idx}: {res:?}");
				res.unwrap().to_string()
			} else {
				assert!(res.is_err(), "Unexpected success for test case {idx}: {res:?}");
				res.unwrap_err().to_string()
			};

			assert!(
				res.contains(&contains),
				"Unexpected result for test case {idx}: expected to contain = `{contains}`, got `{res}`"
			);
		}

		server1.verify().await;
		server2.verify().await;
	}
}
