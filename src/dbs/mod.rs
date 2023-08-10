use crate::cli::CF;
use crate::err::Error;
use clap::{ArgAction, Args};
use std::sync::OnceLock;
use std::time::Duration;
use surrealdb::dbs::capabilities::{Capabilities, FuncTarget, NetTarget, Targets};
use surrealdb::kvs::Datastore;
use surrealdb::opt::auth::Root;

pub static DB: OnceLock<Datastore> = OnceLock::new();

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
	#[arg(help = "Whether to enable authentication", help_heading = "Authentication")]
	#[arg(env = "SURREAL_AUTH", long = "auth")]
	#[arg(default_value_t = false)]
	auth_enabled: bool,
	#[command(flatten)]
	#[command(next_help_heading = "Capabilities")]
	caps: DbsCapabilities,
}

#[derive(Args, Debug)]
struct DbsCapabilities {
	//
	// Allow
	//
	#[arg(help = "Allow all capabilities")]
	#[arg(env = "SURREAL_CAPS_ALLOW_ALL", short = 'A', long, conflicts_with = "deny_all")]
	#[arg(default_missing_value_os = "true", action = ArgAction::Set, num_args = 0..)]
	#[arg(default_value_t = false, hide_default_value = true)]
	allow_all: bool,

	#[cfg(feature = "scripting")]
	#[arg(help = "Allow execution of scripting functions")]
	#[arg(env = "SURREAL_CAPS_ALLOW_SCRIPT", long, conflicts_with = "allow_all")]
	#[arg(default_missing_value_os = "true", action = ArgAction::Set, num_args = 0..)]
	#[arg(default_value_t = true, hide_default_value = true)]
	allow_scripting: bool,

	#[arg(
		help = "Allow execution of functions. Optionally, you can provide a comma-separated list of function names to allow",
		long_help = r#"Allow execution of functions. Optionally, you can provide a comma-separated list of function names to allow.
Function names must be in the form <family>[::<name>]. For example:
 - 'http' or 'http::*' -> Include all functions in the 'http' family
 - 'http::get' -> Include only the 'get' function in the 'http' family
"#
	)]
	#[arg(env = "SURREAL_CAPS_ALLOW_FUNC", long, conflicts_with = "allow_all")]
	// If the arg is provided without value, then assume it's "", which gets parsed into Targets::All
	#[arg(default_value_os = "", default_missing_value_os = "", num_args = 0..)]
	#[arg(value_parser = super::cli::validator::func_targets)]
	allow_funcs: Option<Targets<FuncTarget>>,

	#[arg(
		help = "Allow all outbound network access. Optionally, you can provide a comma-separated list of targets to allow",
		long_help = r#"Allow all outbound network access. Optionally, you can provide a comma-separated list of targets to allow.
Targets must be in the form of <host>[:<port>], <ipv4|ipv6>[/<mask>]. For example:
 - 'surrealdb.com', '127.0.0.1' or 'fd00::1' -> Match outbound connections to these hosts on any port
 - 'surrealdb.com:80', '127.0.0.1:80' or 'fd00::1:80' -> Match outbound connections to these hosts on port 80
 - '10.0.0.0/8' or 'fd00::/8' -> Match outbound connections to any host in these networks
"#
	)]
	#[arg(env = "SURREAL_CAPS_ALLOW_NET", long, conflicts_with = "allow_all")]
	// If the arg is provided without value, then assume it's "", which gets parsed into Targets::All
	#[arg(default_value_os = "", default_missing_value_os = "", num_args = 0..)]
	#[arg(value_parser = super::cli::validator::net_targets)]
	allow_net: Option<Targets<NetTarget>>,

	//
	// Deny
	//
	#[arg(help = "Deny all capabilities")]
	#[arg(env = "SURREAL_CAPS_DENY_ALL", short = 'D', long, conflicts_with = "allow_all")]
	#[arg(default_missing_value_os = "true", action = ArgAction::Set, num_args = 0..)]
	#[arg(default_value_t = false, hide_default_value = true)]
	deny_all: bool,

	#[cfg(feature = "scripting")]
	#[arg(help = "Deny execution of scripting functions")]
	#[arg(env = "SURREAL_CAPS_DENY_SCRIPT", long, conflicts_with = "deny_all")]
	#[arg(default_missing_value_os = "true", action = ArgAction::Set, num_args = 0..)]
	#[arg(default_value_t = false, hide_default_value = true)]
	deny_scripting: bool,

	#[arg(
		help = "Deny execution of functions. Optionally, you can provide a comma-separated list of function names to deny",
		long_help = r#"Deny execution of functions. Optionally, you can provide a comma-separated list of function names to deny.
Function names must be in the form <family>[::<name>]. For example:
 - 'http' or 'http::*' -> Include all functions in the 'http' family
 - 'http::get' -> Include only the 'get' function in the 'http' family
"#
	)]
	#[arg(env = "SURREAL_CAPS_DENY_FUNC", long, conflicts_with = "deny_all")]
	// If the arg is provided without value, then assume it's "", which gets parsed into Targets::All
	#[arg(default_missing_value_os = "", num_args = 0..)]
	#[arg(value_parser = super::cli::validator::func_targets)]
	deny_funcs: Option<Targets<FuncTarget>>,

	#[arg(
		help = "Deny all outbound network access. Optionally, you can provide a comma-separated list of targets to deny",
		long_help = r#"Deny all outbound network access. Optionally, you can provide a comma-separated list of targets to deny.
Targets must be in the form of <host>[:<port>], <ipv4|ipv6>[/<mask>]. For example:
 - 'surrealdb.com', '127.0.0.1' or 'fd00::1' -> Match outbound connections to these hosts on any port
 - 'surrealdb.com:80', '127.0.0.1:80' or 'fd00::1:80' -> Match outbound connections to these hosts on port 80
 - '10.0.0.0/8' or 'fd00::/8' -> Match outbound connections to any host in these networks
"#
	)]
	#[arg(env = "SURREAL_CAPS_DENY_NET", long, conflicts_with = "deny_all")]
	// If the arg is provided without value, then assume it's "", which gets parsed into Targets::All
	#[arg(default_missing_value_os = "", num_args = 0..)]
	// If deny_all is true, disable this arg and assume a default of Targets::All
	#[arg(conflicts_with = "deny_all", default_value_if("deny_all", "true", ""))]
	#[arg(value_parser = super::cli::validator::net_targets)]
	deny_net: Option<Targets<NetTarget>>,
}

impl DbsCapabilities {
	#[cfg(feature = "scripting")]
	fn get_scripting(&self) -> bool {
		(self.allow_all || self.allow_scripting) && !(self.deny_all || self.deny_scripting)
	}

	#[cfg(not(feature = "scripting"))]
	fn get_scripting(&self) -> bool {
		false
	}

	fn get_allow_funcs(&self) -> Targets<FuncTarget> {
		if self.deny_all || matches!(self.deny_funcs, Some(Targets::All)) {
			return Targets::None;
		}

		if self.allow_all {
			return Targets::All;
		}

		// If allow_funcs was not provided and allow_all is false, then don't allow anything (Targets::None)
		self.allow_funcs.clone().unwrap_or(Targets::None)
	}

	fn get_allow_net(&self) -> Targets<NetTarget> {
		if self.deny_all || matches!(self.deny_net, Some(Targets::All)) {
			return Targets::None;
		}

		if self.allow_all {
			return Targets::All;
		}

		// If allow_net was not provided and allow_all is false, then don't allow anything (Targets::None)
		self.allow_net.clone().unwrap_or(Targets::None)
	}

	fn get_deny_funcs(&self) -> Targets<FuncTarget> {
		if self.deny_all {
			return Targets::All;
		}

		// If deny_funcs was not provided and deny_all is false, then don't deny anything (Targets::None)
		self.deny_funcs.clone().unwrap_or(Targets::None)
	}

	fn get_deny_net(&self) -> Targets<NetTarget> {
		if self.deny_all {
			return Targets::All;
		}

		// If deny_net was not provided and deny_all is false, then don't deny anything (Targets::None)
		self.deny_net.clone().unwrap_or(Targets::None)
	}
}

impl From<DbsCapabilities> for Capabilities {
	fn from(caps: DbsCapabilities) -> Self {
		Capabilities::default()
			.with_scripting(caps.get_scripting())
			.with_allow_funcs(caps.get_allow_funcs())
			.with_deny_funcs(caps.get_deny_funcs())
			.with_allow_net(caps.get_allow_net())
			.with_deny_net(caps.get_deny_net())
	}
}

pub async fn init(
	StartCommandDbsOptions {
		strict_mode,
		query_timeout,
		transaction_timeout,
		auth_enabled,
		caps,
	}: StartCommandDbsOptions,
) -> Result<(), Error> {
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

	if auth_enabled {
		info!("✅🔒 Authentication is enabled 🔒✅");
	} else {
		warn!("❌🔒 IMPORTANT: Authentication is disabled. This is not recommended for production use. 🔒❌");
	}
	// Parse and setup the desired kv datastore
	let dbs = Datastore::new(&opt.path)
		.await?
		.with_notifications()
		.with_strict_mode(strict_mode)
		.with_query_timeout(query_timeout)
		.with_transaction_timeout(transaction_timeout)
		.with_auth_enabled(auth_enabled)
		.with_capabilities(caps.into());

	dbs.bootstrap().await?;

	if let Some(user) = opt.user.as_ref() {
		dbs.setup_initial_creds(Root {
			username: user,
			password: opt.pass.as_ref().unwrap(),
		})
		.await?;
	}

	// Store database instance
	let _ = DB.set(dbs);

	// All ok
	Ok(())
}

#[cfg(test)]
mod tests {
	use std::str::FromStr;

	use surrealdb::dbs::Session;
	use surrealdb::iam::verify::verify_creds;
	use surrealdb::kvs::Datastore;
	use test_log::test;

	use super::*;

	#[test(tokio::test)]
	async fn test_setup_superuser() {
		let ds = Datastore::new("memory").await.unwrap();
		let creds = Root {
			username: "root",
			password: "root",
		};

		// Setup the initial user if there are no root users
		assert_eq!(
			ds.transaction(false, false).await.unwrap().all_root_users().await.unwrap().len(),
			0
		);
		ds.setup_initial_creds(creds).await.unwrap();
		assert_eq!(
			ds.transaction(false, false).await.unwrap().all_root_users().await.unwrap().len(),
			1
		);
		verify_creds(&ds, None, None, creds.username, creds.password).await.unwrap();

		// Do not setup the initial root user if there are root users:
		// Test the scenario by making sure the custom password doesn't change.
		let sql = "DEFINE USER root ON ROOT PASSWORD 'test' ROLES OWNER";
		let sess = Session::owner();
		ds.execute(sql, &sess, None).await.unwrap();
		let pass_hash = ds
			.transaction(false, false)
			.await
			.unwrap()
			.get_root_user(creds.username)
			.await
			.unwrap()
			.hash;

		ds.setup_initial_creds(creds).await.unwrap();
		assert_eq!(
			pass_hash,
			ds.transaction(false, false)
				.await
				.unwrap()
				.get_root_user(creds.username)
				.await
				.unwrap()
				.hash
		)
	}

	#[test(tokio::test)]
	async fn test_capabilities() {
		// (Capabilities, Query, Succeeds, Response Contains)
		let cases = vec![
			//
			// Functions and Networking is allowed
			//
			(
				Capabilities::default(),
				"RETURN http::get('http://127.0.0.1')",
				false,
				"Connection refused",
			),
			//
			// Scripting is allowed
			//
			(Capabilities::default(), "RETURN function() { return '1' }", true, "1"),
			//
			// Scripting is not allowed
			//
			(
				Capabilities::default().with_scripting(false),
				"RETURN function() { return '1' }",
				false,
				"Scripting functions are not allowed",
			),
			//
			// Some functions are not allowed
			//
			(
				Capabilities::default()
					.with_allow_funcs(Targets::<FuncTarget>::Some(
						[FuncTarget::from_str("http::*").unwrap()].into(),
					))
					.with_deny_funcs(Targets::<FuncTarget>::Some(
						[FuncTarget::from_str("http::get").unwrap()].into(),
					)),
				"RETURN http::get('http://127.0.0.1')",
				false,
				"Function 'http::get' is not allowed",
			),
			(
				Capabilities::default()
					.with_allow_funcs(Targets::<FuncTarget>::Some(
						[FuncTarget::from_str("http::*").unwrap()].into(),
					))
					.with_deny_funcs(Targets::<FuncTarget>::Some(
						[FuncTarget::from_str("http::get").unwrap()].into(),
					)),
				"RETURN http::head('http://127.0.0.1')",
				false,
				"Connection refused",
			),
			(
				Capabilities::default()
					.with_allow_funcs(Targets::<FuncTarget>::Some(
						[FuncTarget::from_str("http::*").unwrap()].into(),
					))
					.with_deny_funcs(Targets::<FuncTarget>::Some(
						[FuncTarget::from_str("http::get").unwrap()].into(),
					)),
				"RETURN string::len('a')",
				false,
				"Function 'string::len' is not allowed",
			),
			//
			// Some net targets are not allowed
			//
			(
				Capabilities::default()
					.with_allow_net(Targets::<NetTarget>::Some(
						[
							NetTarget::from_str("localhost").unwrap(),
							NetTarget::from_str("127.0.0.1").unwrap(),
						]
						.into(),
					))
					.with_deny_net(Targets::<NetTarget>::Some(
						[NetTarget::from_str("127.0.0.1").unwrap()].into(),
					)),
				"RETURN http::get('http://127.0.0.1')",
				false,
				"Acess to network target 'http://127.0.0.1/' is not allowed",
			),
			(
				Capabilities::default()
					.with_allow_net(Targets::<NetTarget>::Some(
						[
							NetTarget::from_str("localhost").unwrap(),
							NetTarget::from_str("127.0.0.1").unwrap(),
						]
						.into(),
					))
					.with_deny_net(Targets::<NetTarget>::Some(
						[NetTarget::from_str("127.0.0.1").unwrap()].into(),
					)),
				"RETURN http::get('http://1.1.1.1')",
				false,
				"Acess to network target 'http://1.1.1.1/' is not allowed",
			),
			(
				Capabilities::default()
					.with_allow_net(Targets::<NetTarget>::Some(
						[
							NetTarget::from_str("localhost").unwrap(),
							NetTarget::from_str("127.0.0.1").unwrap(),
						]
						.into(),
					))
					.with_deny_net(Targets::<NetTarget>::Some(
						[NetTarget::from_str("127.0.0.1").unwrap()].into(),
					)),
				"RETURN http::get('http://localhost')",
				false,
				"Connection refused",
			),
		];

		for (idx, (caps, query, succeeds, contains)) in cases.into_iter().enumerate() {
			let ds = Datastore::new("memory").await.unwrap().with_capabilities(caps);

			let sess = Session::owner();
			let res = ds.execute(query, &sess, None).await;

			let res = res.unwrap().remove(0).output();
			let res = if succeeds {
				assert!(res.is_ok(), "Unexpected error for test case {}: {:?}", idx, res);
				res.unwrap().to_string()
			} else {
				assert!(res.is_err(), "Unexpected success for test case {}: {:?}", idx, res);
				res.unwrap_err().to_string()
			};

			assert!(res.contains(contains), "Unexpected result for test case {}: {}", idx, res);
		}
	}
}
