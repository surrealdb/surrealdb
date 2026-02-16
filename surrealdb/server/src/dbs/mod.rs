use std::fs;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::Result;
use clap::Args;
use rand::Rng;
use surrealdb::opt::capabilities::Capabilities as SdkCapabilities;
use surrealdb_core::buc::BucketStoreProvider;
use surrealdb_core::kvs::{Datastore, TransactionBuilderFactory};
use tokio::time::{Instant, sleep, timeout};
use tokio_util::sync::CancellationToken;

use crate::cli::Config;
use crate::core::dbs::capabilities::{
	ArbitraryQueryTarget, Capabilities, ExperimentalTarget, FuncTarget, MethodTarget, NetTarget,
	RouteTarget, Targets,
};
use crate::core::dbs::{NewPlannerStrategy, Session};

const TARGET: &str = "surreal::dbs";

#[derive(Args, Debug)]
pub struct StartCommandDbsOptions {
	#[arg(help = "Whether strict mode is enabled on this database instance")]
	#[arg(env = "SURREAL_STRICT", short = 's', long = "strict", hide = true)]
	strict_mode: Option<bool>,
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
	#[arg(help = "Path to a SurrealQL file that will be imported when starting the server")]
	#[arg(env = "SURREAL_IMPORT_FILE", long = "import-file")]
	#[arg(value_parser = super::cli::validator::file_exists)]
	import_file: Option<PathBuf>,
	// Slow query logging configuration. When `slow_log_threshold` is set, any
	// statement taking longer than the threshold will be logged along with a
	// normalized, single-line SQL rendering. You can control which `$param`
	// values appear in the log with the following lists:
	// - `slow_log_param_deny` takes precedence and excludes matches.
	// - If `slow_log_param_allow` is non-empty, only listed parameter names are included;
	//   otherwise all parameters are allowed by default (subject to deny).
	#[arg(help = "The minimum execution time in milliseconds to trigger slow query logging")]
	#[arg(env = "SURREAL_SLOW_QUERY_LOG_THRESHOLD", long = "slow-log-threshold")]
	#[arg(value_parser = super::cli::validator::duration)]
	slow_log_threshold: Option<Duration>,
	#[arg(help = "A comma-separated list of parameter names to include in slow query logs")]
	#[arg(env = "SURREAL_SLOW_QUERY_LOG_PARAM_ALLOW", long = "slow-log-param-allow")]
	#[arg(value_delimiter = ',', num_args = 1..)]
	slow_log_param_allow: Vec<String>,
	#[arg(help = "A comma-separated list of parameter names to omit from slow query logs")]
	#[arg(env = "SURREAL_SLOW_QUERY_LOG_PARAM_DENY", long = "slow-log-param-deny")]
	#[arg(value_delimiter = ',', num_args = 1..)]
	slow_log_param_deny: Vec<String>,
	#[arg(help = "The default namespace for a new instance")]
	#[arg(env = "SURREAL_DEFAULT_NAMESPACE", long = "default-namespace")]
	default_namespace: Option<String>,
	#[arg(help = "The default database for a new instance")]
	#[arg(env = "SURREAL_DEFAULT_DATABASE", long = "default-database")]
	default_database: Option<String>,
	#[arg(help = "Whether to disable default namespace and database creation")]
	#[arg(env = "SURREAL_NO_DEFAULTS", long = "no-defaults", conflicts_with_all = ["default_namespace", "default_database"])]
	#[arg(default_value_t = false)]
	no_defaults: bool,
}

#[derive(Args, Debug)]
pub struct DbsCapabilities {
	//
	// Allow
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
	// If the arg is provided without value, then assume it's "", which gets parsed into
	// Targets::All
	#[arg(default_missing_value_os = "", num_args = 0..)]
	#[arg(value_parser = super::cli::validator::func_targets)]
	allow_funcs: Option<Targets<FuncTarget>>,

	#[arg(hide = true)]
	#[arg(env = "SURREAL_CAPS_ALLOW_EXPERIMENTAL", long)]
	// If the arg is provided without value, then assume it's "", which gets parsed into
	// Targets::All
	#[arg(default_missing_value_os = "", num_args = 0..)]
	#[arg(value_parser = super::cli::validator::experimental_targets)]
	allow_experimental: Option<Targets<ExperimentalTarget>>,

	#[arg(
		help = "Allow execution of arbitrary queries by certain user groups except when specifically denied.",
		long_help = r#"Allow execution of arbitrary queries by certain user groups except when specifically denied. Alternatively, you can provide a comma-separated list of user groups to allow
Specifically denied user groups prevail over any other allowed user group.
User groups must be one of "guest", "record" or "system".
"#
	)]
	#[arg(env = "SURREAL_CAPS_ALLOW_ARBITRARY_QUERY", long)]
	// If the arg is provided without value, then assume it's "", which gets parsed into
	// Targets::All
	#[arg(default_missing_value_os = "", num_args = 0..)]
	#[arg(value_parser = super::cli::validator::query_arbitrary_targets)]
	allow_arbitrary_query: Option<Targets<ArbitraryQueryTarget>>,

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
	// If the arg is provided without value, then assume it's "", which gets parsed into
	// Targets::All
	#[arg(default_missing_value_os = "", num_args = 0..)]
	#[arg(value_parser = super::cli::validator::net_targets)]
	allow_net: Option<Targets<NetTarget>>,

	#[arg(
		help = "Allow all RPC methods to be called except for routes that are specifically denied. Alternatively, you can provide a comma-separated list of RPC methods to allow."
	)]
	#[arg(env = "SURREAL_CAPS_ALLOW_RPC", long)]
	// If the arg is provided without value, then assume it's "", which gets parsed into
	// Targets::All
	#[arg(default_missing_value_os = "", num_args = 0..)]
	#[arg(default_value_os = "")] // Allow all RPC methods by default
	#[arg(value_parser = super::cli::validator::method_targets)]
	allow_rpc: Option<Targets<MethodTarget>>,

	#[arg(
		help = "Allow all HTTP routes to be requested except for routes that are specifically denied. Alternatively, you can provide a comma-separated list of HTTP routes to allow."
	)]
	#[arg(env = "SURREAL_CAPS_ALLOW_HTTP", long)]
	// If the arg is provided without value, then assume it's "", which gets parsed into
	// Targets::All
	#[arg(default_missing_value_os = "", num_args = 0..)]
	#[arg(default_value_os = "")] // Allow all HTTP routes by default
	#[arg(value_parser = super::cli::validator::route_targets)]
	allow_http: Option<Targets<RouteTarget>>,

	//
	// Deny
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
	// If the arg is provided without value, then assume it's "", which gets parsed into
	// Targets::All
	#[arg(default_missing_value_os = "", num_args = 0..)]
	#[arg(value_parser = super::cli::validator::func_targets)]
	deny_funcs: Option<Targets<FuncTarget>>,

	#[arg(hide = true)]
	#[arg(env = "SURREAL_CAPS_DENY_EXPERIMENTAL", long)]
	// If the arg is provided without value, then assume it's "", which gets parsed into
	// Targets::All
	#[arg(default_missing_value_os = "", num_args = 0..)]
	#[arg(value_parser = super::cli::validator::experimental_targets)]
	deny_experimental: Option<Targets<ExperimentalTarget>>,

	#[arg(
		help = "Deny execution of arbitrary queries by certain user groups except when specifically allowed.",
		long_help = r#"Deny execution of arbitrary queries by certain user groups except when specifically allowed. Alternatively, you can provide a comma-separated list of user groups to deny
Specifically allowed user groups prevail over a general denial of user group.
User groups must be one of "guest", "record" or "system".
"#
	)]
	#[arg(env = "SURREAL_CAPS_DENY_ARBITRARY_QUERY", long)]
	// If the arg is provided without value, then assume it's "", which gets parsed into
	// Targets::All
	#[arg(default_missing_value_os = "", num_args = 0..)]
	#[arg(value_parser = super::cli::validator::query_arbitrary_targets)]
	deny_arbitrary_query: Option<Targets<ArbitraryQueryTarget>>,

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
	// If the arg is provided without value, then assume it's "", which gets parsed into
	// Targets::All
	#[arg(default_missing_value_os = "", num_args = 0..)]
	#[arg(value_parser = super::cli::validator::net_targets)]
	deny_net: Option<Targets<NetTarget>>,

	#[arg(
		help = "Deny all RPC methods from being called except for methods that are specifically allowed. Alternatively, you can provide a comma-separated list of RPC methods to deny."
	)]
	#[arg(env = "SURREAL_CAPS_DENY_RPC", long)]
	// If the arg is provided without value, then assume it's "", which gets parsed into
	// Targets::All
	#[arg(default_missing_value_os = "", num_args = 0..)]
	#[arg(value_parser = super::cli::validator::method_targets)]
	deny_rpc: Option<Targets<MethodTarget>>,

	#[arg(
		help = "Deny all HTTP routes from being requested except for routes that are specifically allowed. Alternatively, you can provide a comma-separated list of HTTP routes to deny."
	)]
	#[arg(env = "SURREAL_CAPS_DENY_HTTP", long)]
	// If the arg is provided without value, then assume it's "", which gets parsed into
	// Targets::All
	#[arg(default_missing_value_os = "", num_args = 0..)]
	#[arg(value_parser = super::cli::validator::route_targets)]
	deny_http: Option<Targets<RouteTarget>>,

	#[arg(
		help = "Strategy for the streaming query planner: 'best-effort' (default), 'compute-only', or 'all-read-only'"
	)]
	#[arg(env = "SURREAL_PLANNER_STRATEGY", long = "planner-strategy")]
	#[arg(default_value = "best-effort")]
	planner_strategy: NewPlannerStrategy,
}

impl DbsCapabilities {
	#[cfg(feature = "scripting")]
	fn get_scripting(&self) -> bool {
		// Even if there was a global deny, we allow if there is a specific allow for
		// scripting Even if there is a global allow, we deny if there is a specific
		// deny for scripting
		self.allow_scripting || (self.allow_all && !self.deny_scripting)
	}

	#[cfg(not(feature = "scripting"))]
	fn get_scripting(&self) -> bool {
		false
	}

	fn get_allow_guests(&self) -> bool {
		// Even if there was a global deny, we allow if there is a specific allow for
		// guests Even if there is a global allow, we deny if there is a specific deny
		// for guests
		self.allow_guests || (self.allow_all && !self.deny_guests)
	}

	fn get_allow_funcs(&self) -> Targets<FuncTarget> {
		// If there was a global deny, we allow if there is a general allow or some
		// specific allows for functions
		if self.deny_all {
			if let Some(targets) = &self.allow_funcs {
				match targets {
					Targets::None => {}
					Targets::Some(_) => {
						return targets.clone();
					}
					Targets::All => {
						return Targets::All;
					}
				}
			}
			return Targets::None;
		}

		// If there was a general deny for functions, we allow if there are specific
		// allows for functions
		if let Some(Targets::All) = self.deny_funcs {
			if let Some(targets) = &self.allow_funcs
				&& let Targets::Some(_) = targets
			{
				return targets.clone();
			}
			return Targets::None;
		}

		// If there are no high level denies but there is a global allow, we allow
		// functions
		if self.allow_all {
			return Targets::All;
		}

		// If there are no high level, we allow the provided functions
		// If nothing was provided, we allow functions by default (Targets::All)
		self.allow_funcs.clone().unwrap_or(Targets::All) // Functions are enabled by default for the server
	}

	fn get_allow_net(&self) -> Targets<NetTarget> {
		// If there was a global deny, we allow if there is a general allow or some
		// specific allows for networks
		if self.deny_all {
			if let Some(targets) = &self.allow_net {
				match targets {
					Targets::None => {}
					Targets::Some(_) => {
						return targets.clone();
					}
					Targets::All => {
						return Targets::All;
					}
				}
			}
			return Targets::None;
		}

		// If there was a general deny for networks, we allow if there are specific
		// allows for networks
		if let Some(Targets::All) = self.deny_net {
			if let Some(targets) = &self.allow_net
				&& let Targets::Some(_) = targets
			{
				return targets.clone();
			}
			return Targets::None;
		}

		// If there are no high level denies but there is a global allow, we allow
		// networks
		if self.allow_all {
			return Targets::All;
		}

		// If there are no high level denies, we allow the provided networks
		// If nothing was provided, we do not allow network by default (Targets::None)
		self.allow_net.clone().unwrap_or(Targets::None)
	}

	fn get_allow_rpc(&self) -> Targets<MethodTarget> {
		// If there was a global deny, we allow if there is a general allow or some
		// specific allows for RPC
		if self.deny_all {
			if let Some(targets) = &self.allow_rpc {
				match targets {
					Targets::None => {}
					Targets::Some(_) => {
						return targets.clone();
					}
					Targets::All => {
						return Targets::All;
					}
				}
			}
			return Targets::None;
		}

		// If there was a general deny for RPC, we allow if there are specific allows
		// for RPC methods
		if let Some(Targets::All) = self.deny_rpc {
			if let Some(targets) = self.allow_rpc.as_ref()
				&& let Targets::Some(_) = targets
			{
				return targets.clone();
			}
			return Targets::None;
		}

		// If there are no high level denies but there is a global allow, we allow RPC
		if self.allow_all {
			return Targets::All;
		}

		// If there are no high level denies, we allow the provided RPC methods
		// If nothing was provided, we allow RPC by default (Targets::All)
		self.allow_rpc.clone().unwrap_or(Targets::All) // RPC is enabled by default for the server
	}

	fn get_allow_http(&self) -> Targets<RouteTarget> {
		// If there was a global deny, we allow if there is a general allow or some
		// specific allows for HTTP
		if self.deny_all {
			if let Some(targets) = self.allow_http.as_ref() {
				match targets {
					Targets::None => {}
					Targets::Some(_) => {
						return targets.clone();
					}
					Targets::All => {
						return Targets::All;
					}
				}
			}
			return Targets::None;
		}

		// If there was a general deny for HTTP, we allow if there are specific allows
		// for HTTP routes
		if let Some(Targets::All) = self.deny_http {
			if let Some(targets) = self.allow_http.as_ref()
				&& let Targets::Some(_) = targets
			{
				return targets.clone();
			}
			return Targets::None;
		}

		// If there are no high level denies but there is a global allow, we allow HTTP
		if self.allow_all {
			return Targets::All;
		}

		// If there are no high level denies, we allow the provided HTTP routes
		// If nothing was provided, we allow HTTP by default (Targets::All)
		self.allow_http.clone().unwrap_or(Targets::All) // HTTP is enabled by default for the server
	}

	fn get_allow_experimental(&self) -> Targets<ExperimentalTarget> {
		// If there was a global deny, we allow if there is a general allow or some
		// specific allows for experimental features
		if self.deny_all {
			return self.allow_experimental.clone().unwrap_or(Targets::None);
		}

		// If there was a general deny for experimental features, we allow if there are
		// specific targets
		if let Some(Targets::All) = self.deny_experimental {
			match &self.allow_experimental {
				Some(t @ Targets::Some(_)) => return t.clone(),
				_ => return Targets::None,
			}
		}

		// If there are no high level denies, we allow the provided Experimental
		// features If nothing was provided, we deny Experimental targets by default
		// (Targets::None)
		self.allow_experimental.clone().unwrap_or(Targets::None) // Experimental targets are disabled by default for the server
	}

	fn get_allow_arbitrary_query(&self) -> Targets<ArbitraryQueryTarget> {
		// If there was a general deny for arbitrary queries, we allow if there are
		// specific allows for arbitrary query subjects
		if let Some(Targets::All) = self.deny_arbitrary_query {
			match &self.allow_arbitrary_query {
				Some(t @ Targets::Some(_)) => return t.clone(),
				_ => return Targets::None,
			}
		}

		// If there are no high level denies but there is a global allow, we allow
		// arbitrary queries
		if self.allow_all {
			return Targets::All;
		}

		// If there are no high level denies, we allow the provided arbitrary query
		// subjects If nothing was provided, we allow arbitrary queries by default
		// (Targets::All)
		self.allow_arbitrary_query.clone().unwrap_or(Targets::All) // arbitrary queries are enabled by default for the server
	}

	fn get_deny_funcs(&self) -> Targets<FuncTarget> {
		// Allowed functions already consider a global deny and a general deny for
		// functions On top of what is explicitly allowed, we deny what is
		// specifically denied
		if let Some(targets) = &self.deny_funcs
			&& let Targets::Some(_) = targets
		{
			return targets.clone();
		}
		Targets::None
	}

	fn get_deny_net(&self) -> Targets<NetTarget> {
		// Allowed networks already consider a global deny and a general deny for
		// networks On top of what is explicitly allowed, we deny what is specifically
		// denied
		if let Some(targets) = &self.deny_net
			&& let Targets::Some(_) = targets
		{
			return targets.clone();
		}
		Targets::None
	}

	fn get_deny_all(&self) -> bool {
		self.deny_all
	}

	fn get_deny_rpc(&self) -> Targets<MethodTarget> {
		// Allowed RPC methods already consider a global deny and a general deny for RPC
		// On top of what is explicitly allowed, we deny what is specifically denied
		if let Some(targets) = &self.deny_rpc
			&& let Targets::Some(_) = targets
		{
			return targets.clone();
		}
		Targets::None
	}

	fn get_deny_http(&self) -> Targets<RouteTarget> {
		// Allowed HTTP routes already consider a global deny and a general deny for
		// HTTP On top of what is explicitly allowed, we deny what is specifically
		// denied
		if let Some(targets) = self.deny_http.as_ref()
			&& let Targets::Some(_) = targets
		{
			return targets.clone();
		}
		Targets::None
	}

	fn get_deny_experimental(&self) -> Targets<ExperimentalTarget> {
		// Allowed experimental targets already consider a global deny and a general
		// deny for experimental targets On top of what is explicitly allowed, we deny
		// what is specifically denied
		if let Some(t @ Targets::Some(_)) = &self.deny_experimental {
			t.clone()
		} else {
			Targets::None
		}
	}

	fn get_deny_arbitrary_query(&self) -> Targets<ArbitraryQueryTarget> {
		// Allowed arbitrary queryies already consider a global deny and a general deny
		// for arbitr On top of what is explicitly allowed, we deny what is
		// specifically denied
		if let Some(t @ Targets::Some(_)) = &self.deny_arbitrary_query {
			t.clone()
		} else {
			Targets::None
		}
	}

	pub fn into_cli_capabilities(self) -> Capabilities {
		merge_capabilities(SdkCapabilities::all().into(), self)
	}
}

fn merge_capabilities(initial: Capabilities, caps: DbsCapabilities) -> Capabilities {
	initial
		.with_scripting(caps.get_scripting())
		.with_guest_access(caps.get_allow_guests())
		.with_functions(caps.get_allow_funcs())
		.without_functions(caps.get_deny_funcs())
		.with_network_targets(caps.get_allow_net())
		.without_network_targets(caps.get_deny_net())
		.with_rpc_methods(caps.get_allow_rpc())
		.without_rpc_methods(caps.get_deny_rpc())
		.with_http_routes(caps.get_allow_http())
		.without_http_routes(caps.get_deny_http())
		.with_experimental(caps.get_allow_experimental())
		.without_experimental(caps.get_deny_experimental())
		.with_arbitrary_query(caps.get_allow_arbitrary_query())
		.without_arbitrary_query(caps.get_deny_arbitrary_query())
		.with_planner_strategy(caps.planner_strategy)
}

impl From<DbsCapabilities> for Capabilities {
	fn from(caps: DbsCapabilities) -> Self {
		merge_capabilities(Default::default(), caps)
	}
}

/// Retry an async operation until it succeeds or a timeout is reached.
/// This is required for operations that rely on remote or distributed KV store
/// that may not be immediately available.
///
/// # Parameters
/// - `operation_name`: Name of the operation for logging purposes
/// - `f`: The async function to retry
///
/// # Returns
/// The result of the operation if successful within the timeout
async fn retry_with_timeout<F, Fut, T, E>(operation_name: &str, f: F) -> Result<T, anyhow::Error>
where
	F: Fn() -> Fut,
	Fut: Future<Output = Result<T, E>>,
	E: std::fmt::Display + std::fmt::Debug,
{
	let timeout_duration = Duration::from_secs(60);
	let start = Instant::now();
	let mut attempt = 0;

	loop {
		attempt += 1;

		match timeout(timeout_duration.saturating_sub(start.elapsed()), f()).await {
			Ok(Ok(result)) => {
				if attempt > 1 {
					info!(target: TARGET, operation = operation_name, attempts = attempt, "Operation succeeded after retry");
				} else {
					info!(target: TARGET, operation = operation_name, attempts = attempt, "Operation succeeded");
				}
				return Ok(result);
			}
			Ok(Err(e)) => {
				let elapsed = start.elapsed();
				if elapsed >= timeout_duration {
					return Err(anyhow::anyhow!(
						"Operation '{}' failed after {} attempts over {:?}: {}",
						operation_name,
						attempt,
						elapsed,
						e
					));
				}

				debug!(
					target: TARGET,
					operation = operation_name,
					attempt = attempt,
					error = %e,
					"Operation failed, retrying..."
				);

				// Exponential backoff with jitter: 100ms, 200ms, 400ms, 800ms, 1600ms, capped at 5s
				// Jitter adds randomness (0.5 to 1.5x) to prevent thundering herd
				let base_backoff = Duration::from_millis(100 * 2u64.pow((attempt - 1).min(5)));
				let base_backoff = base_backoff.min(Duration::from_secs(5));
				let jitter = rand::thread_rng().gen_range(0.5..=1.5);
				let backoff = base_backoff.mul_f64(jitter);
				sleep(backoff).await;
			}
			Err(_) => {
				return Err(anyhow::anyhow!(
					"Operation '{}' timed out after {} attempts over {:?}",
					operation_name,
					attempt,
					start.elapsed()
				));
			}
		}
	}
}

#[instrument(level = "trace", target = "surreal::dbs", skip_all)]
/// Initialise the database server
///
/// Creates and configures the datastore with the provided options.
///
/// # Parameters
/// - `factory`: Transaction builder factory for datastore backend selection
/// - `opt`: Server configuration including database path and authentication
/// - `canceller`: Token for graceful shutdown and cancellation of long-running operations
///
/// # Generic parameters
/// - `F`: Transaction builder factory type implementing `TransactionBuilderFactory`
pub async fn init<C: TransactionBuilderFactory + BucketStoreProvider>(
	composer: C,
	opt: &Config,
	canceller: CancellationToken,
	#[cfg_attr(not(storage), allow(unused_variables))] StartCommandDbsOptions {
		strict_mode,
		query_timeout,
		transaction_timeout,
		unauthenticated,
		capabilities,
		temporary_directory,
		import_file,
		slow_log_threshold,
		slow_log_param_allow,
		slow_log_param_deny,
		default_namespace,
		default_database,
		no_defaults,
	}: StartCommandDbsOptions,
) -> Result<Datastore> {
	// Warn about the strict mode flag being unused.
	if let Some(true) = strict_mode {
		warn!(
			"Strict mode is no longer defined on the server level. Use `DEFINE DATABASE <db> STRICT` instead. Ignoring strict mode flag."
		);
	}
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
		warn!(
			"❌🔒 IMPORTANT: Authentication is disabled. This is not recommended for production use. 🔒❌"
		);
	}
	// Warn about the impact of denying all capabilities
	if capabilities.get_deny_all() {
		warn!(
			"You are denying all capabilities by default. Although this is recommended, beware that any new capabilities will also be denied."
		);
	}
	if let Some(v) = slow_log_threshold {
		debug!("Slow log threshold is {v:?}");
	}
	if !slow_log_param_allow.is_empty() {
		debug!("Slow log param allow is {:?}", slow_log_param_allow);
	}
	if !slow_log_param_deny.is_empty() {
		debug!("Slow log param deny is {:?}", slow_log_param_deny);
	}
	// Convert the capabilities
	let capabilities = capabilities.into();
	// Log the specified server capabilities
	debug!("Server capabilities: {capabilities}");
	// Parse and setup the desired kv datastore
	let dbs = Datastore::new_with_factory::<C>(composer, &opt.path, canceller)
		.await?
		.with_notifications()
		.with_query_timeout(query_timeout)
		.with_transaction_timeout(transaction_timeout)
		.with_auth_enabled(!unauthenticated)
		.with_capabilities(capabilities)
		.with_slow_log(slow_log_threshold, slow_log_param_allow, slow_log_param_deny);
	#[cfg(storage)]
	let dbs = dbs.with_temporary_directory(temporary_directory);
	// Ensure the storage version is up to date to prevent corruption
	let (_, is_new) =
		retry_with_timeout("check_version", || async { dbs.check_version().await }).await?;
	// Create default namespace and database if not disabled
	if is_new && !no_defaults {
		let default_namespace = default_namespace.unwrap_or_else(|| "main".to_string());
		let default_database = default_database.unwrap_or_else(|| "main".to_string());
		// Initialise defaults
		retry_with_timeout("initialise_defaults", || async {
			dbs.initialise_defaults(&default_namespace, &default_database).await
		})
		.await?;
	}
	// Import file at start, if provided
	if let Some(file) = import_file {
		// Log the startup import path
		info!(target: TARGET, file = ?file, "Importing data from file");
		// Read the full file contents
		let sql = fs::read_to_string(file)?;
		// Execute the SurrealQL file
		retry_with_timeout("startup", || async { dbs.startup(&sql, &Session::owner()).await })
			.await?;
	}
	// Setup initial server auth credentials
	if let (Some(user), Some(pass)) = (opt.user.as_ref(), opt.pass.as_ref()) {
		// Log the initialisation of credentials
		info!(target: TARGET, user = %user, "Initialising credentials");
		// Initialise the credentials
		retry_with_timeout("initialise_credentials", || async {
			dbs.initialise_credentials(user, pass).await
		})
		.await?;
	}
	// Bootstrap the datastore
	retry_with_timeout("Insert node", || async { dbs.insert_node().await }).await?;
	retry_with_timeout("Expire nodes", || async { dbs.expire_nodes().await }).await?;
	retry_with_timeout("Remove nodes", || async { dbs.remove_nodes().await }).await?;
	// All ok
	Ok(dbs)
}

#[cfg(test)]
mod tests {
	use std::str::FromStr;

	use surrealdb_types::ToSql;
	use test_log::test;
	use wiremock::matchers::{method, path};
	use wiremock::{Mock, MockServer, ResponseTemplate};

	use super::*;

	#[test(tokio::test(flavor = "multi_thread"))]
	async fn test_capabilities() {
		let server1 = {
			let s = MockServer::start().await;
			let get = Mock::given(method("GET"))
				.and(path("/"))
				.respond_with(ResponseTemplate::new(200).set_body_string("SUCCESS"))
				.expect(1);

			let get2 = Mock::given(method("GET"))
				.and(path("/test"))
				.respond_with(ResponseTemplate::new(200).set_body_string("SUCCESS"))
				.expect(1);

			s.register(get).await;
			s.register(get2).await;
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

		let server3 = {
			let s = MockServer::start().await;
			let redirect_res = ResponseTemplate::new(301).append_header("Location", server1.uri());

			let redirect = Mock::given(method("GET"))
				.and(path("redirect"))
				.respond_with(redirect_res)
				.expect(1);

			s.register(redirect).await;
			s
		};

		// (Datastore, Session, Query, Succeeds, Response Contains)
		let cases = vec![
			//
			// 0 - Functions and Networking are allowed
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
			// 1 - Scripting is allowed
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
			// 2 - Scripting is not allowed
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
			// 3 - Anonymous actor when guest access is allowed and auth is enabled, succeeds
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
			// 4 - Anonymous actor when guest access is not allowed and auth is enabled, throws
			// error
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
			// 5 - Anonymous actor when guest access is not allowed and auth is disabled, succeeds
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
			// 6 - Authenticated user when guest access is not allowed and auth is enabled,
			// succeeds
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
			// 7 - Specific experimental feature enabled
			(
				Datastore::new("memory").await.unwrap().with_capabilities(
					Capabilities::default().with_experimental(ExperimentalTarget::Files.into()),
				),
				Session::owner().with_ns("test").with_db("test"),
				"DEFINE BUCKET test BACKEND \"memory\";".to_string(),
				true,
				"NONE".to_string(),
			),
			// 8 - Specific experimental feature disabled
			(
				Datastore::new("memory").await.unwrap().with_capabilities(
					Capabilities::default().without_experimental(ExperimentalTarget::Files.into()),
				),
				Session::owner().with_ns("test").with_db("test"),
				"DEFINE BUCKET test BACKEND \"memory\";".to_string(),
				false,
				"expected the experimental files feature to be enabled".to_string(),
			),
			//
			// 9 - Some functions are not allowed
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
			// 10 -
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
			// 11 -
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
			// 12 - Some net targets are not allowed
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
			// 13 -
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
			// 14 -
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
			(
				// 15 - Ensure redirect fails
				Datastore::new("memory").await.unwrap().with_capabilities(
					Capabilities::default()
						.with_functions(Targets::<FuncTarget>::All)
						.with_network_targets(Targets::<NetTarget>::Some(
							[NetTarget::from_str(&server3.address().to_string()).unwrap()].into(),
						))
						.without_network_targets(Targets::<NetTarget>::Some(
							[NetTarget::from_str(&server1.address().to_string()).unwrap()].into(),
						)),
				),
				Session::owner(),
				format!("RETURN http::get('{}/redirect')", server3.uri()),
				false,
				format!(
					"There was an error processing a remote HTTP request: error following redirect for url ({}/redirect)",
					server3.uri()
				),
			),
			(
				// 16 - Ensure connecting via localhost succeed
				Datastore::new("memory").await.unwrap().with_capabilities(
					Capabilities::default()
						.with_functions(Targets::<FuncTarget>::All)
						.with_network_targets(Targets::<NetTarget>::All),
				),
				Session::owner(),
				format!("RETURN http::get('http://localhost:{}/test')", server1.address().port()),
				true,
				"SUCCESS".to_string(),
			),
			// - 17
			(
				// Ensure connecting via localhost is denied when all IPs are blocked
				Datastore::new("memory").await.unwrap().with_capabilities(
					Capabilities::default()
						.with_functions(Targets::<FuncTarget>::All)
						.with_network_targets(Targets::<NetTarget>::All)
						.without_network_targets(Targets::<NetTarget>::Some(
							[
								NetTarget::from_str("127.0.0.1/0").unwrap(),
								NetTarget::from_str("::/0").unwrap(),
							]
							.into(),
						)),
				),
				Session::owner(),
				format!("RETURN http::get('http://localhost:{}')", server1.address().port()),
				false,
				"is not allowed".to_string(),
			),
			// 18 - Ensure redirect succeed
			(
				Datastore::new("memory").await.unwrap().with_capabilities(
					Capabilities::default()
						.with_functions(Targets::<FuncTarget>::All)
						.with_network_targets(Targets::<NetTarget>::Some(
							[NetTarget::from_str("github.com").unwrap()].into(),
						))
						.without_network_targets(Targets::<NetTarget>::Some(
							[
								NetTarget::from_str("0.0.0.0/8").unwrap(),
								NetTarget::from_str("10.0.0.0/8").unwrap(),
								NetTarget::from_str("10.18.0.0/16").unwrap(),
								NetTarget::from_str("10.2.0.0/16").unwrap(),
								NetTarget::from_str("100.64.0.0/10").unwrap(),
								NetTarget::from_str("127.0.0.0/8").unwrap(),
								NetTarget::from_str("169.254.0.0/16").unwrap(),
								NetTarget::from_str("172.16.0.0/12").unwrap(),
								NetTarget::from_str("172.20.0.0/16").unwrap(),
								NetTarget::from_str("192.0.0.0/24").unwrap(),
								NetTarget::from_str("192.168.0.0/16").unwrap(),
								NetTarget::from_str("192.88.99.0/24").unwrap(),
								NetTarget::from_str("198.18.0.0/15").unwrap(),
								NetTarget::from_str("::1/128").unwrap(),
								NetTarget::from_str("fc00::/7").unwrap(),
								NetTarget::from_str("fc00::/8").unwrap(),
							]
							.into(),
						)),
				),
				Session::owner(),
				// This will be redirected to: https://github.com/surrealdb/surrealdb/pull/6293
				"RETURN http::get('https://github.com/surrealdb/surrealdb/issues/6293')"
					.to_string(),
				true,
				"<!DOCTYPE html>".to_string(),
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
				res.unwrap().to_sql()
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
		server3.verify().await;
	}

	#[test]
	fn test_dbs_capabilities_target_all() {
		let caps = DbsCapabilities {
			allow_all: false,
			allow_scripting: false,
			allow_guests: false,
			allow_funcs: None,
			allow_experimental: Some(Targets::All),
			allow_arbitrary_query: Some(Targets::All),
			allow_net: None,
			allow_rpc: None,
			allow_http: None,
			deny_all: false,
			deny_scripting: false,
			deny_guests: false,
			deny_funcs: None,
			deny_experimental: None,
			deny_arbitrary_query: None,
			deny_net: None,
			deny_rpc: None,
			deny_http: None,
			planner_strategy: NewPlannerStrategy::default(),
		};
		assert_eq!(caps.get_allow_experimental(), Targets::All);
		assert_eq!(caps.get_allow_arbitrary_query(), Targets::All);
	}

	#[test]
	fn test_dbs_capabilities_deny_all_vs_allow_all() {
		// Test case 1: When deny_funcs = Targets::All and allow_funcs = Targets::All,
		// the deny should take precedence and return Targets::None
		let caps = DbsCapabilities {
			allow_all: false,
			#[cfg(feature = "scripting")]
			allow_scripting: false,
			allow_guests: false,
			allow_funcs: Some(Targets::All),
			allow_experimental: None,
			allow_arbitrary_query: None,
			allow_net: None,
			allow_rpc: None,
			allow_http: None,
			deny_all: false,
			#[cfg(feature = "scripting")]
			deny_scripting: false,
			deny_guests: false,
			deny_funcs: Some(Targets::All),
			deny_experimental: None,
			deny_arbitrary_query: None,
			deny_net: None,
			deny_rpc: None,
			deny_http: None,
			planner_strategy: NewPlannerStrategy::default(),
		};
		assert_eq!(
			caps.get_allow_funcs(),
			Targets::None,
			"When deny_funcs=All and allow_funcs=All, should deny (return None)"
		);

		// Test case 2: When deny_http = Targets::All and allow_http = Targets::All,
		// the deny should take precedence and return Targets::None
		let caps = DbsCapabilities {
			allow_all: false,
			#[cfg(feature = "scripting")]
			allow_scripting: false,
			allow_guests: false,
			allow_funcs: None,
			allow_experimental: None,
			allow_arbitrary_query: None,
			allow_net: None,
			allow_rpc: None,
			allow_http: Some(Targets::All),
			deny_all: false,
			#[cfg(feature = "scripting")]
			deny_scripting: false,
			deny_guests: false,
			deny_funcs: None,
			deny_experimental: None,
			deny_arbitrary_query: None,
			deny_net: None,
			deny_rpc: None,
			deny_http: Some(Targets::All),
			planner_strategy: NewPlannerStrategy::default(),
		};
		assert_eq!(
			caps.get_allow_http(),
			Targets::None,
			"When deny_http=All and allow_http=All, should deny (return None)"
		);

		// Test case 3: When deny_funcs = Targets::All and allow_funcs = Targets::None,
		// should return Targets::None
		let caps = DbsCapabilities {
			allow_all: false,
			#[cfg(feature = "scripting")]
			allow_scripting: false,
			allow_guests: false,
			allow_funcs: Some(Targets::None),
			allow_experimental: None,
			allow_arbitrary_query: None,
			allow_net: None,
			allow_rpc: None,
			allow_http: None,
			deny_all: false,
			#[cfg(feature = "scripting")]
			deny_scripting: false,
			deny_guests: false,
			deny_funcs: Some(Targets::All),
			deny_experimental: None,
			deny_arbitrary_query: None,
			deny_net: None,
			deny_rpc: None,
			deny_http: None,
			planner_strategy: NewPlannerStrategy::default(),
		};
		assert_eq!(
			caps.get_allow_funcs(),
			Targets::None,
			"When deny_funcs=All and allow_funcs=None, should deny (return None)"
		);
	}
}
