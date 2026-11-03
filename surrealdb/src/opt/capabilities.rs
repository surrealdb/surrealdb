//! The capabilities that can be enabled for a database instance

use std::collections::HashSet;
use std::mem;

use surrealdb_rpc::capabilities::{
	Capabilities as CoreCapabilities, ExperimentalTarget, FuncTarget, McpToolTarget,
	ParseFuncTargetError, ParseMcpToolTargetError, ParseNetTargetError, Targets,
};

/// The retired planner-strategy setting.
///
/// The engine has a single execution path, so no value selects anything. The
/// type remains so that code written against [`Capabilities::with_planner_strategy`]
/// keeps compiling.
#[deprecated(note = "the engine has a single execution path; the planner strategy has no effect")]
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum PlannerStrategy {
	/// Has no effect.
	BestEffort,
	/// Has no effect.
	ComputeOnly,
	/// Has no effect.
	AllReadOnly,
}

/// A list of features that are still experimental
#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash)]
#[non_exhaustive]
pub enum ExperimentalFeature {
	/// Enable Surrealism feature.
	Surrealism,
	/// Enable the `POST /kvexport` endpoint.
	KvExport,
}

/// Not public API
#[doc(hidden)]
impl From<&ExperimentalFeature> for ExperimentalTarget {
	fn from(feature: &ExperimentalFeature) -> Self {
		match feature {
			ExperimentalFeature::Surrealism => ExperimentalTarget::Surrealism,
			ExperimentalFeature::KvExport => ExperimentalTarget::KvExport,
		}
	}
}

/// Capabilities are used to limit what users are allowed to do using queries.
///
/// Capabilities are split into categories:
/// - Scripting: Whether or not users can execute scripts
/// - Guest access: Whether or not unauthenticated users can execute queries
/// - Functions: Whether or not users can execute certain functions
/// - Network: Whether or not users can connect to certain network addresses
///
/// Capabilities are configured globally. By default, capabilities are
/// configured as:
/// - Scripting: false
/// - Guest access: false
/// - Functions: All functions are allowed
/// - Network: No network address is allowed, all are impliticly denied
///
/// The capabilities are defined using allow/deny lists for fine-grained
/// control.
///
/// # Filtering functions and net-targets.
///
/// The filtering of net targets and functions is done with an allow/deny list.
/// These list can either match everything, nothing or a given list.
///
/// By default every function and net-target is disallowed. For a function or
/// net target to be allowed it must match the allow-list and not match the
/// deny-list. This means that if for example a function is both in the
/// allow-list and in the deny-list it will be disallowed.
///
/// With the combination of both these lists you can filter subgroups. For
/// example:
/// ```
/// # use surrealdb::opt::capabilities::Capabilities;
/// # fn cap() -> Result<Capabilities, Box<dyn std::error::Error>> {
/// # let cap =
/// Capabilities::none()
///     .with_function_allowed("http::*")?
///     .with_function_denied("http::post")?
///
///  # ;
///  # Ok(cap)
/// # }
/// ```
///
/// Will allow all and only all `http::*` functions except the function
/// `http::post`.
///
/// Examples:
/// - Allow all functions: `--allow-funcs`
/// - Allow all functions except `http.*`: `--allow-funcs --deny-funcs 'http.*'`
/// - Allow all network addresses except AWS metadata endpoint: `--allow-net
///   --deny-net='169.254.169.254'`
///
/// # Examples
///
/// Create a new instance, and allow all capabilities
#[cfg_attr(feature = "kv-surrealkv", doc = "```no_run")]
#[cfg_attr(not(feature = "kv-surrealkv"), doc = "```ignore")]
/// # use surrealdb::opt::capabilities::Capabilities;
/// # use surrealdb::opt::Config;
/// # use surrealdb::Surreal;
/// # use surrealdb::engine::local::SurrealKv;
/// # #[tokio::main]
/// # async fn main() -> surrealdb::Result<()> {
/// let capabilities = Capabilities::all();
/// let config = Config::default().capabilities(capabilities);
/// let db = Surreal::new::<SurrealKv>(("temp.db", config)).await?;
/// # Ok(())
/// # }
/// ```
/// Create a new instance, and allow certain functions
#[cfg_attr(feature = "kv-surrealkv", doc = "```no_run")]
#[cfg_attr(not(feature = "kv-surrealkv"), doc = "```ignore")]
/// # use std::str::FromStr;
/// # use surrealdb::engine::local::SurrealKv;
/// # use surrealdb::opt::capabilities::Capabilities;
/// # use surrealdb::opt::Config;
/// # use surrealdb::Surreal;
/// # #[tokio::main]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let capabilities = Capabilities::default()
///     .with_function_denied("http::*")?;
/// let config = Config::default().capabilities(capabilities);
/// let db = Surreal::new::<SurrealKv>(("temp.db", config)).await?;
/// # Ok(())
/// # }
/// ```
///
#[derive(Debug, Clone)]
pub struct Capabilities {
	cap: CoreCapabilities,
}

impl Default for Capabilities {
	fn default() -> Self {
		Self::new()
	}
}

impl Capabilities {
	/// Create a builder with default capabilities enabled.
	///
	/// Default capabilities enables live query notifications and all
	/// (non-scripting) functions.
	pub fn new() -> Self {
		Capabilities {
			cap: CoreCapabilities::default()
				.with_functions(Targets::All)
				.without_functions(Targets::None)
				.with_network_targets(Targets::None)
				.without_network_targets(Targets::None),
		}
	}

	/// Create a builder with all capabilities enabled.
	pub fn all() -> Self {
		Capabilities {
			cap: CoreCapabilities::all(),
		}
	}

	/// Create a builder with all capabilities disabled.
	pub fn none() -> Self {
		Capabilities {
			cap: CoreCapabilities::none()
				.with_functions(Targets::None)
				.without_functions(Targets::None)
				.with_network_targets(Targets::None)
				.without_network_targets(Targets::None),
		}
	}

	/// Set whether to enable the embedded javascript scripting runtime.
	pub fn with_scripting(self, enabled: bool) -> Self {
		Self {
			cap: self.cap.with_scripting(enabled),
		}
	}

	/// Set whether to allow non-authenticated users to execute queries when
	/// authentication is enabled.
	pub fn with_guest_access(self, enabled: bool) -> Self {
		Self {
			cap: self.cap.with_guest_access(enabled),
		}
	}

	/// Set whether to enable live query notifications.
	pub fn with_live_query_notifications(self, enabled: bool) -> Self {
		Self {
			cap: self.cap.with_live_query_notifications(enabled),
		}
	}

	/// Has no effect: the engine has a single execution path.
	#[deprecated(
		note = "the engine has a single execution path; the planner strategy has no effect"
	)]
	#[allow(deprecated)]
	pub fn with_planner_strategy(self, _strategy: PlannerStrategy) -> Self {
		self
	}

	/// Set the allow list to allow all functions
	pub fn allow_all_functions(&mut self) -> &mut Self {
		*self.cap.allowed_functions_mut() = Targets::All;
		self
	}

	/// Set the allow list to allow all functions
	pub fn with_all_functions_allowed(mut self) -> Self {
		self.allow_all_functions();
		self
	}

	/// Set the deny list to deny all functions
	pub fn deny_all_functions(&mut self) -> &mut Self {
		*self.cap.denied_functions_mut() = Targets::All;
		self
	}

	/// Set the deny list to deny all functions
	pub fn with_all_functions_denied(mut self) -> Self {
		self.deny_all_functions();
		self
	}

	/// Set the allow list to allow no function
	pub fn allow_no_functions(&mut self) -> &mut Self {
		*self.cap.allowed_functions_mut() = Targets::None;
		self
	}

	/// Set the allow list to allow no function
	pub fn with_no_functions_allowed(mut self) -> Self {
		self.allow_no_functions();
		self
	}

	/// Set the deny list to deny no function
	pub fn deny_no_functions(&mut self) -> &mut Self {
		*self.cap.denied_functions_mut() = Targets::None;
		self
	}

	/// Set the deny list to deny no function
	pub fn with_no_functions_denied(mut self) -> Self {
		self.deny_no_functions();
		self
	}

	/// Add a function to the allow lists
	///
	/// Adding a function to the allow list overwrites previously set allow-all
	/// or allow-none filters.
	pub fn allow_function<S: AsRef<str>>(
		&mut self,
		func: S,
	) -> Result<&mut Self, ParseFuncTargetError> {
		self.allow_function_str(func.as_ref())
	}

	/// Add a function to the allow lists
	///
	/// Adding a function to the allow list overwrites previously set allow-all
	/// or allow-none filters.
	pub fn with_function_allowed<S: AsRef<str>>(
		mut self,
		func: S,
	) -> Result<Self, ParseFuncTargetError> {
		self.allow_function(func)?;
		Ok(self)
	}

	fn allow_function_str(&mut self, s: &str) -> Result<&mut Self, ParseFuncTargetError> {
		let target: FuncTarget = s.parse()?;
		match self.cap.allowed_functions_mut() {
			Targets::None | Targets::All => {
				let mut set = HashSet::new();
				set.insert(target);
				self.cap = mem::take(&mut self.cap).with_functions(Targets::Some(set));
			}
			Targets::Some(x) => {
				x.insert(target);
			}
		}
		Ok(self)
	}

	/// Add a function to the deny lists
	///
	/// Adding a function to the deny list overwrites previously set deny-all or
	/// deny-none filters.
	pub fn deny_function<S: AsRef<str>>(
		&mut self,
		func: S,
	) -> Result<&mut Self, ParseFuncTargetError> {
		self.deny_function_str(func.as_ref())
	}

	/// Add a function to the deny lists
	///
	/// Adding a function to the deny list overwrites previously set deny-all or
	/// deny-none filters.
	pub fn with_function_denied<S: AsRef<str>>(
		mut self,
		func: S,
	) -> Result<Self, ParseFuncTargetError> {
		self.deny_function(func)?;
		Ok(self)
	}

	fn deny_function_str(&mut self, s: &str) -> Result<&mut Self, ParseFuncTargetError> {
		let target: FuncTarget = s.parse()?;
		match self.cap.denied_functions_mut() {
			Targets::None | Targets::All => {
				let mut set = HashSet::new();
				set.insert(target);
				*self.cap.denied_functions_mut() = Targets::Some(set);
			}
			Targets::Some(x) => {
				x.insert(target);
			}
		}
		Ok(self)
	}

	/// Set the allow list to allow all net targets
	pub fn allow_all_net_targets(&mut self) -> &mut Self {
		*self.cap.allowed_network_targets_mut() = Targets::All;
		self
	}

	/// Set the allow list to allow all net targets
	pub fn with_all_net_targets_allowed(mut self) -> Self {
		self.allow_all_net_targets();
		self
	}

	/// Set the deny list to deny all net targets
	pub fn deny_all_net_targets(&mut self) -> &mut Self {
		*self.cap.denied_network_targets_mut() = Targets::All;
		self
	}

	/// Set the deny list to deny all net targets
	pub fn with_all_net_targets_denied(mut self) -> Self {
		self.deny_all_net_targets();
		self
	}

	/// Set the allow list to allow no net targets
	pub fn allow_no_net_targets(&mut self) -> &mut Self {
		*self.cap.allowed_network_targets_mut() = Targets::None;
		self
	}

	/// Set the allow list to allow no net targets
	pub fn with_no_net_targets_allowed(mut self) -> Self {
		self.allow_no_net_targets();
		self
	}

	/// Set the deny list to deny no net targets
	pub fn deny_no_net_targets(&mut self) -> &mut Self {
		*self.cap.denied_network_targets_mut() = Targets::None;
		self
	}

	/// Set the deny list to deny no net targets
	pub fn with_no_net_targets_denied(mut self) -> Self {
		self.deny_no_net_targets();
		self
	}

	/// Add a net target to the allow lists
	///
	/// Adding a net target to the allow list overwrites previously set
	/// allow-all or allow-none filters.
	///
	/// A target is `host[:port]`, `*.domain[:port]` (every subdomain of
	/// `domain`, not `domain` itself), an IP address or a CIDR block.
	pub fn allow_net_target<S: AsRef<str>>(
		&mut self,
		func: S,
	) -> Result<&mut Self, ParseNetTargetError> {
		self.allow_net_target_str(func.as_ref())
	}

	/// Add a net target to the allow lists
	///
	/// Adding a net target to the allow list overwrites previously set
	/// allow-all or allow-none filters.
	///
	/// A target is `host[:port]`, `*.domain[:port]` (every subdomain of
	/// `domain`, not `domain` itself), an IP address or a CIDR block.
	pub fn with_net_target_allowed<S: AsRef<str>>(
		mut self,
		func: S,
	) -> Result<Self, ParseNetTargetError> {
		self.allow_net_target(func)?;
		Ok(self)
	}

	fn allow_net_target_str(&mut self, s: &str) -> Result<&mut Self, ParseNetTargetError> {
		let target = s.parse()?;
		match self.cap.allowed_network_targets_mut() {
			Targets::None | Targets::All => {
				let mut set = HashSet::new();
				set.insert(target);
				*self.cap.allowed_network_targets_mut() = Targets::Some(set);
			}
			Targets::Some(x) => {
				x.insert(target);
			}
		}
		Ok(self)
	}

	/// Add a net target to the deny lists
	///
	/// Adding a net target to the deny list overwrites previously set deny-all
	/// or deny-none filters.
	///
	/// A target is `host[:port]`, `*.domain[:port]` (every subdomain of
	/// `domain`, not `domain` itself), an IP address or a CIDR block.
	pub fn deny_net_target<S: AsRef<str>>(
		&mut self,
		func: S,
	) -> Result<&mut Self, ParseNetTargetError> {
		self.deny_net_target_str(func.as_ref())
	}

	/// Add a net target to the deny lists
	///
	/// Adding a net target to the deny list overwrites previously set deny-all
	/// or deny-none filters.
	///
	/// A target is `host[:port]`, `*.domain[:port]` (every subdomain of
	/// `domain`, not `domain` itself), an IP address or a CIDR block.
	pub fn with_net_target_denied<S: AsRef<str>>(
		mut self,
		func: S,
	) -> Result<Self, ParseNetTargetError> {
		self.deny_net_target(func)?;
		Ok(self)
	}

	fn deny_net_target_str(&mut self, s: &str) -> Result<&mut Self, ParseNetTargetError> {
		let target = s.parse()?;
		match self.cap.denied_network_targets_mut() {
			Targets::None | Targets::All => {
				let mut set = HashSet::new();
				set.insert(target);
				*self.cap.denied_network_targets_mut() = Targets::Some(set);
			}
			Targets::Some(x) => {
				x.insert(target);
			}
		}
		Ok(self)
	}

	/// Set the allow list to allow all MCP tools
	pub fn allow_all_mcp_tools(&mut self) -> &mut Self {
		*self.cap.allowed_mcp_tools_mut() = Targets::All;
		self
	}

	/// Set the allow list to allow all MCP tools
	pub fn with_all_mcp_tools_allowed(mut self) -> Self {
		self.allow_all_mcp_tools();
		self
	}

	/// Set the deny list to deny all MCP tools
	pub fn deny_all_mcp_tools(&mut self) -> &mut Self {
		*self.cap.denied_mcp_tools_mut() = Targets::All;
		self
	}

	/// Set the deny list to deny all MCP tools
	pub fn with_all_mcp_tools_denied(mut self) -> Self {
		self.deny_all_mcp_tools();
		self
	}

	/// Set the allow list to allow no MCP tools
	pub fn allow_no_mcp_tools(&mut self) -> &mut Self {
		*self.cap.allowed_mcp_tools_mut() = Targets::None;
		self
	}

	/// Set the allow list to allow no MCP tools
	pub fn with_no_mcp_tools_allowed(mut self) -> Self {
		self.allow_no_mcp_tools();
		self
	}

	/// Set the deny list to deny no MCP tools
	pub fn deny_no_mcp_tools(&mut self) -> &mut Self {
		*self.cap.denied_mcp_tools_mut() = Targets::None;
		self
	}

	/// Set the deny list to deny no MCP tools
	pub fn with_no_mcp_tools_denied(mut self) -> Self {
		self.deny_no_mcp_tools();
		self
	}

	/// Add an MCP tool to the allow lists
	///
	/// Adding a tool to the allow list overwrites previously set allow-all or
	/// allow-none filters. The name must be a valid MCP tool (e.g. `query`,
	/// `select`, `graphql`); unknown names are rejected.
	pub fn allow_mcp_tool<S: AsRef<str>>(
		&mut self,
		tool: S,
	) -> Result<&mut Self, ParseMcpToolTargetError> {
		self.allow_mcp_tool_str(tool.as_ref())
	}

	/// Add an MCP tool to the allow lists
	///
	/// Adding a tool to the allow list overwrites previously set allow-all or
	/// allow-none filters. The name must be a valid MCP tool (e.g. `query`,
	/// `select`, `graphql`); unknown names are rejected.
	pub fn with_mcp_tool_allowed<S: AsRef<str>>(
		mut self,
		tool: S,
	) -> Result<Self, ParseMcpToolTargetError> {
		self.allow_mcp_tool(tool)?;
		Ok(self)
	}

	fn allow_mcp_tool_str(&mut self, s: &str) -> Result<&mut Self, ParseMcpToolTargetError> {
		let target: McpToolTarget = s.parse()?;
		match self.cap.allowed_mcp_tools_mut() {
			Targets::None | Targets::All => {
				let mut set = HashSet::new();
				set.insert(target);
				*self.cap.allowed_mcp_tools_mut() = Targets::Some(set);
			}
			Targets::Some(x) => {
				x.insert(target);
			}
		}
		Ok(self)
	}

	/// Add an MCP tool to the deny lists
	///
	/// Adding a tool to the deny list overwrites previously set deny-all or
	/// deny-none filters. The name must be a valid MCP tool (e.g. `query`,
	/// `select`, `graphql`); unknown names are rejected.
	pub fn deny_mcp_tool<S: AsRef<str>>(
		&mut self,
		tool: S,
	) -> Result<&mut Self, ParseMcpToolTargetError> {
		self.deny_mcp_tool_str(tool.as_ref())
	}

	/// Add an MCP tool to the deny lists
	///
	/// Adding a tool to the deny list overwrites previously set deny-all or
	/// deny-none filters. The name must be a valid MCP tool (e.g. `query`,
	/// `select`, `graphql`); unknown names are rejected.
	pub fn with_mcp_tool_denied<S: AsRef<str>>(
		mut self,
		tool: S,
	) -> Result<Self, ParseMcpToolTargetError> {
		self.deny_mcp_tool(tool)?;
		Ok(self)
	}

	fn deny_mcp_tool_str(&mut self, s: &str) -> Result<&mut Self, ParseMcpToolTargetError> {
		let target: McpToolTarget = s.parse()?;
		match self.cap.denied_mcp_tools_mut() {
			Targets::None | Targets::All => {
				let mut set = HashSet::new();
				set.insert(target);
				*self.cap.denied_mcp_tools_mut() = Targets::Some(set);
			}
			Targets::Some(x) => {
				x.insert(target);
			}
		}
		Ok(self)
	}

	/// Set the allow list to allow all experimental features
	pub fn allow_all_experimental_features(&mut self) -> &mut Self {
		*self.cap.allowed_experimental_features_mut() = Targets::All;
		self
	}

	/// Set the allow list to allow all experimental features
	pub fn with_all_experimental_features_allowed(mut self) -> Self {
		self.allow_all_experimental_features();
		self
	}

	/// Add a list of experimental features to the allow lists
	///
	/// Adding features to the allow list overwrites previously set allow-all or
	/// allow-none filters.
	pub fn allow_experimental_features(&mut self, features: &[ExperimentalFeature]) -> &mut Self {
		let features = features.iter().map(ExperimentalTarget::from);
		match self.cap.allowed_experimental_features_mut() {
			Targets::None | Targets::All => {
				let mut set = HashSet::new();
				set.extend(features);
				*self.cap.allowed_experimental_features_mut() = Targets::Some(set);
			}
			Targets::Some(set) => {
				set.extend(features);
			}
		}
		self
	}

	/// Add a list of experimental features to the allow lists
	///
	/// Adding features to the allow list overwrites previously set allow-all or
	/// allow-none filters.
	pub fn with_experimental_features_allowed(mut self, features: &[ExperimentalFeature]) -> Self {
		self.allow_experimental_features(features);
		self
	}

	/// Add an experimental feature to the allow lists
	///
	/// Adding a feature to the allow list overwrites previously set allow-all
	/// or allow-none filters.
	pub fn allow_experimental_feature(&mut self, feature: ExperimentalFeature) -> &mut Self {
		let feature = ExperimentalTarget::from(&feature);
		match self.cap.allowed_experimental_features_mut() {
			Targets::None | Targets::All => {
				let mut set = HashSet::new();
				set.insert(feature);
				*self.cap.allowed_experimental_features_mut() = Targets::Some(set);
			}
			Targets::Some(set) => {
				set.insert(feature);
			}
		}
		self
	}

	/// Add an experimental feature to the allow lists
	///
	/// Adding a feature to the allow list overwrites previously set allow-all
	/// or allow-none filters.
	pub fn with_experimental_feature_allowed(mut self, feature: ExperimentalFeature) -> Self {
		self.allow_experimental_feature(feature);
		self
	}

	/// Set the allow list to allow no experimental features
	pub fn allow_no_experimental_features(&mut self) -> &mut Self {
		*self.cap.allowed_experimental_features_mut() = Targets::None;
		self
	}

	/// Set the allow list to allow no experimental features
	pub fn with_no_experimental_features_allowed(mut self) -> Self {
		self.allow_no_experimental_features();
		self
	}

	/// Set the deny list to deny all experimental features
	pub fn deny_all_experimental_features(&mut self) -> &mut Self {
		*self.cap.denied_experimental_features_mut() = Targets::All;
		self
	}

	/// Set the deny list to deny all experimental features
	pub fn with_all_experimental_features_denied(mut self) -> Self {
		self.deny_all_experimental_features();
		self
	}

	/// Add a list of experimental features to the deny lists
	///
	/// Adding features to the deny list overwrites previously set deny-all or
	/// deny-none filters.
	pub fn deny_experimental_features(&mut self, features: &[ExperimentalFeature]) -> &mut Self {
		let features = features.iter().map(ExperimentalTarget::from);
		match self.cap.denied_experimental_features_mut() {
			Targets::None | Targets::All => {
				let mut set = HashSet::new();
				set.extend(features);
				*self.cap.denied_experimental_features_mut() = Targets::Some(set);
			}
			Targets::Some(set) => {
				set.extend(features);
			}
		}
		self
	}

	/// Add a list of experimental features to the deny lists
	///
	/// Adding features to the deny list overwrites previously set deny-all or
	/// deny-none filters.
	pub fn with_experimental_features_denied(mut self, features: &[ExperimentalFeature]) -> Self {
		self.deny_experimental_features(features);
		self
	}

	/// Add an experimental feature to the deny lists
	///
	/// Adding a feature to the deny list overwrites previously set deny-all or
	/// deny-none filters.
	pub fn deny_experimental_feature(&mut self, feature: ExperimentalFeature) -> &mut Self {
		let feature = ExperimentalTarget::from(&feature);
		match self.cap.denied_experimental_features_mut() {
			Targets::None | Targets::All => {
				let mut set = HashSet::new();
				set.insert(feature);
				*self.cap.denied_experimental_features_mut() = Targets::Some(set);
			}
			Targets::Some(set) => {
				set.insert(feature);
			}
		}
		self
	}

	/// Add an experimental feature to the deny lists
	///
	/// Adding a feature to the deny list overwrites previously set deny-all or
	/// deny-none filters.
	pub fn with_experimental_feature_denied(mut self, feature: ExperimentalFeature) -> Self {
		self.deny_experimental_feature(feature);
		self
	}

	/// Set the deny list to deny no experimental features
	pub fn deny_no_experimental_features(&mut self) -> &mut Self {
		*self.cap.denied_experimental_features_mut() = Targets::None;
		self
	}

	/// Set the deny list to deny no experimental features
	pub fn with_no_experimental_features_denied(mut self) -> Self {
		self.deny_no_experimental_features();
		self
	}
}

/// Not public API
#[doc(hidden)]
impl From<Capabilities> for CoreCapabilities {
	fn from(
		Capabilities {
			cap,
		}: Capabilities,
	) -> Self {
		cap
	}
}

/// Not public API
#[doc(hidden)]
impl From<CoreCapabilities> for Capabilities {
	fn from(cap: CoreCapabilities) -> Self {
		Self {
			cap,
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	/// Resolve the built capabilities and ask whether `tool` would be allowed,
	/// exercising the same `allows_mcp_tool` gate the MCP dispatcher uses.
	fn allows(cap: &Capabilities, tool: &str) -> bool {
		let core: CoreCapabilities = cap.clone().into();
		core.allows_mcp_tool(&McpToolTarget(tool.to_string()))
	}

	#[test]
	fn mcp_tools_allowed_by_default() {
		let cap = Capabilities::default();
		assert!(allows(&cap, "query"));
		assert!(allows(&cap, "graphql"));
	}

	#[test]
	fn deny_single_mcp_tool_leaves_the_rest_allowed() {
		let cap = Capabilities::default().with_mcp_tool_denied("graphql").unwrap();
		assert!(allows(&cap, "query"));
		assert!(!allows(&cap, "graphql"));
	}

	#[test]
	fn allow_only_named_mcp_tools_from_none() {
		let cap = Capabilities::none().with_mcp_tool_allowed("query").unwrap();
		assert!(allows(&cap, "query"));
		assert!(!allows(&cap, "select"));
	}

	#[test]
	fn deny_all_mcp_tools() {
		let cap = Capabilities::default().with_all_mcp_tools_denied();
		assert!(!allows(&cap, "query"));
		assert!(!allows(&cap, "graphql"));
	}

	#[test]
	fn unknown_mcp_tool_is_rejected() {
		assert!(Capabilities::default().with_mcp_tool_denied("not_a_tool").is_err());
		assert!(Capabilities::default().with_mcp_tool_allowed("not_a_tool").is_err());
	}
}
