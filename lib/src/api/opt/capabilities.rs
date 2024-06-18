//! The capabilities that can be enabled for a database instance

use std::collections::HashSet;

use surrealdb_core::dbs::capabilities::{
	Capabilities as CoreCapabilities, FuncTarget, NetTarget, ParseFuncTargetError,
	ParseNetTargetError, Targets,
};

/// Capabilities are used to limit what a user can do to the system.
///
/// Capabilities are split into 4 categories:
/// - Scripting: Whether or not the user can execute scripts
/// - Guest access: Whether or not a non-authenticated user can execute queries on the system when authentication is enabled.
/// - Functions: Whether or not the user can execute certain functions
/// - Network: Whether or not the user can access certain network addresses
///
/// Capabilities are configured globally. By default, capabilities are configured as:
/// - Scripting: false
/// - Guest access: false
/// - Functions: All functions are allowed
/// - Network: No network address is allowed nor denied, hence all network addresses are denied unless explicitly allowed
///
/// The capabilities are defined using allow/deny lists for fine-grained control.
///
/// # Filtering functions and net-targets.
///
/// The filtering of net targets and functions is done with an allow/deny list.
/// These list can either match everything, nothing or a given list.
///
/// By default every function and net-target is disallowed. For a function or net target to be
/// allowed it must match the allow-list and not match the deny-list. This means that if for
/// example a function is both in the allow-list and in the deny-list it will be disallowed.
///
/// With the combination of both these lists you can filter subgroups. For example:
/// ```
/// # use surrealdb::opt::capabilities::Capabilities;
/// # fn cap() -> surrealdb::Result<Capabilities>{
/// # let cap =
/// Capabilities::none()
///     .with_allow_function("http::*")?
///     .with_deny_function("http::post")?
///
///  # ;
///  # Ok(cap)
/// # }
/// ```
///
/// Will allow all and only all `http::*` functions except the function `http::post`.
///
/// Examples:
/// - Allow all functions: `--allow-funcs`
/// - Allow all functions except `http.*`: `--allow-funcs --deny-funcs 'http.*'`
/// - Allow all network addresses except AWS metadata endpoint: `--allow-net --deny-net='169.254.169.254'`
///
/// # Examples
///
/// Create a new instance, and allow all capabilities
#[cfg_attr(feature = "kv-rocksdb", doc = "```no_run")]
#[cfg_attr(not(feature = "kv-rocksdb"), doc = "```ignore")]
/// # use surrealdb::opt::capabilities::Capabilities;
/// # use surrealdb::opt::Config;
/// # use surrealdb::Surreal;
/// # use surrealdb::engine::local::File;
/// # #[tokio::main]
/// # async fn main() -> surrealdb::Result<()> {
/// let capabilities = Capabilities::all();
/// let config = Config::default().capabilities(capabilities);
/// let db = Surreal::new::<File>(("temp.db", config)).await?;
/// # Ok(())
/// # }
/// ```
/// Create a new instance, and allow certain functions
#[cfg_attr(feature = "kv-rocksdb", doc = "```no_run")]
#[cfg_attr(not(feature = "kv-rocksdb"), doc = "```ignore")]
/// # use std::str::FromStr;
/// # use surrealdb::engine::local::File;
/// # use surrealdb::opt::capabilities::Capabilities;
/// # use surrealdb::opt::Config;
/// # use surrealdb::Surreal;
/// # #[tokio::main]
/// # async fn main() -> surrealdb::Result<()> {
/// let capabilities = Capabilities::default()
///     .with_deny_function("http::*")?;
/// let config = Config::default().capabilities(capabilities);
/// let db = Surreal::new::<File>(("temp.db", config)).await?;
/// # Ok(())
/// # }
/// ```
///
#[derive(Debug, Clone)]
pub struct Capabilities {
	cap: CoreCapabilities,
	allow_funcs: Targets<FuncTarget>,
	deny_funcs: Targets<FuncTarget>,
	allow_net: Targets<NetTarget>,
	deny_net: Targets<NetTarget>,
}

impl Default for Capabilities {
	fn default() -> Self {
		Self::new()
	}
}

impl Capabilities {
	/// Create a builder with default capabilities enabled.
	///
	/// Default capabilities enables live query notifications.
	pub fn new() -> Self {
		Capabilities {
			cap: CoreCapabilities::default(),
			allow_funcs: Targets::None,
			deny_funcs: Targets::None,
			allow_net: Targets::None,
			deny_net: Targets::None,
		}
	}

	/// Create a builder with all capabilities enabled.
	pub fn all() -> Self {
		Capabilities {
			cap: CoreCapabilities::all(),
			allow_funcs: Targets::All,
			deny_funcs: Targets::None,
			allow_net: Targets::All,
			deny_net: Targets::None,
		}
	}

	/// Create a builder with all capabilities disabled.
	pub fn none() -> Self {
		Capabilities {
			cap: CoreCapabilities::none(),
			allow_funcs: Targets::None,
			deny_funcs: Targets::None,
			allow_net: Targets::None,
			deny_net: Targets::None,
		}
	}

	/// Set whether to enable the embedded javascript scripting runtime.
	pub fn with_scripting(self, enabled: bool) -> Self {
		Self {
			cap: self.cap.with_scripting(enabled),
			..self
		}
	}

	/// Set whether to allow non-authenticated users to execute queries when authentication is
	/// enabled.
	pub fn with_guest_access(self, enabled: bool) -> Self {
		Self {
			cap: self.cap.with_guest_access(enabled),
			..self
		}
	}

	/// Set wether to enable live query notifications.
	pub fn with_live_query_notifications(self, enabled: bool) -> Self {
		Self {
			cap: self.cap.with_live_query_notifications(enabled),
			..self
		}
	}

	/// Set the allow list to allow all functions
	pub fn allow_all_functions(&mut self) -> &mut Self {
		self.allow_funcs = Targets::All;
		self
	}

	/// Set the allow list to allow all functions
	pub fn with_allow_all_functions(mut self) -> Self {
		self.allow_all_functions();
		self
	}

	/// Set the deny list to deny all functions
	pub fn deny_all_functions(&mut self) -> &mut Self {
		self.deny_funcs = Targets::All;
		self
	}

	/// Set the deny list to deny all functions
	pub fn with_deny_all_function(mut self) -> Self {
		self.deny_all_functions();
		self
	}

	/// Set the allow list to allow no function
	pub fn allow_none_functions(&mut self) -> &mut Self {
		self.allow_funcs = Targets::None;
		self
	}

	/// Set the allow list to allow no function
	pub fn with_allow_none_functions(mut self) -> Self {
		self.allow_none_functions();
		self
	}

	/// Set the deny list to deny no function
	pub fn deny_none_functions(&mut self) -> &mut Self {
		self.deny_funcs = Targets::None;
		self
	}

	/// Set the deny list to deny no function
	pub fn with_deny_none_function(mut self) -> Self {
		self.deny_none_functions();
		self
	}

	/// Add a function to the allow lists
	///
	/// Adding a function to the allow list overwrites previously set allow-all or allow-none
	/// filters.
	pub fn allow_function<S: AsRef<str>>(
		&mut self,
		func: S,
	) -> Result<&mut Self, ParseFuncTargetError> {
		self.allow_function_str(func.as_ref())
	}

	/// Add a function to the allow lists
	///
	/// Adding a function to the allow list overwrites previously set allow-all or allow-none
	/// filters.
	pub fn with_allow_function<S: AsRef<str>>(
		mut self,
		func: S,
	) -> Result<Self, ParseFuncTargetError> {
		self.allow_function(func)?;
		Ok(self)
	}

	fn allow_function_str(&mut self, s: &str) -> Result<&mut Self, ParseFuncTargetError> {
		let target: FuncTarget = s.parse()?;
		match self.allow_funcs {
			Targets::None | Targets::All => {
				let mut set = HashSet::new();
				set.insert(target);
				self.allow_funcs = Targets::Some(set);
			}
			Targets::Some(ref mut x) => {
				x.insert(target);
			}
			_ => unreachable!(),
		}
		Ok(self)
	}

	/// Add a function to the deny lists
	///
	/// Adding a function to the deny list overwrites previously set deny-all or deny-none
	/// filters.
	pub fn deny_function<S: AsRef<str>>(
		&mut self,
		func: S,
	) -> Result<&mut Self, ParseFuncTargetError> {
		self.deny_function_str(func.as_ref())
	}

	/// Add a function to the deny lists
	///
	/// Adding a function to the deny list overwrites previously set deny-all or deny-none
	/// filters.
	pub fn with_deny_function<S: AsRef<str>>(
		mut self,
		func: S,
	) -> Result<Self, ParseFuncTargetError> {
		self.deny_function(func)?;
		Ok(self)
	}

	fn deny_function_str(&mut self, s: &str) -> Result<&mut Self, ParseFuncTargetError> {
		let target: FuncTarget = s.parse()?;
		match self.deny_funcs {
			Targets::None | Targets::All => {
				let mut set = HashSet::new();
				set.insert(target);
				self.deny_funcs = Targets::Some(set);
			}
			Targets::Some(ref mut x) => {
				x.insert(target);
			}
			_ => unreachable!(),
		}
		Ok(self)
	}

	/// Set the allow list to allow all net targets
	pub fn allow_all_net_targets(&mut self) -> &mut Self {
		self.allow_net = Targets::All;
		self
	}

	/// Set the allow list to allow all net targets
	pub fn with_allow_all_net_targets(mut self) -> Self {
		self.allow_all_net_targets();
		self
	}

	/// Set the deny list to deny all net targets
	pub fn deny_all_net_targets(&mut self) -> &mut Self {
		self.deny_net = Targets::All;
		self
	}

	/// Set the deny list to deny all net targets
	pub fn with_deny_all_net_target(mut self) -> Self {
		self.deny_all_net_targets();
		self
	}

	/// Set the allow list to allow no net targets
	pub fn allow_none_net_targets(&mut self) -> &mut Self {
		self.allow_net = Targets::None;
		self
	}

	/// Set the allow list to allow no net targets
	pub fn with_allow_none_net_targets(mut self) -> Self {
		self.allow_none_net_targets();
		self
	}

	/// Set the deny list to deny no net targets
	pub fn deny_none_net_targets(&mut self) -> &mut Self {
		self.deny_net = Targets::None;
		self
	}

	/// Set the deny list to deny no net targets
	pub fn with_deny_none_net_target(mut self) -> Self {
		self.deny_none_net_targets();
		self
	}

	/// Add a net target to the allow lists
	///
	/// Adding a net target to the allow list overwrites previously set allow-all or allow-none
	/// filters.
	pub fn allow_net_target<S: AsRef<str>>(
		&mut self,
		func: S,
	) -> Result<&mut Self, ParseNetTargetError> {
		self.allow_net_target_str(func.as_ref())
	}

	/// Add a net target to the allow lists
	///
	/// Adding a net target to the allow list overwrites previously set allow-all or allow-none
	/// filters.
	pub fn with_allow_net_target<S: AsRef<str>>(
		mut self,
		func: S,
	) -> Result<Self, ParseNetTargetError> {
		self.allow_net_target(func)?;
		Ok(self)
	}

	fn allow_net_target_str(&mut self, s: &str) -> Result<&mut Self, ParseNetTargetError> {
		let target = s.parse()?;
		match self.allow_net {
			Targets::None | Targets::All => {
				let mut set = HashSet::new();
				set.insert(target);
				self.allow_net = Targets::Some(set);
			}
			Targets::Some(ref mut x) => {
				x.insert(target);
			}
			_ => unreachable!(),
		}
		Ok(self)
	}

	/// Add a net target to the deny lists
	///
	/// Adding a net target to the deny list overwrites previously set deny-all or deny-none
	/// filters.
	pub fn deny_net_target<S: AsRef<str>>(
		&mut self,
		func: S,
	) -> Result<&mut Self, ParseNetTargetError> {
		self.deny_net_target_str(func.as_ref())
	}

	/// Add a net target to the deny lists
	///
	/// Adding a net target to the deny list overwrites previously set deny-all or deny-none
	/// filters.
	pub fn with_deny_net_target<S: AsRef<str>>(
		mut self,
		func: S,
	) -> Result<Self, ParseNetTargetError> {
		self.deny_net_target(func)?;
		Ok(self)
	}

	fn deny_net_target_str(&mut self, s: &str) -> Result<&mut Self, ParseNetTargetError> {
		let target = s.parse()?;
		match self.deny_net {
			Targets::None | Targets::All => {
				let mut set = HashSet::new();
				set.insert(target);
				self.deny_net = Targets::Some(set);
			}
			Targets::Some(ref mut x) => {
				x.insert(target);
			}
			_ => unreachable!(),
		}
		Ok(self)
	}

	pub(crate) fn build(self) -> CoreCapabilities {
		self.cap
			.with_functions(self.allow_funcs)
			.without_functions(self.deny_funcs)
			.with_network_targets(self.allow_net)
			.without_network_targets(self.deny_net)
	}
}
