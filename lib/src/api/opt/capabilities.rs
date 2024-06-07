//! The capabilities that can be enabled for a database instance

use std::collections::HashSet;

use surrealdb_core::dbs::capabilities::{
	Capabilities, FuncTarget, NetTarget, ParseFuncTargetError, ParseNetTargetError, Targets,
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
/// let capabilities = CapabilitiesBuilder::all();
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
/// # use surrealdb::opt::capabilities::FuncTarget;
/// # use surrealdb::opt::capabilities::Targets;
/// # use surrealdb::opt::Config;
/// # use surrealdb::Surreal;
/// # #[tokio::main]
/// # async fn main() -> surrealdb::Result<()> {
/// let capabilities = CapabilitiesBuilder::default()
///     .without_functions("http::*");
/// let config = Config::default().capabilities(capabilities);
/// let db = Surreal::new::<File>(("temp.db", config)).await?;
/// # Ok(())
/// # }
/// ```

pub struct CapabilitiesBuilder {
	cap: Capabilities,
	allow_funcs: Targets<FuncTarget>,
	deny_funcs: Targets<FuncTarget>,
	allow_net: Targets<NetTarget>,
	deny_net: Targets<NetTarget>,
}

impl CapabilitiesBuilder {
	pub fn new() -> Self {
		CapabilitiesBuilder {
			cap: Capabilities::default(),
			allow_funcs: Targets::All,
			deny_funcs: Targets::None,
			allow_net: Targets::None,
			deny_net: Targets::None,
		}
	}

	pub fn all() -> Self {
		CapabilitiesBuilder {
			cap: Capabilities::all(),
			allow_funcs: Targets::All,
			deny_funcs: Targets::None,
			allow_net: Targets::All,
			deny_net: Targets::None,
		}
	}

	pub fn none() -> Self {
		CapabilitiesBuilder {
			cap: Capabilities::default(),
			allow_funcs: Targets::None,
			deny_funcs: Targets::None,
			allow_net: Targets::None,
			deny_net: Targets::None,
		}
	}

	pub fn with_scripting(self, enabled: bool) -> Self {
		Self {
			cap: self.cap.with_scripting(enabled),
			..self
		}
	}

	pub fn with_quest_access(self, enabled: bool) -> Self {
		Self {
			cap: self.cap.with_guest_access(enabled),
			..self
		}
	}

	pub fn with_live_query_notifications(self, enabled: bool) -> Self {
		Self {
			cap: self.cap.with_live_query_notifications(enabled),
			..self
		}
	}
	pub fn allow_all_functions(&mut self) -> &mut Self {
		self.allow_funcs = Targets::All;
		self
	}

	pub fn with_allow_all_functions(mut self) -> Self {
		self.allow_all_functions();
		self
	}

	pub fn deny_all_functions(&mut self) -> &mut Self {
		self.deny_funcs = Targets::All;
		self
	}

	pub fn with_deny_all_function(mut self) -> Self {
		self.deny_all_functions();
		self
	}

	pub fn allow_none_functions(&mut self) -> &mut Self {
		self.allow_funcs = Targets::None;
		self
	}

	pub fn with_allow_none_functions(mut self) -> Self {
		self.allow_none_functions();
		self
	}

	pub fn deny_none_functions(&mut self) -> &mut Self {
		self.deny_funcs = Targets::None;
		self
	}

	pub fn with_deny_none_function(mut self) -> Self {
		self.deny_none_functions();
		self
	}

	pub fn allow_function<S: AsRef<str>>(
		&mut self,
		func: S,
	) -> Result<&mut Self, ParseFuncTargetError> {
		self.allow_function_str(func.as_ref())
	}

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

	pub fn deny_function<S: AsRef<str>>(
		&mut self,
		func: S,
	) -> Result<&mut Self, ParseFuncTargetError> {
		self.deny_function_str(func.as_ref())
	}

	pub fn with_deny_function<S: AsRef<str>>(
		mut self,
		func: S,
	) -> Result<Self, ParseFuncTargetError> {
		self.deny_function(func)?;
		Ok(self)
	}

	pub fn allow_all_net_targets(&mut self) -> &mut Self {
		self.allow_net = Targets::All;
		self
	}

	pub fn with_allow_all_net_targets(mut self) -> Self {
		self.allow_all_net_targets();
		self
	}

	pub fn deny_all_net_targets(&mut self) -> &mut Self {
		self.deny_net = Targets::All;
		self
	}

	pub fn with_deny_all_net_target(mut self) -> Self {
		self.deny_all_net_targets();
		self
	}

	pub fn allow_none_net_targets(&mut self) -> &mut Self {
		self.allow_net = Targets::None;
		self
	}

	pub fn with_allow_none_net_targets(mut self) -> Self {
		self.allow_none_net_targets();
		self
	}

	pub fn deny_none_net_targets(&mut self) -> &mut Self {
		self.deny_net = Targets::None;
		self
	}

	pub fn with_deny_none_net_target(mut self) -> Self {
		self.deny_none_net_targets();
		self
	}

	pub fn allow_net_target<S: AsRef<str>>(
		&mut self,
		func: S,
	) -> Result<&mut Self, ParseNetTargetError> {
		self.allow_net_target_str(func.as_ref())
	}

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

	pub fn deny_net_target<S: AsRef<str>>(
		&mut self,
		func: S,
	) -> Result<&mut Self, ParseNetTargetError> {
		self.deny_net_target_str(func.as_ref())
	}

	pub fn with_deny_net_target<S: AsRef<str>>(
		mut self,
		func: S,
	) -> Result<Self, ParseNetTargetError> {
		self.deny_net_target(func)?;
		Ok(self)
	}

	pub(crate) fn build(self) -> Capabilities {
		let cap = self
			.cap
			.with_functions(self.allow_funcs)
			.without_functions(self.deny_funcs)
			.with_network_targets(self.allow_net)
			.without_network_targets(self.deny_net);

		cap
	}
}
