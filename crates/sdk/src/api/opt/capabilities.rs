//! The capabilities that can be enabled for a database instance

use std::collections::HashSet;
use std::mem;

use crate::core::dbs::capabilities::{
	Capabilities as CoreCapabilities, ExperimentalTarget, FuncTarget, ParseFuncTargetError,
	ParseNetTargetError, Targets,
};

/// A list of features that are still experimental
#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash)]
#[non_exhaustive]
pub enum ExperimentalFeature {
	RecordReferences,
	GraphQl,
	BearerAccess,
	DefineApi,
}

/// Not public API
#[doc(hidden)]
impl From<&ExperimentalFeature> for ExperimentalTarget {
	fn from(feature: &ExperimentalFeature) -> Self {
		match feature {
			ExperimentalFeature::RecordReferences => ExperimentalTarget::RecordReferences,
			ExperimentalFeature::GraphQl => ExperimentalTarget::GraphQL,
			ExperimentalFeature::BearerAccess => ExperimentalTarget::BearerAccess,
			ExperimentalFeature::DefineApi => ExperimentalTarget::DefineApi,
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
/// # fn cap() -> surrealdb::Result<Capabilities>{
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
#[cfg_attr(feature = "kv-rocksdb", doc = "```no_run")]
#[cfg_attr(not(feature = "kv-rocksdb"), doc = "```ignore")]
/// # use surrealdb::opt::capabilities::Capabilities;
/// # use surrealdb::opt::Config;
/// # use surrealdb::Surreal;
/// # use surrealdb::engine::local::RocksDb;
/// # #[tokio::main]
/// # async fn main() -> surrealdb::Result<()> {
/// let capabilities = Capabilities::all();
/// let config = Config::default().capabilities(capabilities);
/// let db = Surreal::new::<RocksDb>(("temp.db", config)).await?;
/// # Ok(())
/// # }
/// ```
/// Create a new instance, and allow certain functions
#[cfg_attr(feature = "kv-rocksdb", doc = "```no_run")]
#[cfg_attr(not(feature = "kv-rocksdb"), doc = "```ignore")]
/// # use std::str::FromStr;
/// # use surrealdb::engine::local::RocksDb;
/// # use surrealdb::opt::capabilities::Capabilities;
/// # use surrealdb::opt::Config;
/// # use surrealdb::Surreal;
/// # #[tokio::main]
/// # async fn main() -> surrealdb::Result<()> {
/// let capabilities = Capabilities::default()
///     .with_function_denied("http::*")?;
/// let config = Config::default().capabilities(capabilities);
/// let db = Surreal::new::<RocksDb>(("temp.db", config)).await?;
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
			cap: CoreCapabilities::all()
				.with_functions(Targets::All)
				.without_functions(Targets::None)
				.with_network_targets(Targets::All)
				.without_network_targets(Targets::None),
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

	/// Set wether to enable live query notifications.
	pub fn with_live_query_notifications(self, enabled: bool) -> Self {
		Self {
			cap: self.cap.with_live_query_notifications(enabled),
		}
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
