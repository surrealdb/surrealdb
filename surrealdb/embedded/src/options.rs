//! Connection options as the JavaScript side supplies them.
//!
//! Deserialized from the FFI shim's own representation (a `serde_json::Value`
//! on node, a `JsValue` on wasm) and consumed once, when the connection opens.

use std::collections::HashSet;

use anyhow::{Context, Result};
use serde::Deserialize;
use surrealdb_rpc::capabilities;
use surrealdb_rpc::capabilities::NewPlannerStrategy;

#[derive(Deserialize, Default)]
pub struct Options {
	/// Timeouts in whole seconds, applied to the datastore as
	/// [`std::time::Duration::from_secs`].
	pub query_timeout: Option<u64>,
	pub transaction_timeout: Option<u64>,
	pub capabilities: Option<CapabilitiesConfig>,
	pub defaults: Option<DefaultsConfig>,
}

#[derive(Deserialize, Clone)]
#[serde(untagged)]
pub enum DefaultsConfig {
	Bool(bool),
	Config {
		namespace: Option<String>,
		database: Option<String>,
	},
}

impl Default for DefaultsConfig {
	fn default() -> Self {
		DefaultsConfig::Bool(true)
	}
}

impl DefaultsConfig {
	pub fn get_defaults(self) -> Option<(String, String)> {
		match self {
			DefaultsConfig::Bool(false) => None,
			DefaultsConfig::Bool(true) => Some(("main".to_string(), "main".to_string())),
			DefaultsConfig::Config {
				namespace,
				database,
			} => Some((
				namespace.unwrap_or("main".to_string()),
				database.unwrap_or("main".to_string()),
			)),
		}
	}
}

// Deserialized once, when a connection is opened, and dropped as soon as the
// capabilities are built. Boxing to even out the variants would buy nothing.
#[expect(clippy::large_enum_variant)]
#[derive(Deserialize)]
#[serde(untagged)]
pub enum CapabilitiesConfig {
	Bool(bool),
	Capabilities {
		scripting: Option<bool>,
		guest_access: Option<bool>,
		live_query_notifications: Option<bool>,
		functions: Option<Targets>,
		network_targets: Option<Targets>,
		experimental: Option<Targets>,
		planner_strategy: Option<PlannerStrategy>,
	},
}

#[derive(Deserialize)]
#[serde(untagged)]
pub enum Targets {
	Bool(bool),
	Array(HashSet<String>),
	Config {
		allow: Option<TargetsConfig>,
		deny: Option<TargetsConfig>,
	},
}

#[derive(Deserialize)]
#[serde(untagged)]
pub enum TargetsConfig {
	Bool(bool),
	Array(HashSet<String>),
}

/// Parses a set of capability target patterns.
///
/// Returns an error rather than panicking on a malformed pattern: this runs on
/// a connection options object supplied by JavaScript, and the addons are built
/// with `panic = "abort"`, so a panic here would take the host process down
/// over a typo in a config literal.
macro_rules! process_targets {
	($set:ident) => {{
		let mut functions = HashSet::with_capacity($set.len());
		for function in $set {
			let parsed = function
				.parse()
				.with_context(|| format!("invalid capability target: {function}"))?;
			functions.insert(parsed);
		}
		capabilities::Targets::Some(functions)
	}};
}

#[derive(Deserialize, Clone, Default)]
#[serde(rename_all = "kebab-case")]
pub enum PlannerStrategy {
	#[default]
	BestEffort,
	ComputeOnly,
	AllReadOnly,
}

impl From<PlannerStrategy> for NewPlannerStrategy {
	fn from(strategy: PlannerStrategy) -> Self {
		match strategy {
			PlannerStrategy::BestEffort => NewPlannerStrategy::BestEffortReadOnlyStatements,
			PlannerStrategy::ComputeOnly => NewPlannerStrategy::ComputeOnly,
			PlannerStrategy::AllReadOnly => NewPlannerStrategy::AllReadOnlyStatements,
		}
	}
}

impl TryFrom<CapabilitiesConfig> for capabilities::Capabilities {
	type Error = anyhow::Error;

	fn try_from(config: CapabilitiesConfig) -> Result<Self> {
		let caps = match config {
			CapabilitiesConfig::Bool(true) => Self::all(),
			CapabilitiesConfig::Bool(false) => {
				Self::default().with_functions(capabilities::Targets::None)
			}
			CapabilitiesConfig::Capabilities {
				scripting,
				guest_access,
				live_query_notifications,
				functions,
				network_targets,
				experimental,
				planner_strategy,
			} => {
				let mut capabilities = Self::default();

				if let Some(scripting) = scripting {
					capabilities = capabilities.with_scripting(scripting);
				}

				if let Some(guest_access) = guest_access {
					capabilities = capabilities.with_guest_access(guest_access);
				}

				if let Some(live_query_notifications) = live_query_notifications {
					capabilities =
						capabilities.with_live_query_notifications(live_query_notifications);
				}

				if let Some(functions) = functions {
					match functions {
						Targets::Bool(functions) => match functions {
							true => {
								capabilities =
									capabilities.with_functions(capabilities::Targets::All);
							}
							false => {
								capabilities =
									capabilities.with_functions(capabilities::Targets::None);
							}
						},
						Targets::Array(set) => {
							capabilities = capabilities.with_functions(process_targets!(set));
						}
						Targets::Config {
							allow,
							deny,
						} => {
							if let Some(config) = allow {
								match config {
									TargetsConfig::Bool(functions) => match functions {
										true => {
											capabilities = capabilities
												.with_functions(capabilities::Targets::All);
										}
										false => {
											capabilities = capabilities
												.with_functions(capabilities::Targets::None);
										}
									},
									TargetsConfig::Array(set) => {
										capabilities =
											capabilities.with_functions(process_targets!(set));
									}
								}
							}

							if let Some(config) = deny {
								match config {
									TargetsConfig::Bool(functions) => match functions {
										true => {
											capabilities = capabilities
												.without_functions(capabilities::Targets::All);
										}
										false => {
											capabilities = capabilities
												.without_functions(capabilities::Targets::None);
										}
									},
									TargetsConfig::Array(set) => {
										capabilities =
											capabilities.without_functions(process_targets!(set));
									}
								}
							}
						}
					}
				}

				if let Some(network_targets) = network_targets {
					match network_targets {
						Targets::Bool(network_targets) => match network_targets {
							true => {
								capabilities =
									capabilities.with_network_targets(capabilities::Targets::All);
							}
							false => {
								capabilities =
									capabilities.with_network_targets(capabilities::Targets::None);
							}
						},
						Targets::Array(set) => {
							capabilities = capabilities.with_network_targets(process_targets!(set));
						}
						Targets::Config {
							allow,
							deny,
						} => {
							if let Some(config) = allow {
								match config {
									TargetsConfig::Bool(network_targets) => match network_targets {
										true => {
											capabilities = capabilities
												.with_network_targets(capabilities::Targets::All);
										}
										false => {
											capabilities = capabilities
												.with_network_targets(capabilities::Targets::None);
										}
									},
									TargetsConfig::Array(set) => {
										capabilities = capabilities
											.with_network_targets(process_targets!(set));
									}
								}
							}

							if let Some(config) = deny {
								match config {
									TargetsConfig::Bool(network_targets) => match network_targets {
										true => {
											capabilities = capabilities.without_network_targets(
												capabilities::Targets::All,
											);
										}
										false => {
											capabilities = capabilities.without_network_targets(
												capabilities::Targets::None,
											);
										}
									},
									TargetsConfig::Array(set) => {
										capabilities = capabilities
											.without_network_targets(process_targets!(set));
									}
								}
							}
						}
					}
				}

				if let Some(experimental) = experimental {
					match experimental {
						Targets::Bool(experimental) => match experimental {
							true => {
								capabilities =
									capabilities.with_experimental(capabilities::Targets::All);
							}
							false => {
								capabilities =
									capabilities.with_experimental(capabilities::Targets::None);
							}
						},
						Targets::Array(set) => {
							capabilities = capabilities.with_experimental(process_targets!(set));
						}
						Targets::Config {
							allow,
							deny,
						} => {
							if let Some(config) = allow {
								match config {
									TargetsConfig::Bool(experimental) => match experimental {
										true => {
											capabilities = capabilities
												.with_experimental(capabilities::Targets::All);
										}
										false => {
											capabilities = capabilities
												.with_experimental(capabilities::Targets::None);
										}
									},
									TargetsConfig::Array(set) => {
										capabilities =
											capabilities.with_experimental(process_targets!(set));
									}
								}
							}

							if let Some(config) = deny {
								match config {
									TargetsConfig::Bool(experimental) => match experimental {
										true => {
											capabilities = capabilities
												.without_experimental(capabilities::Targets::All);
										}
										false => {
											capabilities = capabilities
												.without_experimental(capabilities::Targets::None);
										}
									},
									TargetsConfig::Array(set) => {
										capabilities = capabilities
											.without_experimental(process_targets!(set));
									}
								}
							}
						}
					}
				}

				if let Some(planner_strategy) = planner_strategy {
					capabilities = capabilities.with_planner_strategy(planner_strategy.into());
				}

				capabilities
			}
		};

		Ok(caps
			.with_arbitrary_query(capabilities::Targets::All)
			.without_arbitrary_query(capabilities::Targets::None))
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn options(json: &str) -> Options {
		serde_json::from_str(json).expect("the options literal should deserialize")
	}

	fn capabilities_from(json: &str) -> Result<capabilities::Capabilities> {
		options(json).capabilities.expect("the literal sets capabilities").try_into()
	}

	/// A malformed target is reported, not panicked on. The shims are built with
	/// `panic = "abort"`, so panicking here would take the host process down
	/// over a typo in a JavaScript config literal.
	#[test]
	fn a_malformed_capability_target_is_an_error() {
		for json in [
			r#"{"capabilities":{"functions":["http::get","not-a-function-name"]}}"#,
			r#"{"capabilities":{"experimental":["no-such-feature"]}}"#,
			r#"{"capabilities":{"functions":{"deny":["also-not-a-function"]}}}"#,
		] {
			let err = capabilities_from(json).expect_err("a malformed target should be rejected");
			assert!(
				format!("{err:#}").contains("invalid capability target"),
				"error did not name the cause: {err:#}"
			);
		}
	}

	/// The well-formed shapes still convert, so the error path above is not just
	/// rejecting everything.
	#[test]
	fn well_formed_capability_targets_convert() {
		for json in [
			r#"{"capabilities":true}"#,
			r#"{"capabilities":false}"#,
			r#"{"capabilities":{"functions":["http::get"]}}"#,
			r#"{"capabilities":{"network_targets":{"allow":true,"deny":["example.com"]}}}"#,
			r#"{"capabilities":{"experimental":["files","gql"]}}"#,
			r#"{"capabilities":{"planner_strategy":"compute-only"}}"#,
		] {
			capabilities_from(json).unwrap_or_else(|e| panic!("{json} should convert: {e:#}"));
		}
	}

	/// `defaults` accepts both the boolean and the explicit pair, and the
	/// boolean's `true` is what an omitted `defaults` means.
	#[test]
	fn defaults_config_resolves_both_shapes() {
		assert_eq!(
			DefaultsConfig::default().get_defaults(),
			Some(("main".to_owned(), "main".to_owned()))
		);
		assert_eq!(DefaultsConfig::Bool(false).get_defaults(), None);
		let explicit = options(r#"{"defaults":{"namespace":"ns","database":"db"}}"#);
		assert_eq!(
			explicit.defaults.unwrap().get_defaults(),
			Some(("ns".to_owned(), "db".to_owned()))
		);
	}

	/// The documented timeouts are whole seconds and must accept values past a
	/// byte — the README's own example uses 30_000.
	#[test]
	fn timeouts_accept_values_past_a_byte() {
		let opts = options(r#"{"query_timeout":30000,"transaction_timeout":65536}"#);
		assert_eq!(opts.query_timeout, Some(30_000));
		assert_eq!(opts.transaction_timeout, Some(65_536));
	}
}
