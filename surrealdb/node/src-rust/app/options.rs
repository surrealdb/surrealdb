use std::collections::HashSet;

use napi::Error;
use serde::Deserialize;
use surrealdb_core::dbs::{NewPlannerStrategy, capabilities};

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

macro_rules! process_targets {
	($set:ident) => {{
		let mut functions = HashSet::with_capacity($set.len());
		for function in $set {
			functions.insert(function.parse().expect("invalid function name"));
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
	type Error = Error;

	fn try_from(config: CapabilitiesConfig) -> Result<Self, Self::Error> {
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
