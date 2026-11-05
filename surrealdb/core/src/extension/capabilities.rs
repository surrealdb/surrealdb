//! What an extension may do in one invocation: its definition's
//! `CAPABILITIES`, intersected with the server's own capabilities.
//!
//! ```surql
//! DEFINE MODULE mod::color FROM f"modules:/color.surli" UNSIGNED
//!     CAPABILITIES { net: ["api.color.io:443", "10.0.0.0/8"], functions: ["http::get"] };
//! ```
//!
//! On a server started with `--allow-net api.color.io --deny-net 10.0.0.0/8
//! --allow-funcs http`, [`effective_capabilities`] grants `api.color.io:443`
//! (the server allows it) and drops `10.0.0.0/8` (the server does not allow
//! all of it), and grants `http::get`. The server's deny lists travel with the
//! result and are checked again on every use, so a server deny always wins.
//!
//! An extension's archive may ask for capabilities in its package file. That
//! request is never read here: only the definition grants anything.

use std::str::FromStr;

use anyhow::{Result, anyhow};

use crate::dbs::Capabilities;
use crate::dbs::capabilities::{FuncTarget, NetTarget, Targets};
use crate::expr::{CapabilityTargets, ExtensionCapabilities};

/// What one invocation of an extension may do.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EffectiveCapabilities {
	/// Network targets the extension may connect to. Every target is one the
	/// server allows.
	pub allow_net: Targets<NetTarget>,
	/// The server's network deny list, which overrides `allow_net`.
	pub deny_net: Targets<NetTarget>,
	/// Functions the extension may call. Every target is one the server
	/// allows.
	pub allow_functions: Targets<FuncTarget>,
	/// The server's function deny list, which overrides `allow_functions`.
	pub deny_functions: Targets<FuncTarget>,
	/// Whether the extension may run arbitrary SurrealQL text. The server's
	/// arbitrary-query capability still applies to the session the query runs
	/// in.
	pub queries: bool,
	/// Whether the extension may use embedded scripting: granted by the
	/// definition and allowed by the server.
	pub scripting: bool,
	/// Whether the extension's writing functions may write.
	pub write: bool,
	/// The definition's memory limit per invocation, in bytes. The runner
	/// applies the smaller of this and its own configured ceiling.
	pub memory: Option<u64>,
	/// The definition's time limit per invocation. The runner applies the
	/// smaller of this and its own configured ceiling.
	pub timeout: Option<std::time::Duration>,
	/// `false` runs the extension without timeout checks.
	pub strict_timeout: bool,
}

impl EffectiveCapabilities {
	/// Whether the extension's own sockets may reach `target`. The runner
	/// enforces this through the network policy built from the same lists.
	#[cfg(test)]
	pub fn allows_network_target(&self, target: &NetTarget) -> bool {
		self.allow_net.matches(target) && !self.deny_net.matches(target)
	}

	/// Whether the extension may call the function named `name`, such as
	/// `http::get`.
	pub fn allows_function_name(&self, name: &str) -> bool {
		self.allow_functions.matches(name) && !self.deny_functions.matches(name)
	}
}

/// The capabilities one invocation of an extension defined with `definition`
/// gets on a server running with `server`.
///
/// A target the definition lists is kept only when the server allows the
/// whole of it: `*.color.io` is dropped on a server that allows only
/// `api.color.io`. `true` grants whatever the server allows. The server's deny
/// lists are carried over unchanged.
///
/// Fails when the definition lists a target that is not a valid network or
/// function target.
pub fn effective_capabilities(
	definition: &ExtensionCapabilities,
	server: &Capabilities,
) -> Result<EffectiveCapabilities> {
	let allow_net = match &definition.net {
		CapabilityTargets::None => Targets::None,
		CapabilityTargets::All => server.allowed_network_targets_ref().clone(),
		CapabilityTargets::Some(list) => {
			let mut kept = std::collections::HashSet::new();
			for entry in list {
				let target = NetTarget::from_str(entry).map_err(|e| {
					anyhow!("Invalid network target `{entry}` in CAPABILITIES: {e}")
				})?;
				if server.allows_network_target(&target) {
					kept.insert(target);
				}
			}
			Targets::Some(kept)
		}
	};
	let allow_functions = match &definition.functions {
		CapabilityTargets::None => Targets::None,
		CapabilityTargets::All => server.allowed_functions_ref().clone(),
		CapabilityTargets::Some(list) => {
			let mut kept = std::collections::HashSet::new();
			for entry in list {
				let target = FuncTarget::from_str(entry).map_err(|e| {
					anyhow!("Invalid function target `{entry}` in CAPABILITIES: {e}")
				})?;
				if server.allows_function_target(&target) {
					kept.insert(target);
				}
			}
			Targets::Some(kept)
		}
	};
	Ok(EffectiveCapabilities {
		allow_net,
		deny_net: server.denied_network_targets_ref().clone(),
		allow_functions,
		deny_functions: server.denied_functions_ref().clone(),
		queries: definition.queries,
		scripting: definition.scripting && server.allows_scripting(),
		write: definition.write,
		memory: definition.memory,
		timeout: definition.timeout.map(|d| d.0),
		strict_timeout: definition.strict_timeout,
	})
}

#[cfg(test)]
mod tests {
	use super::*;

	fn net(s: &str) -> NetTarget {
		NetTarget::from_str(s).unwrap()
	}

	fn func(s: &str) -> FuncTarget {
		FuncTarget::from_str(s).unwrap()
	}

	/// The default definition grants nothing, whatever the server allows.
	#[test]
	fn an_empty_definition_grants_nothing() {
		let eff = effective_capabilities(&ExtensionCapabilities::default(), &Capabilities::all())
			.unwrap();
		assert!(!eff.allows_network_target(&net("api.color.io:443")));
		assert!(!eff.allows_function_name("http::get"));
		assert!(!eff.queries && !eff.scripting && !eff.write);
		assert_eq!(eff.memory, None);
		assert_eq!(eff.timeout, None);
		assert!(eff.strict_timeout);
	}

	/// A listed target survives only where the server allows it.
	#[test]
	fn listed_targets_are_narrowed_to_the_server() {
		let server = Capabilities::none()
			.with_network_targets(Targets::Some([net("api.color.io")].into_iter().collect()))
			.with_functions(Targets::Some([func("http")].into_iter().collect()))
			.with_scripting(false);
		let definition = ExtensionCapabilities {
			net: CapabilityTargets::Some(vec!["api.color.io:443".into(), "10.0.0.0/8".into()]),
			functions: CapabilityTargets::Some(vec!["http::get".into(), "crypto::md5".into()]),
			scripting: true,
			..Default::default()
		};
		let eff = effective_capabilities(&definition, &server).unwrap();
		assert_eq!(eff.allow_net, Targets::Some([net("api.color.io:443")].into_iter().collect()));
		assert!(eff.allows_function_name("http::get"));
		assert!(!eff.allows_function_name("crypto::md5"));
		assert!(!eff.scripting, "the server refuses scripting");
	}

	/// A server deny wins over `true` in the definition.
	#[test]
	fn a_server_deny_always_wins() {
		let server = Capabilities::all()
			.without_network_targets(Targets::Some([net("10.0.0.0/8")].into_iter().collect()))
			.without_functions(Targets::Some([func("http::post")].into_iter().collect()));
		let definition = ExtensionCapabilities {
			net: CapabilityTargets::All,
			functions: CapabilityTargets::All,
			..Default::default()
		};
		let eff = effective_capabilities(&definition, &server).unwrap();
		assert!(eff.allows_network_target(&net("api.color.io:443")));
		assert!(!eff.allows_network_target(&net("10.1.2.3")));
		assert!(eff.allows_function_name("http::get"));
		assert!(!eff.allows_function_name("http::post"));
	}

	/// A wildcard survives where the server's allow list covers all of it: a
	/// server wildcard covers its subdomains and narrower wildcards, an exact
	/// host never covers a wildcard, and a server wildcard deny drops every
	/// target within it.
	#[test]
	fn a_wildcard_target_is_narrowed_to_the_server() {
		let granted = |server: &Capabilities, entry: &str| {
			let definition = ExtensionCapabilities {
				net: CapabilityTargets::Some(vec![entry.into()]),
				..Default::default()
			};
			effective_capabilities(&definition, server).unwrap().allow_net
				== Targets::Some([net(entry)].into_iter().collect())
		};
		let server = Capabilities::none()
			.with_network_targets(Targets::Some([net("*.color.io")].into_iter().collect()));
		for within in ["api.color.io", "*.api.color.io", "*.color.io:443"] {
			assert!(granted(&server, within), "{within}");
		}
		for outside in ["color.io", "*.io.color", "other.org"] {
			assert!(!granted(&server, outside), "{outside}");
		}
		let server = Capabilities::none()
			.with_network_targets(Targets::Some([net("api.color.io")].into_iter().collect()));
		assert!(!granted(&server, "*.color.io"));
		let server = Capabilities::all()
			.without_network_targets(Targets::Some([net("*.color.io")].into_iter().collect()));
		for denied in ["api.color.io", "*.api.color.io"] {
			assert!(!granted(&server, denied), "{denied}");
		}
		assert!(granted(&server, "color.io"));
	}

	#[test]
	fn an_invalid_target_is_an_error() {
		let definition = ExtensionCapabilities {
			net: CapabilityTargets::Some(vec!["not a host".into()]),
			..Default::default()
		};
		effective_capabilities(&definition, &Capabilities::all()).unwrap_err();
	}
}
