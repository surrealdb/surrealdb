pub(crate) mod cache;
pub(crate) mod config;
pub(crate) mod host;
#[cfg(feature = "http")]
pub(crate) mod silo;

/// Checks whether capabilities required by a Surrealism package are allowed
/// by the server configuration. The `allow_arbitrary_queries` capability is
/// not checked here as that is enforced at runtime by the host `sql()` call.
pub(crate) fn validate_surrealism_capabilities(
	caps: &crate::dbs::Capabilities,
	capabilities: &surrealism_runtime::capabilities::SurrealismCapabilities,
) -> anyhow::Result<()> {
	use std::str::FromStr;

	use anyhow::bail;
	use surrealism_runtime::capabilities::FunctionTargets;

	use crate::dbs::capabilities::{FuncTarget, NetTarget, Targets};

	if capabilities.allow_scripting && !caps.allows_scripting() {
		bail!("Surrealism package requires scripting, but it is not allowed");
	}

	match &capabilities.allow_functions {
		FunctionTargets::None => {}
		FunctionTargets::All => {
			if !matches!(caps.allowed_functions_ref(), Targets::All) {
				bail!(
					"Surrealism package requires access to all functions, but the server does not allow all functions"
				);
			}
		}
		FunctionTargets::Some(patterns) => {
			for pattern in patterns {
				let target = FuncTarget::from_str(pattern).map_err(|e| {
					anyhow::anyhow!(
						"Surrealism package has invalid function pattern '{}': {}",
						pattern,
						e
					)
				})?;
				if !caps.allows_function_target(&target) {
					bail!(
						"Surrealism package requires function pattern '{}', but it is not allowed by the server",
						pattern
					);
				}
			}
		}
	}

	if !capabilities.allow_net.is_empty() {
		for net in capabilities.allow_net.iter() {
			let target = NetTarget::from_str(net)?;
			if !caps.allows_network_target(&target) {
				bail!(
					"Surrealism package requires network target '{}', but it is not allowed",
					net
				);
			}
		}
	}

	Ok(())
}
