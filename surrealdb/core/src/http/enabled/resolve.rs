use std::error::Error;
use std::str::FromStr;
use std::sync::Arc;

use ipnet::IpNet;
use reqwest::dns::{Addrs, Name, Resolve, Resolving};
use tokio::net::lookup_host;

use crate::dbs::Capabilities;
use crate::dbs::capabilities::NetTarget;

pub struct FilteringResolver {
	pub cap: Arc<Capabilities>,
}

impl FilteringResolver {
	pub fn from_capabilities(cap: Arc<Capabilities>) -> Self {
		FilteringResolver {
			cap,
		}
	}
}

impl Resolve for FilteringResolver {
	fn resolve(&self, name: Name) -> Resolving {
		let cap = self.cap.clone();
		let name_str = name.as_str().to_string();
		Box::pin(async move {
			// Check the domain name (if any) matches the allowlist
			let name_target = NetTarget::from_str(&name_str)
				.map_err(|x| Box::new(x) as Box<dyn Error + Send + Sync>)?;
			let name_is_allowed =
				cap.matches_any_allow_net(&name_target) && !cap.matches_any_deny_net(&name_target);
			// If the domain name itself is not allowed, return an error
			if !name_is_allowed {
				return Err(
					Box::new(crate::err::Error::NetTargetNotAllowed(name_target.to_string()))
						as Box<dyn Error + Send + Sync>,
				);
			}
			// Resolve the addresses
			let addrs: Vec<std::net::SocketAddr> = lookup_host((name_str, 0_u16))
				.await
				.map_err(|x| Box::new(x) as Box<dyn Error + Send + Sync>)?
				.collect();
			// Check each resolved address against the deny list, collecting allowed ones
			// and tracking the first denied address for error reporting
			let mut allowed = Vec::new();
			let mut first_denied = None;
			for addr in addrs {
				let target = IpNet::from(addr.ip());
				if cap.matches_any_deny_net(&NetTarget::IPNet(target)) {
					if first_denied.is_none() {
						first_denied = Some(target);
					}
				} else {
					allowed.push(addr);
				}
			}
			// If all addresses were denied, return a proper error
			if allowed.is_empty()
				&& let Some(denied) = first_denied
			{
				return Err(Box::new(crate::err::Error::NetTargetNotAllowed(denied.to_string()))
					as Box<dyn Error + Send + Sync>);
			}
			Ok(Box::new(allowed.into_iter()) as Addrs)
		}) as Resolving
	}
}
