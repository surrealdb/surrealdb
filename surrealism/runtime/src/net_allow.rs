//! Resolve `allow_net` strings once at module load time.
//!
//! DNS lookups run here (sync, on the thread loading the module — typically not on a Tokio
//! worker). Used to build the outbound socket allowlist for WASI (`parse_filters`).

use std::net::{IpAddr, SocketAddr, ToSocketAddrs};
use std::sync::Arc;

use ipnet::IpNet;

/// One resolved allow-net rule, aligned with [`crate::wasi_context`] socket filtering.
#[derive(Debug, Clone)]
pub enum ResolvedNetAllow {
	/// IP or CIDR — any port.
	Net(IpNet),
	/// Specific IP and port (from e.g. `host:443` or resolved hostname with port).
	IpPort(IpAddr, u16),
}

impl ResolvedNetAllow {
	/// Same semantics as the WASI `socket_addr_check` filter for outbound connections.
	pub fn matches_socket_addr(&self, addr: &SocketAddr) -> bool {
		match self {
			Self::Net(net) => net.contains(&addr.ip()),
			Self::IpPort(ip, port) => addr.ip() == *ip && addr.port() == *port,
		}
	}

	fn push_from_socket_addr(port: Option<u16>, addr: SocketAddr, out: &mut Vec<Self>) {
		if let Some(port) = port {
			out.push(Self::IpPort(addr.ip(), port));
		} else {
			out.push(Self::Net(IpNet::from(addr.ip())));
		}
	}
}

/// Resolve `allow_net` entries the same way as SurrealDB `NetTarget::from_str` ordering:
/// 1. `IpNet` (CIDR)
/// 2. `IpAddr` → `/32` or `/128`
/// 3. URL-style host, optional port; hostnames → DNS to IPs (blocking).
///
/// Returns an error if any entry fails to parse or any hostname fails to resolve,
/// aligning with the core pattern where DNS failures propagate rather than being
/// silently swallowed.
pub fn resolve_allow_net(entries: &[String]) -> anyhow::Result<Arc<Vec<ResolvedNetAllow>>> {
	let mut out = Vec::new();
	for entry in entries {
		resolve_one(entry, &mut out)?;
	}
	Ok(Arc::new(out))
}

fn resolve_one(entry: &str, out: &mut Vec<ResolvedNetAllow>) -> anyhow::Result<()> {
	if let Ok(net) = entry.parse::<IpNet>() {
		out.push(ResolvedNetAllow::Net(net));
		return Ok(());
	}
	if let Ok(ip) = entry.parse::<IpAddr>() {
		out.push(ResolvedNetAllow::Net(IpNet::from(ip)));
		return Ok(());
	}
	let url = url::Url::parse(&format!("http://{entry}"))
		.map_err(|e| anyhow::anyhow!("failed to parse allow_net entry '{entry}': {e}"))?;
	let host =
		url.host().ok_or_else(|| anyhow::anyhow!("allow_net entry '{entry}' has no host"))?;

	let port: Option<u16> = entry.rsplit_once(':').and_then(|(_, p)| p.parse::<u16>().ok());

	match host {
		url::Host::Ipv4(ip) => {
			let ip: IpAddr = ip.into();
			if let Some(port) = port {
				out.push(ResolvedNetAllow::IpPort(ip, port));
			} else {
				out.push(ResolvedNetAllow::Net(IpNet::from(ip)));
			}
		}
		url::Host::Ipv6(ip) => {
			let ip: IpAddr = ip.into();
			if let Some(port) = port {
				out.push(ResolvedNetAllow::IpPort(ip, port));
			} else {
				out.push(ResolvedNetAllow::Net(IpNet::from(ip)));
			}
		}
		url::Host::Domain(domain) => {
			resolve_hostname(domain, port, out)?;
		}
	}
	Ok(())
}

/// Blocking DNS — only call from module load / `Runtime::new`, not from async request paths.
fn resolve_hostname(
	hostname: &str,
	port: Option<u16>,
	out: &mut Vec<ResolvedNetAllow>,
) -> anyhow::Result<()> {
	let addrs = (hostname, port.unwrap_or(80))
		.to_socket_addrs()
		.map_err(|e| anyhow::anyhow!("failed to resolve allow_net hostname '{hostname}': {e}"))?;
	for addr in addrs {
		ResolvedNetAllow::push_from_socket_addr(port, addr, out);
	}
	Ok(())
}

#[cfg(test)]
mod tests {
	use std::net::SocketAddr;

	use super::*;

	#[test]
	fn parses_ip_and_cidr() {
		let r = resolve_allow_net(&["192.168.1.1".into(), "10.0.0.0/8".into()]).unwrap();
		assert_eq!(r.len(), 2);
		let a: SocketAddr = "192.168.1.1:8080".parse().unwrap();
		assert!(r[0].matches_socket_addr(&a));
		let inside: SocketAddr = "10.1.2.3:443".parse().unwrap();
		assert!(r[1].matches_socket_addr(&inside));
	}

	#[test]
	fn parses_ip_with_port() {
		let r = resolve_allow_net(&["192.168.1.1:80".into()]).unwrap();
		assert_eq!(r.len(), 1);
		let ok: SocketAddr = "192.168.1.1:80".parse().unwrap();
		assert!(r[0].matches_socket_addr(&ok));
		let wrong: SocketAddr = "192.168.1.1:443".parse().unwrap();
		assert!(!r[0].matches_socket_addr(&wrong));
	}
}
