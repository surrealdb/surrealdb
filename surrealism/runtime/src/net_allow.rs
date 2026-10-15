//! Outbound network policy for a module's `allow_net`.
//!
//! Two checks share one policy:
//!
//! - **By name**, when the guest resolves a hostname. Enforced by the DNS shim in [`crate::dns`],
//!   which replaces wasmtime's `wasi:sockets/ip-name-lookup`.
//! - **By address**, when the guest connects. Enforced by WASI's `socket_addr_check`.
//!
//! Allowed hostnames are **not** resolved when the policy is built. A name is
//! matched as a name, and the addresses the shim hands back for an allowed name
//! are recorded so the later connect recognises them. Pre-resolving at load time
//! would pin whatever the record pointed at then, which goes stale behind any
//! rotating DNS and cannot be refreshed without reloading the module.
//!
//! Denied hostnames are resolved, once per invocation, when an allow rule admits
//! a connect by address alone (`*`, a network or an IP): otherwise a guest could
//! reach a denied host by connecting to its address literal. See
//! [`NetPolicy::for_invocation`].
//!
//! A resolved address that is private or otherwise special is rejected unless an
//! explicit IP rule covers it, even when the hostname that produced it was
//! allowed. The predicate is [`common::net::is_special_use_ip`], which core's
//! own filtering resolver calls too, so a guest socket and a SurrealQL
//! `http::*` call cannot disagree about what a name may reach.
//!
//! Every address decision is taken on the address a connect actually reaches,
//! which for an IPv6 transition form is the IPv4 destination it carries — see
//! [`common::net::embedded_ipv4`]. A rule naming the outer IPv6 range says
//! nothing about where the packet lands, so `2000::/3` grants the public IPv6
//! internet and not, through 6to4, every IPv4 host as well.
//!
//! The operator's deny rules are applied on top of the module's allowlist and
//! win over it. A bare deny rule refuses the name, each resolved address and the
//! connect; a port-bearing one names a port rather than a host, so it refuses
//! the connect on that port — at every address the name it names resolved to.
//! A module cannot widen what the server forbids by declaring it.

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;

use ipnet::IpNet;
use parking_lot::Mutex;
use web_time::{Duration, Instant};

/// How long an address stays connectable after the name that produced it
/// resolved to it.
///
/// A guest resolves and then connects, so this only has to outlast that gap.
/// Keeping it short is what makes the authorisation follow a rotating record
/// rather than accumulate: an address that stops being returned stops being
/// reachable, instead of staying reachable for the module's lifetime.
const VENDED_TTL: Duration = Duration::from_secs(120);

/// The most addresses kept at once, across every name.
///
/// The TTL alone bounds the set by the rate a name can rotate, which is chosen
/// by whoever controls that name. This bounds it by a constant instead, so a
/// module looping on resolution cannot grow host memory. The oldest entries go
/// first; losing one costs a re-resolution, not a refusal.
const MAX_VENDED: usize = 1024;

/// How long [`NetPolicy::for_invocation`] waits for the operator's denied
/// hostnames to resolve. Past it, the invocation may connect only to
/// addresses its own lookups produced.
const DENY_RESOLVE_TIMEOUT: Duration = Duration::from_secs(2);

/// One parsed `allow_net` entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NetRule {
	/// An IP or CIDR, any port.
	Net(IpNet),
	/// A specific IP and port.
	IpPort(IpAddr, u16),
	/// A hostname, optionally pinned to a port. Matched case-insensitively and
	/// exactly — `api.example.com` does not cover `sub.api.example.com`.
	Host {
		host: String,
		port: Option<u16>,
	},
	/// Any hostname, and any address that is not special-use, on any port.
	/// Spelled `*`. A special-use address is still reached only through an
	/// explicit IP rule.
	Any,
}

/// A queried name in the form the rules are stored in.
///
/// `allow_net` entries are parsed through `url::Url::parse`, which lowercases
/// and punycodes, so a guest asking for the Unicode spelling of a name the
/// module allowed has to be put in the same form before it is matched.
/// Otherwise `allow_net = ["bücher.example"]` refuses a guest resolving exactly
/// that name.
///
/// Falls back to lowercasing whatever will not parse as a host, which then
/// matches nothing and fails closed.
fn normalise_name(name: &str) -> String {
	url::Url::parse(&format!("http://{name}"))
		.ok()
		.and_then(|u| u.host_str().map(str::to_ascii_lowercase))
		.unwrap_or_else(|| name.to_ascii_lowercase())
}

/// The address a connection to `ip` reaches.
///
/// For every IPv6 spelling that carries an IPv4 destination — mapped, 6to4,
/// NAT64, Teredo — that destination is where the packet goes, so it is what a
/// rule has to name and what the special-use judgement applies to.
fn destination(ip: &IpAddr) -> IpAddr {
	common::net::embedded_ipv4(*ip).map_or(*ip, IpAddr::V4)
}

/// The operator's `--deny-net`, in the form this crate can evaluate.
///
/// Built from the server's own deny rules and handed to [`NetPolicy::parse`].
/// Kept apart from the allow rules because it is not the module's to declare:
/// the module chooses what it wants to reach, the operator chooses what nothing
/// may reach.
#[derive(Debug, Clone, Default)]
pub enum NetDeny {
	/// The operator denied nothing.
	#[default]
	Nothing,
	/// The operator denied everything.
	Everything,
	/// The operator denied these targets. Shared, because every invocation's
	/// copy of a policy carries them.
	Rules(Arc<[NetRule]>),
}

impl NetDeny {
	/// Parse deny entries in the same spellings `allow_net` accepts.
	pub fn parse(entries: &[String]) -> anyhow::Result<Self> {
		if entries.is_empty() {
			return Ok(Self::Nothing);
		}
		if entries.iter().any(|e| e == "*") {
			return Ok(Self::Everything);
		}
		let mut rules = Vec::with_capacity(entries.len());
		for entry in entries {
			rules.push(parse_one(entry)?);
		}
		Ok(Self::Rules(rules.into()))
	}

	/// Whether resolving `name` is denied.
	///
	/// A port-bearing deny rule does not deny the name: it forbids one port,
	/// and the connect is where that is decided — see
	/// [`Self::ports_denied_for_name`]. This matches how the engine's own
	/// resolver reads a deny rule against a bare name.
	fn denies_name(&self, name: &str) -> bool {
		match self {
			Self::Nothing => false,
			Self::Everything => true,
			Self::Rules(rules) => {
				if let Ok(ip) = name.parse::<IpAddr>() {
					let dst = destination(&ip);
					return rules.iter().any(|r| rule_covers_ip(r, &dst));
				}
				let name = normalise_name(name);
				rules
					.iter()
					.any(|r| matches!(r, NetRule::Host { host, port: None } if *host == name))
			}
		}
	}

	/// The ports a deny rule pins on `name`, which is already lowercased.
	///
	/// A rule such as `example.com:443` forbids one port on whatever that name
	/// resolves to. The name itself stays resolvable, so the refusal has to
	/// travel with the addresses it produced: they are recorded against these
	/// ports when they are vended, and [`NetPolicy::allows_socket_addr`]
	/// refuses the connect. Without this a port-bearing deny rule would bind
	/// nothing for a guest socket while still refusing a SurrealQL `http::*`
	/// call to the same host and port.
	fn ports_denied_for_name(&self, lowered: &str) -> Vec<u16> {
		match self {
			Self::Nothing | Self::Everything => Vec::new(),
			Self::Rules(rules) => rules
				.iter()
				.filter_map(|r| match r {
					NetRule::Host {
						host,
						port: Some(port),
					} if host == lowered => Some(*port),
					_ => None,
				})
				.collect(),
		}
	}

	/// Whether an address is denied on every port.
	fn denies_ip(&self, ip: &IpAddr) -> bool {
		match self {
			Self::Nothing => false,
			Self::Everything => true,
			Self::Rules(rules) => {
				let dst = destination(ip);
				rules.iter().any(|r| match r {
					NetRule::Net(net) => net.contains(&dst),
					// Pinned to one port, so it does not deny the address itself.
					NetRule::IpPort(..)
					| NetRule::Host {
						..
					} => false,
					// `parse` turns `*` into `Everything`.
					NetRule::Any => true,
				})
			}
		}
	}

	/// Whether connecting to `addr` is denied.
	fn denies_socket_addr(&self, addr: &SocketAddr) -> bool {
		match self {
			Self::Nothing => false,
			Self::Everything => true,
			Self::Rules(rules) => {
				let dst = destination(&addr.ip());
				rules.iter().any(|r| match r {
					NetRule::Net(net) => net.contains(&dst),
					NetRule::IpPort(rule_ip, port) => *rule_ip == dst && *port == addr.port(),
					// A deny rule naming a host reaches the connect through the
					// addresses that name resolved to, which carry it: a bare
					// rule refused the name outright, and a port-bearing one is
					// recorded against those addresses when they are vended.
					NetRule::Host {
						..
					} => false,
					NetRule::Any => true,
				})
			}
		}
	}
}

/// An address the DNS shim handed to the guest, and what it carries from the
/// name that produced it.
#[derive(Debug)]
struct Vended {
	/// Indices into [`NetPolicy::rules`] of the hostname rules that produced
	/// this address.
	///
	/// The binding is the point: a hostname rule authorises the addresses *its
	/// own* name resolved to, on the port that rule names. A flat set of
	/// addresses would let one rule's port constraint be satisfied by another
	/// rule's address, and a port-less rule would then open every port on every
	/// address any name vended.
	///
	/// Unioned across names: two allowed names routinely share one address, and
	/// replacing the list would make each resolution revoke the other's grant.
	by: Vec<usize>,
	/// Ports the operator denied on a name that resolved to this address,
	/// unioned the same way. See [`NetDeny::ports_denied_for_name`].
	denied_ports: Vec<u16>,
	/// When it was last vended, for [`VENDED_TTL`].
	at: Instant,
	/// Vending order, so eviction under [`MAX_VENDED`] is decided by age rather
	/// than by `HashMap` iteration order. Every address from one resolution
	/// shares a timestamp, which alone cannot order them.
	seq: u64,
}

/// The addresses the DNS shim has handed to the guest.
#[derive(Debug, Default)]
struct VendedTable {
	by_ip: HashMap<IpAddr, Vended>,
	next_seq: u64,
}

/// A module's outbound network policy.
#[derive(Debug)]
pub struct NetPolicy {
	/// Shared by every invocation's copy of the policy.
	rules: Arc<[NetRule]>,
	deny: NetDeny,
	/// Addresses handed to the guest by the DNS shim for an allowed hostname.
	///
	/// `socket_addr_check` consults this so a connect to a freshly resolved
	/// address is recognised without the policy having pre-resolved anything.
	/// Entries expire after [`VENDED_TTL`] and are capped at [`MAX_VENDED`].
	vended: Mutex<VendedTable>,
	/// What the operator's hostname deny rules resolved to when this
	/// invocation's copy was made. See [`Self::for_invocation`].
	denied_addrs: DeniedAddrs,
}

/// The addresses behind the operator's hostname deny rules, for one invocation.
#[derive(Debug, Clone)]
enum DeniedAddrs {
	/// Each address with the port its rule pins, or `None` for every port.
	/// Empty when nothing needed resolving.
	Resolved(Arc<[(IpAddr, Option<u16>)]>),
	/// A denied hostname did not resolve, or not within
	/// [`DENY_RESOLVE_TIMEOUT`]. Its addresses are unknown, so a connect is
	/// allowed only to an address the DNS shim vended for a name it checked.
	Unknown,
}

impl DeniedAddrs {
	fn none() -> Self {
		Self::Resolved(Arc::new([]))
	}
}

impl NetPolicy {
	/// Parse `allow_net` entries under the operator's deny rules. Performs no DNS.
	pub fn parse(entries: &[String], deny: NetDeny) -> anyhow::Result<Arc<Self>> {
		let mut rules = Vec::new();
		for entry in entries {
			rules.push(parse_one(entry)?);
		}
		Ok(Arc::new(Self {
			rules: rules.into(),
			deny,
			vended: Mutex::new(VendedTable::default()),
			denied_addrs: DeniedAddrs::none(),
		}))
	}

	/// A policy that allows nothing.
	pub fn deny_all() -> Arc<Self> {
		Arc::new(Self {
			rules: Arc::new([]),
			deny: NetDeny::Nothing,
			vended: Mutex::new(VendedTable::default()),
			denied_addrs: DeniedAddrs::none(),
		})
	}

	/// The same rules with nothing vended yet, for one invocation.
	///
	/// The addresses the DNS shim hands out are what lets a later connect
	/// through, so they belong to the invocation that resolved them. A copy per
	/// invocation keeps one invocation from connecting on the strength of
	/// another's lookup. A policy with no rules vends nothing, so it is
	/// shared rather than copied. The rules themselves are shared either way.
	///
	/// When an allow rule matches addresses (`*`, a network or an IP) and the
	/// operator denies a hostname, the copy also holds what that hostname
	/// resolves to now, so a guest that connects to the address directly is
	/// refused as one that resolved the name would be. The resolution is the
	/// system resolver's, made once per invocation and bounded by
	/// [`DENY_RESOLVE_TIMEOUT`]. If a denied name fails to resolve in that
	/// time, its addresses are unknown and the copy fails closed: the guest
	/// may connect only to addresses the DNS shim vended for names it checked.
	/// Without an address allow rule an address is reachable only through such
	/// a name, so nothing is resolved.
	pub async fn for_invocation(self: &Arc<Self>) -> Arc<Self> {
		if self.rules.is_empty() {
			return Arc::clone(self);
		}
		let denied_addrs = if self.allows_by_address() {
			resolve_denied_hosts(&self.deny).await
		} else {
			DeniedAddrs::none()
		};
		Arc::new(Self {
			rules: Arc::clone(&self.rules),
			deny: self.deny.clone(),
			vended: Mutex::new(VendedTable::default()),
			denied_addrs,
		})
	}

	/// Whether an allow rule admits a connect by its address alone.
	fn allows_by_address(&self) -> bool {
		self.rules.iter().any(|r| matches!(r, NetRule::Any | NetRule::Net(_) | NetRule::IpPort(..)))
	}

	/// Whether the module declared any network access at all.
	pub fn is_empty(&self) -> bool {
		self.rules.is_empty()
	}

	/// Whether the guest may resolve `name`.
	///
	/// A literal address is matched against the IP rules, so a module allowed
	/// `127.0.0.1` can still "resolve" it.
	pub fn allows_name(&self, name: &str) -> bool {
		if self.deny.denies_name(name) {
			return false;
		}
		if let Ok(ip) = name.parse::<IpAddr>() {
			return self.names_destination(&ip);
		}
		let name = normalise_name(name);
		self.host_rule_indices(&name).next().is_some()
	}

	/// Filter addresses resolved for `name`, and record which rules vended them
	/// so the subsequent connect is recognised.
	///
	/// A private or special address survives only when an explicit IP rule
	/// covers it; a denied address never survives.
	pub fn accept_resolved(&self, name: &str, addrs: Vec<IpAddr>) -> Vec<IpAddr> {
		let lowered = normalise_name(name);
		let by: Vec<usize> = self.host_rule_indices(&lowered).collect();
		let denied_ports = self.deny.ports_denied_for_name(&lowered);
		let kept: Vec<IpAddr> = addrs
			.into_iter()
			.filter(|ip| !self.deny.denies_ip(ip))
			.filter(|ip| !common::net::is_special_use_ip(*ip) || self.names_destination(ip))
			.collect();
		// A literal address resolves through the IP rules and vends nothing:
		// those rules authorise the connect on their own.
		if kept.is_empty() || (by.is_empty() && denied_ports.is_empty()) {
			return kept;
		}
		let now = Instant::now();
		let mut table = self.vended.lock();
		for ip in &kept {
			// A special-use address survived because an explicit IP rule names
			// it, and that rule authorises the connect on its own terms —
			// including any port it pins. Binding it to the hostname rule as
			// well would let a port-less name rule open every port on an address
			// the operator granted on one. The operator's own port refusals
			// still travel with it: those are a restriction, not a grant.
			let by = if common::net::is_special_use_ip(*ip) {
				&[][..]
			} else {
				&by[..]
			};
			if by.is_empty() && denied_ports.is_empty() {
				continue;
			}
			let seq = table.next_seq;
			table.next_seq += 1;
			match table.by_ip.entry(*ip) {
				Entry::Occupied(mut e) => {
					let entry = e.get_mut();
					for index in by {
						if !entry.by.contains(index) {
							entry.by.push(*index);
						}
					}
					for port in &denied_ports {
						if !entry.denied_ports.contains(port) {
							entry.denied_ports.push(*port);
						}
					}
					entry.at = now;
					entry.seq = seq;
				}
				Entry::Vacant(e) => {
					e.insert(Vended {
						by: by.to_vec(),
						denied_ports: denied_ports.clone(),
						at: now,
						seq,
					});
				}
			}
		}
		prune(&mut table, now);
		kept
	}

	/// Whether the guest may connect to `addr`.
	pub fn allows_socket_addr(&self, addr: &SocketAddr) -> bool {
		if self.deny.denies_socket_addr(addr) {
			return false;
		}
		let ip = addr.ip();
		let dst = destination(&ip);
		match &self.denied_addrs {
			DeniedAddrs::Resolved(denied) => {
				if denied.iter().any(|(d, port)| *d == dst && port.is_none_or(|p| p == addr.port()))
				{
					return false;
				}
			}
			DeniedAddrs::Unknown => {
				if self.vended_entry(&ip).is_none() {
					return false;
				}
			}
		}
		// Decided on the destination, so an IPv6 transition form cannot carry a
		// special-use IPv4 address past a rule that names only the outer range.
		// A literal connect never passes through `accept_resolved`, so this is
		// the only point that asks.
		if common::net::is_special_use_ip(ip) && !self.names_destination(&ip) {
			return false;
		}
		let vended = self.vended_entry(&ip);
		if let Some((_, denied_ports)) = &vended
			&& denied_ports.contains(&addr.port())
		{
			return false;
		}
		self.rules.iter().enumerate().any(|(index, r)| match r {
			NetRule::Net(net) => net.contains(&dst),
			NetRule::IpPort(rule_ip, port) => *rule_ip == dst && *port == addr.port(),
			// A hostname rule authorises the addresses its own name resolved
			// to, on the port it names — not addresses some other rule vended.
			NetRule::Host {
				port,
				..
			} => {
				port.is_none_or(|p| p == addr.port())
					&& vended.as_ref().is_some_and(|(by, _)| by.contains(&index))
			}
			// Special-use destinations were refused above unless an explicit
			// IP rule names them.
			NetRule::Any => true,
		})
	}

	/// The indices of the hostname rules matching an already-lowercased name.
	fn host_rule_indices<'a>(&'a self, lowered: &'a str) -> impl Iterator<Item = usize> + 'a {
		self.rules.iter().enumerate().filter_map(move |(index, r)| match r {
			NetRule::Host {
				host,
				..
			} if host == lowered => Some(index),
			NetRule::Any => Some(index),
			_ => None,
		})
	}

	/// Whether an explicit address rule names the destination a connect to `ip`
	/// would reach, ignoring any port those rules pin.
	///
	/// Asked where the question is "did the operator name this address at all"
	/// — readmitting a special-use address, and matching a literal name.
	fn names_destination(&self, ip: &IpAddr) -> bool {
		let dst = destination(ip);
		self.rules.iter().any(|r| rule_covers_ip(r, &dst))
	}

	/// What was recorded for `ip`, if it is still within its TTL: the hostname
	/// rules that vended it, and the ports denied on the names that did.
	fn vended_entry(&self, ip: &IpAddr) -> Option<(Vec<usize>, Vec<u16>)> {
		let table = self.vended.lock();
		let entry = table.by_ip.get(ip)?;
		(entry.at.elapsed() < VENDED_TTL).then(|| (entry.by.clone(), entry.denied_ports.clone()))
	}
}

/// Whether an address rule covers `ip`, ignoring any port it pins.
fn rule_covers_ip(rule: &NetRule, ip: &IpAddr) -> bool {
	match rule {
		NetRule::Net(net) => net.contains(ip),
		NetRule::IpPort(rule_ip, _) => rule_ip == ip,
		NetRule::Host {
			..
		}
		| NetRule::Any => false,
	}
}

/// Drop expired entries, then the oldest until the map is within [`MAX_VENDED`].
fn prune(table: &mut VendedTable, now: Instant) {
	table.by_ip.retain(|_, entry| now.duration_since(entry.at) < VENDED_TTL);
	if table.by_ip.len() <= MAX_VENDED {
		return;
	}
	let mut ages: Vec<(IpAddr, u64)> = table.by_ip.iter().map(|(ip, e)| (*ip, e.seq)).collect();
	ages.sort_unstable_by_key(|(_, seq)| *seq);
	for (ip, _) in ages.into_iter().take(table.by_ip.len() - MAX_VENDED) {
		table.by_ip.remove(&ip);
	}
}

/// What the hostname rules in `deny` resolve to now, each address with the
/// port its rule pins; [`DeniedAddrs::Unknown`] if any of them fails to
/// resolve within [`DENY_RESOLVE_TIMEOUT`].
async fn resolve_denied_hosts(deny: &NetDeny) -> DeniedAddrs {
	let NetDeny::Rules(rules) = deny else {
		return DeniedAddrs::none();
	};
	let hosts: Vec<(String, Option<u16>)> = rules
		.iter()
		.filter_map(|r| match r {
			NetRule::Host {
				host,
				port,
			} => Some((host.clone(), *port)),
			_ => None,
		})
		.collect();
	if hosts.is_empty() {
		return DeniedAddrs::none();
	}
	let resolve = async {
		let mut addrs = Vec::new();
		for (host, port) in &hosts {
			match tokio::net::lookup_host((host.as_str(), 0)).await {
				Ok(found) => {
					addrs.extend(found.map(|a| (destination(&a.ip().to_canonical()), *port)))
				}
				Err(e) => {
					tracing::warn!(
						host = %host,
						error = %e,
						"A host in deny_net did not resolve: this invocation may connect only to addresses it looked up by name"
					);
					return None;
				}
			}
		}
		Some(addrs)
	};
	match tokio::time::timeout(DENY_RESOLVE_TIMEOUT, resolve).await {
		Ok(Some(addrs)) => DeniedAddrs::Resolved(addrs.into()),
		Ok(None) => DeniedAddrs::Unknown,
		Err(_) => {
			tracing::warn!(
				timeout = ?DENY_RESOLVE_TIMEOUT,
				"The hosts in deny_net did not resolve in time: this invocation may connect only to addresses it looked up by name"
			);
			DeniedAddrs::Unknown
		}
	}
}

fn parse_one(entry: &str) -> anyhow::Result<NetRule> {
	if entry == "*" {
		return Ok(NetRule::Any);
	}
	if let Ok(net) = entry.parse::<IpNet>() {
		return Ok(NetRule::Net(net));
	}
	if let Ok(ip) = entry.parse::<IpAddr>() {
		return Ok(NetRule::Net(IpNet::from(ip)));
	}
	let url = url::Url::parse(&format!("http://{entry}"))
		.map_err(|e| anyhow::anyhow!("failed to parse allow_net entry '{entry}': {e}"))?;
	let host =
		url.host().ok_or_else(|| anyhow::anyhow!("allow_net entry '{entry}' has no host"))?;
	// `Url::port` is `None` for a default port, so read the port off the entry
	// to tell `example.com` from `example.com:80`.
	let port: Option<u16> = entry.rsplit_once(':').and_then(|(_, p)| p.parse::<u16>().ok());

	Ok(match host {
		url::Host::Ipv4(ip) => match port {
			Some(port) => NetRule::IpPort(ip.into(), port),
			None => NetRule::Net(IpNet::from(IpAddr::from(ip))),
		},
		url::Host::Ipv6(ip) => match port {
			Some(port) => NetRule::IpPort(ip.into(), port),
			None => NetRule::Net(IpNet::from(IpAddr::from(ip))),
		},
		url::Host::Domain(domain) => NetRule::Host {
			host: domain.to_ascii_lowercase(),
			port,
		},
	})
}

#[cfg(test)]
mod tests {
	use super::*;

	/// `*` reaches any name and any public address, but a special-use address
	/// only through an explicit IP rule, as for a named host.
	#[test]
	fn any_allows_public_destinations_only() {
		use std::net::SocketAddr;

		let any = NetPolicy::parse(&["*".to_string()], NetDeny::Nothing).unwrap();
		assert!(any.allows_name("api.example.com"));
		assert!(any.allows_socket_addr(&"93.184.216.34:443".parse::<SocketAddr>().unwrap()));
		assert!(!any.allows_socket_addr(&"127.0.0.1:80".parse::<SocketAddr>().unwrap()));
		assert!(!any.allows_socket_addr(&"169.254.169.254:80".parse::<SocketAddr>().unwrap()));
		assert!(
			any.accept_resolved("metadata", vec!["169.254.169.254".parse().unwrap()]).is_empty()
		);

		let any_and_loopback =
			NetPolicy::parse(&["*".to_string(), "127.0.0.1".to_string()], NetDeny::Nothing)
				.unwrap();
		assert!(
			any_and_loopback.allows_socket_addr(&"127.0.0.1:80".parse::<SocketAddr>().unwrap())
		);

		let denied =
			NetPolicy::parse(&["*".to_string()], NetDeny::parse(&["*".to_string()]).unwrap())
				.unwrap();
		assert!(!denied.allows_name("api.example.com"));
	}

	/// A hostname the operator denies stays denied when the guest connects to
	/// its address directly under an allow rule that matches addresses: here
	/// `*`, with `127.0.0.1` named so that a loopback address is reachable at
	/// all.
	#[tokio::test(flavor = "current_thread")]
	async fn a_denied_hostname_is_refused_by_address_under_an_address_allow() {
		use std::net::SocketAddr;

		let allow = ["*".to_string(), "127.0.0.1".to_string()];
		let loopback: SocketAddr = "127.0.0.1:8080".parse().unwrap();

		let open = NetPolicy::parse(&allow, NetDeny::Nothing).unwrap().for_invocation().await;
		assert!(open.allows_socket_addr(&loopback), "the control is reachable");

		let deny = NetDeny::parse(&["localhost".to_string()]).unwrap();
		let denied = NetPolicy::parse(&allow, deny).unwrap().for_invocation().await;
		assert!(!denied.allows_name("localhost"), "the name is refused at the DNS shim");
		assert!(!denied.allows_socket_addr(&loopback), "and its address is refused too");

		// A port-bearing deny refuses that port only.
		let deny = NetDeny::parse(&["localhost:8080".to_string()]).unwrap();
		let pinned = NetPolicy::parse(&allow, deny).unwrap().for_invocation().await;
		assert!(!pinned.allows_socket_addr(&loopback));
		assert!(pinned.allows_socket_addr(&"127.0.0.1:9090".parse().unwrap()));
	}

	/// Under a bare `*` allow, a public address a denied hostname resolved to
	/// is refused while any other public address stays reachable.
	#[test]
	fn a_resolved_deny_refuses_its_address_under_any() {
		use std::net::SocketAddr;

		let deny = NetDeny::parse(&["api.internal.example".to_string()]).unwrap();
		let base = NetPolicy::parse(&["*".to_string()], deny).unwrap();
		let denied: IpAddr = "8.8.8.8".parse().unwrap();
		let policy = NetPolicy {
			rules: Arc::clone(&base.rules),
			deny: base.deny.clone(),
			vended: Mutex::new(VendedTable::default()),
			denied_addrs: DeniedAddrs::Resolved(Arc::new([(denied, None)])),
		};
		assert!(!policy.allows_socket_addr(&SocketAddr::new(denied, 443)));
		// The IPv4-mapped form reaches the same destination.
		assert!(!policy.allows_socket_addr(&"[::ffff:8.8.8.8]:443".parse().unwrap()));
		assert!(policy.allows_socket_addr(&"1.1.1.1:443".parse().unwrap()));
	}

	/// A denied hostname that does not resolve leaves its addresses unknown, so
	/// the invocation may connect only to addresses its own lookups produced:
	/// an IP literal is refused even where an IP rule names it.
	#[tokio::test(flavor = "current_thread")]
	async fn an_unresolvable_deny_fails_closed_under_an_address_allow() {
		use std::net::SocketAddr;

		let allow = ["*".to_string(), "127.0.0.1".to_string()];
		let deny = NetDeny::parse(&["no-such-host.invalid".to_string()]).unwrap();
		let policy = NetPolicy::parse(&allow, deny).unwrap().for_invocation().await;
		let loopback: SocketAddr = "127.0.0.1:8080".parse().unwrap();
		assert!(!policy.allows_socket_addr(&loopback));
		assert!(!policy.allows_socket_addr(&"1.1.1.1:443".parse().unwrap()));
		// An address the shim vended for a checked name is still reachable.
		let public: IpAddr = "1.1.1.1".parse().unwrap();
		assert_eq!(policy.accept_resolved("one.example", vec![public]), vec![public]);
		assert!(policy.allows_socket_addr(&SocketAddr::new(public, 443)));
	}

	/// Without an allow rule that matches addresses, nothing is resolved: an
	/// address is reachable only through a name the DNS shim checked.
	#[tokio::test(flavor = "current_thread")]
	async fn a_deny_is_not_resolved_without_an_address_allow() {
		let deny = NetDeny::parse(&["no-such-host.invalid".to_string()]).unwrap();
		let policy = NetPolicy::parse(&["api.example.com".to_string()], deny)
			.unwrap()
			.for_invocation()
			.await;
		assert!(matches!(&policy.denied_addrs, DeniedAddrs::Resolved(a) if a.is_empty()));
	}

	fn policy(entries: &[&str]) -> Arc<NetPolicy> {
		NetPolicy::parse(
			&entries.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
			NetDeny::Nothing,
		)
		.unwrap()
	}

	fn policy_denying(entries: &[&str], deny: &[&str]) -> Arc<NetPolicy> {
		let deny = NetDeny::parse(&deny.iter().map(|s| s.to_string()).collect::<Vec<_>>()).unwrap();
		NetPolicy::parse(&entries.iter().map(|s| s.to_string()).collect::<Vec<_>>(), deny).unwrap()
	}

	#[test]
	fn parses_cidr_ip_and_host() {
		assert_eq!(parse_one("10.0.0.0/8").unwrap(), NetRule::Net("10.0.0.0/8".parse().unwrap()));
		assert_eq!(
			parse_one("1.2.3.4").unwrap(),
			NetRule::Net(IpNet::from(IpAddr::from([1, 2, 3, 4])))
		);
		assert_eq!(
			parse_one("1.2.3.4:443").unwrap(),
			NetRule::IpPort(IpAddr::from([1, 2, 3, 4]), 443)
		);
		assert_eq!(
			parse_one("API.Example.com:8080").unwrap(),
			NetRule::Host {
				host: "api.example.com".to_string(),
				port: Some(8080),
			}
		);
	}

	#[test]
	fn name_matching_is_exact_and_case_insensitive() {
		let p = policy(&["api.example.com"]);
		assert!(p.allows_name("api.example.com"));
		assert!(p.allows_name("API.EXAMPLE.COM"));
		assert!(!p.allows_name("evil.example.com"));
		// Not a suffix match.
		assert!(!p.allows_name("sub.api.example.com"));
	}

	#[test]
	fn literal_addresses_match_ip_rules() {
		let p = policy(&["127.0.0.1"]);
		assert!(p.allows_name("127.0.0.1"));
		assert!(!p.allows_name("127.0.0.2"));
	}

	#[test]
	fn a_hostname_alone_cannot_reach_loopback() {
		let p = policy(&["api.example.com"]);
		// The name is allowed, but the address it produced is special and no
		// explicit rule covers it.
		assert!(
			p.accept_resolved("api.example.com", vec![IpAddr::from([127, 0, 0, 1])]).is_empty()
		);
		// Nothing was vended, so the connect is refused too.
		assert!(!p.allows_socket_addr(&"127.0.0.1:80".parse().unwrap()));
	}

	/// The special-use predicate canonicalises, so the mapped spelling of a
	/// loopback address is not a way past it.
	#[test]
	fn an_ipv4_mapped_special_address_is_still_special() {
		let p = policy(&["api.example.com"]);
		let mapped: IpAddr = "::ffff:127.0.0.1".parse().unwrap();
		assert!(p.accept_resolved("api.example.com", vec![mapped]).is_empty());
	}

	#[test]
	fn an_explicit_rule_readmits_a_special_address() {
		let p = policy(&["localhost", "127.0.0.1"]);
		let kept = p.accept_resolved("localhost", vec![IpAddr::from([127, 0, 0, 1])]);
		assert_eq!(kept, vec![IpAddr::from([127, 0, 0, 1])]);
	}

	#[test]
	fn link_local_metadata_is_refused() {
		let p = policy(&["metadata.example"]);
		assert!(
			p.accept_resolved("metadata.example", vec![IpAddr::from([169, 254, 169, 254])])
				.is_empty()
		);
	}

	#[test]
	fn connect_is_allowed_only_after_the_name_vended_the_address() {
		let p = policy(&["api.example.com"]);
		let addr: SocketAddr = "93.184.216.34:443".parse().unwrap();
		assert!(!p.allows_socket_addr(&addr), "not vended yet");
		p.accept_resolved("api.example.com", vec![addr.ip()]);
		assert!(p.allows_socket_addr(&addr), "vended, so recognised");
	}

	#[test]
	fn a_hostname_port_pins_the_connect_port() {
		let p = policy(&["api.example.com:443"]);
		let ip = IpAddr::from([93, 184, 216, 34]);
		p.accept_resolved("api.example.com", vec![ip]);
		assert!(p.allows_socket_addr(&SocketAddr::new(ip, 443)));
		assert!(!p.allows_socket_addr(&SocketAddr::new(ip, 8080)));
	}

	/// One rule's address must not satisfy another rule's port constraint: the
	/// vending rule is what authorises the connect, not the set of all rules.
	#[test]
	fn a_port_is_pinned_to_the_rule_whose_name_vended_the_address() {
		let p = policy(&["a.example.com:443", "b.example.com:8080"]);
		let ip = IpAddr::from([93, 184, 216, 34]);
		p.accept_resolved("b.example.com", vec![ip]);
		assert!(p.allows_socket_addr(&SocketAddr::new(ip, 8080)), "b's own port");
		assert!(
			!p.allows_socket_addr(&SocketAddr::new(ip, 443)),
			"443 was authorised for a.example.com, not for b's address"
		);
	}

	/// A port-less rule opens every port on the addresses *it* vended, and on
	/// no others.
	#[test]
	fn a_port_less_rule_does_not_open_another_rules_address() {
		let p = policy(&["a.example.com", "b.example.com:8080"]);
		let ip = IpAddr::from([93, 184, 216, 34]);
		p.accept_resolved("b.example.com", vec![ip]);
		assert!(
			!p.allows_socket_addr(&SocketAddr::new(ip, 22)),
			"port 22 reached through the port-less a.example.com rule"
		);
		assert!(p.allows_socket_addr(&SocketAddr::new(ip, 8080)), "b's own port still works");
	}

	/// Two rules naming the same host are both bound to what it vends, so the
	/// looser of them still applies.
	#[test]
	fn every_rule_for_the_resolved_name_is_bound_to_it() {
		let p = policy(&["api.example.com:443", "api.example.com:8443"]);
		let ip = IpAddr::from([93, 184, 216, 34]);
		p.accept_resolved("api.example.com", vec![ip]);
		assert!(p.allows_socket_addr(&SocketAddr::new(ip, 443)));
		assert!(p.allows_socket_addr(&SocketAddr::new(ip, 8443)));
		assert!(!p.allows_socket_addr(&SocketAddr::new(ip, 80)));
	}

	#[test]
	fn explicit_ip_rules_need_no_resolution() {
		let p = policy(&["10.0.0.0/8"]);
		assert!(p.allows_socket_addr(&"10.1.2.3:5432".parse().unwrap()));
		assert!(!p.allows_socket_addr(&"11.1.2.3:5432".parse().unwrap()));
	}

	#[test]
	fn empty_policy_allows_nothing() {
		let p = policy(&[]);
		assert!(p.is_empty());
		assert!(!p.allows_name("api.example.com"));
		assert!(!p.allows_socket_addr(&"1.2.3.4:80".parse().unwrap()));
	}

	/// An expired entry stops authorising the connect, so authorisation
	/// follows the record rather than outliving it.
	#[test]
	fn a_vended_address_expires() {
		let p = policy(&["api.example.com"]);
		let ip = IpAddr::from([93, 184, 216, 34]);
		p.accept_resolved("api.example.com", vec![ip]);
		assert!(p.allows_socket_addr(&SocketAddr::new(ip, 443)));

		// Age the entry past its TTL without waiting for it.
		{
			let mut table = p.vended.lock();
			let entry = table.by_ip.get_mut(&ip).expect("vended");
			entry.at = Instant::now() - VENDED_TTL - Duration::from_secs(1);
		}
		assert!(!p.allows_socket_addr(&SocketAddr::new(ip, 443)), "expired, so no longer allowed");
	}

	/// A name that rotates through addresses cannot grow the set without
	/// bound: the cap holds however many it returns.
	#[test]
	fn the_vended_set_is_capped() {
		let p = policy(&["rotate.example.com"]);
		for i in 0..(MAX_VENDED as u32 + 500) {
			// 93.184.0.0/16 upwards, all ordinary public addresses.
			let octets = (93u32 << 24) | (184 << 16) | i;
			p.accept_resolved("rotate.example.com", vec![IpAddr::from(octets.to_be_bytes())]);
		}
		let held = p.vended.lock().by_ip.len();
		assert!(held <= MAX_VENDED, "cap exceeded: {held}");
	}

	/// The operator's deny rules win over anything the module declares, at
	/// every one of the three points a decision is made.
	#[test]
	fn the_operator_deny_list_overrides_the_module() {
		let p = policy_denying(&["dns.google", "8.8.8.0/24"], &["8.8.8.8"]);
		// The address is denied outright, so resolution cannot vend it ...
		let kept = p.accept_resolved("dns.google", vec![IpAddr::from([8, 8, 8, 8])]);
		assert!(kept.is_empty(), "a denied address must not be vended");
		// ... and the connect is refused even though `8.8.8.0/24` allows it.
		assert!(!p.allows_socket_addr(&"8.8.8.8:53".parse().unwrap()));
		// A sibling address in the same allowed range is unaffected.
		assert!(p.allows_socket_addr(&"8.8.8.9:53".parse().unwrap()));
	}

	#[test]
	fn a_denied_name_cannot_be_resolved() {
		let p = policy_denying(&["api.example.com"], &["api.example.com"]);
		assert!(!p.allows_name("api.example.com"));
	}

	/// A port-bearing deny rule forbids that port, not the name: the engine's
	/// own resolver reads it the same way.
	#[test]
	fn a_port_bearing_deny_rule_does_not_deny_the_name() {
		let p = policy_denying(&["db.example.com"], &["1.2.3.4:6379"]);
		assert!(p.allows_name("db.example.com"));
		let ip = IpAddr::from([1, 2, 3, 4]);
		p.accept_resolved("db.example.com", vec![ip]);
		assert!(!p.allows_socket_addr(&SocketAddr::new(ip, 6379)), "the denied port");
		assert!(p.allows_socket_addr(&SocketAddr::new(ip, 443)), "every other port");
	}

	#[test]
	fn denying_everything_allows_nothing() {
		let p = NetPolicy::parse(&["api.example.com".to_string()], NetDeny::Everything).unwrap();
		assert!(!p.allows_name("api.example.com"));
		assert!(p.accept_resolved("api.example.com", vec![IpAddr::from([1, 2, 3, 4])]).is_empty());
		assert!(!p.allows_socket_addr(&"1.2.3.4:80".parse().unwrap()));
	}

	/// A grant on one port must not become a grant on every port because a
	/// hostname also resolved to that address. The IP rule readmitted the
	/// special-use address; the IP rule's port is therefore what applies.
	#[test]
	fn a_port_scoped_ip_grant_is_not_widened_by_a_name_resolving_to_it() {
		let p = policy(&["evil.example", "169.254.169.254:80"]);
		let metadata = IpAddr::from([169, 254, 169, 254]);
		// The address survives because the operator named it explicitly.
		assert_eq!(p.accept_resolved("evil.example", vec![metadata]), vec![metadata]);
		// ... on the one port they named, and no other.
		assert!(p.allows_socket_addr(&SocketAddr::new(metadata, 80)), "the granted port");
		assert!(!p.allows_socket_addr(&SocketAddr::new(metadata, 8080)));
		assert!(!p.allows_socket_addr(&SocketAddr::new(metadata, 22)));
	}

	/// A 6to4 address routes to the IPv4 host it carries, so allowing the
	/// public IPv6 internet must not reach an RFC 1918 address through it.
	/// Nothing resolves here: a literal connect never passes the DNS filter.
	#[test]
	fn a_transition_address_is_judged_on_what_it_carries() {
		let p = policy(&["2000::/3"]);
		// 2002:0a01:0203::/48 is the 6to4 form of 10.1.2.3.
		let six_to_four: IpAddr = "2002:a01:203::".parse().unwrap();
		assert!(!p.allows_socket_addr(&SocketAddr::new(six_to_four, 22)));
		// An ordinary IPv6 address in the same range is unaffected.
		let public: IpAddr = "2606:2800:220:1:248:1893:25c8:1946".parse().unwrap();
		assert!(p.allows_socket_addr(&SocketAddr::new(public, 443)));
	}

	/// The operator's deny rules are read against the destination too, so a
	/// transition spelling is not a way around `--deny-net 10.0.0.0/8`.
	#[test]
	fn a_deny_rule_follows_the_carried_destination() {
		let p = policy_denying(&["2000::/3", "64:ff9b::/96"], &["10.0.0.0/8"]);
		for spelling in ["2002:a01:203::", "64:ff9b::a01:203"] {
			let ip: IpAddr = spelling.parse().unwrap();
			assert!(!p.allows_socket_addr(&SocketAddr::new(ip, 22)), "{spelling}");
		}
	}

	/// Naming the destination explicitly is how a transition address is
	/// reached, and it still works.
	#[test]
	fn naming_the_destination_admits_the_transition_spelling() {
		let p = policy(&["10.0.0.0/8"]);
		let six_to_four: IpAddr = "2002:a01:203::".parse().unwrap();
		assert!(p.allows_socket_addr(&SocketAddr::new(six_to_four, 5432)));
	}

	/// A port-bearing deny rule leaves the name resolvable and refuses that
	/// port on everything it resolved to. `http::*` reads the same rule the
	/// same way, so a guest socket must not be the looser of the two.
	#[test]
	fn a_port_bearing_deny_rule_refuses_that_port_on_the_resolved_address() {
		let p = policy_denying(&["example.com"], &["example.com:443"]);
		assert!(p.allows_name("example.com"), "the rule names a port, not the host");
		let ip = IpAddr::from([93, 184, 216, 34]);
		assert_eq!(p.accept_resolved("example.com", vec![ip]), vec![ip]);
		assert!(!p.allows_socket_addr(&SocketAddr::new(ip, 443)), "the denied port");
		assert!(p.allows_socket_addr(&SocketAddr::new(ip, 80)), "every other port");
	}

	/// Two allowed names routinely share one CDN address. Resolving the second
	/// must not revoke what the first authorised.
	#[test]
	fn two_names_sharing_an_address_do_not_revoke_each_other() {
		let p = policy(&["a.example.com", "b.example.com:8080"]);
		let shared = IpAddr::from([93, 184, 216, 34]);
		p.accept_resolved("a.example.com", vec![shared]);
		p.accept_resolved("b.example.com", vec![shared]);
		assert!(p.allows_socket_addr(&SocketAddr::new(shared, 443)), "a's port-less grant");
		assert!(p.allows_socket_addr(&SocketAddr::new(shared, 8080)), "b's pinned port");
	}

	/// `allow_net` entries are punycoded when parsed, so the guest asking for
	/// the Unicode spelling of the very name the module allowed has to match.
	#[test]
	fn an_internationalised_name_matches_either_spelling() {
		let p = policy(&["bücher.example"]);
		assert!(p.allows_name("bücher.example"));
		assert!(p.allows_name("xn--bcher-kva.example"));
		assert!(!p.allows_name("bucher.example"));
	}

	/// A port the operator denied on a name stays denied at an address the
	/// name resolved to, even when that address is readmitted by an explicit
	/// IP rule rather than vended by the name.
	#[test]
	fn a_port_bearing_deny_rule_reaches_a_readmitted_special_address() {
		let p = policy_denying(&["localhost", "127.0.0.1"], &["localhost:8080"]);
		let loopback = IpAddr::from([127, 0, 0, 1]);
		assert!(p.allows_name("localhost"), "the rule names a port, not the host");
		assert_eq!(p.accept_resolved("localhost", vec![loopback]), vec![loopback]);
		assert!(!p.allows_socket_addr(&SocketAddr::new(loopback, 8080)), "the denied port");
		assert!(p.allows_socket_addr(&SocketAddr::new(loopback, 9000)), "every other port");
	}
}
