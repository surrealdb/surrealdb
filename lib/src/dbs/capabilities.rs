use std::hash::Hash;
use std::net::IpAddr;
use std::{collections::HashSet, sync::Arc};

use ipnet::IpNet;
use url::Url;

pub trait Target {
	fn matches(&self, elem: &Self) -> bool;
}

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct FuncTarget(pub String, pub Option<String>);

impl Target for FuncTarget {
	fn matches(&self, elem: &Self) -> bool {
		match self {
			Self(family, Some(name)) => {
				family == &elem.0 && (elem.1.as_ref().is_some_and(|n| n == name))
			}
			Self(family, None) => family == &elem.0,
		}
	}
}

impl std::str::FromStr for FuncTarget {
	type Err = String;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		// 'family::*' is treated as 'family'. They both match all functions in the family.
		let s = s.replace("::*", "");

		let target = match s.split_once("::") {
			Some((family, name)) => Self(family.to_string(), Some(name.to_string())),
			_ => Self(s.to_string(), None),
		};
		Ok(target)
	}
}

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub enum NetTarget {
	Host(url::Host<String>, Option<u16>),
	IPNet(ipnet::IpNet),
}

impl Target for NetTarget {
	fn matches(&self, elem: &Self) -> bool {
		match self {
			// If self contains a host and port, the elem must match both the host and port
			Self::Host(host, Some(port)) => match elem {
				Self::Host(_host, Some(_port)) => host == _host && port == _port,
				_ => false,
			},
			// If self contains a host but no port, the elem must match the host only
			Self::Host(host, None) => match elem {
				Self::Host(_host, _) => host == _host,
				_ => false,
			},
			// If self is an IPNet, it can match both an IPNet or a Host elem that contains an IPAddr
			Self::IPNet(ipnet) => match elem {
				Self::IPNet(_ipnet) => ipnet.contains(_ipnet),
				Self::Host(host, _) => match host {
					url::Host::Ipv4(ip) => ipnet.contains(&IpAddr::from(ip.to_owned())),
					url::Host::Ipv6(ip) => ipnet.contains(&IpAddr::from(ip.to_owned())),
					_ => false,
				},
			},
		}
	}
}

impl std::str::FromStr for NetTarget {
	type Err = String;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		// If it's a valid IPNet, return it
		if let Ok(ipnet) = s.parse::<IpNet>() {
			return Ok(NetTarget::IPNet(ipnet));
		}

		// If it's a valid IPAddr, return it as an IPNet
		if let Ok(ipnet) = s.parse::<IpAddr>() {
			return Ok(NetTarget::IPNet(IpNet::from(ipnet)));
		}

		// Parse the host and port parts from a string in the form of 'host' or 'host:port'
		if let Ok(url) = Url::parse(format!("http://{s}").as_str()) {
			if let Some(host) = url.host() {
				// Url::parse will return port=None if the provided port was 80 (given we are using the http scheme). Get the original port from the string.
				if let Some(Ok(port)) = s.split(':').last().map(|p| p.parse::<u16>()) {
					return Ok(NetTarget::Host(host.to_owned(), Some(port)));
				} else {
					return Ok(NetTarget::Host(host.to_owned(), None));
				}
			}
		}

		Err(format!(
			"The provided network target `{s}` is not a valid host, ip address or ip network"
		))
	}
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum Targets<T: Target + Hash + Eq + PartialEq> {
	None,
	Some(HashSet<T>),
	All,
}

impl<T: Target + Hash + Eq + PartialEq + std::fmt::Debug> Targets<T> {
	fn matches(&self, elem: &T) -> bool {
		match self {
			Self::None => false,
			Self::All => true,
			Self::Some(targets) => targets.iter().any(|t| t.matches(elem)),
		}
	}
}

/// Capabilities are used to limit what a user can do to the system.
///
/// Capabilities are split into 3 categories:
/// - Scripting: Whether or not the user can execute scripts
/// - Functions: Whether or not the user can execute certain functions
/// - Network: Whether or not the user can access certain network addresses
///
/// Capabilities can be configured globally or per context based on the Namespace/Database/Scope configuration. The default capabilities are:
/// - Scripting: true
/// - Functions: All functions are allowed
/// - Network: All network addresses are allowed
///
/// The capabilities are defined using allow/deny lists for fine-grained control.
///
/// Examples:
/// - Allow all functions: `--allow-funcs`
/// - Allow all functions except `http.*`: `--allow-funcs --deny-funcs 'http.*'`
/// - Allow all network addresses except AWS metadata endpoint: `--allow-net --deny-net='169.254.169.254'`
///
#[derive(Debug, Clone)]
pub struct Capabilities {
	scripting: bool,

	allow_funcs: Arc<Targets<FuncTarget>>,
	deny_funcs: Arc<Targets<FuncTarget>>,
	allow_net: Arc<Targets<NetTarget>>,
	deny_net: Arc<Targets<NetTarget>>,
}

impl Default for Capabilities {
	// By default, enable all capabilities
	fn default() -> Self {
		Self {
			scripting: true,

			allow_funcs: Arc::new(Targets::All),
			deny_funcs: Arc::new(Targets::None),
			allow_net: Arc::new(Targets::All),
			deny_net: Arc::new(Targets::None),
		}
	}
}

impl Capabilities {
	pub fn with_scripting(mut self, scripting: bool) -> Self {
		self.scripting = scripting;
		self
	}

	pub fn with_allow_funcs(mut self, allow_funcs: Targets<FuncTarget>) -> Self {
		self.allow_funcs = Arc::new(allow_funcs);
		self
	}

	pub fn with_deny_funcs(mut self, deny_funcs: Targets<FuncTarget>) -> Self {
		self.deny_funcs = Arc::new(deny_funcs);
		self
	}

	pub fn with_allow_net(mut self, allow_net: Targets<NetTarget>) -> Self {
		self.allow_net = Arc::new(allow_net);
		self
	}

	pub fn with_deny_net(mut self, deny_net: Targets<NetTarget>) -> Self {
		self.deny_net = Arc::new(deny_net);
		self
	}

	pub fn is_allowed_scripting(&self) -> bool {
		self.scripting
	}

	pub fn is_allowed_func(&self, target: &FuncTarget) -> bool {
		self.allow_funcs.matches(target) && !self.deny_funcs.matches(target)
	}

	pub fn is_allowed_net(&self, target: &NetTarget) -> bool {
		self.allow_net.matches(target) && !self.deny_net.matches(target)
	}
}

#[cfg(test)]
mod tests {
	use std::str::FromStr;
	use test_log::test;

	use super::*;

	#[test]
	fn test_func_target() {
		assert!(FuncTarget::from_str("test")
			.unwrap()
			.matches(&FuncTarget::from_str("test").unwrap()));
		assert!(!FuncTarget::from_str("test")
			.unwrap()
			.matches(&FuncTarget::from_str("test2").unwrap()));

		assert!(!FuncTarget::from_str("test::")
			.unwrap()
			.matches(&FuncTarget::from_str("test").unwrap()));

		assert!(FuncTarget::from_str("test::*")
			.unwrap()
			.matches(&FuncTarget::from_str("test::name").unwrap()));
		assert!(!FuncTarget::from_str("test::*")
			.unwrap()
			.matches(&FuncTarget::from_str("test2::name").unwrap()));

		assert!(FuncTarget::from_str("test::name")
			.unwrap()
			.matches(&FuncTarget::from_str("test::name").unwrap()));
		assert!(!FuncTarget::from_str("test::name")
			.unwrap()
			.matches(&FuncTarget::from_str("test::name2").unwrap()));
	}

	#[test]
	fn test_net_target() {
		// IPNet IPv4
		assert!(NetTarget::from_str("10.0.0.0/8")
			.unwrap()
			.matches(&NetTarget::from_str("10.0.1.0/24").unwrap()));
		assert!(NetTarget::from_str("10.0.0.0/8")
			.unwrap()
			.matches(&NetTarget::from_str("10.0.1.2").unwrap()));
		assert!(!NetTarget::from_str("10.0.0.0/8")
			.unwrap()
			.matches(&NetTarget::from_str("20.0.1.0/24").unwrap()));
		assert!(!NetTarget::from_str("10.0.0.0/8")
			.unwrap()
			.matches(&NetTarget::from_str("20.0.1.0").unwrap()));

		// IPNet IPv6
		assert!(NetTarget::from_str("2001:db8::1")
			.unwrap()
			.matches(&NetTarget::from_str("2001:db8::1").unwrap()));
		assert!(NetTarget::from_str("2001:db8::/32")
			.unwrap()
			.matches(&NetTarget::from_str("2001:db8::1").unwrap()));
		assert!(NetTarget::from_str("2001:db8::/32")
			.unwrap()
			.matches(&NetTarget::from_str("2001:db8:abcd:12::/64").unwrap()));
		assert!(!NetTarget::from_str("2001:db8::/32")
			.unwrap()
			.matches(&NetTarget::from_str("2001:db9::1").unwrap()));
		assert!(!NetTarget::from_str("2001:db8::/32")
			.unwrap()
			.matches(&NetTarget::from_str("2001:db9:abcd:12::1/64").unwrap()));

		// Host domain with and without port
		assert!(NetTarget::from_str("example.com")
			.unwrap()
			.matches(&NetTarget::from_str("example.com").unwrap()));
		assert!(NetTarget::from_str("example.com")
			.unwrap()
			.matches(&NetTarget::from_str("example.com:80").unwrap()));
		assert!(!NetTarget::from_str("example.com")
			.unwrap()
			.matches(&NetTarget::from_str("www.example.com").unwrap()));
		assert!(!NetTarget::from_str("example.com")
			.unwrap()
			.matches(&NetTarget::from_str("www.example.com:80").unwrap()));
		assert!(NetTarget::from_str("example.com:80")
			.unwrap()
			.matches(&NetTarget::from_str("example.com:80").unwrap()));
		assert!(!NetTarget::from_str("example.com:80")
			.unwrap()
			.matches(&NetTarget::from_str("example.com:443").unwrap()));
		assert!(!NetTarget::from_str("example.com:80")
			.unwrap()
			.matches(&NetTarget::from_str("example.com").unwrap()));

		// Host IPv4 with and without port
		assert!(
			NetTarget::from_str("127.0.0.1")
				.unwrap()
				.matches(&NetTarget::from_str("127.0.0.1").unwrap()),
			"Host IPv4 without port matches itself"
		);
		assert!(
			NetTarget::from_str("127.0.0.1")
				.unwrap()
				.matches(&NetTarget::from_str("127.0.0.1:80").unwrap()),
			"Host IPv4 without port matches Host IPv4 with port"
		);
		assert!(
			NetTarget::from_str("10.0.0.0/8")
				.unwrap()
				.matches(&NetTarget::from_str("10.0.0.1:80").unwrap()),
			"IPv4 network matches Host IPv4 with port"
		);
		assert!(
			NetTarget::from_str("127.0.0.1:80")
				.unwrap()
				.matches(&NetTarget::from_str("127.0.0.1:80").unwrap()),
			"Host IPv4 with port matches itself"
		);
		assert!(
			!NetTarget::from_str("127.0.0.1:80")
				.unwrap()
				.matches(&NetTarget::from_str("127.0.0.1").unwrap()),
			"Host IPv4 with port does not match Host IPv4 without port"
		);
		assert!(
			!NetTarget::from_str("127.0.0.1:80")
				.unwrap()
				.matches(&NetTarget::from_str("127.0.0.1:443").unwrap()),
			"Host IPv4 with port does not match Host IPv4 with different port"
		);

		// Host IPv6 with and without port
		assert!(
			NetTarget::from_str("[2001:db8::1]")
				.unwrap()
				.matches(&NetTarget::from_str("[2001:db8::1]").unwrap()),
			"Host IPv6 without port matches itself"
		);
		assert!(
			NetTarget::from_str("[2001:db8::1]")
				.unwrap()
				.matches(&NetTarget::from_str("[2001:db8::1]:80").unwrap()),
			"Host IPv6 without port matches Host IPv6 with port"
		);
		assert!(
			NetTarget::from_str("2001:db8::1")
				.unwrap()
				.matches(&NetTarget::from_str("[2001:db8::1]:80").unwrap()),
			"IPv6 addr matches Host IPv6 with port"
		);
		assert!(
			NetTarget::from_str("2001:db8::/64")
				.unwrap()
				.matches(&NetTarget::from_str("[2001:db8::1]:80").unwrap()),
			"IPv6 network matches Host IPv6 with port"
		);
		assert!(
			NetTarget::from_str("[2001:db8::1]:80")
				.unwrap()
				.matches(&NetTarget::from_str("[2001:db8::1]:80").unwrap()),
			"Host IPv6 with port matches itself"
		);
		assert!(
			!NetTarget::from_str("[2001:db8::1]:80")
				.unwrap()
				.matches(&NetTarget::from_str("[2001:db8::1]").unwrap()),
			"Host IPv6 with port does not match Host IPv6 without port"
		);
		assert!(
			!NetTarget::from_str("[2001:db8::1]:80")
				.unwrap()
				.matches(&NetTarget::from_str("[2001:db8::1]:443").unwrap()),
			"Host IPv6 with port does not match Host IPv6 with different port"
		);

		// Test invalid targets
		assert!(NetTarget::from_str("exam^ple.com").is_err());
		assert!(NetTarget::from_str("example.com:80:80").is_err());
		assert!(NetTarget::from_str("11111.3.4.5").is_err());
		assert!(NetTarget::from_str("2001:db8::1/129").is_err());
		assert!(NetTarget::from_str("[2001:db8::1").is_err());
	}

	#[test]
	fn test_targets() {
		assert!(Targets::<NetTarget>::All.matches(&NetTarget::from_str("example.com").unwrap()));
		assert!(Targets::<FuncTarget>::All.matches(&FuncTarget::from_str("http::get").unwrap()));
		assert!(!Targets::<NetTarget>::None.matches(&NetTarget::from_str("example.com").unwrap()));
		assert!(!Targets::<FuncTarget>::None.matches(&FuncTarget::from_str("http::get").unwrap()));
		assert!(Targets::<NetTarget>::Some([NetTarget::from_str("example.com").unwrap()].into())
			.matches(&NetTarget::from_str("example.com").unwrap()));
		assert!(!Targets::<NetTarget>::Some([NetTarget::from_str("example.com").unwrap()].into())
			.matches(&NetTarget::from_str("www.example.com").unwrap()));
		assert!(Targets::<FuncTarget>::Some([FuncTarget::from_str("http::get").unwrap()].into())
			.matches(&FuncTarget::from_str("http::get").unwrap()));
		assert!(!Targets::<FuncTarget>::Some([FuncTarget::from_str("http::get").unwrap()].into())
			.matches(&FuncTarget::from_str("http::post").unwrap()));
	}

	#[test]
	fn test_capabilities() {
		// When scripting is allowed
		{
			let caps = Capabilities::default().with_scripting(true);
			assert!(caps.is_allowed_scripting());
		}

		// When scripting is denied
		{
			let caps = Capabilities::default().with_scripting(false);
			assert!(!caps.is_allowed_scripting());
		}

		// When all nets are allowed
		{
			let caps = Capabilities::default()
				.with_allow_net(Targets::<NetTarget>::All)
				.with_deny_net(Targets::<NetTarget>::None);
			assert!(caps.is_allowed_net(&NetTarget::from_str("example.com").unwrap()));
			assert!(caps.is_allowed_net(&NetTarget::from_str("example.com:80").unwrap()));
		}

		// When all nets are allowed and denied at the same time
		{
			let caps = Capabilities::default()
				.with_allow_net(Targets::<NetTarget>::All)
				.with_deny_net(Targets::<NetTarget>::All);
			assert!(!caps.is_allowed_net(&NetTarget::from_str("example.com").unwrap()));
			assert!(!caps.is_allowed_net(&NetTarget::from_str("example.com:80").unwrap()));
		}

		// When some nets are allowed and some are denied, deny overrides the allow rules
		{
			let caps = Capabilities::default()
				.with_allow_net(Targets::<NetTarget>::Some(
					[NetTarget::from_str("example.com").unwrap()].into(),
				))
				.with_deny_net(Targets::<NetTarget>::Some(
					[NetTarget::from_str("example.com:80").unwrap()].into(),
				));
			assert!(caps.is_allowed_net(&NetTarget::from_str("example.com").unwrap()));
			assert!(caps.is_allowed_net(&NetTarget::from_str("example.com:443").unwrap()));
			assert!(!caps.is_allowed_net(&NetTarget::from_str("example.com:80").unwrap()));
		}

		// When all funcs are allowed
		{
			let caps = Capabilities::default()
				.with_allow_funcs(Targets::<FuncTarget>::All)
				.with_deny_funcs(Targets::<FuncTarget>::None);
			assert!(caps.is_allowed_func(&FuncTarget::from_str("http::get").unwrap()));
			assert!(caps.is_allowed_func(&FuncTarget::from_str("http::post").unwrap()));
		}

		// When all funcs are allowed and denied at the same time
		{
			let caps = Capabilities::default()
				.with_allow_funcs(Targets::<FuncTarget>::All)
				.with_deny_funcs(Targets::<FuncTarget>::All);
			assert!(!caps.is_allowed_func(&FuncTarget::from_str("http::get").unwrap()));
			assert!(!caps.is_allowed_func(&FuncTarget::from_str("http::post").unwrap()));
		}

		// When some funcs are allowed and some are denied, deny overrides the allow rules
		{
			let caps = Capabilities::default()
				.with_allow_funcs(Targets::<FuncTarget>::Some(
					[FuncTarget::from_str("http::*").unwrap()].into(),
				))
				.with_deny_funcs(Targets::<FuncTarget>::Some(
					[FuncTarget::from_str("http::post").unwrap()].into(),
				));
			assert!(caps.is_allowed_func(&FuncTarget::from_str("http::get").unwrap()));
			assert!(caps.is_allowed_func(&FuncTarget::from_str("http::put").unwrap()));
			assert!(!caps.is_allowed_func(&FuncTarget::from_str("http::post").unwrap()));
		}
	}
}
