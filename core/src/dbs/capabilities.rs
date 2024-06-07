use std::fmt;
use std::hash::Hash;
use std::net::IpAddr;
use std::{collections::HashSet, sync::Arc};

use ipnet::IpNet;
use url::Url;

pub trait Target {
	type Item: ?Sized;

	fn matches(&self, elem: &Self::Item) -> bool;
}

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
#[non_exhaustive]
pub struct FuncTarget(pub String, pub Option<String>);

impl FuncTarget {
	pub fn matches_func_name(&self, name: &str) -> bool {
		if let Some(x) = self.1.as_ref() {
			let Some((f, r)) = name.split_once("::") else {
				return false;
			};

			f == self.0 && r == x
		} else {
			let f = name.split_once("::").map(|(f, _)| f).unwrap_or(name);
			f == self.0
		}
	}
}

impl fmt::Display for FuncTarget {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match &self.1 {
			Some(name) => write!(f, "{}:{}", self.0, name),
			None => write!(f, "{}::*", self.0),
		}
	}
}

impl Target for FuncTarget {
	type Item = str;

	fn matches(&self, elem: &Self::Item) -> bool {
		if let Some(x) = self.1.as_ref() {
			let Some((f, r)) = elem.split_once("::") else {
				return false;
			};

			f == self.0 && r == x
		} else {
			let f = elem.split_once("::").map(|(f, _)| f).unwrap_or(elem);
			f == self.0
		}
	}
}

#[derive(Debug, Clone)]
pub enum ParseFuncTargetError {
	InvalidWildcardFamily,
	InvalidName,
}

impl fmt::Display for ParseFuncTargetError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match *self {
			ParseFuncTargetError::InvalidName => {
				write!(f, "invalid function target name")
			}
			ParseFuncTargetError::InvalidWildcardFamily => {
				write!(
					f,
					"invalid function target wildcard family, only first part of function can be wildcarded"
				)
			}
		}
	}
}

impl std::str::FromStr for FuncTarget {
	type Err = ParseFuncTargetError;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		dbg!(s);
		let s = s.trim();

		if s.is_empty() {
			return Err(ParseFuncTargetError::InvalidName);
		}

		if s.bytes().any(|x| !(x.is_ascii_alphanumeric() || x == b':')) {
			return Err(ParseFuncTargetError::InvalidName);
		}

		if let Some(s) = s.strip_suffix("::*") {
			if s.contains("::") {
				return Err(ParseFuncTargetError::InvalidWildcardFamily);
			}

			return Ok(FuncTarget(s.to_string(), None));
		}

		if let Some((first, rest)) = s.split_once("::") {
			let rest = (!rest.is_empty()).then(|| rest.to_string());
			Ok(FuncTarget(first.to_string(), rest))
		} else {
			Ok(FuncTarget(s.to_string(), None))
		}
	}
}

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
#[non_exhaustive]
pub enum NetTarget {
	Host(url::Host<String>, Option<u16>),
	IPNet(ipnet::IpNet),
}

// impl display
impl fmt::Display for NetTarget {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::Host(host, Some(port)) => write!(f, "{}:{}", host, port),
			Self::Host(host, None) => write!(f, "{}", host),
			Self::IPNet(ipnet) => write!(f, "{}", ipnet),
		}
	}
}

impl Target for NetTarget {
	type Item = Self;

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
#[non_exhaustive]
pub enum Targets<T: Target + Hash + Eq + PartialEq> {
	None,
	Some(HashSet<T>),
	All,
}

impl<T: Target + Hash + Eq + PartialEq + fmt::Debug + fmt::Display> Targets<T> {
	fn matches(&self, elem: &T::Item) -> bool {
		match self {
			Self::None => false,
			Self::All => true,
			Self::Some(targets) => targets.iter().any(|t| t.matches(elem)),
		}
	}
}

impl<T: Target + Hash + Eq + PartialEq + fmt::Display> fmt::Display for Targets<T> {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::None => write!(f, "none"),
			Self::All => write!(f, "all"),
			Self::Some(targets) => {
				let targets =
					targets.iter().map(|t| t.to_string()).collect::<Vec<String>>().join(", ");
				write!(f, "{}", targets)
			}
		}
	}
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct Capabilities {
	scripting: bool,
	guest_access: bool,
	live_query_notifications: bool,

	allow_funcs: Arc<Targets<FuncTarget>>,
	deny_funcs: Arc<Targets<FuncTarget>>,
	allow_net: Arc<Targets<NetTarget>>,
	deny_net: Arc<Targets<NetTarget>>,
}

impl fmt::Display for Capabilities {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(
            f,
            "scripting={}, guest_access={}, live_query_notifications={}, allow_funcs={}, deny_funcs={}, allow_net={}, deny_net={}",
            self.scripting, self.guest_access, self.live_query_notifications, self.allow_funcs, self.deny_funcs, self.allow_net, self.deny_net
        )
	}
}

impl Default for Capabilities {
	fn default() -> Self {
		Self {
			scripting: false,
			guest_access: false,
			live_query_notifications: true,

			allow_funcs: Arc::new(Targets::All),
			deny_funcs: Arc::new(Targets::None),
			allow_net: Arc::new(Targets::None),
			deny_net: Arc::new(Targets::None),
		}
	}
}

impl Capabilities {
	pub fn all() -> Self {
		Self {
			scripting: true,
			guest_access: true,
			live_query_notifications: true,

			allow_funcs: Arc::new(Targets::All),
			deny_funcs: Arc::new(Targets::None),
			allow_net: Arc::new(Targets::All),
			deny_net: Arc::new(Targets::None),
		}
	}

	pub fn with_scripting(mut self, scripting: bool) -> Self {
		self.scripting = scripting;
		self
	}

	pub fn with_guest_access(mut self, guest_access: bool) -> Self {
		self.guest_access = guest_access;
		self
	}

	pub fn with_live_query_notifications(mut self, live_query_notifications: bool) -> Self {
		self.live_query_notifications = live_query_notifications;
		self
	}

	pub fn with_functions(mut self, allow_funcs: Targets<FuncTarget>) -> Self {
		self.allow_funcs = Arc::new(allow_funcs);
		self
	}

	pub fn without_functions(mut self, deny_funcs: Targets<FuncTarget>) -> Self {
		self.deny_funcs = Arc::new(deny_funcs);
		self
	}

	pub fn with_network_targets(mut self, allow_net: Targets<NetTarget>) -> Self {
		self.allow_net = Arc::new(allow_net);
		self
	}

	pub fn without_network_targets(mut self, deny_net: Targets<NetTarget>) -> Self {
		self.deny_net = Arc::new(deny_net);
		self
	}

	pub fn allows_scripting(&self) -> bool {
		self.scripting
	}

	pub fn allows_guest_access(&self) -> bool {
		self.guest_access
	}

	pub fn allows_live_query_notifications(&self) -> bool {
		self.live_query_notifications
	}

	// function is public API so we can't remove it, but you should prefer allows_function_name
	pub fn allows_function(&self, target: &FuncTarget) -> bool {
		if let Some(x) = target.1.as_ref() {
			let target = format!("{}::{}", target.0, x);
			self.allow_funcs.matches(&target) && !self.deny_funcs.matches(&target)
		} else {
			self.allow_funcs.matches(&target.0) && !self.deny_funcs.matches(&target.0)
		}
	}

	// doc hidden so we don't extend api in the library.
	#[doc(hidden)]
	pub fn allows_function_name(&self, target: &str) -> bool {
		self.allow_funcs.matches(target) && !self.deny_funcs.matches(target)
	}

	pub fn allows_network_target(&self, target: &NetTarget) -> bool {
		self.allow_net.matches(target) && !self.deny_net.matches(target)
	}
}

#[cfg(test)]
mod tests {
	use std::str::FromStr;
	use test_log::test;

	use super::*;

	#[test]
	fn test_invalid_func_target() {
		FuncTarget::from_str("te::*st").unwrap_err();
		FuncTarget::from_str("\0::st").unwrap_err();
		FuncTarget::from_str("").unwrap_err();
		FuncTarget::from_str("❤️").unwrap_err();
	}

	#[test]
	fn test_func_target() {
		assert!(FuncTarget::from_str("test").unwrap().matches("test"));
		assert!(!FuncTarget::from_str("test").unwrap().matches("test2"));

		assert!(!FuncTarget::from_str("test::").unwrap().matches("test"));

		assert!(FuncTarget::from_str("test::*").unwrap().matches("test::name"));
		assert!(!FuncTarget::from_str("test::*").unwrap().matches("test2::name"));

		assert!(FuncTarget::from_str("test::name").unwrap().matches("test::name"));
		assert!(!FuncTarget::from_str("test::name").unwrap().matches("test::name2"));
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
		assert!(Targets::<FuncTarget>::All.matches("http::get"));
		assert!(!Targets::<NetTarget>::None.matches(&NetTarget::from_str("example.com").unwrap()));
		assert!(!Targets::<FuncTarget>::None.matches("http::get"));
		assert!(Targets::<NetTarget>::Some([NetTarget::from_str("example.com").unwrap()].into())
			.matches(&NetTarget::from_str("example.com").unwrap()));
		assert!(!Targets::<NetTarget>::Some([NetTarget::from_str("example.com").unwrap()].into())
			.matches(&NetTarget::from_str("www.example.com").unwrap()));
		assert!(Targets::<FuncTarget>::Some([FuncTarget::from_str("http::get").unwrap()].into())
			.matches("http::get"));
		assert!(!Targets::<FuncTarget>::Some([FuncTarget::from_str("http::get").unwrap()].into())
			.matches("http::post"));
	}

	#[test]
	fn test_capabilities() {
		// When scripting is allowed
		{
			let caps = Capabilities::default().with_scripting(true);
			assert!(caps.allows_scripting());
		}

		// When scripting is denied
		{
			let caps = Capabilities::default().with_scripting(false);
			assert!(!caps.allows_scripting());
		}

		// When guest access is allowed
		{
			let caps = Capabilities::default().with_guest_access(true);
			assert!(caps.allows_guest_access());
		}

		// When guest access is denied
		{
			let caps = Capabilities::default().with_guest_access(false);
			assert!(!caps.allows_guest_access());
		}

		// When live query notifications are allowed
		{
			let cap = Capabilities::default().with_live_query_notifications(true);
			assert!(cap.allows_live_query_notifications());
		}

		// When live query notifications are disabled
		{
			let cap = Capabilities::default().with_live_query_notifications(false);
			assert!(!cap.allows_live_query_notifications());
		}

		// When all nets are allowed
		{
			let caps = Capabilities::default()
				.with_network_targets(Targets::<NetTarget>::All)
				.without_network_targets(Targets::<NetTarget>::None);
			assert!(caps.allows_network_target(&NetTarget::from_str("example.com").unwrap()));
			assert!(caps.allows_network_target(&NetTarget::from_str("example.com:80").unwrap()));
		}

		// When all nets are allowed and denied at the same time
		{
			let caps = Capabilities::default()
				.with_network_targets(Targets::<NetTarget>::All)
				.without_network_targets(Targets::<NetTarget>::All);
			assert!(!caps.allows_network_target(&NetTarget::from_str("example.com").unwrap()));
			assert!(!caps.allows_network_target(&NetTarget::from_str("example.com:80").unwrap()));
		}

		// When some nets are allowed and some are denied, deny overrides the allow rules
		{
			let caps = Capabilities::default()
				.with_network_targets(Targets::<NetTarget>::Some(
					[NetTarget::from_str("example.com").unwrap()].into(),
				))
				.without_network_targets(Targets::<NetTarget>::Some(
					[NetTarget::from_str("example.com:80").unwrap()].into(),
				));
			assert!(caps.allows_network_target(&NetTarget::from_str("example.com").unwrap()));
			assert!(caps.allows_network_target(&NetTarget::from_str("example.com:443").unwrap()));
			assert!(!caps.allows_network_target(&NetTarget::from_str("example.com:80").unwrap()));
		}

		// When all funcs are allowed
		{
			let caps = Capabilities::default()
				.with_functions(Targets::<FuncTarget>::All)
				.without_functions(Targets::<FuncTarget>::None);
			assert!(caps.allows_function(&FuncTarget::from_str("http::get").unwrap()));
			assert!(caps.allows_function(&FuncTarget::from_str("http::post").unwrap()));
		}

		// When all funcs are allowed and denied at the same time
		{
			let caps = Capabilities::default()
				.with_functions(Targets::<FuncTarget>::All)
				.without_functions(Targets::<FuncTarget>::All);
			assert!(!caps.allows_function(&FuncTarget::from_str("http::get").unwrap()));
			assert!(!caps.allows_function(&FuncTarget::from_str("http::post").unwrap()));
		}

		// When some funcs are allowed and some are denied, deny overrides the allow rules
		{
			let caps = Capabilities::default()
				.with_functions(Targets::<FuncTarget>::Some(
					[FuncTarget::from_str("http::*").unwrap()].into(),
				))
				.without_functions(Targets::<FuncTarget>::Some(
					[FuncTarget::from_str("http::post").unwrap()].into(),
				));
			assert!(caps.allows_function(&FuncTarget::from_str("http::get").unwrap()));
			assert!(caps.allows_function(&FuncTarget::from_str("http::put").unwrap()));
			assert!(!caps.allows_function(&FuncTarget::from_str("http::post").unwrap()));
		}
	}
}
