use std::fmt;
use std::hash::Hash;
use std::net::IpAddr;
use std::{collections::HashSet, sync::Arc};

use ipnet::IpNet;
use url::Url;

pub trait Target<Item: ?Sized = Self> {
	fn matches(&self, elem: &Item) -> bool;
}

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
#[non_exhaustive]
pub struct FuncTarget(pub String, pub Option<String>);

impl fmt::Display for FuncTarget {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match &self.1 {
			Some(name) => write!(f, "{}:{}", self.0, name),
			None => write!(f, "{}::*", self.0),
		}
	}
}

impl Target for FuncTarget {
	fn matches(&self, elem: &FuncTarget) -> bool {
		match self {
			Self(family, Some(name)) => {
				family == &elem.0 && (elem.1.as_ref().is_some_and(|n| n == name))
			}
			Self(family, None) => family == &elem.0,
		}
	}
}

impl Target<str> for FuncTarget {
	fn matches(&self, elem: &str) -> bool {
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

impl std::error::Error for ParseFuncTargetError {}
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
		let s = s.trim();

		if s.is_empty() {
			return Err(ParseFuncTargetError::InvalidName);
		}

		if let Some(family) = s.strip_suffix("::*") {
			if family.contains("::") {
				return Err(ParseFuncTargetError::InvalidWildcardFamily);
			}

			if !family.bytes().all(|x| x.is_ascii_alphanumeric()) {
				return Err(ParseFuncTargetError::InvalidName);
			}

			return Ok(FuncTarget(family.to_string(), None));
		}

		if !s.bytes().all(|x| x.is_ascii_alphanumeric() || x == b':') {
			return Err(ParseFuncTargetError::InvalidName);
		}

		if let Some((first, rest)) = s.split_once("::") {
			Ok(FuncTarget(first.to_string(), Some(rest.to_string())))
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

#[derive(Debug)]
pub struct ParseNetTargetError;

impl std::error::Error for ParseNetTargetError {}
impl fmt::Display for ParseNetTargetError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "The provided network target is not a valid host, ip address or ip network")
	}
}

impl std::str::FromStr for NetTarget {
	type Err = ParseNetTargetError;

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

		Err(ParseNetTargetError)
	}
}

#[derive(Debug, Clone, Eq, PartialEq)]
#[non_exhaustive]
pub enum Targets<T: Hash + Clone + Eq + PartialEq> {
	None,
	Some(HashSet<T>),
	All,
}

impl<T: Hash + Clone + Eq + PartialEq + fmt::Debug + fmt::Display> Targets<T> {
	fn matches<S>(&self, elem: &S) -> bool
	where
		S: ?Sized,
		T: Target<S>,
	{
		match self {
			Self::None => false,
			Self::All => true,
			Self::Some(targets) => targets.iter().any(|t| t.matches(elem)),
		}
	}

	fn add(&self, elem: T) -> Targets<T> {
		match self {
			// If all targets are already added, we don't need to do anything for allows
			// For denies, we want to add it specifically, which is handled outside of this method
			// This match arm should only be reached when allowing a specific target
			Self::All => Self::All,
			// If no targets are added, we add the provided target
			// This works the same for allows and denies
			Self::None => {
				let mut targets = HashSet::new();
				targets.insert(elem);
				Self::Some(targets)
			}
			// If some targets are addedd, we add the provided target
			// This works the same for allows and denies
			// TODO(PR): Consider checking if none already match
			Self::Some(targets) => {
				let mut new = targets.clone();
				new.insert(elem);
				Self::Some(new)
			}
		}
	}
}

impl<T: Target + Hash + Clone + Eq + PartialEq + fmt::Display> fmt::Display for Targets<T> {
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

	pub fn none() -> Self {
		Self {
			scripting: false,
			guest_access: false,
			live_query_notifications: false,

			allow_funcs: Arc::new(Targets::None),
			deny_funcs: Arc::new(Targets::None),
			allow_net: Arc::new(Targets::None),
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

	pub fn allow_network_target(mut self, target: NetTarget) -> Self {
		// We check if the target is generically denied or explicitly denied
		// If it is not denied, we add it to allowed
		// If it is generically denied, we add it to allowed
		// If it is explicitly denied, we DO NOT add it to allowed
		match *self.deny_net {
			Targets::Some(_) => {
				if self.deny_net.matches(&target) {
					// If already explicitly denied, it cannot be allowed
					// TODO(PR): Print warning
					return self;
				}
			}
			Targets::All => {
				// If generically denied, an allow can overwrite it
				// We are no longer denying all, we rely on the default deny policy
				self.deny_net = Arc::new(Targets::None);
			}
			Targets::None => {}
		}

		// We add the specific targets provided to the allowed list
		self.allow_net = Arc::new(self.allow_net.add(target));
		self
	}

	pub fn deny_network_target(mut self, target: NetTarget) -> Self {
		// We check if we are already generically or explicitly denying the target
		// If it is not denied, we add it to denied
		// If it is generically denied, we add it to denied, as explicit denies have priority
		// If it is explicitly denied, we DO NOT add it to denied, as it already is
		match *self.deny_net {
			Targets::Some(_) => {
				// If already explicitly denied, we do not need to do anything
				// If not already explicitly denied, we add it to denied
				if !self.deny_net.matches(&target) {
					self.deny_net = Arc::new(self.deny_net.add(target));
				}
			}
			Targets::All => {
				// If generically denied, we add it to denied
				// We are no longer denying all, we rely on the default deny policy
				self.deny_net = Arc::new(Targets::<NetTarget>::Some([target].into()));
			}
			Targets::None => {
				// If nothing is denied, we add it to denied
				self.deny_net = Arc::new(Targets::<NetTarget>::Some([target].into()));
			}
		};

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
			assert!(caps.allows_function_name("http::get"));
			assert!(caps.allows_function_name("http::post"));
		}

		// When all funcs are allowed and denied at the same time
		{
			let caps = Capabilities::default()
				.with_functions(Targets::<FuncTarget>::All)
				.without_functions(Targets::<FuncTarget>::All);
			assert!(!caps.allows_function_name("http::get"));
			assert!(!caps.allows_function_name("http::post"));
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
			assert!(caps.allows_function_name("http::get"));
			assert!(caps.allows_function_name("http::put"));
			assert!(!caps.allows_function_name("http::post"));
		}
	}

	#[test]
	fn test_capabilities_with_order() {
		// Allow only one address
		{
			let caps = Capabilities::default()
				.allow_network_target(NetTarget::from_str("192.168.1.1").unwrap());

			// Only that specific address is allowed
			assert!(caps.allows_network_target(&NetTarget::from_str("192.168.1.1").unwrap()));
			assert!(!caps.allows_network_target(&NetTarget::from_str("192.168.1.2").unwrap()));
			assert!(!caps.allows_network_target(&NetTarget::from_str("192.168.1.100").unwrap()));
		}
		// Allow a subnet, deny a subset of it
		{
			let caps = Capabilities::default()
				.allow_network_target(NetTarget::from_str("192.168.1.1/24").unwrap())
				.deny_network_target(NetTarget::from_str("192.168.1.1/28").unwrap());

			// The subset of the allowed subnet is denied, the rest of the subnet is allowed
			assert!(caps.allows_network_target(&NetTarget::from_str("192.168.1.100").unwrap()));
			assert!(caps.allows_network_target(&NetTarget::from_str("192.168.1.200").unwrap()));
			assert!(!caps.allows_network_target(&NetTarget::from_str("192.168.1.1").unwrap()));
			assert!(!caps.allows_network_target(&NetTarget::from_str("192.168.1.15").unwrap()));
		}
		// Allow a subnet, deny a subset of it, then allow a superset of it
		{
			let caps = Capabilities::default()
				.allow_network_target(NetTarget::from_str("192.168.1.1/28").unwrap())
				.deny_network_target(NetTarget::from_str("192.168.1.1/30").unwrap())
				.allow_network_target(NetTarget::from_str("192.168.1.1/24").unwrap());

			// The superset of the allowed subnet is allowed, except for the subset that is denied
			assert!(caps.allows_network_target(&NetTarget::from_str("192.168.1.4").unwrap()));
			assert!(caps.allows_network_target(&NetTarget::from_str("192.168.1.100").unwrap()));
			assert!(caps.allows_network_target(&NetTarget::from_str("192.168.1.200").unwrap()));
			assert!(!caps.allows_network_target(&NetTarget::from_str("192.168.1.1").unwrap()));
			assert!(!caps.allows_network_target(&NetTarget::from_str("192.168.1.2").unwrap()));
			assert!(!caps.allows_network_target(&NetTarget::from_str("192.168.1.3").unwrap()));
		}
		// Deny only one address
		{
			let caps = Capabilities::default()
				.deny_network_target(NetTarget::from_str("192.168.1.1").unwrap());

			// Every address is disallowed, including that one
			assert!(!caps.allows_network_target(&NetTarget::from_str("192.168.1.1").unwrap()));
			assert!(!caps.allows_network_target(&NetTarget::from_str("192.168.1.100").unwrap()));
			assert!(!caps.allows_network_target(&NetTarget::from_str("192.168.1.200").unwrap()));
		}
		// Deny a subnet, allow a subset of it
		{
			let caps = Capabilities::default()
				.deny_network_target(NetTarget::from_str("192.168.1.1/24").unwrap())
				.allow_network_target(NetTarget::from_str("192.168.1.1/28").unwrap());

			// Every address is disallowed, including the allowed subset of the denied subnet
			assert!(!caps.allows_network_target(&NetTarget::from_str("192.168.1.100").unwrap()));
			assert!(!caps.allows_network_target(&NetTarget::from_str("192.168.1.200").unwrap()));
			assert!(!caps.allows_network_target(&NetTarget::from_str("192.168.1.1").unwrap()));
			assert!(!caps.allows_network_target(&NetTarget::from_str("192.168.1.15").unwrap()));
		}
		// Deny a subnet, allow a subset of it, then deny a superset of it
		{
			let caps = Capabilities::default()
				.deny_network_target(NetTarget::from_str("192.168.1.1/28").unwrap())
				.allow_network_target(NetTarget::from_str("192.168.1.1/30").unwrap())
				.deny_network_target(NetTarget::from_str("192.168.1.1/24").unwrap());

			// Every address is disallowed, including the allowed subset of the denied subnets
			assert!(!caps.allows_network_target(&NetTarget::from_str("192.168.1.4").unwrap()));
			assert!(!caps.allows_network_target(&NetTarget::from_str("192.168.1.100").unwrap()));
			assert!(!caps.allows_network_target(&NetTarget::from_str("192.168.1.200").unwrap()));
			assert!(!caps.allows_network_target(&NetTarget::from_str("192.168.1.1").unwrap()));
			assert!(!caps.allows_network_target(&NetTarget::from_str("192.168.1.2").unwrap()));
			assert!(!caps.allows_network_target(&NetTarget::from_str("192.168.1.3").unwrap()));
		}
		// Deny all networks, then allow a single address
		{
			let caps = Capabilities::none()
				.allow_network_target(NetTarget::from_str("192.168.1.1").unwrap());

			// Only that specific address is allowed
			assert!(caps.allows_network_target(&NetTarget::from_str("192.168.1.1").unwrap()));
			assert!(!caps.allows_network_target(&NetTarget::from_str("192.168.1.2").unwrap()));
			assert!(!caps.allows_network_target(&NetTarget::from_str("192.168.1.100").unwrap()));
		}
		// Allow all networks, then deny a single address
		{
			let caps = Capabilities::all()
				.deny_network_target(NetTarget::from_str("192.168.1.1").unwrap());

			// Only that specific address is denied
			assert!(!caps.allows_network_target(&NetTarget::from_str("192.168.1.1").unwrap()));
			assert!(caps.allows_network_target(&NetTarget::from_str("192.168.1.2").unwrap()));
			assert!(caps.allows_network_target(&NetTarget::from_str("192.168.1.100").unwrap()));
		}
		// Deny all networks, then deny a single address
		{
			let caps = Capabilities::none()
				.deny_network_target(NetTarget::from_str("192.168.1.1").unwrap());

			// Every network is disallowed
			assert!(!caps.allows_network_target(&NetTarget::from_str("192.168.1.1").unwrap()));
			assert!(!caps.allows_network_target(&NetTarget::from_str("192.168.1.2").unwrap()));
			assert!(!caps.allows_network_target(&NetTarget::from_str("192.168.1.100").unwrap()));
		}
		// Allow all networks, then allow a single address
		{
			let caps = Capabilities::all()
				.allow_network_target(NetTarget::from_str("192.168.1.1").unwrap());

			// Every network is allowed
			assert!(caps.allows_network_target(&NetTarget::from_str("192.168.1.1").unwrap()));
			assert!(caps.allows_network_target(&NetTarget::from_str("192.168.1.2").unwrap()));
			assert!(caps.allows_network_target(&NetTarget::from_str("192.168.1.100").unwrap()));
		}
	}
}
