use std::fmt;
use std::hash::Hash;
use std::net::IpAddr;
use std::{collections::HashSet, sync::Arc};

use crate::rpc::method::Method;
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
pub enum ExperimentalTarget {
	RecordReferences,
	GraphQL,
}

impl fmt::Display for ExperimentalTarget {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::RecordReferences => write!(f, "record_references"),
			Self::GraphQL => write!(f, "graphql"),
		}
	}
}

impl Target for ExperimentalTarget {
	fn matches(&self, elem: &ExperimentalTarget) -> bool {
		self == elem
	}
}

impl Target<str> for ExperimentalTarget {
	fn matches(&self, elem: &str) -> bool {
		match self {
			Self::RecordReferences => elem.eq_ignore_ascii_case("record_references"),
			Self::GraphQL => elem.eq_ignore_ascii_case("graphql"),
		}
	}
}

#[derive(Debug, Clone)]
pub enum ParseExperimentalTargetError {
	InvalidName,
}

impl std::error::Error for ParseExperimentalTargetError {}
impl fmt::Display for ParseExperimentalTargetError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match *self {
			ParseExperimentalTargetError::InvalidName => {
				write!(f, "invalid experimental target name")
			}
		}
	}
}

impl std::str::FromStr for ExperimentalTarget {
	type Err = ParseExperimentalTargetError;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		match s.trim() {
			s if s.eq_ignore_ascii_case("record_references") => Ok(ExperimentalTarget::RecordReferences),
			s if s.eq_ignore_ascii_case("graphql") => Ok(ExperimentalTarget::GraphQL),
			_ => Err(ParseExperimentalTargetError::InvalidName),
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
		write!(f, "The provided network target is not a valid host name, IP address or CIDR block")
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

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct MethodTarget {
	pub method: Method,
}

// impl display
impl fmt::Display for MethodTarget {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "{}", self.method.to_str())
	}
}

impl Target for MethodTarget {
	fn matches(&self, elem: &Self) -> bool {
		self.method == elem.method
	}
}

#[derive(Debug)]
pub struct ParseMethodTargetError;

impl std::error::Error for ParseMethodTargetError {}
impl fmt::Display for ParseMethodTargetError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "The provided method target is not a valid RPC method")
	}
}

impl std::str::FromStr for MethodTarget {
	type Err = ParseMethodTargetError;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		match Method::parse(s) {
			Method::Unknown => Err(ParseMethodTargetError),
			method => Ok(MethodTarget {
				method,
			}),
		}
	}
}

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
#[non_exhaustive]
pub enum RouteTarget {
	Health,
	Export,
	Import,
	Rpc,
	Version,
	Sync,
	Sql,
	Signin,
	Signup,
	Key,
	Ml,
	GraphQL,
}

// impl display
impl fmt::Display for RouteTarget {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			RouteTarget::Health => write!(f, "health"),
			RouteTarget::Export => write!(f, "export"),
			RouteTarget::Import => write!(f, "import"),
			RouteTarget::Rpc => write!(f, "rpc"),
			RouteTarget::Version => write!(f, "version"),
			RouteTarget::Sync => write!(f, "sync"),
			RouteTarget::Sql => write!(f, "sql"),
			RouteTarget::Signin => write!(f, "signin"),
			RouteTarget::Signup => write!(f, "signup"),
			RouteTarget::Key => write!(f, "key"),
			RouteTarget::Ml => write!(f, "ml"),
			RouteTarget::GraphQL => write!(f, "graphql"),
		}
	}
}

impl Target for RouteTarget {
	fn matches(&self, elem: &Self) -> bool {
		*self == *elem
	}
}

#[derive(Debug)]
pub struct ParseRouteTargetError;

impl std::error::Error for ParseRouteTargetError {}
impl fmt::Display for ParseRouteTargetError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "The provided route target is not a valid HTTP route")
	}
}

impl std::str::FromStr for RouteTarget {
	type Err = ParseRouteTargetError;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		match s {
			"health" => Ok(RouteTarget::Health),
			"export" => Ok(RouteTarget::Export),
			"import" => Ok(RouteTarget::Import),
			"rpc" => Ok(RouteTarget::Rpc),
			"version" => Ok(RouteTarget::Version),
			"sync" => Ok(RouteTarget::Sync),
			"sql" => Ok(RouteTarget::Sql),
			"signin" => Ok(RouteTarget::Signin),
			"signup" => Ok(RouteTarget::Signup),
			"key" => Ok(RouteTarget::Key),
			"ml" => Ok(RouteTarget::Ml),
			"graphql" => Ok(RouteTarget::GraphQL),
			_ => Err(ParseRouteTargetError),
		}
	}
}

#[derive(Debug, Clone, Eq, PartialEq)]
#[non_exhaustive]
pub enum Targets<T: Hash + Eq + PartialEq> {
	None,
	Some(HashSet<T>),
	All,
}

impl<T: Hash + Eq + PartialEq + fmt::Debug + fmt::Display> Targets<T> {
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
	allow_rpc: Arc<Targets<MethodTarget>>,
	deny_rpc: Arc<Targets<MethodTarget>>,
	allow_http: Arc<Targets<RouteTarget>>,
	deny_http: Arc<Targets<RouteTarget>>,
	allow_experimental: Arc<Targets<ExperimentalTarget>>,
	deny_experimental: Arc<Targets<ExperimentalTarget>>,
}

impl fmt::Display for Capabilities {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(
            f,
            "scripting={}, guest_access={}, live_query_notifications={}, allow_funcs={}, deny_funcs={}, allow_net={}, deny_net={}, allow_rpc={}, deny_rpc={}, allow_http={}, deny_http={}",
            self.scripting, self.guest_access, self.live_query_notifications, self.allow_funcs, self.deny_funcs, self.allow_net, self.deny_net, self.allow_rpc, self.deny_rpc, self.allow_http, self.deny_http,
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
			allow_rpc: Arc::new(Targets::All),
			deny_rpc: Arc::new(Targets::None),
			allow_http: Arc::new(Targets::All),
			deny_http: Arc::new(Targets::None),
			allow_experimental: Arc::new(Targets::None),
			deny_experimental: Arc::new(Targets::None),
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
			allow_rpc: Arc::new(Targets::All),
			deny_rpc: Arc::new(Targets::None),
			allow_http: Arc::new(Targets::All),
			deny_http: Arc::new(Targets::None),
			allow_experimental: Arc::new(Targets::All),
			deny_experimental: Arc::new(Targets::None),
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
			allow_rpc: Arc::new(Targets::None),
			deny_rpc: Arc::new(Targets::None),
			allow_http: Arc::new(Targets::None),
			deny_http: Arc::new(Targets::None),
			allow_experimental: Arc::new(Targets::None),
			deny_experimental: Arc::new(Targets::None),
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

	pub fn with_experimental(mut self, allow_experimental: Targets<ExperimentalTarget>) -> Self {
		self.allow_experimental = Arc::new(allow_experimental);
		self
	}

	pub fn without_experimental(mut self, deny_experimental: Targets<ExperimentalTarget>) -> Self {
		self.deny_experimental = Arc::new(deny_experimental);
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

	pub fn with_rpc_methods(mut self, allow_rpc: Targets<MethodTarget>) -> Self {
		self.allow_rpc = Arc::new(allow_rpc);
		self
	}

	pub fn without_rpc_methods(mut self, deny_rpc: Targets<MethodTarget>) -> Self {
		self.deny_rpc = Arc::new(deny_rpc);
		self
	}

	pub fn with_http_routes(mut self, allow_http: Targets<RouteTarget>) -> Self {
		self.allow_http = Arc::new(allow_http);
		self
	}

	pub fn without_http_routes(mut self, deny_http: Targets<RouteTarget>) -> Self {
		self.deny_http = Arc::new(deny_http);
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

	pub fn allows_experimental(&self, target: &ExperimentalTarget) -> bool {
		self.allow_experimental.matches(target) && !self.deny_experimental.matches(target)
	}

	pub fn allows_experimental_name(&self, target: &str) -> bool {
		self.allow_experimental.matches(target) && !self.deny_experimental.matches(target)
	}

	pub fn allows_network_target(&self, target: &NetTarget) -> bool {
		self.allow_net.matches(target) && !self.deny_net.matches(target)
	}

	pub fn allows_rpc_method(&self, target: &MethodTarget) -> bool {
		self.allow_rpc.matches(target) && !self.deny_rpc.matches(target)
	}

	pub fn allows_http_route(&self, target: &RouteTarget) -> bool {
		self.allow_http.matches(target) && !self.deny_http.matches(target)
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
	fn test_method_target() {
		assert!(MethodTarget::from_str("query")
			.unwrap()
			.matches(&MethodTarget::from_str("query").unwrap()));
		assert!(MethodTarget::from_str("query")
			.unwrap()
			.matches(&MethodTarget::from_str("Query").unwrap()));
		assert!(MethodTarget::from_str("query")
			.unwrap()
			.matches(&MethodTarget::from_str("QUERY").unwrap()));
		assert!(!MethodTarget::from_str("query")
			.unwrap()
			.matches(&MethodTarget::from_str("ping").unwrap()));
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

		// When all RPC methods are allowed
		{
			let caps = Capabilities::default()
				.with_rpc_methods(Targets::<MethodTarget>::All)
				.without_rpc_methods(Targets::<MethodTarget>::None);
			assert!(caps.allows_rpc_method(&MethodTarget::from_str("ping").unwrap()));
			assert!(caps.allows_rpc_method(&MethodTarget::from_str("select").unwrap()));
			assert!(caps.allows_rpc_method(&MethodTarget::from_str("query").unwrap()));
		}

		// When all RPC methods are allowed and denied at the same time
		{
			let caps = Capabilities::default()
				.with_rpc_methods(Targets::<MethodTarget>::All)
				.without_rpc_methods(Targets::<MethodTarget>::All);
			assert!(!caps.allows_rpc_method(&MethodTarget::from_str("ping").unwrap()));
			assert!(!caps.allows_rpc_method(&MethodTarget::from_str("select").unwrap()));
			assert!(!caps.allows_rpc_method(&MethodTarget::from_str("query").unwrap()));
		}

		// When some RPC methods are allowed and some are denied, deny overrides the allow rules
		{
			let caps = Capabilities::default()
				.with_rpc_methods(Targets::<MethodTarget>::Some(
					[
						MethodTarget::from_str("select").unwrap(),
						MethodTarget::from_str("create").unwrap(),
						MethodTarget::from_str("insert").unwrap(),
						MethodTarget::from_str("update").unwrap(),
						MethodTarget::from_str("query").unwrap(),
						MethodTarget::from_str("run").unwrap(),
					]
					.into(),
				))
				.without_rpc_methods(Targets::<MethodTarget>::Some(
					[
						MethodTarget::from_str("query").unwrap(),
						MethodTarget::from_str("run").unwrap(),
					]
					.into(),
				));

			assert!(caps.allows_rpc_method(&MethodTarget::from_str("select").unwrap()));
			assert!(caps.allows_rpc_method(&MethodTarget::from_str("create").unwrap()));
			assert!(caps.allows_rpc_method(&MethodTarget::from_str("insert").unwrap()));
			assert!(caps.allows_rpc_method(&MethodTarget::from_str("update").unwrap()));
			assert!(!caps.allows_rpc_method(&MethodTarget::from_str("query").unwrap()));
			assert!(!caps.allows_rpc_method(&MethodTarget::from_str("run").unwrap()));
		}

		// When all HTTP routes are allowed
		{
			let caps = Capabilities::default()
				.with_http_routes(Targets::<RouteTarget>::All)
				.without_http_routes(Targets::<RouteTarget>::None);
			assert!(caps.allows_http_route(&RouteTarget::from_str("version").unwrap()));
			assert!(caps.allows_http_route(&RouteTarget::from_str("export").unwrap()));
			assert!(caps.allows_http_route(&RouteTarget::from_str("sql").unwrap()));
		}

		// When all HTTP routes are allowed and denied at the same time
		{
			let caps = Capabilities::default()
				.with_http_routes(Targets::<RouteTarget>::All)
				.without_http_routes(Targets::<RouteTarget>::All);
			assert!(!caps.allows_http_route(&RouteTarget::from_str("version").unwrap()));
			assert!(!caps.allows_http_route(&RouteTarget::from_str("export").unwrap()));
			assert!(!caps.allows_http_route(&RouteTarget::from_str("sql").unwrap()));
		}

		// When some HTTP rotues are allowed and some are denied, deny overrides the allow rules
		{
			let caps = Capabilities::default()
				.with_http_routes(Targets::<RouteTarget>::Some(
					[
						RouteTarget::from_str("version").unwrap(),
						RouteTarget::from_str("import").unwrap(),
						RouteTarget::from_str("export").unwrap(),
						RouteTarget::from_str("key").unwrap(),
						RouteTarget::from_str("sql").unwrap(),
						RouteTarget::from_str("rpc").unwrap(),
					]
					.into(),
				))
				.without_http_routes(Targets::<RouteTarget>::Some(
					[RouteTarget::from_str("sql").unwrap(), RouteTarget::from_str("rpc").unwrap()]
						.into(),
				));

			assert!(caps.allows_http_route(&RouteTarget::from_str("version").unwrap()));
			assert!(caps.allows_http_route(&RouteTarget::from_str("import").unwrap()));
			assert!(caps.allows_http_route(&RouteTarget::from_str("export").unwrap()));
			assert!(caps.allows_http_route(&RouteTarget::from_str("key").unwrap()));
			assert!(!caps.allows_http_route(&RouteTarget::from_str("sql").unwrap()));
			assert!(!caps.allows_http_route(&RouteTarget::from_str("rpc").unwrap()));
		}
	}
}
