use std::collections::HashSet;
use std::fmt;
use std::hash::Hash;
use std::net::IpAddr;
#[cfg(all(target_family = "wasm", feature = "http"))]
use std::net::ToSocketAddrs;

use ipnet::IpNet;
#[cfg(all(not(target_family = "wasm"), feature = "http"))]
use tokio::net::lookup_host;
use url::Url;

use crate::iam::{Auth, Level};
use crate::rpc::Method;

pub trait Target<Item: ?Sized = Self> {
	fn matches(&self, elem: &Item) -> bool;
}

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct FuncTarget(pub String, pub Option<String>);

impl fmt::Display for FuncTarget {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match &self.1 {
			Some(name) => write!(f, "{}:{name}", self.0),
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
pub enum ExperimentalTarget {
	RecordReferences,
	GraphQL,
	BearerAccess,
	DefineApi,
	Files,
}

impl fmt::Display for ExperimentalTarget {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::RecordReferences => write!(f, "record_references"),
			Self::GraphQL => write!(f, "graphql"),
			Self::BearerAccess => write!(f, "bearer_access"),
			Self::DefineApi => write!(f, "define_api"),
			Self::Files => write!(f, "files"),
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
			Self::BearerAccess => elem.eq_ignore_ascii_case("bearer_access"),
			Self::DefineApi => elem.eq_ignore_ascii_case("define_api"),
			Self::Files => elem.eq_ignore_ascii_case("files"),
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
		match s.trim().to_ascii_lowercase().as_str() {
			"record_references" => Ok(ExperimentalTarget::RecordReferences),
			"graphql" => Ok(ExperimentalTarget::GraphQL),
			"bearer_access" => Ok(ExperimentalTarget::BearerAccess),
			"define_api" => Ok(ExperimentalTarget::DefineApi),
			"files" => Ok(ExperimentalTarget::Files),
			_ => Err(ParseExperimentalTargetError::InvalidName),
		}
	}
}

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub enum NetTarget {
	Host(url::Host<String>, Option<u16>),
	IPNet(IpNet),
}

#[cfg(feature = "http")]
impl NetTarget {
	/// Resolves a `NetTarget` to its associated IP address representations.
	///
	/// This function performs an asynchronous resolution of a `NetTarget` enum
	/// instance. If the `NetTarget` is of variant `Host`, it attempts to
	/// resolve the provided hostname and optional port into a list of `IPNet`
	/// values. If the port is not provided, port 80 is used by default. If the
	/// `NetTarget` is of variant `IPNet`, it simply returns an empty vector, as
	/// there is nothing to resolve.
	///
	/// # Returns
	/// - On success, this function returns a `Vec<Self>` where each resolved `NetTarget::Host` is
	///   transformed into a `NetTarget::IPNet`.
	/// - On error, it returns a `std::io::Error` indicating the issue during resolution.
	///
	/// # Variants
	/// - `NetTarget::Host(h, p)`:
	///    - Resolves the given hostname `h` with an optional port `p` (default is 80) to a list of
	///      IPs.
	///    - Each resolved IP is converted into a `NetTarget::IPNet` value.
	/// - `NetTarget::IPNet(_)`:
	///    - Returns an empty vector, as `IPNet` does not require resolution.
	///
	/// # Errors
	/// - Returns `std::io::Error` if there is an issue in the asynchronous DNS resolution process.
	///
	/// # Notes
	/// - The function uses `lookup_host` for DNS resolution, which must be awaited.
	/// - The optional port is replaced by port 80 as a default if not provided.
	#[cfg(not(target_family = "wasm"))]
	pub(crate) async fn resolve(&self) -> Result<Vec<Self>, std::io::Error> {
		match self {
			NetTarget::Host(h, p) => {
				let r = lookup_host((h.to_string(), p.unwrap_or(80)))
					.await?
					.map(|a| NetTarget::IPNet(a.ip().into()))
					.collect();
				Ok(r)
			}
			NetTarget::IPNet(_) => Ok(vec![]),
		}
	}

	#[cfg(target_family = "wasm")]
	pub(crate) fn resolve(&self) -> Result<Vec<Self>, std::io::Error> {
		match self {
			NetTarget::Host(h, p) => {
				let r = (h.to_string(), p.unwrap_or(80))
					.to_socket_addrs()?
					.map(|a| NetTarget::IPNet(a.ip().into()))
					.collect();
				Ok(r)
			}
			NetTarget::IPNet(_) => Ok(vec![]),
		}
	}
}

// impl display
impl fmt::Display for NetTarget {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::Host(host, Some(port)) => write!(f, "{host}:{port}"),
			Self::Host(host, None) => write!(f, "{host}"),
			Self::IPNet(ipnet) => write!(f, "{ipnet}"),
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
			// If self is an IPNet, it can match both an IPNet or a Host elem that contains an
			// IPAddr
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

		// Parse the host and port parts from a string in the form of 'host' or
		// 'host:port'
		if let Ok(url) = Url::parse(format!("http://{s}").as_str()) {
			if let Some(host) = url.host() {
				// Url::parse will return port=None if the provided port was 80 (given we are
				// using the http scheme). Get the original port from the string.
				if let Some(Ok(port)) = s.split(':').next_back().map(|p| p.parse::<u16>()) {
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
		match Method::parse_case_insensitive(s) {
			Method::Unknown => Err(ParseMethodTargetError),
			method => Ok(MethodTarget {
				method,
			}),
		}
	}
}

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
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
	Api,
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
			RouteTarget::Api => write!(f, "api"),
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
			"api" => Ok(RouteTarget::Api),
			_ => Err(ParseRouteTargetError),
		}
	}
}

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub enum ArbitraryQueryTarget {
	Guest,
	Record,
	System,
}

impl fmt::Display for ArbitraryQueryTarget {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::Guest => write!(f, "guest"),
			Self::Record => write!(f, "record"),
			Self::System => write!(f, "system"),
		}
	}
}

impl<'a> From<&'a Level> for ArbitraryQueryTarget {
	fn from(level: &'a Level) -> Self {
		match level {
			Level::No => ArbitraryQueryTarget::Guest,
			Level::Root => ArbitraryQueryTarget::System,
			Level::Namespace(_) => ArbitraryQueryTarget::System,
			Level::Database(_, _) => ArbitraryQueryTarget::System,
			Level::Record(_, _, _) => ArbitraryQueryTarget::Record,
		}
	}
}

impl<'a> From<&'a Auth> for ArbitraryQueryTarget {
	fn from(auth: &'a Auth) -> Self {
		auth.level().into()
	}
}

impl Target for ArbitraryQueryTarget {
	fn matches(&self, elem: &ArbitraryQueryTarget) -> bool {
		self == elem
	}
}

impl Target<str> for ArbitraryQueryTarget {
	fn matches(&self, elem: &str) -> bool {
		match self {
			Self::Guest => elem.eq_ignore_ascii_case("guest"),
			Self::Record => elem.eq_ignore_ascii_case("record"),
			Self::System => elem.eq_ignore_ascii_case("system"),
		}
	}
}

#[derive(Debug, Clone)]
pub enum ParseArbitraryQueryTargetError {
	InvalidName,
}

impl std::error::Error for ParseArbitraryQueryTargetError {}
impl fmt::Display for ParseArbitraryQueryTargetError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match *self {
			ParseArbitraryQueryTargetError::InvalidName => {
				write!(f, "invalid query target name")
			}
		}
	}
}

impl std::str::FromStr for ArbitraryQueryTarget {
	type Err = ParseArbitraryQueryTargetError;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		match s.trim().to_ascii_lowercase().as_str() {
			"guest" => Ok(ArbitraryQueryTarget::Guest),
			"record" => Ok(ArbitraryQueryTarget::Record),
			"system" => Ok(ArbitraryQueryTarget::System),
			_ => Err(ParseArbitraryQueryTargetError::InvalidName),
		}
	}
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum Targets<T: Hash + Eq + PartialEq> {
	None,
	Some(HashSet<T>),
	All,
}

impl<T: Target + Hash + Eq + PartialEq> From<T> for Targets<T> {
	fn from(t: T) -> Self {
		let mut set = HashSet::new();
		set.insert(t);
		Self::Some(set)
	}
}

impl<T: Hash + Eq + PartialEq + fmt::Debug + fmt::Display> Targets<T> {
	pub(crate) fn matches<S>(&self, elem: &S) -> bool
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
			Self::Some(targets) => write!(
				f,
				"{}",
				targets.iter().map(|t| t.to_string()).collect::<Vec<String>>().join(", ")
			),
		}
	}
}

#[derive(Debug, Clone)]
pub struct Capabilities {
	scripting: bool,
	guest_access: bool,
	live_query_notifications: bool,

	allow_funcs: Targets<FuncTarget>,
	deny_funcs: Targets<FuncTarget>,
	allow_net: Targets<NetTarget>,
	deny_net: Targets<NetTarget>,
	allow_rpc: Targets<MethodTarget>,
	deny_rpc: Targets<MethodTarget>,
	allow_http: Targets<RouteTarget>,
	deny_http: Targets<RouteTarget>,
	allow_experimental: Targets<ExperimentalTarget>,
	deny_experimental: Targets<ExperimentalTarget>,
	allow_arbitrary_query: Targets<ArbitraryQueryTarget>,
	deny_arbitrary_query: Targets<ArbitraryQueryTarget>,
}

impl fmt::Display for Capabilities {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(
			f,
			"scripting={}, guest_access={}, live_query_notifications={}, allow_funcs={}, deny_funcs={}, allow_net={}, deny_net={}, allow_rpc={}, deny_rpc={}, allow_http={}, deny_http={}, allow_experimental={}, deny_experimental={}, allow_arbitrary_query={}, deny_arbitrary_query={}",
			self.scripting,
			self.guest_access,
			self.live_query_notifications,
			self.allow_funcs,
			self.deny_funcs,
			self.allow_net,
			self.deny_net,
			self.allow_rpc,
			self.deny_rpc,
			self.allow_http,
			self.deny_http,
			self.allow_experimental,
			self.deny_experimental,
			self.allow_arbitrary_query,
			self.deny_arbitrary_query,
		)
	}
}

impl Default for Capabilities {
	fn default() -> Self {
		Self {
			scripting: false,
			guest_access: false,
			live_query_notifications: true,

			allow_funcs: Targets::All,
			deny_funcs: Targets::None,
			allow_net: Targets::None,
			deny_net: Targets::None,
			allow_rpc: Targets::All,
			deny_rpc: Targets::None,
			allow_http: Targets::All,
			deny_http: Targets::None,
			allow_experimental: Targets::None,
			deny_experimental: Targets::None,
			allow_arbitrary_query: Targets::All,
			deny_arbitrary_query: Targets::None,
		}
	}
}

impl Capabilities {
	pub fn all() -> Self {
		Self {
			scripting: true,
			guest_access: true,
			live_query_notifications: true,

			allow_funcs: Targets::All,
			deny_funcs: Targets::None,
			allow_net: Targets::All,
			deny_net: Targets::None,
			allow_rpc: Targets::All,
			deny_rpc: Targets::None,
			allow_http: Targets::All,
			deny_http: Targets::None,
			allow_experimental: Targets::None,
			deny_experimental: Targets::None,
			allow_arbitrary_query: Targets::All,
			deny_arbitrary_query: Targets::None,
		}
	}

	pub fn none() -> Self {
		Self {
			scripting: false,
			guest_access: false,
			live_query_notifications: false,

			allow_funcs: Targets::None,
			deny_funcs: Targets::None,
			allow_net: Targets::None,
			deny_net: Targets::None,
			allow_rpc: Targets::None,
			deny_rpc: Targets::None,
			allow_http: Targets::None,
			deny_http: Targets::None,
			allow_experimental: Targets::None,
			deny_experimental: Targets::None,
			allow_arbitrary_query: Targets::None,
			deny_arbitrary_query: Targets::None,
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
		self.allow_funcs = allow_funcs;
		self
	}

	pub fn allowed_functions_mut(&mut self) -> &mut Targets<FuncTarget> {
		&mut self.allow_funcs
	}

	pub fn without_functions(mut self, deny_funcs: Targets<FuncTarget>) -> Self {
		self.deny_funcs = deny_funcs;
		self
	}

	pub fn denied_functions_mut(&mut self) -> &mut Targets<FuncTarget> {
		&mut self.deny_funcs
	}

	pub fn with_experimental(mut self, allow_experimental: Targets<ExperimentalTarget>) -> Self {
		self.allow_experimental = allow_experimental;
		self
	}

	pub fn allowed_experimental_features_mut(&mut self) -> &mut Targets<ExperimentalTarget> {
		&mut self.allow_experimental
	}

	pub fn without_experimental(mut self, deny_experimental: Targets<ExperimentalTarget>) -> Self {
		self.deny_experimental = deny_experimental;
		self
	}

	pub fn denied_experimental_features_mut(&mut self) -> &mut Targets<ExperimentalTarget> {
		&mut self.deny_experimental
	}

	pub fn with_arbitrary_query(
		mut self,
		allow_arbitrary_query: Targets<ArbitraryQueryTarget>,
	) -> Self {
		self.allow_arbitrary_query = allow_arbitrary_query;
		self
	}

	pub fn without_arbitrary_query(
		mut self,
		deny_arbitrary_query: Targets<ArbitraryQueryTarget>,
	) -> Self {
		self.deny_arbitrary_query = deny_arbitrary_query;
		self
	}

	pub fn with_network_targets(mut self, allow_net: Targets<NetTarget>) -> Self {
		self.allow_net = allow_net;
		self
	}

	pub fn allowed_network_targets_mut(&mut self) -> &mut Targets<NetTarget> {
		&mut self.allow_net
	}

	pub fn without_network_targets(mut self, deny_net: Targets<NetTarget>) -> Self {
		self.deny_net = deny_net;
		self
	}

	pub fn denied_network_targets_mut(&mut self) -> &mut Targets<NetTarget> {
		&mut self.deny_net
	}

	pub fn with_rpc_methods(mut self, allow_rpc: Targets<MethodTarget>) -> Self {
		self.allow_rpc = allow_rpc;
		self
	}

	pub fn without_rpc_methods(mut self, deny_rpc: Targets<MethodTarget>) -> Self {
		self.deny_rpc = deny_rpc;
		self
	}

	pub fn with_http_routes(mut self, allow_http: Targets<RouteTarget>) -> Self {
		self.allow_http = allow_http;
		self
	}

	pub fn without_http_routes(mut self, deny_http: Targets<RouteTarget>) -> Self {
		self.deny_http = deny_http;
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

	pub fn allows_query(&self, target: &ArbitraryQueryTarget) -> bool {
		self.allow_arbitrary_query.matches(target) && !self.deny_arbitrary_query.matches(target)
	}

	pub fn allows_network_target(&self, target: &NetTarget) -> bool {
		self.allow_net.matches(target) && !self.deny_net.matches(target)
	}

	#[cfg(feature = "http")]
	pub(crate) fn matches_any_allow_net(&self, target: &NetTarget) -> bool {
		self.allow_net.matches(target)
	}

	#[cfg(feature = "http")]
	pub(crate) fn matches_any_deny_net(&self, target: &NetTarget) -> bool {
		self.deny_net.matches(target)
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
		assert!(
			NetTarget::from_str("10.0.0.0/8")
				.unwrap()
				.matches(&NetTarget::from_str("10.0.1.0/24").unwrap())
		);
		assert!(
			NetTarget::from_str("10.0.0.0/8")
				.unwrap()
				.matches(&NetTarget::from_str("10.0.1.2").unwrap())
		);
		assert!(
			!NetTarget::from_str("10.0.0.0/8")
				.unwrap()
				.matches(&NetTarget::from_str("20.0.1.0/24").unwrap())
		);
		assert!(
			!NetTarget::from_str("10.0.0.0/8")
				.unwrap()
				.matches(&NetTarget::from_str("20.0.1.0").unwrap())
		);

		// IPNet IPv6
		assert!(
			NetTarget::from_str("2001:db8::1")
				.unwrap()
				.matches(&NetTarget::from_str("2001:db8::1").unwrap())
		);
		assert!(
			NetTarget::from_str("2001:db8::/32")
				.unwrap()
				.matches(&NetTarget::from_str("2001:db8::1").unwrap())
		);
		assert!(
			NetTarget::from_str("2001:db8::/32")
				.unwrap()
				.matches(&NetTarget::from_str("2001:db8:abcd:12::/64").unwrap())
		);
		assert!(
			!NetTarget::from_str("2001:db8::/32")
				.unwrap()
				.matches(&NetTarget::from_str("2001:db9::1").unwrap())
		);
		assert!(
			!NetTarget::from_str("2001:db8::/32")
				.unwrap()
				.matches(&NetTarget::from_str("2001:db9:abcd:12::1/64").unwrap())
		);

		// Host domain with and without port
		assert!(
			NetTarget::from_str("example.com")
				.unwrap()
				.matches(&NetTarget::from_str("example.com").unwrap())
		);
		assert!(
			NetTarget::from_str("example.com")
				.unwrap()
				.matches(&NetTarget::from_str("example.com:80").unwrap())
		);
		assert!(
			!NetTarget::from_str("example.com")
				.unwrap()
				.matches(&NetTarget::from_str("www.example.com").unwrap())
		);
		assert!(
			!NetTarget::from_str("example.com")
				.unwrap()
				.matches(&NetTarget::from_str("www.example.com:80").unwrap())
		);
		assert!(
			NetTarget::from_str("example.com:80")
				.unwrap()
				.matches(&NetTarget::from_str("example.com:80").unwrap())
		);
		assert!(
			!NetTarget::from_str("example.com:80")
				.unwrap()
				.matches(&NetTarget::from_str("example.com:443").unwrap())
		);
		assert!(
			!NetTarget::from_str("example.com:80")
				.unwrap()
				.matches(&NetTarget::from_str("example.com").unwrap())
		);

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

	#[tokio::test]
	#[cfg(all(not(target_family = "wasm"), feature = "http"))]
	async fn test_net_target_resolve_async() {
		// This test is dependend on system configuration.
		// Some systems don't configure localhost to have a ipv6 address for example.
		// You can ignore this test failing on your own machine as long as they work on the github
		// runners.
		let r = NetTarget::from_str("localhost").unwrap().resolve().await.unwrap();
		assert!(r.contains(&NetTarget::from_str("127.0.0.1").unwrap()));
		assert!(r.contains(&NetTarget::from_str("::1/128").unwrap()));
	}

	#[test]
	#[cfg(all(target_family = "wasm", feature = "http"))]
	fn test_net_target_resolve_sync() {
		// This test is dependend on system configuration.
		// Some systems don't configure localhost to have a ipv6 address for example.
		// You can ignore this test failing on your own machine as long as they work on the github
		// runners.
		let r = NetTarget::from_str("localhost").unwrap().resolve().unwrap();
		assert!(r.contains(&NetTarget::from_str("127.0.0.1").unwrap()));
		assert!(r.contains(&NetTarget::from_str("::1/128").unwrap()));
	}

	#[test]
	fn test_method_target() {
		assert!(
			MethodTarget::from_str("query")
				.unwrap()
				.matches(&MethodTarget::from_str("query").unwrap())
		);
		assert!(
			MethodTarget::from_str("query")
				.unwrap()
				.matches(&MethodTarget::from_str("Query").unwrap())
		);
		assert!(
			MethodTarget::from_str("query")
				.unwrap()
				.matches(&MethodTarget::from_str("QUERY").unwrap())
		);
		assert!(
			!MethodTarget::from_str("query")
				.unwrap()
				.matches(&MethodTarget::from_str("ping").unwrap())
		);
	}

	#[test]
	fn test_targets() {
		assert!(Targets::<NetTarget>::All.matches(&NetTarget::from_str("example.com").unwrap()));
		assert!(Targets::<FuncTarget>::All.matches("http::get"));
		assert!(!Targets::<NetTarget>::None.matches(&NetTarget::from_str("example.com").unwrap()));
		assert!(!Targets::<FuncTarget>::None.matches("http::get"));
		assert!(
			Targets::<NetTarget>::Some([NetTarget::from_str("example.com").unwrap()].into())
				.matches(&NetTarget::from_str("example.com").unwrap())
		);
		assert!(
			!Targets::<NetTarget>::Some([NetTarget::from_str("example.com").unwrap()].into())
				.matches(&NetTarget::from_str("www.example.com").unwrap())
		);
		assert!(
			Targets::<FuncTarget>::Some([FuncTarget::from_str("http::get").unwrap()].into())
				.matches("http::get")
		);
		assert!(
			!Targets::<FuncTarget>::Some([FuncTarget::from_str("http::get").unwrap()].into())
				.matches("http::post")
		);
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

		// When some nets are allowed and some are denied, deny overrides the allow
		// rules
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

		// When some funcs are allowed and some are denied, deny overrides the allow
		// rules
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

		// When all RPC methods are denied
		{
			let caps = Capabilities::default().without_rpc_methods(Targets::<MethodTarget>::All);
			assert!(!caps.allows_rpc_method(&MethodTarget::from_str("ping").unwrap()));
			assert!(!caps.allows_rpc_method(&MethodTarget::from_str("select").unwrap()));
			assert!(!caps.allows_rpc_method(&MethodTarget::from_str("query").unwrap()));
		}

		// When some RPC methods are allowed and some are denied, deny overrides the
		// allow rules
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

		// When all HTTP routes are denied at the same time
		{
			let caps = Capabilities::default().without_http_routes(Targets::<RouteTarget>::All);
			assert!(!caps.allows_http_route(&RouteTarget::from_str("version").unwrap()));
			assert!(!caps.allows_http_route(&RouteTarget::from_str("export").unwrap()));
			assert!(!caps.allows_http_route(&RouteTarget::from_str("sql").unwrap()));
		}

		// When some HTTP routes are allowed and some are denied, deny overrides the
		// allow rules
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

		// When all arbitrary query targets are allowed
		{
			let caps = Capabilities::default()
				.with_arbitrary_query(Targets::<ArbitraryQueryTarget>::All)
				.without_arbitrary_query(Targets::<ArbitraryQueryTarget>::None);
			assert!(caps.allows_query(&ArbitraryQueryTarget::from_str("guest").unwrap()));
			assert!(caps.allows_query(&ArbitraryQueryTarget::from_str("record").unwrap()));
			assert!(caps.allows_query(&ArbitraryQueryTarget::from_str("system").unwrap()));
		}

		// When all arbitrary query targets are allowed and denied at the same time
		{
			let caps = Capabilities::default()
				.with_arbitrary_query(Targets::<ArbitraryQueryTarget>::All)
				.without_arbitrary_query(Targets::<ArbitraryQueryTarget>::All);
			assert!(!caps.allows_query(&ArbitraryQueryTarget::from_str("guest").unwrap()));
			assert!(!caps.allows_query(&ArbitraryQueryTarget::from_str("record").unwrap()));
			assert!(!caps.allows_query(&ArbitraryQueryTarget::from_str("system").unwrap()));
		}

		// When all arbitrary query targets are denied
		{
			let caps = Capabilities::default()
				.without_arbitrary_query(Targets::<ArbitraryQueryTarget>::All);
			assert!(!caps.allows_query(&ArbitraryQueryTarget::from_str("guest").unwrap()));
			assert!(!caps.allows_query(&ArbitraryQueryTarget::from_str("record").unwrap()));
			assert!(!caps.allows_query(&ArbitraryQueryTarget::from_str("system").unwrap()));
		}

		// When some arbitrary query targets are allowed and some are denied, deny
		// overrides the allow rules
		{
			let caps = Capabilities::default()
				.with_arbitrary_query(Targets::<ArbitraryQueryTarget>::Some(
					[
						ArbitraryQueryTarget::from_str("guest").unwrap(),
						ArbitraryQueryTarget::from_str("record").unwrap(),
					]
					.into(),
				))
				.without_arbitrary_query(Targets::<ArbitraryQueryTarget>::Some(
					[
						ArbitraryQueryTarget::from_str("record").unwrap(),
						ArbitraryQueryTarget::from_str("system").unwrap(),
					]
					.into(),
				));

			assert!(caps.allows_query(&ArbitraryQueryTarget::from_str("guest").unwrap()));
			assert!(!caps.allows_query(&ArbitraryQueryTarget::from_str("record").unwrap()));
			assert!(!caps.allows_query(&ArbitraryQueryTarget::from_str("system").unwrap()));
		}
	}
}
