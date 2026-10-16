//! The `CAPABILITIES` object of a `DEFINE EXTENSION` statement, as the client
//! builds it, narrows it and compares it.
//!
//! An empty object grants nothing: no network, no functions, no arbitrary
//! queries, no scripting, no writes, the server's own memory and time limits,
//! and timeouts enforced. [`Capabilities::default`] is that empty object, and
//! [`Capabilities::to_sql`] writes only the fields that differ from it.

use std::fmt::Write as _;
use std::time::Duration;

use anyhow::{Context as _, Result, bail};
use surrealdb_types::{Value, fmt_duration_sql};
use surrealism_runtime::capabilities::{FunctionTargets, SurrealismCapabilities};

use super::sql::quote;

/// What an extension may do. Field names follow the `CAPABILITIES` object.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Capabilities {
	/// Hosts and ports the extension may connect to, e.g. `api.color.io:443`.
	pub net: Vec<String>,
	/// SurrealDB functions it may call, e.g. `http::get` or `http::*`.
	pub functions: Vec<String>,
	/// May run any SurrealQL text through `tx.sql`.
	pub queries: bool,
	/// May use embedded scripting.
	pub scripting: bool,
	/// Functions marked as writing get write access.
	pub write: bool,
	/// Maximum linear memory per invocation, in bytes. `None` is the server's
	/// limit.
	pub memory: Option<u64>,
	/// Maximum duration of one invocation. `None` is the server's limit.
	pub timeout: Option<Duration>,
	/// `false` removes timeout checks, so a call can run forever.
	pub strict_timeout: bool,
}

impl Default for Capabilities {
	fn default() -> Self {
		Self {
			net: Vec::new(),
			functions: Vec::new(),
			queries: false,
			scripting: false,
			write: false,
			memory: None,
			timeout: None,
			strict_timeout: true,
		}
	}
}

/// A request that a client must confirm twice before granting it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum HighRisk {
	/// `strict_timeout: false`: an invocation can run forever.
	NoStrictTimeout,
	/// `queries: true`: the extension can run any SurrealQL.
	ArbitraryQueries,
	/// `net: true` (`["*"]`): the extension can connect anywhere.
	AnyNetwork,
	/// A target that covers every address (`0.0.0.0/0`, `::/0`) or uses a
	/// wildcard in a host name.
	WideNetwork(String),
	/// A target that reaches a private, loopback, link-local or otherwise
	/// internal address, such as `169.254.169.254` (cloud metadata) or
	/// `localhost`.
	InternalNetwork(String),
}

impl HighRisk {
	pub fn describe(&self) -> String {
		match self {
			HighRisk::NoStrictTimeout => {
				"strict_timeout: false — timeouts are not enforced, so a call can run forever"
					.into()
			}
			HighRisk::ArbitraryQueries => {
				"queries: true — the extension can run any SurrealQL with the caller's rights"
					.into()
			}
			HighRisk::AnyNetwork => "net: true — the extension can connect to any host".into(),
			HighRisk::WideNetwork(t) => {
				format!("net: {t} — covers every address or any matching host name")
			}
			HighRisk::InternalNetwork(t) => format!(
				"net: {t} — reaches private, loopback or link-local addresses inside your network"
			),
		}
	}
}

/// Address ranges that are not on the public internet, as (base, prefix
/// length) in IPv6 form; IPv4 ranges are mapped (`::ffff:a.b.c.d`).
const INTERNAL_RANGES: &[(&str, u8)] = &[
	("::ffff:0.0.0.0", 104),     // 0.0.0.0/8 "this network"
	("::ffff:10.0.0.0", 104),    // 10.0.0.0/8
	("::ffff:100.64.0.0", 106),  // 100.64.0.0/10 carrier-grade NAT
	("::ffff:127.0.0.0", 104),   // 127.0.0.0/8 loopback
	("::ffff:169.254.0.0", 112), // 169.254.0.0/16 link-local, cloud metadata
	("::ffff:172.16.0.0", 108),  // 172.16.0.0/12
	("::ffff:192.168.0.0", 112), // 192.168.0.0/16
	("::", 127),                 // :: and ::1 loopback
	("fc00::", 7),               // unique local
	("fe80::", 10),              // link-local
];

/// Whether `addr/len` overlaps an internal range.
fn covers_internal(addr: std::net::IpAddr, len: u8) -> bool {
	let (bits, len) = match addr {
		std::net::IpAddr::V4(v4) => (u128::from(v4.to_ipv6_mapped()), len + 96),
		std::net::IpAddr::V6(v6) => (u128::from(v6), len),
	};
	INTERNAL_RANGES.iter().any(|(base, base_len)| {
		let base = u128::from(base.parse::<std::net::Ipv6Addr>().expect("valid range"));
		let common = len.min(*base_len);
		let mask = if common == 0 {
			0
		} else {
			u128::MAX << (128 - u32::from(common))
		};
		bits & mask == base & mask
	})
}

/// The second-confirmation risk of one `net` entry, if any.
fn net_risk(target: &str) -> Option<HighRisk> {
	use std::str::FromStr as _;

	use surrealdb_rpc::capabilities::NetTarget;
	if target == "*" {
		return Some(HighRisk::AnyNetwork);
	}
	if target.contains('*') {
		return Some(HighRisk::WideNetwork(target.into()));
	}
	let internal = match NetTarget::from_str(target).ok()? {
		NetTarget::IPNet(net) if net.prefix_len() == 0 => {
			return Some(HighRisk::WideNetwork(target.into()));
		}
		NetTarget::IPNet(net) => covers_internal(net.network(), net.prefix_len()),
		// Unreachable past the `*` check above, and judged the same way.
		NetTarget::Wildcard {
			..
		} => return Some(HighRisk::WideNetwork(target.into())),
		NetTarget::Host(host, _) => {
			let name = host.to_string();
			match name.trim_start_matches('[').trim_end_matches(']').parse::<std::net::IpAddr>() {
				Ok(ip) => covers_internal(
					ip,
					if ip.is_ipv4() {
						32
					} else {
						128
					},
				),
				Err(_) => {
					let name = name.trim_end_matches('.').to_ascii_lowercase();
					name == "localhost"
						|| [".localhost", ".local", ".internal", ".home.arpa"]
							.iter()
							.any(|suffix| name.ends_with(suffix))
				}
			}
		}
	};
	internal.then(|| HighRisk::InternalNetwork(target.into()))
}

/// One line of a capability diff between an installed and a new object.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Change {
	/// A list entry or a flag that the new object grants and the old does not.
	Added {
		field: &'static str,
		value: String,
	},
	/// A list entry or a flag that the old object grants and the new does not.
	Removed {
		field: &'static str,
		value: String,
	},
	/// A limit whose value differs.
	Changed {
		field: &'static str,
		from: String,
		to: String,
	},
}

impl Capabilities {
	/// The archive's request: `[capabilities]` from `package.toml`, plus `write`
	/// when any export is marked as writing.
	pub fn from_request(request: &SurrealismCapabilities, any_export_writes: bool) -> Self {
		Self {
			net: request.allow_net.clone(),
			functions: match &request.allow_functions {
				FunctionTargets::None => Vec::new(),
				FunctionTargets::All => vec!["*".into()],
				FunctionTargets::Some(patterns) => patterns.clone(),
			},
			queries: request.allow_arbitrary_queries,
			scripting: request.allow_scripting,
			write: any_export_writes,
			memory: request.max_memory_bytes.map(|m| m as u64),
			timeout: request.max_execution_time,
			strict_timeout: request.strict_timeout,
		}
	}

	/// Reads a `CAPABILITIES` object. Absent fields keep the value in `base`,
	/// so `from_value(v, &Capabilities::default())` reads a full object and
	/// `from_value(v, &request)` reads an overlay on the request.
	pub fn from_value(value: &Value, base: &Capabilities) -> Result<Self> {
		let Value::Object(obj) = value else {
			bail!("expected a capabilities object such as {{ net: [], write: false }}");
		};
		let mut out = base.clone();
		for (key, v) in obj.iter() {
			match key.as_str() {
				"net" => out.net = strings(key, v)?,
				"functions" => out.functions = strings(key, v)?,
				"queries" => out.queries = boolean(key, v)?,
				"scripting" => out.scripting = boolean(key, v)?,
				"write" => out.write = boolean(key, v)?,
				"strict_timeout" => out.strict_timeout = boolean(key, v)?,
				"memory" => {
					out.memory = match v {
						Value::None | Value::Null => None,
						Value::Number(n) => match n.to_int() {
							Some(i) if i >= 0 => Some(i as u64),
							_ => bail!("`memory` must be a whole number of bytes"),
						},
						_ => bail!("`memory` must be a whole number of bytes"),
					}
				}
				"timeout" => {
					out.timeout = match v {
						Value::None | Value::Null => None,
						Value::Duration(d) => Some(d.into_inner()),
						_ => bail!("`timeout` must be a duration such as 5s"),
					}
				}
				other => bail!("unknown capability `{other}`"),
			}
		}
		Ok(out)
	}

	/// What an upgrade grants when a person reviews the request.
	///
	/// It starts from `installed`, the grant the person approved before, and
	/// never gives back what was withheld then. Of the new archive's request
	/// (`requested`), it keeps what `installed` granted and adds only what is
	/// genuinely new: requested now but not by the installed archive
	/// (`previous`). When the installed archive cannot be read, `previous` is
	/// `None` and nothing counts as new, so a new request is granted only
	/// through `--with-capability`. The result never exceeds `requested`.
	pub fn carry_over(
		installed: &Capabilities,
		previous: Option<&Capabilities>,
		requested: &Capabilities,
	) -> Self {
		let is_new = |was: bool, now: bool| now && previous.is_some() && !was;
		let prev = previous.cloned().unwrap_or_default();
		Self {
			net: carry_list(&installed.net, previous.map(|p| p.net.as_slice()), &requested.net),
			functions: carry_list(
				&installed.functions,
				previous.map(|p| p.functions.as_slice()),
				&requested.functions,
			),
			queries: (installed.queries && requested.queries)
				|| is_new(prev.queries, requested.queries),
			scripting: (installed.scripting && requested.scripting)
				|| is_new(prev.scripting, requested.scripting),
			write: (installed.write && requested.write) || is_new(prev.write, requested.write),
			// `strict_timeout: false` is the grant; `true` withholds it.
			strict_timeout: !((!installed.strict_timeout && !requested.strict_timeout)
				|| is_new(!prev.strict_timeout, !requested.strict_timeout)),
			memory: carry_limit(installed.memory, previous.map(|p| p.memory), requested.memory),
			timeout: carry_limit(installed.timeout, previous.map(|p| p.timeout), requested.timeout),
		}
	}

	/// The grant that holds nothing but follows `request`'s limits: no
	/// network, functions or flags, `memory` and `timeout` as requested, and
	/// timeouts enforced. What a non-interactive install grants without
	/// `--with-capability`.
	pub fn nothing_within(request: &Capabilities) -> Self {
		Self {
			memory: request.memory,
			timeout: request.timeout,
			..Self::default()
		}
	}

	/// `self` with each `--with-capability` token added, which must stay
	/// within `request`. A token is one of `net:<target>`,
	/// `functions:<pattern>` (`*` for every one the server allows), `queries`,
	/// `scripting`, `write`, `no-strict-timeout`, `memory=<bytes>` and
	/// `timeout=<duration>` (the last two lower a limit).
	pub fn with_tokens(&self, tokens: &[String], request: &Capabilities) -> Result<Self> {
		let mut out = self.clone();
		let add = |list: &mut Vec<String>, item: &str| {
			if item == "*" {
				*list = vec!["*".into()];
			} else if !is_any(list) && !list.iter().any(|v| v == item) {
				list.push(item.to_string());
			}
		};
		for token in tokens {
			match token.split_once([':', '=']) {
				Some(("net", target)) if !target.is_empty() => add(&mut out.net, target),
				Some(("functions", pattern)) if !pattern.is_empty() => {
					add(&mut out.functions, pattern)
				}
				Some(("memory", bytes)) => {
					out.memory = Some(bytes.parse().with_context(|| {
						format!("--with-capability {token}: memory must be a number of bytes")
					})?)
				}
				Some(("timeout", duration)) => {
					out.timeout = match crate::core::syn::value(duration) {
						Ok(Value::Duration(d)) => Some(d.into_inner()),
						_ => bail!(
							"--with-capability {token}: timeout must be a duration such as 5s"
						),
					}
				}
				None if token == "queries" => out.queries = true,
				None if token == "scripting" => out.scripting = true,
				None if token == "write" => out.write = true,
				None if token == "no-strict-timeout" => out.strict_timeout = false,
				_ => bail!(
					"--with-capability {token}: expected net:<target>, functions:<pattern>, \
					 queries, scripting, write, no-strict-timeout, memory=<bytes> or \
					 timeout=<duration>"
				),
			}
		}
		out.ensure_within(request)?;
		Ok(out)
	}

	/// Fails when `self` grants anything that `limit` does not.
	pub fn ensure_within(&self, limit: &Capabilities) -> Result<()> {
		// `["*"]` is the clause's `true`: every target the server allows, so
		// any list narrows it.
		let any = |list: &[String]| list.iter().any(|v| v == "*");
		for host in self.net.iter().filter(|_| !any(&limit.net)) {
			if !limit.net.contains(host) {
				bail!(
					"--with-capability cannot add network target `{host}`: the extension does not request it"
				);
			}
		}
		for func in self.functions.iter().filter(|_| !any(&limit.functions)) {
			if !limit.functions.contains(func) {
				bail!(
					"--with-capability cannot add function `{func}`: the extension does not request it"
				);
			}
		}
		for (name, granted, requested) in [
			("queries", self.queries, limit.queries),
			("scripting", self.scripting, limit.scripting),
			("write", self.write, limit.write),
		] {
			if granted && !requested {
				bail!(
					"--with-capability cannot set `{name}: true`: the extension does not request it"
				);
			}
		}
		if !self.strict_timeout && limit.strict_timeout {
			bail!(
				"--with-capability cannot set `strict_timeout: false`: the extension does not request it"
			);
		}
		if let Some(requested) = limit.memory {
			match self.memory {
				Some(granted) if granted <= requested => {}
				_ => bail!(
					"--with-capability can lower `memory` below {requested} bytes, but not raise or remove it"
				),
			}
		}
		if let Some(requested) = limit.timeout {
			match self.timeout {
				Some(granted) if granted <= requested => {}
				_ => bail!(
					"--with-capability can lower `timeout` below {}, but not raise or remove it",
					duration_sql(requested)
				),
			}
		}
		Ok(())
	}

	/// Requests that need a second confirmation.
	pub fn high_risk(&self) -> Vec<HighRisk> {
		let mut out = Vec::new();
		if !self.strict_timeout {
			out.push(HighRisk::NoStrictTimeout);
		}
		if self.queries {
			out.push(HighRisk::ArbitraryQueries);
		}
		if is_any(&self.net) {
			out.push(HighRisk::AnyNetwork);
		} else {
			out.extend(self.net.iter().filter_map(|n| net_risk(n)));
		}
		out
	}

	/// What changes from `old` to `self`, in field order.
	pub fn diff(&self, old: &Capabilities) -> Vec<Change> {
		let mut out = Vec::new();
		list_diff(&mut out, "net", &old.net, &self.net);
		list_diff(&mut out, "functions", &old.functions, &self.functions);
		for (field, was, now) in [
			("queries", old.queries, self.queries),
			("scripting", old.scripting, self.scripting),
			("write", old.write, self.write),
		] {
			match (was, now) {
				(false, true) => out.push(Change::Added {
					field,
					value: "true".into(),
				}),
				(true, false) => out.push(Change::Removed {
					field,
					value: "true".into(),
				}),
				_ => {}
			}
		}
		if old.memory != self.memory {
			out.push(Change::Changed {
				field: "memory",
				from: memory_text(old.memory),
				to: memory_text(self.memory),
			});
		}
		if old.timeout != self.timeout {
			out.push(Change::Changed {
				field: "timeout",
				from: timeout_text(old.timeout),
				to: timeout_text(self.timeout),
			});
		}
		if old.strict_timeout != self.strict_timeout {
			out.push(Change::Changed {
				field: "strict_timeout",
				from: old.strict_timeout.to_string(),
				to: self.strict_timeout.to_string(),
			});
		}
		out
	}

	/// The object as SurrealQL, writing only the fields that grant more than,
	/// or limit differently from, the empty object.
	pub fn to_sql(&self) -> String {
		let mut fields: Vec<String> = Vec::new();
		if !self.net.is_empty() {
			fields.push(format!("net: {}", grant_list(&self.net)));
		}
		if !self.functions.is_empty() {
			fields.push(format!("functions: {}", grant_list(&self.functions)));
		}
		if self.queries {
			fields.push("queries: true".into());
		}
		if self.scripting {
			fields.push("scripting: true".into());
		}
		if self.write {
			fields.push("write: true".into());
		}
		if let Some(memory) = self.memory {
			fields.push(format!("memory: {memory}"));
		}
		if let Some(timeout) = self.timeout {
			fields.push(format!("timeout: {}", duration_sql(timeout)));
		}
		if !self.strict_timeout {
			fields.push("strict_timeout: false".into());
		}
		if fields.is_empty() {
			"{}".into()
		} else {
			format!("{{ {} }}", fields.join(", "))
		}
	}

	/// The prompt's `Needs` block: one `(label, value)` row per granted item.
	pub fn prompt_rows(&self) -> Vec<(&'static str, String)> {
		let mut rows = Vec::new();
		for host in &self.net {
			rows.push(("network", host.clone()));
		}
		for func in &self.functions {
			rows.push(("functions", func.clone()));
		}
		if self.queries {
			rows.push(("queries", "any SurrealQL".into()));
		}
		if self.scripting {
			rows.push(("scripting", "yes".into()));
		}
		if self.write {
			rows.push(("write", "yes".into()));
		}
		if let Some(memory) = self.memory {
			rows.push(("memory", memory_text(Some(memory))));
		}
		if let Some(timeout) = self.timeout {
			rows.push(("timeout", timeout_text(Some(timeout))));
		}
		if !self.strict_timeout {
			rows.push(("timeouts", "NOT enforced (strict_timeout: false)".into()));
		}
		rows
	}
}

/// Whether a `net` or `functions` list is the clause's `true`.
fn is_any(list: &[String]) -> bool {
	list.iter().any(|v| v == "*")
}

/// [`Capabilities::carry_over`] for a `net` or `functions` list.
fn carry_list(
	installed: &[String],
	previous: Option<&[String]>,
	requested: &[String],
) -> Vec<String> {
	if is_any(requested) {
		// Everything the server allows: kept if it was granted. Otherwise the
		// installed list stays as it is, whatever the previous request was,
		// so what the install withheld stays withheld; `*` is then shown as
		// requested but not granted, and only `--with-capability` grants it.
		return if is_any(installed) {
			vec!["*".into()]
		} else {
			installed.to_vec()
		};
	}
	if is_any(installed) {
		// Everything was granted, so all of the narrower request is.
		return requested.to_vec();
	}
	requested
		.iter()
		.filter(|v| installed.contains(v) || previous.is_some_and(|p| !is_any(p) && !p.contains(v)))
		.cloned()
		.collect()
}

/// [`Capabilities::carry_over`] for `memory` or `timeout`, where `None` is the
/// server's own limit, the widest. The new request applies when the installed
/// value was not narrowed below the previous request; otherwise the installed
/// value is kept, capped by the new request.
fn carry_limit<T: Ord + Copy>(
	installed: Option<T>,
	previous: Option<Option<T>>,
	requested: Option<T>,
) -> Option<T> {
	if previous == Some(installed) {
		return requested;
	}
	match (installed, requested) {
		(None, r) => r,
		(i, None) => i,
		(Some(i), Some(r)) => Some(i.min(r)),
	}
}

/// The prompt label for a capability field.
pub fn field_label(field: &str) -> &str {
	match field {
		"net" => "network",
		other => other,
	}
}

fn list_diff(out: &mut Vec<Change>, field: &'static str, old: &[String], new: &[String]) {
	for v in new.iter().filter(|v| !old.contains(v)) {
		out.push(Change::Added {
			field,
			value: v.clone(),
		});
	}
	for v in old.iter().filter(|v| !new.contains(v)) {
		out.push(Change::Removed {
			field,
			value: v.clone(),
		});
	}
}

/// A `net` or `functions` field: `false` grants nothing, `true` everything the
/// server allows (read as `["*"]`), and an array lists what it grants.
fn strings(key: &str, v: &Value) -> Result<Vec<String>> {
	let items = match v {
		Value::Bool(false) => return Ok(Vec::new()),
		Value::Bool(true) => return Ok(vec!["*".into()]),
		Value::Array(items) => items,
		_ => bail!("`{key}` must be true, false or an array of strings"),
	};
	items
		.iter()
		.map(|item| match item {
			Value::String(s) => Ok(s.clone()),
			_ => bail!("`{key}` must be true, false or an array of strings"),
		})
		.collect()
}

fn boolean(key: &str, v: &Value) -> Result<bool> {
	match v {
		Value::Bool(b) => Ok(*b),
		_ => bail!("`{key}` must be true or false"),
	}
}

/// `["*"]` is written as `true`, the clause's spelling for "everything the
/// server allows"; any other list as an array.
fn grant_list(items: &[String]) -> String {
	if items == ["*"] {
		return "true".into();
	}
	string_list(items)
}

fn string_list(items: &[String]) -> String {
	let mut out = String::from("[");
	for (i, item) in items.iter().enumerate() {
		if i > 0 {
			out.push_str(", ");
		}
		out.push_str(&quote(item));
	}
	out.push(']');
	out
}

fn duration_sql(d: Duration) -> String {
	let mut out = String::new();
	fmt_duration_sql(d, &mut out);
	out
}

fn memory_text(memory: Option<u64>) -> String {
	match memory {
		None => "server limit".into(),
		Some(m) if m >= 1 << 20 && m % (1 << 20) == 0 => format!("{} MiB", m >> 20),
		Some(m) => {
			let mut out = String::new();
			let _ = write!(out, "{m} bytes");
			out
		}
	}
}

fn timeout_text(timeout: Option<Duration>) -> String {
	match timeout {
		None => "server limit".into(),
		Some(d) => duration_sql(d),
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn parse(s: &str) -> Value {
		crate::core::syn::value(s).expect("valid SurrealQL value")
	}

	fn requested() -> Capabilities {
		Capabilities {
			net: vec!["api.color.io:443".into(), "cdn.color.io:443".into()],
			functions: vec!["http::get".into()],
			write: true,
			memory: Some(64 << 20),
			timeout: Some(Duration::from_secs(5)),
			..Capabilities::default()
		}
	}

	#[test]
	fn empty_object_renders_empty() {
		assert_eq!(Capabilities::default().to_sql(), "{}");
	}

	#[test]
	fn renders_only_non_default_fields_in_order() {
		let caps = Capabilities {
			queries: true,
			strict_timeout: false,
			..requested()
		};
		assert_eq!(
			caps.to_sql(),
			r#"{ net: ['api.color.io:443', 'cdn.color.io:443'], functions: ['http::get'], queries: true, write: true, memory: 67108864, timeout: 5s, strict_timeout: false }"#
		);
	}

	#[test]
	fn rendered_object_reads_back_equal() {
		let caps = Capabilities {
			scripting: true,
			strict_timeout: false,
			..requested()
		};
		let back = Capabilities::from_value(&parse(&caps.to_sql()), &Capabilities::default())
			.expect("reads back");
		assert_eq!(back, caps);
	}

	fn tokens(list: &[&str]) -> Vec<String> {
		list.iter().map(|t| t.to_string()).collect()
	}

	#[test]
	fn with_capability_adds_to_nothing_within_the_request() {
		let req = requested();
		let none = Capabilities::nothing_within(&req);
		assert!(none.net.is_empty() && none.functions.is_empty() && !none.write);
		assert_eq!(
			(none.memory, none.timeout),
			(req.memory, req.timeout),
			"limits follow the request"
		);
		none.ensure_within(&req).expect("nothing is within anything");

		let granted = none
			.with_tokens(&tokens(&["net:api.color.io:443", "write", "timeout=2s"]), &req)
			.expect("within the request");
		assert_eq!(granted.net, vec!["api.color.io:443".to_string()]);
		assert!(granted.functions.is_empty());
		assert!(granted.write);
		assert_eq!(granted.timeout, Some(Duration::from_secs(2)));
		assert_eq!(granted.memory, Some(64 << 20));
	}

	#[test]
	fn with_capability_cannot_widen_the_request() {
		let req = requested();
		let none = Capabilities::nothing_within(&req);
		for token in [
			"net:evil.example:443",
			"functions:http::*",
			"queries",
			"no-strict-timeout",
			"memory=134217728",
			"timeout=10s",
			"net:*",
		] {
			assert!(none.with_tokens(&tokens(&[token]), &req).is_err(), "{token} must be refused");
		}
		let err = none.with_tokens(&tokens(&["allow_net"]), &req).unwrap_err();
		assert!(err.to_string().contains("expected net:<target>"), "{err}");
	}

	#[test]
	fn with_capability_narrows_a_wildcard_request_to_a_list() {
		let any = Capabilities {
			net: vec!["*".into()],
			functions: vec!["*".into()],
			..Capabilities::default()
		};
		let none = Capabilities::nothing_within(&any);
		let granted = none
			.with_tokens(&tokens(&["net:api.color.io:443", "functions:http::get"]), &any)
			.expect("a list within `true`");
		assert_eq!(granted.net, vec!["api.color.io:443".to_string()]);
		assert_eq!(granted.functions, vec!["http::get".to_string()]);
		let all =
			none.with_tokens(&tokens(&["net:*", "functions:*"]), &any).expect("`true` itself");
		assert_eq!(all.net, vec!["*".to_string()]);
	}

	#[test]
	fn a_wildcard_request_on_upgrade_keeps_what_install_withheld() {
		// Installed with `net: [a]` granted of `[a, b]`; the upgrade asks `*`.
		let previous = Capabilities {
			net: vec!["a.example:443".into(), "b.example:443".into()],
			functions: vec!["http::get".into(), "http::post".into()],
			..Capabilities::default()
		};
		let installed = Capabilities {
			net: vec!["a.example:443".into()],
			functions: vec!["http::get".into()],
			..Capabilities::default()
		};
		let requested = Capabilities {
			net: vec!["*".into()],
			functions: vec!["*".into()],
			..Capabilities::default()
		};
		let carried = Capabilities::carry_over(&installed, Some(&previous), &requested);
		assert_eq!(carried.net, vec!["a.example:443".to_string()], "b stays withheld");
		assert_eq!(carried.functions, vec!["http::get".to_string()]);
		assert!(carried.high_risk().is_empty(), "`*` is not granted");
		// `*` shows as requested but not granted.
		let withheld = requested.diff(&carried);
		assert!(
			withheld
				.iter()
				.any(|c| matches!(c, Change::Added { field: "net", value } if value == "*"))
		);
	}

	#[test]
	fn an_upgrade_keeps_what_install_withheld() {
		let previous = requested();
		// Installed with `cdn` and `write` withheld and a lower timeout.
		let installed = Capabilities {
			net: vec!["api.color.io:443".into()],
			write: false,
			timeout: Some(Duration::from_secs(2)),
			..requested()
		};
		// The new release also asks for `scripting`, a new host and more time.
		let mut requested = requested();
		requested.net.push("eu.color.io:443".into());
		requested.scripting = true;
		requested.timeout = Some(Duration::from_secs(10));

		let carried = Capabilities::carry_over(&installed, Some(&previous), &requested);
		assert_eq!(
			carried.net,
			vec!["api.color.io:443".to_string(), "eu.color.io:443".to_string()]
		);
		assert!(!carried.write, "withheld at install, still withheld");
		assert!(carried.scripting, "new in this release");
		assert_eq!(carried.timeout, Some(Duration::from_secs(2)), "narrowed at install");
		assert_eq!(carried.memory, Some(64 << 20), "not narrowed: follows the request");
		carried.ensure_within(&requested).expect("never more than the request");

		// Without the installed archive nothing counts as new.
		let blind = Capabilities::carry_over(&installed, None, &requested);
		assert_eq!(blind.net, vec!["api.color.io:443".to_string()]);
		assert!(!blind.scripting && !blind.write);
	}

	#[test]
	fn wildcard_lists_render_as_true_and_read_back() {
		let caps = Capabilities {
			net: vec!["*".into()],
			functions: vec!["*".into()],
			..Capabilities::default()
		};
		assert_eq!(caps.to_sql(), "{ net: true, functions: true }");
		let back =
			Capabilities::from_value(&parse(&caps.to_sql()), &Capabilities::default()).unwrap();
		assert_eq!(back, caps);
		let none =
			Capabilities::from_value(&parse("{ net: false, functions: false }"), &caps).unwrap();
		assert_eq!(none, Capabilities::default());
	}

	#[test]
	fn high_risk_requests() {
		assert!(requested().high_risk().is_empty());
		let risky = Capabilities {
			net: vec!["*".into()],
			queries: true,
			strict_timeout: false,
			..Capabilities::default()
		};
		assert_eq!(
			risky.high_risk(),
			vec![HighRisk::NoStrictTimeout, HighRisk::ArbitraryQueries, HighRisk::AnyNetwork]
		);
	}

	#[test]
	fn wide_and_internal_network_targets_are_high_risk() {
		let risks = |net: &[&str]| {
			Capabilities {
				net: net.iter().map(|n| n.to_string()).collect(),
				..Capabilities::default()
			}
			.high_risk()
		};
		for wide in ["0.0.0.0/0", "::/0", "*.color.io"] {
			assert_eq!(risks(&[wide]), vec![HighRisk::WideNetwork(wide.into())], "{wide}");
		}
		for internal in [
			"169.254.169.254",
			"169.254.169.254:80",
			"10.1.2.3:5432",
			"192.168.0.0/16",
			"172.20.0.0/16",
			"127.0.0.1:8000",
			"[::1]:8000",
			"fd00::/8",
			"fe80::1",
			"8.0.0.0/4",
			"localhost:6379",
			"metadata.google.internal",
		] {
			assert_eq!(
				risks(&[internal]),
				vec![HighRisk::InternalNetwork(internal.into())],
				"{internal}"
			);
		}
		for public in ["api.color.io:443", "1.1.1.1", "203.0.113.0/24", "2001:db8::/32"] {
			assert!(risks(&[public]).is_empty(), "{public}");
		}
	}

	#[test]
	fn diff_lists_additions_removals_and_changes() {
		let old = Capabilities {
			net: vec!["api.color.io:443".into()],
			functions: vec!["http::get".into(), "http::post".into()],
			write: true,
			memory: Some(64 << 20),
			..Capabilities::default()
		};
		let new = Capabilities {
			net: vec!["api.color.io:443".into(), "cdn.color.io:443".into()],
			functions: vec!["http::get".into()],
			queries: true,
			write: false,
			memory: Some(128 << 20),
			..Capabilities::default()
		};
		assert_eq!(
			new.diff(&old),
			vec![
				Change::Added {
					field: "net",
					value: "cdn.color.io:443".into()
				},
				Change::Removed {
					field: "functions",
					value: "http::post".into()
				},
				Change::Added {
					field: "queries",
					value: "true".into()
				},
				Change::Removed {
					field: "write",
					value: "true".into()
				},
				Change::Changed {
					field: "memory",
					from: "64 MiB".into(),
					to: "128 MiB".into()
				},
			]
		);
		assert!(new.diff(&new).is_empty());
	}
}
