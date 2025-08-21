use anyhow::Result;

use super::args::Optional;
use crate::ctx::Context;
use crate::err::Error;
use crate::val::Value;

#[cfg(not(feature = "http"))]
pub async fn head(_: &Context, (_, _): (Value, Optional<Value>)) -> Result<Value> {
	anyhow::bail!(Error::HttpDisabled)
}

#[cfg(not(feature = "http"))]
pub async fn get(_: &Context, (_, _): (Value, Optional<Value>)) -> Result<Value> {
	anyhow::bail!(Error::HttpDisabled)
}

#[cfg(not(feature = "http"))]
pub async fn put(
	_: &Context,
	(_, _, _): (Value, Optional<Value>, Optional<Value>),
) -> Result<Value> {
	anyhow::bail!(Error::HttpDisabled)
}

#[cfg(not(feature = "http"))]
pub async fn post(
	_: &Context,
	(_, _, _): (Value, Optional<Value>, Optional<Value>),
) -> Result<Value> {
	anyhow::bail!(Error::HttpDisabled)
}

#[cfg(not(feature = "http"))]
pub async fn patch(
	_: &Context,
	(_, _, _): (Value, Optional<Value>, Optional<Value>),
) -> Result<Value> {
	anyhow::bail!(Error::HttpDisabled)
}

#[cfg(not(feature = "http"))]
pub async fn delete(_: &Context, (_, _): (Value, Optional<Value>)) -> Result<Value> {
	anyhow::bail!(Error::HttpDisabled)
}

#[cfg(feature = "http")]
fn try_as_uri(fn_name: &str, value: Value) -> Result<crate::val::Strand> {
	match value {
		// Pre-check URI.
		Value::Strand(uri) if crate::fnc::util::http::uri_is_valid(&uri) => Ok(uri),
		_ => Err(anyhow::Error::new(Error::InvalidArguments {
			name: fn_name.to_owned(),
			// Assumption is that URI is first argument.
			message: String::from("The first argument should be a string containing a valid URI."),
		})),
	}
}

#[cfg(feature = "http")]
fn try_as_opts(
	fn_name: &str,
	error_message: &str,
	value: Option<Value>,
) -> Result<Option<crate::val::Object>> {
	match value {
		Some(Value::Object(opts)) => Ok(Some(opts)),
		None => Ok(None),
		Some(_) => Err(anyhow::Error::new(Error::InvalidArguments {
			name: fn_name.to_owned(),
			message: error_message.to_owned(),
		})),
	}
}

#[cfg(feature = "http")]
pub async fn head(ctx: &Context, (uri, Optional(opts)): (Value, Optional<Value>)) -> Result<Value> {
	let uri = try_as_uri("http::head", uri)?;
	let opts = try_as_opts("http::head", "The second argument should be an object.", opts)?;
	crate::fnc::util::http::head(ctx, uri, opts).await
}

#[cfg(feature = "http")]
pub async fn get(ctx: &Context, (uri, Optional(opts)): (Value, Optional<Value>)) -> Result<Value> {
	let uri = try_as_uri("http::get", uri)?;
	let opts = try_as_opts("http::get", "The second argument should be an object.", opts)?;
	crate::fnc::util::http::get(ctx, uri, opts).await
}

#[cfg(feature = "http")]
pub async fn put(
	ctx: &Context,
	(uri, Optional(body), Optional(opts)): (Value, Optional<Value>, Optional<Value>),
) -> Result<Value> {
	let uri = try_as_uri("http::put", uri)?;
	let opts = try_as_opts("http::put", "The third argument should be an object.", opts)?;
	crate::fnc::util::http::put(ctx, uri, body.unwrap_or(Value::Null), opts).await
}

#[cfg(feature = "http")]
pub async fn post(
	ctx: &Context,
	(uri, Optional(body), Optional(opts)): (Value, Optional<Value>, Optional<Value>),
) -> Result<Value> {
	let uri = try_as_uri("http::post", uri)?;
	let opts = try_as_opts("http::post", "The third argument should be an object.", opts)?;
	crate::fnc::util::http::post(ctx, uri, body.unwrap_or(Value::Null), opts).await
}

#[cfg(feature = "http")]
pub async fn patch(
	ctx: &Context,
	(uri, Optional(body), Optional(opts)): (Value, Optional<Value>, Optional<Value>),
) -> Result<Value> {
	let uri = try_as_uri("http::patch", uri)?;
	let opts = try_as_opts("http::patch", "The third argument should be an object.", opts)?;
	crate::fnc::util::http::patch(ctx, uri, body.unwrap_or(Value::Null), opts).await
}

#[cfg(feature = "http")]
pub async fn delete(
	ctx: &Context,
	(uri, Optional(opts)): (Value, Optional<Value>),
) -> Result<Value> {
	let uri = try_as_uri("http::delete", uri)?;
	let opts = try_as_opts("http::delete", "The second argument should be an object.", opts)?;
	crate::fnc::util::http::delete(ctx, uri, opts).await
}

#[cfg(all(not(target_family = "wasm"), feature = "http"))]
pub mod resolver {
	use std::error::Error;
	use std::net::ToSocketAddrs;
	use std::str::FromStr;
	use std::sync::Arc;

	use ipnet::IpNet;
	use reqwest::dns::{Addrs, Name, Resolve, Resolving};

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

			let blocking = tokio::task::spawn_blocking(
				move || -> Result<Addrs, Box<dyn Error + Send + Sync>> {
					// Check the domain name (if any) matches the allowlist
					let name_target = NetTarget::from_str(name.as_str())
						.map_err(|x| Box::new(x) as Box<dyn Error + Send + Sync>)?;
					let name_is_allowed = cap.matches_any_allow_net(&name_target)
						&& !cap.matches_any_deny_net(&name_target);
					// Resolve the addresses
					let addrs = (name.as_str(), 0)
						.to_socket_addrs()
						.map_err(|x| Box::new(x) as Box<dyn Error + Send + Sync>)?;
					// Build an iterator checking the addresses
					let iterator = Box::new(addrs.filter(move |addr| {
						let target = IpNet::new_assert(addr.ip(), 0);
						name_is_allowed && !cap.matches_any_deny_net(&NetTarget::IPNet(target))
					}));
					Ok(iterator as Addrs)
				},
			);
			Box::pin(async {
				blocking.await.map_err(|x| Box::new(x) as Box<dyn Error + Send + Sync>)?
			}) as Resolving
		}
	}
}
