//! A name-filtering replacement for `wasi:sockets/ip-name-lookup`.
//!
//! `WasiCtxBuilder::allow_ip_name_lookup` is all-or-nothing, and
//! `SocketAddrUse` has no name-lookup variant, so WASI's own `socket_addr_check`
//! can filter connects but never names. Leaving resolution on unfiltered would
//! let a module encode data into hostnames and reach any nameserver, without
//! needing an allowlisted destination.
//!
//! This shadows the interface in the linker and checks each name against the
//! module's [`NetPolicy`] before resolving, then filters the addresses it hands
//! back and records them so the guest's subsequent connect is recognised.
//!
//! # Why the dynamic linker API
//!
//! The generated `sockets::ip_name_lookup::Host` trait is hard-wired to
//! `wasmtime_wasi::p2::ip_name_lookup::ResolveAddressStream`, which lives in a
//! private module and therefore cannot be named — let alone implemented — from
//! outside that crate. `LinkerInstance::func_new_async` takes `&[Val]` instead,
//! so the stream resource is ours, and the `network` handle arrives as an opaque
//! `ResourceAny` carrying no policy we need.
//!
//! Three details are load-bearing, each learned from a trap:
//!
//! 1. Every **borrowed** handle a function receives must be dropped before that function returns,
//!    or the canonical ABI rejects the call with "borrow handles still remain at the end of the
//!    call". That applies to the `network` argument even though it is unused.
//! 2. Dropping one needs `ResourceAny::resource_drop_async`, because the sync form refuses an async
//!    store via `validate_sync_call`. Hence `func_new_async` rather than `func_new`.
//! 3. A method's `self` borrow must **not** be dropped explicitly: `try_into_resource` already
//!    takes it out of the guest's table, and a second drop reports "unknown handle index".

use std::net::IpAddr;
use std::sync::Arc;

use surrealism_types::err::{PrefixErr, SurrealismResult};
use wasmtime::Engine;
use wasmtime::component::{Component, Linker, Resource, ResourceAny, ResourceType, Val};
use wasmtime_wasi::p2::{DynPollable, Pollable, subscribe};
use wasmtime_wasi::runtime::AbortOnDropJoinHandle;

use crate::net_allow::NetPolicy;
use crate::store::StoreData;

/// The interface this shim replaces, without its version.
///
/// The name the linker is keyed on carries a version (`@0.2.9`, `@0.2.12`, ...)
/// chosen by whatever toolchain built the guest, while the version wasmtime
/// registers is chosen by wasmtime. Binding a literal that had to track both
/// would fail open the moment either moved: the exact key would miss, the
/// linker's `@0.2` alternate track would resolve to wasmtime's own *unfiltered*
/// resolver, and every name check would silently disappear while
/// `allow_ip_name_lookup` stayed on. So the name is read off the component's
/// own import list instead — see [`imported_interfaces`].
const INTERFACE_PREFIX: &str = "wasi:sockets/ip-name-lookup";

/// What a lookup produced: the addresses the policy let through, or the WIT
/// `error-code` case the guest is told about.
type Resolved = Result<Vec<IpAddr>, &'static str>;

/// Our own `resolve-address-stream`.
///
/// Resolution is started eagerly on a blocking task so `subscribe` has
/// something to await, matching the shape of wasmtime's own implementation.
pub struct AddrStream {
	task: Option<AbortOnDropJoinHandle<Resolved>>,
	ready: Option<Result<std::vec::IntoIter<IpAddr>, &'static str>>,
}

#[async_trait::async_trait]
impl Pollable for AddrStream {
	/// Awaits the lookup **in place**.
	///
	/// Cancellation is the ordinary path here, not an edge case: `poll` drops
	/// every pollable that loses a race, and `HostPollable::ready` polls once
	/// and drops. Moving the join handle out of the resource would abort the
	/// lookup for good on the first such drop, and leave a stream that reports
	/// ready while `resolve-next-address` answers `would-block` for ever.
	async fn ready(&mut self) {
		if let Some(task) = self.task.as_mut() {
			let resolved = task.await;
			self.ready = Some(resolved.map(Vec::into_iter));
			self.task = None;
		}
	}
}

/// WIT `error-code` cases this shim returns. Names must match
/// `wasi:sockets/network`'s enum.
///
/// The distinctions matter to a guest: `name-unresolvable` says "this name has
/// no records, give up", a resolver failure says "retry", and `access-denied`
/// says the host refused. Reporting any of them as an exhausted address list
/// would make a plugin that falls back to a secondary endpoint never take that
/// branch.
const PERMANENT_FAILURE: &str = "permanent-resolver-failure";
const NAME_UNRESOLVABLE: &str = "name-unresolvable";
const ACCESS_DENIED: &str = "access-denied";
const WOULD_BLOCK: &str = "would-block";

fn err(case: &str) -> Val {
	Val::Result(Err(Some(Box::new(Val::Enum(case.to_string())))))
}

/// Lower an `IpAddr` onto WIT's `ip-address` variant.
fn ip_to_val(addr: IpAddr) -> Val {
	match addr {
		IpAddr::V4(v4) => Val::Variant(
			"ipv4".to_string(),
			Some(Box::new(Val::Tuple(v4.octets().iter().map(|b| Val::U8(*b)).collect()))),
		),
		IpAddr::V6(v6) => Val::Variant(
			"ipv6".to_string(),
			Some(Box::new(Val::Tuple(v6.segments().iter().map(|g| Val::U16(*g)).collect()))),
		),
	}
}

/// Every `wasi:sockets/ip-name-lookup` import name `component` asks for,
/// versions included.
///
/// **All** of them, not the first: a component composed from sub-components
/// built against different WASI releases imports the interface at two versions,
/// as two distinct top-level imports. Shadowing one would leave the other
/// exact-matching wasmtime's own unfiltered resolver, and the guest would
/// resolve arbitrary names through it while `dns_filtered()` still answered
/// `true`.
///
/// A guest that imports none cannot resolve anything, so there is nothing to
/// shadow.
fn imported_interfaces(engine: &Engine, component: &Component) -> Vec<String> {
	matching_imports(component.component_type().imports(engine).map(|(name, _)| name))
}

/// The [`is_ip_name_lookup`] imports among `names`, deduplicated.
///
/// Split out from [`imported_interfaces`] so the selection is testable without
/// a component to hand.
fn matching_imports<'a>(names: impl Iterator<Item = &'a str>) -> Vec<String> {
	let mut found: Vec<String> = Vec::new();
	for name in names.filter(|name| is_ip_name_lookup(name)) {
		if !found.iter().any(|f| f == name) {
			found.push(name.to_owned());
		}
	}
	found
}

/// Whether an import name is `wasi:sockets/ip-name-lookup` at any version.
///
/// Either the bare name or the name plus an `@version` suffix, and never a
/// longer interface that merely starts with the same text.
fn is_ip_name_lookup(name: &str) -> bool {
	name == INTERFACE_PREFIX
		|| name.strip_prefix(INTERFACE_PREFIX).is_some_and(|rest| rest.starts_with('@'))
}

/// Replace wasmtime's `wasi:sockets/ip-name-lookup` with the filtering version.
///
/// Call **after** `wasmtime_wasi::p2::add_to_linker_async`; shadowing is enabled
/// for the duration and restored afterwards.
///
/// Binds every name `component` actually imports, and reports whether it bound
/// anything. A component that imports no resolver is left alone; one that does
/// is guaranteed to reach this shim rather than wasmtime's unfiltered resolver
/// on every import, because the names came from the component itself and all of
/// them are shadowed or the whole load fails.
pub fn add_to_linker(
	engine: &Engine,
	component: &Component,
	linker: &mut Linker<StoreData>,
	policy: &Arc<NetPolicy>,
) -> SurrealismResult<bool> {
	let interfaces = imported_interfaces(engine, component);
	if interfaces.is_empty() {
		tracing::debug!(
			target: "surrealism::dns",
			"component imports no ip-name-lookup; nothing to filter"
		);
		return Ok(false);
	}
	linker.allow_shadowing(true);
	let mut result = Ok(());
	for interface in &interfaces {
		tracing::debug!(
			target: "surrealism::dns",
			interface = %interface,
			"shadowing the resolver"
		);
		result = install(linker, interface, Arc::clone(policy));
		if result.is_err() {
			break;
		}
	}
	linker.allow_shadowing(false);
	result.map(|()| true)
}

fn install(
	linker: &mut Linker<StoreData>,
	interface: &str,
	policy: Arc<NetPolicy>,
) -> SurrealismResult<()> {
	let mut instance =
		linker.instance(interface).prefix_err(|| "failed to define the DNS instance")?;

	instance
		.resource("resolve-address-stream", ResourceType::host::<AddrStream>(), |mut store, rep| {
			store.data_mut().table.delete(Resource::<AddrStream>::new_own(rep))?;
			Ok(())
		})
		.prefix_err(|| "failed to register resolve-address-stream")?;

	instance
		.func_new_async("resolve-addresses", move |mut store, _ty, params, results| {
			let policy = Arc::clone(&policy);
			Box::new(async move {
				// The `network` borrow carries no policy we need, but it must
				// still be dropped before returning.
				if let Some(Val::Resource(network)) = params.first() {
					network.resource_drop_async(&mut store).await?;
				}
				let Some(Val::String(name)) = params.get(1) else {
					return Err(wasmtime::Error::msg("resolve-addresses: expected a name"));
				};

				if !policy.allows_name(name) {
					tracing::debug!(
						target: "surrealism::dns",
						name = %name,
						"refused: not in allow_net"
					);
					results[0] = err(PERMANENT_FAILURE);
					return Ok(());
				}

				let host = name.clone();
				let filter = Arc::clone(&policy);
				let task = wasmtime_wasi::runtime::spawn(async move {
					let resolved: Vec<IpAddr> =
						match tokio::net::lookup_host((host.as_str(), 0)).await {
							Ok(addrs) => addrs.map(|a| a.ip().to_canonical()).collect(),
							Err(e) => {
								tracing::debug!(
									target: "surrealism::dns",
									name = %host,
									error = %e,
									"resolution failed"
								);
								// Told the name has no records rather than handed an
								// empty list, which would read as "all addresses
								// exhausted".
								return Err(NAME_UNRESOLVABLE);
							}
						};
					let had_records = !resolved.is_empty();
					// Drop private/special addresses a hostname must not reach,
					// and record the rest against this name so the connect is
					// recognised — and recognised only for the rules that name
					// matched.
					let kept = filter.accept_resolved(&host, resolved);
					if kept.is_empty() && had_records {
						tracing::debug!(
							target: "surrealism::dns",
							name = %host,
							"refused: every resolved address is special-use or denied"
						);
						// A policy decision, reported as one: an empty list here
						// is indistinguishable from a name with no records.
						return Err(ACCESS_DENIED);
					}
					Ok(kept)
				});

				let stream = store.data_mut().table.push(AddrStream {
					task: Some(task),
					ready: None,
				})?;
				let handle = ResourceAny::try_from_resource(stream, &mut store)?;
				results[0] = Val::Result(Ok(Some(Box::new(Val::Resource(handle)))));
				Ok(())
			})
		})
		.prefix_err(|| "failed to register resolve-addresses")?;

	instance
		.func_new_async(
			"[method]resolve-address-stream.resolve-next-address",
			|mut store, _ty, params, results| {
				Box::new(async move {
					let Some(Val::Resource(handle)) = params.first() else {
						return Err(wasmtime::Error::msg(
							"resolve-next-address: expected a stream",
						));
					};
					// Takes the borrow out of the guest's table; do not also drop it.
					let stream: Resource<AddrStream> = handle.try_into_resource(&mut store)?;
					let stream = store.data_mut().table.get_mut(&stream)?;
					let next = match stream.ready.as_mut() {
						// The guest is expected to `subscribe` and block first.
						None => {
							results[0] = err(WOULD_BLOCK);
							return Ok(());
						}
						// Reported on every call, as wasmtime's own stream does,
						// so a guest that retries still learns why.
						Some(Err(code)) => {
							results[0] = err(code);
							return Ok(());
						}
						Some(Ok(addrs)) => addrs.next(),
					};
					results[0] = Val::Result(Ok(Some(Box::new(match next {
						Some(addr) => Val::Option(Some(Box::new(ip_to_val(addr)))),
						None => Val::Option(None),
					}))));
					Ok(())
				})
			},
		)
		.prefix_err(|| "failed to register resolve-next-address")?;

	instance
		.func_new_async(
			"[method]resolve-address-stream.subscribe",
			|mut store, _ty, params, results| {
				Box::new(async move {
					let Some(Val::Resource(handle)) = params.first() else {
						return Err(wasmtime::Error::msg("subscribe: expected a stream"));
					};
					let stream: Resource<AddrStream> = handle.try_into_resource(&mut store)?;
					let pollable: Resource<DynPollable> =
						subscribe(&mut store.data_mut().table, stream)?;
					results[0] =
						Val::Resource(ResourceAny::try_from_resource(pollable, &mut store)?);
					Ok(())
				})
			},
		)
		.prefix_err(|| "failed to register subscribe")?;

	Ok(())
}

#[cfg(test)]
mod tests {
	use super::{is_ip_name_lookup, matching_imports};

	/// The guest's WASI version is chosen by whatever toolchain built it, and
	/// moves independently of the one wasmtime registers. Matching has to hold
	/// across every version either of them can name, because a miss is silent:
	/// the shim would bind a name nothing imports while the guest reached
	/// wasmtime's own unfiltered resolver.
	#[test]
	fn every_version_of_the_interface_is_recognised() {
		for name in [
			"wasi:sockets/ip-name-lookup",
			"wasi:sockets/ip-name-lookup@0.2.0",
			"wasi:sockets/ip-name-lookup@0.2.9",
			"wasi:sockets/ip-name-lookup@0.2.12",
			"wasi:sockets/ip-name-lookup@0.3.0",
			"wasi:sockets/ip-name-lookup@1.0.0-rc.1",
		] {
			assert!(is_ip_name_lookup(name), "{name}");
		}
	}

	/// Shadowing the wrong interface would leave the real one unfiltered, so a
	/// name that merely starts with the same text must not match.
	#[test]
	fn a_longer_interface_name_is_not_the_one() {
		for name in [
			"wasi:sockets/ip-name-lookup-extra",
			"wasi:sockets/ip-name-lookupx@0.2.9",
			"wasi:sockets/instance-network@0.2.9",
			"wasi:sockets/tcp@0.2.9",
			"wasi:clocks/monotonic-clock@0.2.9",
		] {
			assert!(!is_ip_name_lookup(name), "{name}");
		}
	}

	/// A component composed from sub-components built against different WASI
	/// releases imports the interface twice. Shadowing only one would leave the
	/// other resolving through wasmtime's own unfiltered resolver, with no
	/// signal that it had happened.
	#[test]
	fn every_matching_import_is_selected() {
		let imports = [
			"wasi:cli/environment@0.2.9",
			"wasi:sockets/ip-name-lookup@0.2.9",
			"wasi:sockets/tcp@0.2.9",
			"wasi:sockets/ip-name-lookup@0.2.12",
		];
		assert_eq!(
			matching_imports(imports.into_iter()),
			vec![
				"wasi:sockets/ip-name-lookup@0.2.9".to_string(),
				"wasi:sockets/ip-name-lookup@0.2.12".to_string(),
			]
		);
	}

	/// One name, bound once: defining the same interface twice is an error even
	/// with shadowing enabled.
	#[test]
	fn a_repeated_import_is_selected_once() {
		let imports = ["wasi:sockets/ip-name-lookup@0.2.9", "wasi:sockets/ip-name-lookup@0.2.9"];
		assert_eq!(
			matching_imports(imports.into_iter()),
			vec!["wasi:sockets/ip-name-lookup@0.2.9".to_string()]
		);
	}
}
