use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use surrealism_types::err::PrefixErr;
use wasmtime::component::ResourceTable;
use wasmtime_wasi::sockets::SocketAddrUse;
use wasmtime_wasi::{DirPerms, FilePerms, WasiCtx, WasiCtxBuilder};

use crate::net_allow::ResolvedNetAllow;

pub fn build(
	fs_root: Option<&Path>,
	allow_net: Arc<Vec<ResolvedNetAllow>>,
) -> Result<(WasiCtx, ResourceTable)> {
	let mut builder = WasiCtxBuilder::new();
	builder.inherit_stdout().inherit_stderr();

	if allow_net.is_empty() {
		builder.allow_tcp(false);
		builder.allow_udp(false);
		builder.allow_ip_name_lookup(false);
	} else {
		// Hostnames are resolved at module load in `net_allow`, so guests
		// don't need runtime DNS. Disabling it prevents DNS tunneling.
		builder.allow_ip_name_lookup(false);
		let filters = allow_net;
		builder.socket_addr_check(move |addr, reason| {
			let is_outbound = matches!(
				reason,
				SocketAddrUse::TcpConnect
					| SocketAddrUse::UdpConnect
					| SocketAddrUse::UdpOutgoingDatagram
			);
			let allowed = is_outbound && filters.iter().any(|f| f.matches_socket_addr(&addr));
			Box::pin(async move { allowed })
		});
	}

	if let Some(root) = fs_root {
		builder
			.preopened_dir(root, "/", DirPerms::READ, FilePerms::READ)
			.prefix_err(|| "Failed to preopen filesystem directory")?;
	}
	Ok((builder.build(), ResourceTable::new()))
}
