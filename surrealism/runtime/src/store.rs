//! WASM store data for the component model.

use std::fmt;
use std::sync::Arc;

use wasmtime::component::ResourceTable;
use wasmtime::*;
use wasmtime_wasi::{WasiCtx, WasiCtxView, WasiView};

use crate::net_allow::NetPolicy;

/// Store data for one invocation.
///
/// A store serves exactly one invocation and is dropped when it returns, so
/// everything here belongs to that invocation alone: the WASI context and its
/// stdio, the resource table, the memory limiter and the network policy.
///
/// A `transaction` handle in `table` is what grants the invocation access to
/// the host, and it is added and removed around each call into the guest.
pub struct StoreData {
	pub wasi: WasiCtx,
	pub table: ResourceTable,
	/// The network policy this invocation was granted. Read by the DNS shim
	/// on every lookup, and captured by WASI's `socket_addr_check` when the
	/// context was built, so both checks answer from the same rules.
	pub(crate) net: Arc<NetPolicy>,
	pub(crate) limiter: StoreLimits,
	/// The value of the engine pool's epoch counter at which the running call
	/// traps; `None` lets it run until it returns. Read on every epoch tick of
	/// the guarded engine ([`crate::epoch::yield_until_deadline`]).
	pub(crate) deadline: Option<u64>,
}

impl WasiView for StoreData {
	fn ctx(&mut self) -> WasiCtxView<'_> {
		WasiCtxView {
			ctx: &mut self.wasi,
			table: &mut self.table,
		}
	}
}

impl fmt::Debug for StoreData {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("StoreData").finish_non_exhaustive()
	}
}
