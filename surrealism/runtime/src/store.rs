//! WASM store data for the component model.

use std::fmt;
use std::sync::Arc;

use wasmtime::component::ResourceTable;
use wasmtime::*;
use wasmtime_wasi::{WasiCtx, WasiCtxView, WasiView};

use crate::config::SurrealismConfig;
use crate::host::InvocationContext;
use crate::wasi_context::StdioCallback;

/// Store data for WASI component model execution.
pub struct StoreData {
	pub wasi: WasiCtx,
	pub table: ResourceTable,
	pub config: Arc<SurrealismConfig>,
	pub(crate) context: Box<dyn InvocationContext>,
	pub(crate) limiter: StoreLimits,
	pub(crate) stdout_cb: StdioCallback,
	pub(crate) stderr_cb: StdioCallback,
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
		write!(f, "StoreData {{ config: {:?}, .. }}", self.config)
	}
}
