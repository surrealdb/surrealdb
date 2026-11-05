//! Runs a wasm32-wasip2 command component with inherited stdio.

use wasmtime::{Error, Result};
use wasmtime::component::{Component, Linker, ResourceTable};
use wasmtime::{Config, Engine, Store};
use wasmtime_wasi::p2::bindings::Command;
use wasmtime_wasi::{WasiCtx, WasiCtxView, WasiView};

struct Host {
	ctx: WasiCtx,
	table: ResourceTable,
}

impl WasiView for Host {
	fn ctx(&mut self) -> WasiCtxView<'_> {
		WasiCtxView {
			ctx: &mut self.ctx,
			table: &mut self.table,
		}
	}
}

#[tokio::main]
async fn main() -> Result<()> {
	let path = std::env::args().nth(1).ok_or_else(|| Error::msg("usage: host <component.wasm>"))?;
	let mut config = Config::new();
	config.async_stack_size(16 << 20);
	config.max_wasm_stack(8 << 20);
	let engine = Engine::new(&config)?;
	let mut linker = Linker::<Host>::new(&engine);
	wasmtime_wasi::p2::add_to_linker_async(&mut linker)?;
	let ctx = WasiCtx::builder().inherit_stdio().inherit_env().build();
	let mut store = Store::new(
		&engine,
		Host {
			ctx,
			table: ResourceTable::new(),
		},
	);
	let component = Component::from_file(&engine, &path)?;
	let command = Command::instantiate_async(&mut store, &component, &linker).await?;
	match command.wasi_cli_run().call_run(&mut store).await? {
		Ok(()) => Ok(()),
		Err(()) => Err(Error::msg("guest exited with an error")),
	}
}
