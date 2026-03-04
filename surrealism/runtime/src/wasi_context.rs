use anyhow::Result;
use wasmtime::component::ResourceTable;
use wasmtime_wasi::p1::WasiP1Ctx;
use wasmtime_wasi::{WasiCtx, WasiCtxBuilder};

pub fn build_p1() -> Result<WasiP1Ctx> {
	let ctx = WasiCtxBuilder::new().inherit_stdout().inherit_stderr().inherit_env().build_p1();
	Ok(ctx)
}

pub fn build_p2() -> Result<(WasiCtx, ResourceTable)> {
	let ctx = WasiCtxBuilder::new().inherit_stdout().inherit_stderr().inherit_env().build();
	let table = ResourceTable::new();
	Ok((ctx, table))
}
