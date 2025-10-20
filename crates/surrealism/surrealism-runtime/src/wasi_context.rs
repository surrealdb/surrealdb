use anyhow::Result;
use wasmtime_wasi::p2::WasiCtxBuilder;
use wasmtime_wasi::preview1::WasiP1Ctx;

pub fn build() -> Result<WasiP1Ctx> {
	// Note: stdout/stderr would need to access context from StoreData
	// For now, inherit from parent process
	let ctx = WasiCtxBuilder::new().inherit_stdout().inherit_stderr().inherit_env().build_p1();

	Ok(ctx)
}

// TODO: Custom stdout/stderr that access context from StoreData
// This requires passing Store through the OutputStream trait somehow
// For now, just inherit from parent process
