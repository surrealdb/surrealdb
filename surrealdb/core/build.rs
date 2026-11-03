fn main() {
	if cfg!(target_family = "wasm") {
		println!("cargo:rustc-cfg=wasm");
		println!("cargo::rustc-check-cfg=cfg(wasm)");
	}
	if cfg!(any(feature = "kv-mem", feature = "kv-surrealkv",)) {
		println!("cargo:rustc-cfg=storage");
		println!("cargo::rustc-check-cfg=cfg(storage)");
	}
	// DiskANN (the `diskann` library of the `surrealdb-diskann` crate) is built for
	// 64-bit targets and for wasm. On 32-bit wasm it relies on that crate's changes
	// to Microsoft's `diskann`: `IntoUsize` for the lossless conversions only, and
	// spawned futures run on the JavaScript event loop where there is no tokio runtime. Other
	// 32-bit targets (i686, armv7, …) leave it out and keep HNSW as their vector index. Expose a
	// single `diskann` cfg so the gating predicate lives in one place instead of being repeated
	// across the index code.
	//
	// Target properties MUST be read from the `CARGO_CFG_*` env vars, which describe
	// the compile *target*. `cfg!(...)` inside a build script reflects the *host* that
	// runs the script, so it would wrongly enable DiskANN when cross-compiling from a
	// 64-bit host to a 32-bit target. check-cfg is emitted unconditionally so the
	// `#[cfg(diskann)]`/`#[cfg(not(diskann))]` gates never trip the unexpected_cfgs lint.
	println!("cargo::rustc-check-cfg=cfg(diskann)");
	let target_family = std::env::var("CARGO_CFG_TARGET_FAMILY").unwrap_or_default();
	let is_wasm = target_family.split(',').any(|family| family == "wasm");
	let is_64_bit = std::env::var("CARGO_CFG_TARGET_POINTER_WIDTH").as_deref() == Ok("64");
	if is_wasm || is_64_bit {
		println!("cargo:rustc-cfg=diskann");
	}
}
