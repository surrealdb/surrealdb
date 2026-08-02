fn main() {
	// DiskANN (Microsoft's `diskann` crate) is only available on non-WASM, 64-bit
	// targets: its `IntoUsize` trait is implemented solely under
	// `target_pointer_width = "64"` and panics otherwise. Expose a single `diskann`
	// cfg so the gating predicate lives in one place instead of being repeated across
	// the index code. The keyspace one layer down declares the DiskANN key families
	// behind the same cfg and sets it the same way; it is duplicated rather than
	// shared because a build script cannot read another crate's.
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
	if !is_wasm && is_64_bit {
		println!("cargo:rustc-cfg=diskann");
	}
}
