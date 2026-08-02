fn main() {
	// The keyspace declares the DiskANN key families behind the same `diskann`
	// cfg the index engine uses, so the predicate has to be set here too. It is
	// duplicated rather than shared because a build script cannot read another
	// crate's, and it is only three lines of target inspection.
	//
	// Target properties MUST be read from the `CARGO_CFG_*` env vars, which
	// describe the compile *target*. `cfg!(...)` inside a build script reflects
	// the *host* that runs it, so it would wrongly enable DiskANN when
	// cross-compiling from a 64-bit host to a 32-bit target.
	println!("cargo::rustc-check-cfg=cfg(diskann)");
	let target_family = std::env::var("CARGO_CFG_TARGET_FAMILY").unwrap_or_default();
	let is_wasm = target_family.split(',').any(|family| family == "wasm");
	let is_64_bit = std::env::var("CARGO_CFG_TARGET_POINTER_WIDTH").as_deref() == Ok("64");
	if !is_wasm && is_64_bit {
		println!("cargo:rustc-cfg=diskann");
	}
}
