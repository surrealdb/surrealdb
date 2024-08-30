fn main() {
	if cfg!(target_arch = "wasm32") {
		println!("cargo:rustc-cfg=wasm");
		println!("cargo::rustc-check-cfg=cfg(wasm)");
	}
	if cfg!(any(
		feature = "kv-mem",
		feature = "kv-fdb",
		feature = "kv-tikv",
		feature = "kv-rocksdb",
		feature = "kv-surrealkv",
		feature = "kv-surrealcs",
	)) {
		println!("cargo:rustc-cfg=storage");
		println!("cargo::rustc-check-cfg=cfg(storage)");
	}

	// check to see if directory exists
	let surrealcs_path = "../../surrealcs/client";
	if !std::path::Path::new("").exists() {
	}
}
