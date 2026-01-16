fn main() {
	if cfg!(target_family = "wasm") {
		println!("cargo:rustc-cfg=wasm");
		println!("cargo::rustc-check-cfg=cfg(wasm)");
	}
	if cfg!(any(
		feature = "storage-mem",
		feature = "storage-tikv",
		feature = "storage-rocksdb",
		feature = "storage-surrealkv",
	)) {
		println!("cargo:rustc-cfg=storage");
		println!("cargo::rustc-check-cfg=cfg(storage)");
	}
}
