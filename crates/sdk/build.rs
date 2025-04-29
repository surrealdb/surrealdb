fn main() {
	if cfg!(target_family = "wasm") {
		println!("cargo:rustc-cfg=wasm");
		println!("cargo::rustc-check-cfg=cfg(wasm)");
	}
	if cfg!(any(feature = "kv-fdb-7_1", feature = "kv-fdb-7_3")) {
		println!("cargo:rustc-cfg=feature=\"kv-fdb\"");
		println!("cargo::rustc-check-cfg=cfg(feature, values(\"kv-fdb\"))");
	}
	if cfg!(any(
		feature = "kv-mem",
		feature = "kv-fdb-7_1",
		feature = "kv-fdb-7_3",
		feature = "kv-tikv",
		feature = "kv-rocksdb",
		feature = "kv-surrealkv",
	)) {
		println!("cargo:rustc-cfg=storage");
		println!("cargo::rustc-check-cfg=cfg(storage)");
	}
}
