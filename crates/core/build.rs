
fn main() {
	if cfg!(target_family = "wasm") {
		println!("cargo:rustc-cfg=wasm");
		println!("cargo::rustc-check-cfg=cfg(wasm)");
	}
	if cfg!(any(
		feature = "kv-mem",
		feature = "kv-fdb",
		feature = "kv-tikv",
		feature = "kv-rocksdb",
		feature = "kv-surrealkv",
	)) {
		println!("cargo:rustc-cfg=storage");
		println!("cargo::rustc-check-cfg=cfg(storage)");
	}

	let mut config = prost_build::Config::new();
	config.btree_map(["."]);
	config.type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]");
	config.compile_well_known_types();

	if let Err(err) = config.compile_protos(&[
			"proto/ast.proto",
			"proto/value.proto",
		],
                                &["proto/"]) {
		eprintln!("Failed to compile protobufs: {}", err);
		std::process::exit(1);
	}
}
