
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
			"protocol/ast.proto",
			"protocol/rpc.proto",
			"protocol/value.proto",
		],
                                &["protocol/"]) {
		eprintln!("Failed to compile protobufs: {}", err);
		std::process::exit(1);
	}

	::capnpc::CompilerCommand::new()
		.file("protocol/expr.capnp")
        .file("protocol/rpc.capnp")
		.import_path("protocol/")
        .no_standard_import()
		.default_parent_module(vec!["protocol".into()])
        .run()
        .expect("compiling schema");
}
