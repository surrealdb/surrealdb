fn main() {
	println!("cargo::rustc-check-cfg=cfg(storage)");
	if cfg!(any(feature = "kv-mem", feature = "kv-surrealkv",)) {
		println!("cargo:rustc-cfg=storage");
	}
}
