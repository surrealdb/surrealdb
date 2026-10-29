fn main() {
	// The cfgs below depend only on the target and the enabled features, both
	// of which cargo tracks, so nothing in the package tree needs to re-run it.
	println!("cargo::rerun-if-changed=build.rs");

	let target_os = std::env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();
	let wasm = std::env::var("CARGO_CFG_TARGET_FAMILY").is_ok_and(|family| family.contains("wasm"));
	// WASI hosts have no browser APIs. The engines that reach the network
	// through `wasm-bindgen` and `web-sys` are compiled for the browser's wasm
	// targets and not for WASI ones.
	let browser = wasm && target_os != "wasi";
	let feature = |name: &str| std::env::var(format!("CARGO_FEATURE_{name}")).is_ok();

	for alias in ["browser", "ws_engine", "http_engine", "storage"] {
		println!("cargo::rustc-check-cfg=cfg({alias})");
	}
	if browser {
		println!("cargo:rustc-cfg=browser");
	}
	// The remote engines are built for native targets and for the browser.
	if feature("PROTOCOL_WS") && (!wasm || browser) {
		println!("cargo:rustc-cfg=ws_engine");
	}
	if feature("PROTOCOL_HTTP") && (!wasm || browser) {
		println!("cargo:rustc-cfg=http_engine");
	}
	if ["KV_MEM", "KV_SURREALKV"].iter().any(|name| feature(name)) {
		println!("cargo:rustc-cfg=storage");
	}
}
