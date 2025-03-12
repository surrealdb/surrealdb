pub fn supported(protocol: &str, _global: bool, _readonly: bool) -> bool {
	matches!(protocol, "memory" | "file")
}
