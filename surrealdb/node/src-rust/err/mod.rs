pub fn err_map(err: impl std::fmt::Display) -> napi::Error {
	napi::Error::from_reason(err.to_string())
}
