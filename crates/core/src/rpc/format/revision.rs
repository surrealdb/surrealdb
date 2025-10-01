use revision::Revisioned;

pub fn decode<D: Revisioned + revision::DeserializeRevisioned>(val: &[u8]) -> Result<D, String> {
	revision::from_slice(val).map_err(|e| e.to_string())
}

pub fn encode<S: Revisioned + revision::SerializeRevisioned>(val: &S) -> Result<Vec<u8>, String> {
	revision::to_vec(val).map_err(|e| e.to_string())
}
