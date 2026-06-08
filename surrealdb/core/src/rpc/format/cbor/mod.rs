mod convert;

use surrealdb_types::Value;

pub fn encode(v: Value) -> anyhow::Result<Vec<u8>> {
	// Convert public value to internal value for encoding
	let encoding = convert::from_value(v).map_err(|e| anyhow::anyhow!(e))?;
	let mut res = Vec::new();
	// `Vec<u8>` never returns an I/O error from `Write`, so the `Io` variant
	// of `ciborium::ser::Error` is unreachable here. The `Value` variant is
	// only constructed by ciborium's tag serializer, which our `CborValue`
	// representation never trips. Propagate as an error rather than panic
	// so any future ciborium change cannot crash the RPC layer.
	ciborium::into_writer(&encoding, &mut res)
		.map_err(|e| anyhow::anyhow!("failed to encode CBOR: {e}"))?;
	Ok(res)
}

pub fn decode(bytes: &[u8], recursion_limit: usize) -> anyhow::Result<Value> {
	let encoding = ciborium::de::from_reader_with_recursion_limit(bytes, recursion_limit)
		.map_err(|e| anyhow::anyhow!(e.to_string()))?;
	convert::to_value(encoding).map_err(|e| anyhow::anyhow!(e))
}
