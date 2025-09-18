mod convert;

use surrealdb_types::Value;

pub fn encode(v: Value) -> anyhow::Result<Vec<u8>> {
	todo!("STU")
	// let encoding = convert::from_value(v)?;
	// let mut res = Vec::new();
	// //TODO: Check if this can ever panic.
	// ciborium::into_writer(&encoding, &mut res).unwrap();
	// Ok(res)
}

pub fn decode(bytes: &[u8]) -> anyhow::Result<Value> {
	todo!("STU")
	// let encoding = ciborium::from_reader(bytes).map_err(|e| e.to_string())?;
	// convert::to_value(encoding).map_err(|x| x.to_owned())
}
