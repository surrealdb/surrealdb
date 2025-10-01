mod convert;

use crate::val::Value;

pub fn encode(v: Value) -> Result<Vec<u8>, String> {
	let encoding = convert::from_value(v)?;
	let mut res = Vec::new();
	//TODO: Check if this can ever panic.
	ciborium::into_writer(&encoding, &mut res).unwrap();
	Ok(res)
}

pub fn decode(bytes: &[u8]) -> Result<Value, String> {
	let encoding = ciborium::from_reader(bytes).map_err(|e| e.to_string())?;
	convert::to_value(encoding).map_err(|x| x.to_owned())
}
