mod convert;

use std::collections::BTreeSet;

use ciborium::Value as CborValue;
use surrealdb_types::{Uuid, Value};

use crate::rpc::{
	Request,
	request::{ID, SESSION_ID},
};

pub fn encode(v: Value) -> anyhow::Result<Vec<u8>> {
	// Convert public value to internal value for encoding
	let encoding = convert::from_value(v).map_err(|e| anyhow::anyhow!(e))?;
	let mut res = Vec::new();
	//TODO: Check if this can ever panic.
	ciborium::into_writer(&encoding, &mut res).expect("writing to vec should not fail");
	Ok(res)
}

pub fn decode(bytes: &[u8]) -> anyhow::Result<Value> {
	let encoding = ciborium::from_reader(bytes).map_err(|e| anyhow::anyhow!(e.to_string()))?;
	convert::to_value(encoding, None).map_err(|e| anyhow::anyhow!(e))
}

/// Try to extract the request `id` and `session_id` from CBOR bytes without
/// fully validating the request structure. This is used to build a meaningful
/// error response when CBOR decoding or request parsing fails.
pub fn extract_context(bytes: &[u8]) -> (Option<Value>, Option<Uuid>) {
	let Ok(encoding) = ciborium::from_reader::<CborValue, _>(bytes) else {
		return (None, None);
	};
	let context_keys: BTreeSet<&str> = BTreeSet::from([ID, SESSION_ID]);
	let Ok(val) = convert::to_value(encoding, Some(context_keys)) else {
		return (None, None);
	};
	let Value::Object(obj) = val else {
		return (None, None);
	};
	let mut obj = obj;
	let Ok(id) = Request::extract_id(&mut obj) else {
		return (None, None);
	};
	let Ok(session_id) = Request::extract_session(&mut obj) else {
		return (None, None);
	};
	(id, session_id)
}
