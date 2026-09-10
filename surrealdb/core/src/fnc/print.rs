use anyhow::Result;

use crate::val::Value;

// Logs the message as an output to server.
pub fn log((message,): (String,)) -> Result<Value> {
	info!("{message}");
	Ok(Value::None)
}
