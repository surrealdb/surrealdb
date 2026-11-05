//! The record-identity root -- `@`.

use crate::val::Value;

/// The record identity `value` carries, or `NONE` when it carries none.
pub(crate) fn record_identity(value: &Value) -> Value {
	if let Value::RecordId(_) = value {
		value.clone()
	} else if let Value::Object(obj) = value {
		if let Some(id @ Value::RecordId(_)) = obj.get("id") {
			id.clone()
		} else {
			Value::None
		}
	} else {
		Value::None
	}
}
