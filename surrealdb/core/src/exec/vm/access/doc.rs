//! The record-identity root -- `@`.

use crate::val::Value;

/// The record identity `value` carries, or `NONE` when it carries none.
pub(crate) fn record_identity(value: &Value) -> Value {
	match value {
		Value::RecordId(_) => value.clone(),
		Value::Object(obj) => match obj.get("id") {
			Some(id @ Value::RecordId(_)) => id.clone(),
			_ => Value::None,
		},
		_ => Value::None,
	}
}
