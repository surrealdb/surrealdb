use crate::sql::value::Value;

impl Value {
	pub fn single(&self) -> &Self {
		match self {
			Value::Array(v) => v.first().unwrap_or(&Value::None),
			v => v,
		}
	}
}
