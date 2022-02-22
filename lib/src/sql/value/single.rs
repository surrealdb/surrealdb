use crate::sql::value::Value;

impl Value {
	pub fn single(&self) -> &Self {
		match self {
			Value::Array(v) => match v.value.first() {
				None => &Value::None,
				Some(v) => v,
			},
			v => v,
		}
	}
}
