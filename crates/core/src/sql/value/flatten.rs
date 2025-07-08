use crate::sql::array::Array;
use crate::sql::value::SqlValue;

impl SqlValue {
	pub fn flatten(self) -> Self {
		match self {
			SqlValue::Array(v) => {
				v.0.into_iter()
					.flat_map(|v| match v {
						SqlValue::Array(v) => v,
						_ => Array::from(v),
					})
					.collect::<Vec<_>>()
					.into()
			}
			v => v,
		}
	}
}
