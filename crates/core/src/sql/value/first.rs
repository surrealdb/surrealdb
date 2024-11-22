use crate::sql::part::Part;
use crate::sql::value::Value;

impl Value {
	pub fn first(&self) -> Self {
		self.pick(&[Part::First])
	}
}
