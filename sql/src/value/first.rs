use crate::part::Part;
use crate::value::Value;

impl Value {
	pub fn first(&self) -> Self {
		self.pick(&[Part::First])
	}
}
