use crate::expr::part::Part;
use crate::val::Value;

impl Value {
	pub fn first(&self) -> Self {
		self.pick(&[Part::First])
	}
}
