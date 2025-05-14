use crate::expr::part::Part;
use crate::expr::value::Value;

impl Value {
	pub fn first(&self) -> Self {
		self.pick(&[Part::First])
	}
}
