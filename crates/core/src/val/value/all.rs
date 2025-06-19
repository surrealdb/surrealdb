use crate::expr::part::Part;
use crate::val::Value;

impl Value {
	pub fn all(&self) -> Self {
		self.pick(&[Part::All])
	}
}
