use crate::expr::part::Part;
use crate::val::Value;

impl Value {
	pub fn last(&self) -> Self {
		self.pick(&[Part::Last])
	}
}
