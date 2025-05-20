use crate::expr::part::Part;
use crate::expr::value::Value;

impl Value {
	pub fn last(&self) -> Self {
		self.pick(&[Part::Last])
	}
}
