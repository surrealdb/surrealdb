use crate::part::Part;
use crate::value::Value;

impl Value {
	pub fn last(&self) -> Self {
		self.pick(&[Part::Last])
	}
}
