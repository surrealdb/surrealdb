use crate::part::Part;
use crate::value::Value;

impl Value {
	pub fn all(&self) -> Self {
		self.pick(&[Part::All])
	}
}
