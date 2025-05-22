use crate::expr::part::Part;
use crate::expr::value::Value;

impl Value {
	pub fn all(&self) -> Self {
		self.pick(&[Part::All])
	}
}
