use crate::expr::part::Part;
use crate::val::Value;

impl Value {
	/// Returns the equivalent of `self.pick(&[Part::All])`
	pub fn all(&self) -> Self {
		self.pick(&[Part::All])
	}
}
