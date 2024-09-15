use crate::sql::part::Part;
use crate::sql::value::Value;

impl Value {
	pub fn last(&self) -> Self {
		self.pick(&[Part::Last])
	}
}
