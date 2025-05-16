use crate::sql::part::Part;
use crate::sql::value::SqlValue;

impl SqlValue {
	pub fn first(&self) -> Self {
		self.pick(&[Part::First])
	}
}
