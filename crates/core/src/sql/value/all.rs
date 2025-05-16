use crate::sql::part::Part;
use crate::sql::value::SqlValue;

impl SqlValue {
	pub fn all(&self) -> Self {
		self.pick(&[Part::All])
	}
}
