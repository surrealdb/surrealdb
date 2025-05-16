use crate::sql::part::Part;
use crate::sql::value::SqlValue;

impl SqlValue {
	pub fn last(&self) -> Self {
		self.pick(&[Part::Last])
	}
}
