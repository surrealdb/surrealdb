use crate::sql::paths::ID;
use crate::sql::thing::Thing;
use crate::sql::value::SqlValue;

impl SqlValue {
	pub(crate) fn def(&mut self, val: &Thing) {
		self.put(&*ID, val.clone().into())
	}
}
