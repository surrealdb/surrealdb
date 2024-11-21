use crate::sql::paths::ID;
use crate::sql::thing::Thing;
use crate::sql::value::Value;

impl Value {
	pub(crate) fn def(&mut self, val: &Thing) {
		self.put(&*ID, val.clone().into())
	}
}
