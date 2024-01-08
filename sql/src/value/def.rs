use crate::paths::ID;
use crate::thing::Thing;
use crate::value::Value;

impl Value {
	pub(crate) fn def(&mut self, val: &Thing) {
		self.put(ID.as_ref(), val.clone().into())
	}
}
