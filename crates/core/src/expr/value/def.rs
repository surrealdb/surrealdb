use crate::expr::paths::ID;
use crate::expr::thing::Thing;
use crate::expr::value::Value;

impl Value {
	pub(crate) fn def(&mut self, val: &Thing) {
		self.put(&*ID, val.clone().into())
	}
}
