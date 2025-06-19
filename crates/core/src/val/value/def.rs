use crate::expr::paths::ID;
use crate::val::RecordId;
use crate::val::Value;

impl Value {
	pub(crate) fn def(&mut self, val: &RecordId) {
		self.put(&*ID, val.clone().into())
	}
}
