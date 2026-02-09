use crate::expr::paths::ID;
use crate::val::{RecordId, Value};

impl Value {
	pub(crate) fn def(&mut self, val: &RecordId) {
		self.put(&*ID, val.clone().into())
	}
}
