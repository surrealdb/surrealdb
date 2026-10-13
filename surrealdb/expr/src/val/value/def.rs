use crate::expr::paths::ID;
use crate::val::{RecordId, Value};

impl Value {
	pub fn def(&mut self, val: RecordId) {
		self.put(&ID, val.into())
	}
}
