use crate::dbs::Executor;
use crate::dbs::Options;
use crate::dbs::Runtime;
use crate::sql::operation::Operations;
use crate::sql::value::Value;

impl Value {
	pub fn patch(self, _: &Runtime, _: &Options, _: &mut Executor, ops: Operations) -> Self {
		self
	}
}
