use crate::sql::thing::Thing;
use crate::sql::value::Value;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Document {
	pub(super) id: Option<Thing>,
	pub(super) current: Value,
	pub(super) initial: Value,
}

impl Document {
	pub fn new(id: Option<Thing>, val: Value) -> Document {
		Document {
			id,
			current: val.clone(),
			initial: val,
		}
	}
}
