use crate::sql::thing::Thing;
use crate::sql::value::Value;
use std::borrow::Cow;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Document<'a> {
	pub(super) id: Option<Thing>,
	pub(super) current: Cow<'a, Value>,
	pub(super) initial: Cow<'a, Value>,
}

impl<'a> Into<Vec<u8>> for &Document<'a> {
	fn into(self) -> Vec<u8> {
		msgpack::to_vec(&self.current).unwrap()
	}
}

impl<'a> Document<'a> {
	pub fn new(id: Option<Thing>, val: &'a Value) -> Self {
		Document {
			id,
			current: Cow::Borrowed(val),
			initial: Cow::Borrowed(val),
		}
	}
}
