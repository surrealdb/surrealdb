use super::SqlValue;
use crate::sql::{
	Array, Part,
	part::{RecurseInstruction, RecursionPlan},
};

#[derive(Clone, Copy, Debug)]
pub struct Recursion<'a> {
	pub min: u32,
	pub max: Option<u32>,
	pub iterated: u32,
	pub current: &'a SqlValue,
	pub path: &'a [Part],
	pub plan: Option<&'a RecursionPlan>,
	pub instruction: Option<&'a RecurseInstruction>,
}

impl<'a> Recursion<'a> {
	pub fn with_iterated(self, iterated: u32) -> Self {
		Self {
			iterated,
			..self
		}
	}

	pub fn with_current(self, current: &'a SqlValue) -> Self {
		Self {
			current,
			..self
		}
	}
}

pub(crate) fn get_final(v: &SqlValue) -> SqlValue {
	match v {
		SqlValue::Array(_) => SqlValue::Array(Array(vec![])),
		SqlValue::Null => SqlValue::Null,
		_ => SqlValue::None,
	}
}
