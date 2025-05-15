use crate::{
	cnf::IDIOM_RECURSION_LIMIT,
	ctx::Context,
	dbs::Options,
	doc::CursorDoc,
	err::Error,
	sql::{
		part::{RecurseInstruction, RecursionPlan},
		Array, FlowResultExt as _, Part,
	},
};

use super::Value;
use reblessive::tree::Stk;

#[derive(Clone, Copy, Debug)]
pub struct Recursion<'a> {
	pub min: u32,
	pub max: Option<u32>,
	pub iterated: u32,
	pub current: &'a Value,
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

	pub fn with_current(self, current: &'a Value) -> Self {
		Self {
			current,
			..self
		}
	}
}

// Method used to check if the value
// inside a recursed idiom path is final
pub(crate) fn is_final(v: &Value) -> bool {
	match v {
		Value::None => true,
		Value::Null => true,
		Value::Array(v) => v.is_empty() || v.is_all_none_or_null(),
		_ => false,
	}
}

pub(crate) fn get_final(v: &Value) -> Value {
	match v {
		Value::Array(_) => Value::Array(Array(vec![])),
		Value::Null => Value::Null,
		_ => Value::None,
	}
}

pub(crate) fn clean_iteration(v: Value) -> Value {
	if let Value::Array(v) = v {
		Value::from(v.0.into_iter().filter(|v| !is_final(v)).collect::<Vec<Value>>()).flatten()
	} else {
		v
	}
}
